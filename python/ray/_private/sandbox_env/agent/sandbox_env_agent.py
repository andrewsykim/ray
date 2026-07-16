import asyncio
import json
import logging
import os
import tempfile

from ray._private.sandbox_env.context import SandboxEnvContext
from ray.core.generated import sandbox_env_agent_pb2


class SandboxEnvAgent:
    def __init__(
        self,
        sandbox_env_dir,
        logging_params,
        gcs_client,
        temp_dir,
        address,
        sandbox_env_agent_port,
    ):
        self._logger = logging.getLogger(__name__)
        self._sandbox_env_dir = sandbox_env_dir
        self._temp_dir = temp_dir

    async def _get_worker_path(self, backend: str, image_uri: str) -> str:
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chmod(tmpdir, 0o777)
            result_file = os.path.join(tmpdir, "worker_path.txt")
            get_worker_path_script = """
import ray._private.workers.default_worker as dw
with open('/shared/worker_path.txt', 'w') as f:
    f.write(dw.__file__)
"""
            cmd = [
                backend,
                "run",
                "--rm",
                "-v",
                f"{tmpdir}:/shared:Z",
                image_uri,
                "python",
                "-c",
                get_worker_path_script,
            ]

            self._logger.info("Pulling image %s", image_uri)

            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                raise RuntimeError(
                    f"{backend} command failed: cmd={cmd}, returncode={process.returncode}, "
                    f"stdout={stdout.decode()}, stderr={stderr.decode()}"
                )

            if not os.path.exists(result_file):
                raise FileNotFoundError(
                    f"Worker path file not created when getting worker path for image {image_uri}"
                )

            with open(result_file, "r") as f:
                worker_path = f.read().strip()

            if not worker_path.endswith(".py"):
                raise ValueError(
                    f"Invalid worker path inferred in image {image_uri}: {worker_path}"
                )

            self._logger.info(
                f"Inferred worker path in image {image_uri}: {worker_path}"
            )
            return worker_path

    async def GetOrCreateSandboxEnv(
        self, request: sandbox_env_agent_pb2.GetOrCreateSandboxEnvRequest
    ):
        self._logger.info(
            f"GetOrCreateSandboxEnv called with {request.serialized_sandbox_env}"
        )

        try:
            env_data = json.loads(request.serialized_sandbox_env)
        except Exception:
            env_data = {}

        backend = env_data.get("backend", "docker")
        image_uri = env_data.get("image_uri", "python:3.11")

        if backend in ["docker", "podman"]:
            worker_path = await self._get_worker_path(backend, image_uri)

            command_prefix = [
                backend,
                "run",
                "--rm",
                "-i",
                "-v",
                f"{self._temp_dir}:{self._temp_dir}",
                "--network=host",
                "--pid=host",
                "--ipc=host",
            ]

            # Podman specific flag in image_uri.py
            if backend == "podman":
                command_prefix.extend(["--cgroup-manager=cgroupfs", "--userns=keep-id"])

            for env_var_name, env_var_value in os.environ.items():
                if env_var_name.startswith("RAY_"):
                    command_prefix.extend(["--env", f"{env_var_name}={env_var_value}"])
            command_prefix.extend(["--env", "RAY_JOB_ID=$RAY_JOB_ID"])

            # Setup the python override entrypoint script
            override_script = f"""
import os
import sys
args = [sys.executable, '{worker_path}'] + sys.argv[3:]
os.execv(sys.executable, args)
"""
            command_prefix.extend(
                ["--entrypoint", "python", image_uri, "-c", override_script]
            )

            context = SandboxEnvContext(command_prefix=command_prefix)

            reply = sandbox_env_agent_pb2.GetOrCreateSandboxEnvReply(
                status=sandbox_env_agent_pb2.AGENT_RPC_STATUS_OK,
                serialized_sandbox_env_context=context.serialize(),
            )
            return reply
        else:
            raise ValueError(f"Unknown backend: {backend}")

    async def DeleteSandboxEnvIfPossible(
        self, request: sandbox_env_agent_pb2.DeleteSandboxEnvIfPossibleRequest
    ):
        reply = sandbox_env_agent_pb2.DeleteSandboxEnvIfPossibleReply(
            status=sandbox_env_agent_pb2.AGENT_RPC_STATUS_OK
        )
        return reply

    async def GetSandboxEnvsInfo(
        self, request: sandbox_env_agent_pb2.GetSandboxEnvsInfoRequest
    ):
        reply = sandbox_env_agent_pb2.GetSandboxEnvsInfoReply()
        return reply
