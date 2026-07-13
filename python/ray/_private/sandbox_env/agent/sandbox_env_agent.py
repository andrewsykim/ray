import json
import logging

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

        if backend == "docker":
            command_prefix = [
                "docker",
                "run",
                "--rm",
                "-i",
                "--network=host",  # Allow connectivity for this prototype
                image_uri,
            ]

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
