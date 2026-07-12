import logging

from ray.core.generated import sandbox_env_agent_pb2, sandbox_env_agent_pb2_grpc

logger = logging.getLogger(__name__)


class SandboxEnvAgent(sandbox_env_agent_pb2_grpc.SandboxEnvAgentServiceServicer):
    def __init__(self, temp_dir, logging_params):
        self.temp_dir = temp_dir

    async def GetOrCreateSandboxEnv(self, request, context=None):
        logger.info(f"GetOrCreateSandboxEnv: {request.sandbox_runtime}")

        reply = sandbox_env_agent_pb2.GetOrCreateSandboxEnvReply()
        reply.status = sandbox_env_agent_pb2.SANDBOX_AGENT_RPC_STATUS_OK

        import json

        sandbox_runtime_type = None
        if request.sandbox_runtime:
            try:
                env_dict = json.loads(request.sandbox_runtime)
                sandbox_env = env_dict.get("sandbox_env")
                if isinstance(sandbox_env, dict):
                    sandbox_runtime_type = sandbox_env.get("container")
            except Exception:
                pass

        if sandbox_runtime_type == "docker":
            # Add basic docker wrapper command
            # This is a very minimal implementation based on the prototype instructions
            reply.wrapper_command.extend(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-i",
                    "-v",
                    f"{self.temp_dir}:{self.temp_dir}",
                    "rayproject/ray:latest",
                ]
            )

        return reply

    async def DeleteSandboxEnvIfPossible(self, request, context=None):
        reply = sandbox_env_agent_pb2.DeleteSandboxEnvIfPossibleReply()
        reply.status = sandbox_env_agent_pb2.SANDBOX_AGENT_RPC_STATUS_OK
        return reply
