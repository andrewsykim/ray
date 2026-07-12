import argparse

import grpc

from ray._common.utils import get_or_create_event_loop
from ray._private.sandbox_env_agent.sandbox_env_agent import SandboxEnvAgent
from ray.core.generated import sandbox_env_agent_pb2_grpc

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sandbox env agent.")
    parser.add_argument("--node-ip-address", required=True, type=str)
    parser.add_argument("--sandbox-env-agent-port", required=True, type=int)
    parser.add_argument("--gcs-address", required=True, type=str)
    parser.add_argument("--temp-dir", required=True, type=str)

    args = parser.parse_args()

    # Disable log rotation for windows platform.
    logging_params = dict()

    agent = SandboxEnvAgent(
        temp_dir=args.temp_dir,
        logging_params=logging_params,
    )

    server = grpc.aio.server()
    sandbox_env_agent_pb2_grpc.add_SandboxEnvAgentServiceServicer_to_server(
        agent, server
    )
    port = server.add_insecure_port(f"0.0.0.0:{args.sandbox_env_agent_port}")

    # We must actually start the loop.
    async def serve():
        await server.start()
        await server.wait_for_termination()

    loop = get_or_create_event_loop()
    loop.run_until_complete(serve())
