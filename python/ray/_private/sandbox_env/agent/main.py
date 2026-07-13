import argparse
import logging
import os
import socket
import sys

import ray
import ray._private.ray_constants as ray_constants
from ray._common.utils import (
    get_or_create_event_loop,
)
from ray._private import logging_utils
from ray._private.authentication.http_token_authentication import (
    get_token_auth_middleware,
)
from ray._private.process_watcher import create_check_raylet_task
from ray._raylet import SANDBOX_ENV_AGENT_PORT_NAME, GcsClient, persist_port
from ray.core.generated import (
    sandbox_env_agent_pb2,
)


def import_libs():
    my_dir = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, os.path.join(my_dir, "thirdparty_files"))  # for aiohttp
    sys.path.insert(0, my_dir)  # for sandbox_env_agent and sandbox_env_consts


import_libs()

import aiohttp  # noqa: E402
import sandbox_env_consts  # noqa: E402
from aiohttp import web  # noqa: E402
from sandbox_env_agent import SandboxEnvAgent  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runtime env agent.")
    parser.add_argument(
        "--node-id",
        required=True,
        type=str,
        help="the unique ID of this node.",
    )
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.",
    )
    parser.add_argument(
        "--sandbox-env-agent-port",
        required=True,
        type=int,
        default=None,
        help="The port on which the sandbox env agent will receive HTTP requests.",
    )
    parser.add_argument(
        "--session-dir",
        required=True,
        type=str,
        default=None,
        help="The path of this ray session directory.",
    )

    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--cluster-id-hex", required=True, type=str, help="The cluster id in hex."
    )
    parser.add_argument(
        "--sandbox-env-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the resource directory used by sandbox_env.",
    )

    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=sandbox_env_consts.SANDBOX_ENV_AGENT_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}".'.format(
            sandbox_env_consts.SANDBOX_ENV_AGENT_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=True,
        type=int,
        help="Specify the max bytes for rotating log file",
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=True,
        type=int,
        help="Specify the backup count of rotated log file",
    )
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.",
    )
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.",
    )
    parser.add_argument(
        "--stdout-filepath",
        required=False,
        type=str,
        default="",
        help="The filepath to dump runtime env agent stdout.",
    )
    parser.add_argument(
        "--stderr-filepath",
        required=False,
        type=str,
        default="",
        help="The filepath to dump runtime env agent stderr.",
    )

    args = parser.parse_args()

    # Disable log rotation for windows platform.
    logging_rotation_bytes = args.logging_rotate_bytes if sys.platform != "win32" else 0
    logging_rotation_backup_count = (
        args.logging_rotate_backup_count if sys.platform != "win32" else 1
    )

    logging_params = dict(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.log_dir,
        filename=args.logging_filename,
        max_bytes=logging_rotation_bytes,
        backup_count=logging_rotation_backup_count,
    )

    # Setup stdout/stderr redirect files if redirection enabled.
    logging_utils.redirect_stdout_stderr_if_needed(
        args.stdout_filepath,
        args.stderr_filepath,
        logging_rotation_bytes,
        logging_rotation_backup_count,
    )

    gcs_client = GcsClient(address=args.gcs_address, cluster_id=args.cluster_id_hex)
    agent = SandboxEnvAgent(
        sandbox_env_dir=args.sandbox_env_dir,
        logging_params=logging_params,
        gcs_client=gcs_client,
        temp_dir=args.temp_dir,
        address=args.node_ip_address,
        sandbox_env_agent_port=args.sandbox_env_agent_port,
    )

    ray._raylet.setproctitle("ray::SandboxEnvAgent")

    # POST /get_or_create_sandbox_env
    # body is serialzied protobuf GetOrCreateSandboxEnvRequest
    # reply is serialzied protobuf GetOrCreateSandboxEnvReply
    async def get_or_create_sandbox_env(request: web.Request) -> web.Response:
        data = await request.read()
        request = sandbox_env_agent_pb2.GetOrCreateSandboxEnvRequest()
        request.ParseFromString(data)
        reply = await agent.GetOrCreateSandboxEnv(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    # POST /delete_sandbox_env_if_possible
    # body is serialzied protobuf DeleteSandboxEnvIfPossibleRequest
    # reply is serialzied protobuf DeleteSandboxEnvIfPossibleReply
    async def delete_sandbox_env_if_possible(request: web.Request) -> web.Response:
        data = await request.read()
        request = sandbox_env_agent_pb2.DeleteSandboxEnvIfPossibleRequest()
        request.ParseFromString(data)
        reply = await agent.DeleteSandboxEnvIfPossible(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    # POST /get_sandbox_envs_info
    # body is serialzied protobuf GetSandboxEnvsInfoRequest
    # reply is serialzied protobuf GetSandboxEnvsInfoReply
    async def get_sandbox_envs_info(request: web.Request) -> web.Response:
        data = await request.read()
        request = sandbox_env_agent_pb2.GetSandboxEnvsInfoRequest()
        request.ParseFromString(data)
        reply = await agent.GetSandboxEnvsInfo(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    app = web.Application(middlewares=[get_token_auth_middleware(aiohttp)])

    app.router.add_post("/get_or_create_sandbox_env", get_or_create_sandbox_env)
    app.router.add_post(
        "/delete_sandbox_env_if_possible", delete_sandbox_env_if_possible
    )
    app.router.add_post("/get_sandbox_envs_info", get_sandbox_envs_info)

    loop = get_or_create_event_loop()
    check_raylet_task = None
    if sys.platform not in ["win32", "cygwin"]:

        def parent_dead_callback(msg):
            agent._logger.info(
                "Raylet is dead! Exiting Sandbox Env Agent. "
                f"addr: {args.node_ip_address}, "
                f"port: {args.sandbox_env_agent_port}\n"
                f"{msg}"
            )

        # No need to await this task.
        check_raylet_task = create_check_raylet_task(
            args.log_dir, gcs_client, parent_dead_callback, loop
        )

    port = args.sandbox_env_agent_port or 0
    infos = socket.getaddrinfo(args.node_ip_address, port, type=socket.SOCK_STREAM)
    family, socktype, proto, _, sockaddr = infos[0]
    sock = socket.socket(family, socktype, proto)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(sockaddr)

    bound_port = sock.getsockname()[1]
    persist_port(
        args.session_dir,
        args.node_id,
        SANDBOX_ENV_AGENT_PORT_NAME,
        bound_port,
    )

    try:
        web.run_app(app, sock=sock, loop=loop)
    except SystemExit as e:
        agent._logger.info(f"SystemExit! {e}")
        # We have to poke the task exception, or there's an error message
        # "task exception was never retrieved".
        if check_raylet_task is not None:
            check_raylet_task.exception()
        sys.exit(e.code)
