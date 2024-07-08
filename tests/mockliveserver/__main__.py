from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections import defaultdict
from socket import AF_INET

from databento.common.publishers import Dataset
from databento_dbn import Schema

from tests.mockliveserver.controller import Controller
from tests.mockliveserver.source import ReplayProtocol

from .server import MockLiveServerProtocol
from .server import SessionMode


logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument(
    "host",
    help="the hostname to bind; defaults to `localhost`",
    nargs="?",
    default="localhost",
)
parser.add_argument(
    "-p",
    "--port",
    help="the port to bind; defaults to an open port",
    type=int,
    default=0,
)
parser.add_argument(
    "-e",
    "--echo",
    help="a file to write echos of gateway control messages to",
    default=os.devnull,
)
parser.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    help="enabled debug logging",
)


async def main() -> None:
    params = parser.parse_args(sys.argv[1:])

    # Setup console logging handler
    log_level = logging.DEBUG if params.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    logger.info("mockliveserver starting")
    loop = asyncio.get_running_loop()

    api_key_table: dict[str, set[str]] = defaultdict(set)
    file_replay_table: dict[tuple[Dataset, Schema], ReplayProtocol] = {}
    sessions: list[MockLiveServerProtocol] = []
    echo_stream = open(params.echo, "wb", buffering=0)

    def protocol_factory() -> MockLiveServerProtocol:
        protocol = MockLiveServerProtocol(
            loop=loop,
            mode=SessionMode.FILE_REPLAY,
            api_key_table=api_key_table,
            file_replay_table=file_replay_table,
            echo_stream=echo_stream,
        )
        sessions.append(protocol)
        return protocol

    # Create server for incoming connections
    server = await loop.create_server(
        protocol_factory=protocol_factory,
        family=AF_INET,  # force ipv4
        host=params.host,
        port=params.port,
        start_serving=True,
    )
    ip, port, *_ = server._sockets[-1].getsockname()  # type: ignore [attr-defined]

    # Create command interface for stdin
    _ = Controller(
        server=server,
        api_key_table=api_key_table,
        file_replay_table=file_replay_table,
        sessions=sessions,
        loop=loop,
    )

    # Log Arguments
    logger.info("host: %s (%s)", params.host, ip)
    logger.info("port: %d", port)
    logger.info("echo: %s", params.echo)
    logger.info("verbose: %s", params.verbose)
    logger.info("mockliveserver now serving")

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        logger.info("terminating mock live server")

    echo_stream.close()


asyncio.run(main())
