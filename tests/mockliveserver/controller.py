from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Mapping
from collections.abc import MutableMapping
from pathlib import Path

from databento.common.cram import BUCKET_ID_LENGTH
from databento.common.publishers import Dataset
from databento_dbn import Schema

from tests.mockliveserver.source import FileReplay
from tests.mockliveserver.source import ReplayProtocol


logger = logging.getLogger(__name__)


class Controller:
    command_parser = argparse.ArgumentParser(prog="mockliveserver")
    subparsers = command_parser.add_subparsers(dest="command")

    close_command = subparsers.add_parser("close", help="close the mock live server")

    active_count = subparsers.add_parser(
        "active_count",
        help="log the number of active connections",
    )

    add_key = subparsers.add_parser("add_key", help="add an API key to the mock live server")
    add_key.add_argument("key", type=str)

    del_key = subparsers.add_parser("del_key", help="delete an API key from the mock live server")
    del_key.add_argument("key", type=str)

    add_dbn = subparsers.add_parser("add_dbn", help="add a dbn file for replay")
    add_dbn.add_argument("dataset", type=str)
    add_dbn.add_argument("schema", type=str)
    add_dbn.add_argument("dbn_file", type=str)

    def __init__(
        self,
        server: asyncio.base_events.Server,
        api_key_table: Mapping[str, set[str]],
        file_replay_table: MutableMapping[tuple[Dataset, Schema], ReplayProtocol],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._server = server
        self._api_key_table = api_key_table
        self._file_replay_table = file_replay_table
        self._loop = loop

        self._read_task = loop.create_task(self._read_commands())

    async def _read_commands(self) -> None:
        while self._server.is_serving():
            line = await self._loop.run_in_executor(None, sys.stdin.readline)
            self.data_received(line.strip())

    def data_received(self, command_str: str) -> None:
        params = self.command_parser.parse_args(command_str.split())
        command_func = getattr(self, f"_command_{params.command}", None)
        if command_func is None:
            raise ValueError(f"{params.command} does not have a command handler")
        else:
            logger.info("received command: %s", command_str)
            command_params = dict(params._get_kwargs())
            command_params.pop("command")
            try:
                command_func(**command_params)
            except Exception:
                logger.exception("error processing command: %s", params.command)
                print(f"nack: {command_str}", flush=True)
            else:
                print(f"ack: {command_str}", flush=True)

    def _command_close(self, *_: str) -> None:
        """
        Close the server.
        """
        self._read_task.cancel()
        self._loop.call_soon(self._server.close)

    def _command_active_count(self, *_: str) -> None:
        """
        Log the number of active connections.
        """
        count = self._server._active_count  # type: ignore [attr-defined]
        logger.info("active connections: %d", count)

    def _command_add_key(self, key: str) -> None:
        """
        Add an API key to the server.
        """
        if len(key) < BUCKET_ID_LENGTH:
            logger.error("api key must be at least %d characters long", BUCKET_ID_LENGTH)
            return

        bucket_id = key[-BUCKET_ID_LENGTH:]
        self._api_key_table[bucket_id].add(key)
        logger.info("added api key '%s'", key)

    def _command_del_key(self, key: str) -> None:
        """
        Remove API key from the server.
        """
        if len(key) < BUCKET_ID_LENGTH:
            logger.error("api key must be at least %d characters long", BUCKET_ID_LENGTH)
            return

        bucket_id = key[-BUCKET_ID_LENGTH:]
        self._api_key_table[bucket_id].remove(key)
        logger.info("deleted api key '%s'", key)

    def _command_add_dbn(self, dataset: str, schema: str, dbn_file: str) -> None:
        """
        Add a DBN file for streaming.
        """
        try:
            dataset_valid = Dataset(dataset)
            schema_valid = Schema(schema)
        except ValueError as exc:
            logger.error("invalid parameter value: %s", exc)
            return

        dbn_path = Path(dbn_file)
        if not dbn_path.exists() or not dbn_path.is_file():
            logger.error("invalid file path: %s", dbn_path)
            return

        self._file_replay_table[(dataset_valid, schema_valid)] = FileReplay(dbn_path)
