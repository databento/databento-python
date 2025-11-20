#!/usr/bin/python3

import argparse
import os
import typing

from databento_dbn import ErrorMsg
from databento_dbn import MBOMsg
from databento_dbn import RType
from databento_dbn import SymbolMappingMsg

from databento import Dataset
from databento import Live
from databento import RecordFlags
from databento import Schema
from databento import SType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="Python client")
    parser.add_argument("--gateway", type=str, help="Gateway to connect")
    parser.add_argument("--port", type=int, default=13000, help="Gatewat port to connect")
    parser.add_argument(
        "--api-key-env-var",
        type=str,
        help="Gateway to connect as Gateway::port",
        default="DATABENTO_API_KEY",
    )
    parser.add_argument("--dataset", type=Dataset, help="Dataset")
    parser.add_argument("--schema", type=Schema, help="Schema")
    parser.add_argument("--stype", type=SType, help="SType")
    parser.add_argument("--symbols", type=str, help="Symbols")
    parser.add_argument("--start", type=str, default=None, help="Start time (rfc-339)")
    parser.add_argument(
        "--use-snapshot",
        action="store_true",
        help="Whether or not to request snapshot subscription",
    )

    return parser.parse_args()


def run_client(args: argparse.Namespace) -> None:
    client = Live(key=get_api_key(args.api_key_env_var), gateway=args.gateway, port=args.port)

    client.subscribe(
        dataset=args.dataset,
        schema=args.schema,
        stype_in=args.stype,
        symbols=args.symbols,
        start=args.start,
    )

    print("Starting client...")

    for record in client:
        if is_expected_record(args, record):
            print(f"Received expected record {record}")
            break
        elif isinstance(record, ErrorMsg):
            raise ValueError(f"Received error {record.err}")
        else:
            print(f"{record}")

    print("Finished client")


def run_client_with_snapshot(args: argparse.Namespace) -> None:
    client = Live(key=get_api_key(args.api_key_env_var), gateway=args.gateway, port=args.port)

    client.subscribe(
        dataset=args.dataset,
        schema=args.schema,
        stype_in=args.stype,
        symbols=args.symbols,
        snapshot=True,
    )

    received_snapshot_record = False

    print("Starting client...")

    for record in client:
        if isinstance(record, MBOMsg):
            if record.flags & RecordFlags.F_SNAPSHOT:
                received_snapshot_record = True
            else:
                print(f"Received expected record {record}")
                break
        elif isinstance(record, ErrorMsg):
            raise ValueError(f"Received error {record.err}")

    print("Finished client")

    assert received_snapshot_record


def is_expected_record(args: argparse.Namespace, record: typing.Any) -> bool:
    try:
        start = int(args.start)
    except Exception:
        start = None

    # For start != 0 we stop at SymbolMappingMsg so that the tests can be run outside trading hours
    should_expect_symbol_mapping = args.stype != SType.INSTRUMENT_ID and (
        start is None or start != 0
    )
    if should_expect_symbol_mapping:
        return isinstance(record, SymbolMappingMsg)
    else:
        return record.rtype == RType.from_schema(args.schema)


def get_api_key(api_key_name: str) -> str:
    api_key = os.getenv(api_key_name)
    if not api_key:
        raise ValueError(f"Invalid api_key {api_key_name}")

    return api_key


def main() -> None:
    args = parse_args()

    if args.use_snapshot:
        run_client_with_snapshot(args)
    else:
        run_client(args)


if __name__ == "__main__":
    main()
