import json
from datetime import date
from json.decoder import JSONDecodeError
from typing import BinaryIO, List, Optional, Tuple, Union

import aiohttp
import numpy as np
import pandas as pd
import requests
import zstandard
from aiohttp import ClientResponse
from databento.common.bento import Bento, FileBento, MemoryBento
from databento.common.buffer import BinaryBuffer
from databento.common.data import CSV_HEADERS, DBZ_RECORD_MAP
from databento.common.enums import Compression, Dataset, Encoding, Schema, SType
from databento.common.logging import log_debug, log_info
from databento.common.metadata import MetadataDecoder
from databento.common.parsing import (
    enum_or_str_lowercase,
    maybe_datetime_to_string,
    maybe_symbols_list_to_string,
)
from databento.historical.error import BentoClientError, BentoServerError
from requests import Response
from requests.auth import HTTPBasicAuth


_NO_DATA_FOUND = b"No data found for query."
_32KB = 1024 * 32  # 32_768


class BentoHttpAPI:
    """The base class for all Databento HTTP API endpoints."""

    TIMEOUT = 100

    def __init__(self, key: str, gateway: str):
        self._key = key
        self._gateway = gateway
        self._headers = {"accept": "application/json"}

    @staticmethod
    def _timeseries_params(
        *,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Schema,
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        limit: Optional[int] = None,
        encoding: Encoding,
        compression: Compression,
        stype_in: SType,
        stype_out: SType = SType.PRODUCT_ID,
    ) -> List[Tuple[str, str]]:
        # Parse inputs
        dataset = enum_or_str_lowercase(dataset, "dataset")
        symbols = maybe_symbols_list_to_string(symbols)
        start = maybe_datetime_to_string(start)
        end = maybe_datetime_to_string(end)

        # Build params list
        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("symbols", symbols),
            ("schema", schema.value),
            ("start", start),
            ("end", end),
            ("encoding", encoding.value),
            ("compression", compression.value),
            ("stype_in", stype_in.value),
            ("stype_out", stype_out.value),
        ]

        if limit is not None:
            params.append(("limit", str(limit)))

        return params

    @staticmethod
    def _create_bento(
        path: str,
        schema: Schema,
        encoding: Encoding,
        compression: Compression,
    ) -> Union[MemoryBento, FileBento]:
        if path is None:
            return MemoryBento(
                schema=schema,
                encoding=encoding,
                compression=compression,
            )
        else:
            return FileBento(
                path=path,
                schema=schema,
                encoding=encoding,
                compression=compression,
            )

    def _check_access_key(self):
        if self._key == "YOUR_ACCESS_KEY":
            raise ValueError(
                "The access key is currently set to 'YOUR_ACCESS_KEY'. "
                "Please replace this value with either a test or production "
                "access key. You will find these through your Databento dashboard.",
            )

    def _get(
        self,
        url: str,
        params: Optional[List[Tuple[str, str]]] = None,
        basic_auth: bool = False,
    ) -> Response:
        self._check_access_key()

        with requests.get(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password=None)
            if basic_auth
            else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
        ) as response:
            check_http_error(response)
            return response

    async def _get_async(
        self,
        url: str,
        params: Optional[List[Tuple[str, str]]] = None,
        basic_auth: bool = False,
    ) -> ClientResponse:
        self._check_access_key()
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                params=params,
                headers=self._headers,
                auth=aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                if basic_auth
                else None,
                timeout=self.TIMEOUT,
            ) as response:
                await check_http_error_async(response)
                return response

    def _post(
        self,
        url: str,
        params: Optional[List[Tuple[str, str]]] = None,
        basic_auth: bool = False,
    ) -> Response:
        self._check_access_key()

        with requests.post(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password=None)
            if basic_auth
            else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
        ) as response:
            check_http_error(response)
            return response

    def _stream(
        self,
        url: str,
        params: List[Tuple[str, str]],
        basic_auth: bool,
        schema: Schema,
        encoding_in: Encoding,
        encoding_out: Encoding,
        compression_in: Compression,
        compression_out: Compression,
        bento: Bento,
    ) -> None:
        self._check_access_key()

        with requests.get(
            url=url,
            params=params,
            headers=self._headers,
            auth=HTTPBasicAuth(username=self._key, password=None)
            if basic_auth
            else None,
            timeout=(self.TIMEOUT, self.TIMEOUT),
            stream=True,
        ) as response:
            check_http_error(response)

            # Setup streams
            binary_stream = BinaryBuffer()
            binary_writer_in = zstandard.ZstdDecompressor().stream_writer(binary_stream)

            inner_writer = bento.writer()
            text_writer_out = inner_writer
            if compression_in == Compression.ZSTD:
                if compression_out == Compression.NONE:
                    # Wrap inner writer with zstd decompressor
                    inner_writer = zstandard.ZstdDecompressor().stream_writer(
                        inner_writer
                    )
                else:
                    # Wrap text writer out with zstd compressor
                    text_writer_out = zstandard.ZstdCompressor().stream_writer(
                        inner_writer
                    )

            # Binary struct format
            binary_map = DBZ_RECORD_MAP[schema]
            binary_size = np.dtype(binary_map).itemsize

            # Header flags
            metadata_header_received = False
            csv_header_written = False

            record_count = 0
            for chunk in response.iter_content(chunk_size=_32KB):
                if chunk == _NO_DATA_FOUND:
                    log_info("No data found for query.")
                    return

                if encoding_in == Encoding.DBZ:
                    # Here we check for a known magic number. Improve this to
                    # check for all valid skippable frame magic numbers.
                    if not metadata_header_received and chunk.startswith(b"Q*M\x18"):
                        self._decode_metadata(chunk, bento)
                        metadata_header_received = True
                        continue

                    if encoding_out != Encoding.DBZ:
                        if encoding_out == Encoding.CSV and not csv_header_written:
                            csv_header: bytes = CSV_HEADERS[schema] + b"\n"
                            text_writer_out.write(csv_header)
                            text_writer_out.flush()
                            csv_header_written = True

                        binary_writer_in.write(chunk)

                        # Check binary length aligns with record size
                        if len(binary_stream) % binary_size > 0:
                            continue

                        record_count += len(binary_stream) // binary_size

                        text_buffer: bytes = self._decode_binary_to_text_buffer(
                            binary_map=binary_map,
                            binary_buffer=binary_stream.raw,
                            schema=schema,
                            encoding=encoding_out,
                        )

                        text_writer_out.write(text_buffer)
                        binary_stream.clear()
                        continue

                inner_writer.write(chunk)

            text_writer_out.flush()

    async def _stream_async(
        self,
        url: str,
        params: List[Tuple[str, Optional[str]]],
        basic_auth: bool,
        schema: Schema,
        encoding_in: Encoding,
        encoding_out: Encoding,
        compression_in: Compression,
        compression_out: Compression,
        bento: Bento,
    ) -> None:
        self._check_access_key()

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url=url,
                params=[x for x in params if x[1] is not None],
                headers=self._headers,
                auth=aiohttp.BasicAuth(login=self._key, password="", encoding="utf-8")
                if basic_auth
                else None,
                timeout=self.TIMEOUT,
            ) as response:
                await check_http_error_async(response)

                # Setup streams
                binary_stream = BinaryBuffer()
                binary_writer_in = zstandard.ZstdDecompressor().stream_writer(
                    binary_stream
                )

                inner_writer = bento.writer()
                text_writer_out = inner_writer
                if compression_in == Compression.ZSTD:
                    if compression_out == Compression.NONE:
                        # Wrap inner writer with zstd decompressor
                        inner_writer = zstandard.ZstdDecompressor().stream_writer(
                            inner_writer
                        )
                    else:
                        # Wrap text writer out with zstd compressor
                        text_writer_out = zstandard.ZstdCompressor().stream_writer(
                            inner_writer
                        )

                # Binary struct format
                binary_map = DBZ_RECORD_MAP[schema]
                binary_size = np.dtype(binary_map).itemsize

                # Header flags
                metadata_header_received = False
                csv_header_written = False

                record_count = 0
                async for async_chunk in response.content.iter_chunks():
                    chunk: bytes = async_chunk[0]
                    if chunk == _NO_DATA_FOUND:
                        log_info("No data found for query.")
                        return

                    if encoding_in == Encoding.DBZ:
                        # Here we check for a known magic number. Improve this to
                        # check for all valid skippable frame magic numbers.
                        if not metadata_header_received and chunk.startswith(
                            b"Q*M\x18"
                        ):
                            self._decode_metadata(chunk, bento)
                            metadata_header_received = True
                            continue

                        if encoding_out != Encoding.DBZ:
                            if encoding_out == Encoding.CSV and not csv_header_written:
                                csv_header: bytes = CSV_HEADERS[schema] + b"\n"
                                text_writer_out.write(csv_header)
                                text_writer_out.flush()
                                csv_header_written = True

                            binary_writer_in.write(chunk)

                            # Check binary length aligns with record size
                            if len(binary_stream) % binary_size > 0:
                                continue

                            record_count += len(binary_stream) // binary_size

                            text_buffer: bytes = self._decode_binary_to_text_buffer(
                                binary_map=binary_map,
                                binary_buffer=binary_stream.raw,
                                schema=schema,
                                encoding=encoding_out,
                            )

                            text_writer_out.write(text_buffer)
                            binary_stream.clear()
                            continue

                    inner_writer.write(chunk)

            text_writer_out.flush()

    def _decode_metadata(self, chunk: bytes, bento: Bento):
        log_debug("Decoding metadata...")
        magic = int.from_bytes(chunk[:4], byteorder="little")
        frame_size = int.from_bytes(chunk[4:8], byteorder="little")
        if len(chunk) != 8 + frame_size:
            raise RuntimeError("Invalid metadata chunk received")
        log_debug(f"magic={magic}, frame_size={frame_size}")

        metadata = MetadataDecoder.decode_to_json(chunk[8 : frame_size + 8])
        log_debug(f"metadata={metadata}")  # TODO(cs): Temporary logging
        bento.set_metadata_json(metadata)

    def _decode_binary_to_text_buffer(
        self,
        binary_map,
        binary_buffer: bytes,
        schema: Schema,
        encoding: Encoding,
    ) -> bytes:
        # Unpack binary into discrete records
        binary_records: np.ndarray = np.frombuffer(binary_buffer, dtype=binary_map)

        text_records: List[bytes]
        if encoding == Encoding.CSV:
            text_records = self._binary_to_csv_records(schema, binary_records)
        elif encoding == Encoding.JSON:
            text_records = self._binary_to_json_records(schema, binary_records)
        else:
            raise NotImplementedError(f"Cannot decode DBZ to {encoding.value}")

        return b"\n".join(text_records) + b"\n"

    def _write_text_records(
        self,
        text_buffer: bytes,
        text_writer_out: BinaryIO,
    ) -> None:
        text_writer_out.write(text_buffer)

    def _binary_to_csv_records(self, schema: Schema, values: np.ndarray) -> List[bytes]:
        if schema == Schema.MBO:
            return list(map(self._binary_mbo_to_csv_record, values))
        elif schema == Schema.MBP_1:
            return list(map(self._binary_mbp_1_to_csv_record, values))
        elif schema == Schema.MBP_10:
            return list(map(self._binary_mbp_10_to_csv_record, values))
        elif schema == Schema.TBBO:
            return list(map(self._binary_tbbo_to_csv_record, values))
        elif schema == Schema.TRADES:
            return list(map(self._binary_trades_to_csv_record, values))
        elif schema in (
            Schema.OHLCV_1S,
            Schema.OHLCV_1M,
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
        ):
            return list(map(self._binary_ohlcv_to_csv_record, values))
        else:
            raise NotImplementedError(
                f"{schema.value} schema DBZ to CSV decoding not implemented yet",
            )

    def _binary_to_json_records(
        self, schema: Schema, values: np.ndarray
    ) -> List[bytes]:
        if schema == Schema.MBO:
            return list(map(self._binary_mbo_to_json_record, values))
        elif schema == Schema.MBP_1:
            return list(map(self._binary_mbp_1_to_json_record, values))
        elif schema == Schema.MBP_10:
            return list(map(self._binary_mbp_10_to_json_record, values))
        elif schema == Schema.TBBO:
            return list(map(self._binary_tbbo_to_json_record, values))
        elif schema == Schema.TRADES:
            return list(map(self._binary_trades_to_json_record, values))
        elif schema in (
            Schema.OHLCV_1S,
            Schema.OHLCV_1M,
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
        ):
            return list(map(self._binary_ohlcv_to_json_record, values))
        else:
            raise NotImplementedError(
                f"{schema.value} schema DBZ to JSON decoding not implemented yet",
            )

    def _binary_mbo_to_csv_record(self, values: Tuple) -> bytes:
        return (
            f"{values[12]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[13]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[5]},"  # order_id
            f"{values[11].decode()},"  # action
            f"{values[10].decode()},"  # side
            f"{values[9]},"  # flags
            f"{values[6]},"  # price
            f"{values[7]},"  # size
            f"{values[14]}"  # sequence
        ).encode()

    def _binary_mbp_1_to_csv_record(self, values: Tuple) -> bytes:
        return (
            f"{values[11]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[12]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[7].decode()},"  # action
            f"{values[8].decode()},"  # side
            f"{values[9]},"  # flags
            f"{values[5]},"  # price
            f"{values[6]},"  # size
            f"{values[13]},"  # sequence
            f"{values[14]},"  # bid_px_00
            f"{values[15]},"  # ask_px_00
            f"{values[16]},"  # bid_sz_00
            f"{values[17]},"  # ask_sz_00
            f"{values[18]},"  # bid_oq_00
            f"{values[19]}"  # ask_oq_00
        ).encode()

    def _binary_mbp_10_to_csv_record(self, values: Tuple) -> bytes:
        # TODO(cs): Refactor this to reduce duplication
        return (
            f"{values[11]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[12]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[7].decode()},"  # action
            f"{values[8].decode()},"  # side
            f"{values[9]},"  # flags
            f"{values[5]},"  # price
            f"{values[6]},"  # size
            f"{values[13]},"  # sequence
            f"{values[14]},"  # bid_px_00
            f"{values[15]},"  # ask_px_00
            f"{values[16]},"  # bid_sz_00
            f"{values[17]},"  # ask_sz_00
            f"{values[18]},"  # bid_oq_00
            f"{values[19]},"  # ask_oq_00
            f"{values[20]},"  # bid_px_01
            f"{values[21]},"  # ask_px_01
            f"{values[22]},"  # bid_sz_01
            f"{values[23]},"  # ask_sz_01
            f"{values[24]},"  # bid_oq_01
            f"{values[25]},"  # ask_oq_01
            f"{values[26]},"  # bid_px_02
            f"{values[27]},"  # ask_px_02
            f"{values[28]},"  # bid_sz_02
            f"{values[29]},"  # ask_sz_02
            f"{values[30]},"  # bid_oq_02
            f"{values[31]},"  # ask_oq_02
            f"{values[32]},"  # bid_px_03
            f"{values[33]},"  # ask_px_03
            f"{values[34]},"  # bid_sz_03
            f"{values[35]},"  # ask_sz_03
            f"{values[36]},"  # bid_oq_03
            f"{values[37]},"  # ask_oq_03
            f"{values[38]},"  # bid_px_04
            f"{values[39]},"  # ask_px_04
            f"{values[40]},"  # bid_sz_04
            f"{values[41]},"  # ask_sz_04
            f"{values[42]},"  # bid_oq_04
            f"{values[43]},"  # ask_oq_04
            f"{values[44]},"  # bid_px_05
            f"{values[45]},"  # ask_px_05
            f"{values[46]},"  # bid_sz_05
            f"{values[47]},"  # ask_sz_05
            f"{values[48]},"  # bid_oq_05
            f"{values[49]},"  # ask_oq_05
            f"{values[50]},"  # bid_px_06
            f"{values[51]},"  # ask_px_06
            f"{values[52]},"  # bid_sz_06
            f"{values[53]},"  # ask_sz_06
            f"{values[54]},"  # bid_oq_06
            f"{values[55]},"  # ask_oq_06
            f"{values[56]},"  # bid_px_07
            f"{values[57]},"  # ask_px_07
            f"{values[58]},"  # bid_sz_07
            f"{values[59]},"  # ask_sz_07
            f"{values[60]},"  # bid_oq_07
            f"{values[61]},"  # ask_oq_07
            f"{values[62]},"  # bid_px_08
            f"{values[63]},"  # ask_px_08
            f"{values[64]},"  # bid_sz_08
            f"{values[65]},"  # ask_sz_08
            f"{values[66]},"  # bid_oq_08
            f"{values[67]},"  # ask_oq_08
            f"{values[68]},"  # bid_px_09
            f"{values[69]},"  # ask_px_09
            f"{values[70]},"  # bid_sz_09
            f"{values[71]},"  # ask_sz_09
            f"{values[72]},"  # bid_oq_09
            f"{values[73]}"  # ask_oq_09
        ).encode()

    def _binary_tbbo_to_csv_record(self, values: Tuple) -> bytes:
        return (
            f"{values[11]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[12]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[7].decode()},"  # action
            f"{values[8].decode()},"  # side
            f"{values[9]},"  # flags
            f"{values[5]},"  # price
            f"{values[6]},"  # size
            f"{values[13]},"  # sequence
            f"{values[14]},"  # bid_px_00
            f"{values[15]},"  # ask_px_00
            f"{values[16]},"  # bid_sz_00
            f"{values[17]},"  # ask_sz_00
            f"{values[18]},"  # bid_oq_00
            f"{values[19]}"  # ask_oq_00
        ).encode()

    def _binary_trades_to_csv_record(self, values: Tuple) -> bytes:
        return (
            f"{values[11]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[12]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[7].decode()},"  # action
            f"{values[8].decode()},"  # side
            f"{values[9]},"  # flags
            f"{values[5]},"  # price
            f"{values[6]},"  # size
            f"{values[13]}"  # sequence
        ).encode()

    def _binary_ohlcv_to_csv_record(self, values: Tuple) -> bytes:
        return (
            f"{values[4]},"  # ts_event
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[5]},"  # open
            f"{values[6]},"  # high
            f"{values[7]},"  # low
            f"{values[8]},"  # close
            f"{values[9]}"  # volume
        ).encode()

    def _binary_mbo_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_recv": int(values[12]),
                "ts_event": int(values[4]),
                "ts_in_delta": int(values[13]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "order_id": int(values[5]),
                "action": values[11].decode(),
                "side": values[10].decode(),
                "flags": int(values[9]),
                "price": int(values[6]),
                "size": int(values[7]),
                "sequence": int(values[14]),
            }
        ).encode()

    def _binary_mbp_1_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_recv": int(values[11]),
                "ts_event": int(values[4]),
                "ts_in_delta": int(values[13]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "action": values[7].decode(),
                "side": values[8].decode(),
                "flags": int(values[9]),
                "price": int(values[5]),
                "size": int(values[6]),
                "sequence": int(values[13]),
                "bid_px_00": int(values[14]),
                "ask_px_00": int(values[15]),
                "bid_sz_00": int(values[16]),
                "ask_sz_00": int(values[17]),
                "bid_oq_00": int(values[18]),
                "ask_oq_00": int(values[19]),
            }
        ).encode()

    def _binary_mbp_10_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_recv": int(values[11]),
                "ts_event": int(values[4]),
                "ts_in_delta": int(values[12]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "action": values[7].decode(),
                "side": values[8].decode(),
                "flags": int(values[9]),
                "price": int(values[5]),
                "size": int(values[6]),
                "sequence": int(values[13]),
                "bid_px_00": int(values[14]),
                "ask_px_00": int(values[15]),
                "bid_sz_00": int(values[16]),
                "ask_sz_00": int(values[17]),
                "bid_oq_00": int(values[18]),
                "ask_oq_00": int(values[19]),
                "bid_px_01": int(values[20]),
                "ask_px_01": int(values[21]),
                "bid_sz_01": int(values[22]),
                "ask_sz_01": int(values[23]),
                "bid_oq_01": int(values[24]),
                "ask_oq_01": int(values[25]),
                "bid_px_02": int(values[26]),
                "ask_px_02": int(values[27]),
                "bid_sz_02": int(values[28]),
                "ask_sz_02": int(values[29]),
                "bid_oq_02": int(values[30]),
                "ask_oq_02": int(values[31]),
                "bid_px_03": int(values[32]),
                "ask_px_03": int(values[33]),
                "bid_sz_03": int(values[34]),
                "ask_sz_03": int(values[35]),
                "bid_oq_03": int(values[36]),
                "ask_oq_03": int(values[37]),
                "bid_px_04": int(values[38]),
                "ask_px_04": int(values[39]),
                "bid_sz_04": int(values[40]),
                "ask_sz_04": int(values[41]),
                "bid_oq_04": int(values[42]),
                "ask_oq_04": int(values[43]),
                "bid_px_05": int(values[44]),
                "ask_px_05": int(values[45]),
                "bid_sz_05": int(values[46]),
                "ask_sz_05": int(values[47]),
                "bid_oq_05": int(values[48]),
                "ask_oq_05": int(values[49]),
                "bid_px_06": int(values[50]),
                "ask_px_06": int(values[51]),
                "bid_sz_06": int(values[52]),
                "ask_sz_06": int(values[53]),
                "bid_oq_06": int(values[54]),
                "ask_oq_06": int(values[55]),
                "bid_px_07": int(values[56]),
                "ask_px_07": int(values[57]),
                "bid_sz_07": int(values[58]),
                "ask_sz_07": int(values[59]),
                "bid_oq_07": int(values[60]),
                "ask_oq_07": int(values[61]),
                "bid_px_08": int(values[62]),
                "ask_px_08": int(values[63]),
                "bid_sz_08": int(values[64]),
                "ask_sz_08": int(values[65]),
                "bid_oq_08": int(values[66]),
                "ask_oq_08": int(values[67]),
                "bid_px_09": int(values[68]),
                "ask_px_09": int(values[69]),
                "bid_sz_09": int(values[70]),
                "ask_sz_09": int(values[71]),
                "bid_oq_09": int(values[72]),
                "ask_oq_09": int(values[73]),
            }
        ).encode()

    def _binary_tbbo_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_recv": int(values[11]),
                "ts_event": int(values[4]),
                "ts_in_delta": int(values[12]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "action": values[7].decode(),
                "side": values[8].decode(),
                "flags": int(values[9]),
                "price": int(values[5]),
                "size": int(values[6]),
                "sequence": int(values[13]),
                "bid_px_00": int(values[14]),
                "ask_px_00": int(values[15]),
                "bid_sz_00": int(values[16]),
                "ask_sz_00": int(values[17]),
                "bid_oq_00": int(values[18]),
                "ask_oq_00": int(values[19]),
            }
        ).encode()

    def _binary_trades_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_recv": int(values[11]),
                "ts_event": int(values[4]),
                "ts_in_delta": int(values[12]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "action": values[7].decode(),
                "side": values[8].decode(),
                "flags": int(values[9]),
                "price": int(values[5]),
                "size": int(values[6]),
                "sequence": int(values[13]),
            }
        ).encode()

    def _binary_ohlcv_to_json_record(self, values: Tuple) -> bytes:
        return json.dumps(
            {
                "ts_event": int(values[4]),
                "pub_id": int(values[2]),
                "product_id": int(values[3]),
                "open": int(values[5]),
                "high": int(values[6]),
                "low": int(values[7]),
                "close": int(values[8]),
                "volume": int(values[9]),
            }
        ).encode()


def is_400_series_error(status: int) -> bool:
    return status // 100 == 4


def is_500_series_error(status: int) -> bool:
    return status // 100 == 5


def check_http_error(response: Response) -> None:
    if is_500_series_error(response.status_code):
        try:
            json_body = response.json()
            message = json_body.get("detail")
        except JSONDecodeError:
            json_body = None
            message = None
        raise BentoServerError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )
    elif is_400_series_error(response.status_code):
        try:
            json_body = response.json()
            message = json_body.get("detail")
        except JSONDecodeError:
            json_body = None
            message = None
        raise BentoClientError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json_body,
            message=message,
            headers=response.headers,
        )


async def check_http_error_async(response: ClientResponse) -> None:
    if is_500_series_error(response.status):
        json_body = await response.json()
        raise BentoServerError(
            http_status=response.status,
            http_body=response.content,
            json_body=json_body,
            message=json_body["detail"],
            headers=response.headers,
        )
    elif is_400_series_error(response.status):
        json_body = await response.json()
        raise BentoClientError(
            http_status=response.status,
            http_body=response.content,
            json_body=json_body,
            message=json_body["detail"],
            headers=response.headers,
        )
