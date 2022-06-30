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
_4MB = 1024 * 1024 * 4  # Backend standard streaming buffer size


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
            writer = bento.writer()
            if (
                compression_in == Compression.ZSTD
                and compression_out == Compression.NONE
            ):
                # Wrap inner writer with zstd decompressor
                writer = zstandard.ZstdDecompressor().stream_writer(writer)

            zstd_decompressor = zstandard.ZstdDecompressor().decompressobj()

            # Binary struct format
            record_map = DBZ_RECORD_MAP[schema]
            record_size = np.dtype(record_map).itemsize

            # Header flags
            received_metadata_header = False
            written_csv_header = False

            buffer = b""
            for chunk in response.iter_content(chunk_size=_4MB):
                if chunk == _NO_DATA_FOUND:
                    log_info("No data found for query.")
                    return

                if encoding_in == Encoding.DBZ:
                    # Here we check for a known magic number. Improve this to
                    # check for all valid skippable frame magic numbers.
                    if not received_metadata_header and chunk.startswith(b"Q*M\x18"):
                        received_metadata_header = True
                        self._decode_metadata(chunk, bento)
                        continue

                    if encoding_out != Encoding.DBZ:
                        if not written_csv_header:
                            csv_header: bytes = CSV_HEADERS[schema] + b"\n"
                            if compression_out == Compression.ZSTD:
                                csv_header = zstandard.compress(csv_header, level=2)
                            writer.write(csv_header)
                            written_csv_header = True

                        if zstd_decompressor.eof:
                            residual: bytes = zstd_decompressor.unused_data
                            zstd_decompressor = (
                                zstandard.ZstdDecompressor().decompressobj()
                            )
                            zstd_decompressor.decompress(residual)

                        binary: bytes = zstd_decompressor.decompress(chunk)
                        if not binary:
                            # Decompressor has not emitted data yet
                            continue

                        if len(buffer) > 0:
                            # Prepend existing buffer
                            binary = buffer + binary

                        # Check binary length aligns with record size
                        binary_length = len(binary)
                        binary_diff = binary_length % record_size
                        if binary_diff > 0:
                            # Add to buffer
                            buffer = binary
                            continue
                        else:
                            # Clear buffer
                            buffer = b""

                        text_buffer: bytes = self._decode_dbz_to_text_buffer(
                            record_map=record_map,
                            binary=binary,
                            schema=schema,
                            encoding=encoding_out,
                        )

                        self._write_text_records(
                            text_buffer=text_buffer,
                            inner_writer=writer,
                            compression_out=compression_out,
                        )
                        continue

                writer.write(chunk)

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
                writer = bento.writer()
                if (
                    compression_in == Compression.ZSTD
                    and compression_out == Compression.NONE
                ):
                    # Wrap inner writer with zstd decompressor
                    writer = zstandard.ZstdDecompressor().stream_writer(writer)

                zstd_decompressor = zstandard.ZstdDecompressor().decompressobj()

                # Binary struct format
                record_map = DBZ_RECORD_MAP[schema]
                record_size = np.dtype(record_map).itemsize

                # Header flags
                received_metadata_header = False
                written_csv_header = False

                buffer = b""
                async for async_chunk in response.content.iter_chunks():
                    chunk: bytes = async_chunk[0]
                    if chunk == _NO_DATA_FOUND:
                        log_info("No data found for query.")
                        return

                    if encoding_in == Encoding.DBZ:
                        # Here we check for a known magic number. Improve this to
                        # check for all valid skippable frame magic numbers.
                        if not received_metadata_header and chunk.startswith(
                            b"Q*M\x18"
                        ):
                            received_metadata_header = True
                            self._decode_metadata(chunk, bento)
                            continue

                        if encoding_out != Encoding.DBZ:
                            if not written_csv_header:
                                csv_header: bytes = CSV_HEADERS[schema] + b"\n"
                                if compression_out == Compression.ZSTD:
                                    csv_header = zstandard.compress(csv_header, level=2)
                                writer.write(csv_header)
                                written_csv_header = True

                            if zstd_decompressor.eof:
                                residual: bytes = zstd_decompressor.unused_data
                                zstd_decompressor = (
                                    zstandard.ZstdDecompressor().decompressobj()
                                )
                                zstd_decompressor.decompress(residual)

                            binary: bytes = zstd_decompressor.decompress(chunk)
                            if not binary:
                                # Decompressor has not emitted data yet
                                continue

                            if len(buffer) > 0:
                                # Prepend existing buffer
                                binary = buffer + binary

                            # Check binary length aligns with record size
                            binary_length = len(binary)
                            binary_diff = binary_length % record_size
                            if binary_diff > 0:
                                # Add to buffer
                                buffer = binary
                                continue
                            else:
                                # Clear buffer
                                buffer = b""

                            text_buffer: bytes = self._decode_dbz_to_text_buffer(
                                record_map=record_map,
                                binary=binary,
                                schema=schema,
                                encoding=encoding_out,
                            )

                            self._write_text_records(
                                text_buffer=text_buffer,
                                inner_writer=writer,
                                compression_out=compression_out,
                            )
                            continue

                    writer.write(chunk)

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

    def _decode_dbz_to_text_buffer(
        self,
        record_map,
        binary: bytes,
        schema: Schema,
        encoding: Encoding,
    ) -> bytes:
        # Unpack binary into discrete records
        records = np.frombuffer(binary, dtype=record_map)

        if encoding == Encoding.CSV:
            csv_records = self._binary_to_csv_records(schema, records)
            return b"\n".join(csv_records) + b"\n"
        elif encoding == Encoding.JSON:
            raise NotImplementedError("Binary to JSON decoding not supported yet")
        else:
            raise NotImplementedError(f"Cannot decode dbz to {encoding.value}")

    def _write_text_records(
        self,
        text_buffer: bytes,
        inner_writer: BinaryIO,
        compression_out: Compression,
    ) -> None:
        if compression_out == Compression.ZSTD:
            text_buffer = zstandard.compress(text_buffer, level=2)
        inner_writer.write(text_buffer)

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
                f"{schema.value} binary decoding not implemented yet",
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
            f"{values[13]}"  # sequence
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
            f"{values[13]}"  # sequence
            f"{values[14]},"  # bid_px_00
            f"{values[15]},"  # ask_px_00
            f"{values[16]},"  # bid_sz_00
            f"{values[17]},"  # ask_sz_00
            f"{values[18]},"  # bid_oq_00
            f"{values[19]}"  # ask_oq_00
            f"{values[20]},"  # bid_px_01
            f"{values[21]},"  # ask_px_01
            f"{values[22]},"  # bid_sz_01
            f"{values[23]},"  # ask_sz_01
            f"{values[24]},"  # bid_oq_01
            f"{values[25]}"  # ask_oq_01
            f"{values[26]},"  # bid_px_02
            f"{values[27]},"  # ask_px_02
            f"{values[28]},"  # bid_sz_02
            f"{values[29]},"  # ask_sz_02
            f"{values[30]},"  # bid_oq_02
            f"{values[31]}"  # ask_oq_02
            f"{values[32]},"  # bid_px_03
            f"{values[33]},"  # ask_px_03
            f"{values[34]},"  # bid_sz_03
            f"{values[35]},"  # ask_sz_03
            f"{values[36]},"  # bid_oq_03
            f"{values[37]}"  # ask_oq_03
            f"{values[38]},"  # bid_px_04
            f"{values[39]},"  # ask_px_04
            f"{values[40]},"  # bid_sz_04
            f"{values[41]},"  # ask_sz_04
            f"{values[42]},"  # bid_oq_04
            f"{values[43]}"  # ask_oq_04
            f"{values[44]},"  # bid_px_05
            f"{values[45]},"  # ask_px_05
            f"{values[46]},"  # bid_sz_05
            f"{values[47]},"  # ask_sz_05
            f"{values[48]},"  # bid_oq_05
            f"{values[49]}"  # ask_oq_05
            f"{values[50]},"  # bid_px_06
            f"{values[51]},"  # ask_px_06
            f"{values[52]},"  # bid_sz_06
            f"{values[53]},"  # ask_sz_06
            f"{values[54]},"  # bid_oq_06
            f"{values[55]}"  # ask_oq_06
            f"{values[56]},"  # bid_px_07
            f"{values[57]},"  # ask_px_07
            f"{values[58]},"  # bid_sz_07
            f"{values[59]},"  # ask_sz_07
            f"{values[60]},"  # bid_oq_07
            f"{values[61]}"  # ask_oq_07
            f"{values[62]},"  # bid_px_08
            f"{values[63]},"  # ask_px_08
            f"{values[64]},"  # bid_sz_08
            f"{values[65]},"  # ask_sz_08
            f"{values[66]},"  # bid_oq_08
            f"{values[67]}"  # ask_oq_08
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
            f"{values[13]}"  # sequence
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


def is_400_series_error(status: int) -> bool:
    return status // 100 == 4


def is_500_series_error(status: int) -> bool:
    return status // 100 == 5


def check_http_error(response: Response) -> None:
    if is_500_series_error(response.status_code):
        try:
            json = response.json()
            message = json.get("detail")
        except JSONDecodeError:
            json = None
            message = None
        raise BentoServerError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json,
            message=message,
            headers=response.headers,
        )
    elif is_400_series_error(response.status_code):
        try:
            json = response.json()
            message = json.get("detail")
        except JSONDecodeError:
            json = None
            message = None
        raise BentoClientError(
            http_status=response.status_code,
            http_body=response.content,
            json_body=json,
            message=message,
            headers=response.headers,
        )


async def check_http_error_async(response: ClientResponse) -> None:
    if is_500_series_error(response.status):
        json = await response.json()
        raise BentoServerError(
            http_status=response.status,
            http_body=response.content,
            json_body=json,
            message=json["detail"],
            headers=response.headers,
        )
    elif is_400_series_error(response.status):
        json = await response.json()
        raise BentoClientError(
            http_status=response.status,
            http_body=response.content,
            json_body=json,
            message=json["detail"],
            headers=response.headers,
        )
