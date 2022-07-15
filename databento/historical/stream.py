import json
from typing import BinaryIO, List, Tuple

import numpy as np
import zstandard
from databento import Bento
from databento.common.buffer import BinaryBuffer
from databento.common.data import CSV_HEADERS, DBZ_STRUCT_MAP
from databento.common.enums import Compression, Encoding, Schema
from databento.common.logging import log_debug
from databento.common.metadata import MetadataDecoder


class StreamOrchestrator:
    """
    Provides an orchestrator for HTTP streaming data flows.

    The orchestrator will arrange the necessary stream writers to handle any
    combination of encoding in and out, as well as any combination of
    compression in and out.
    """

    def __init__(
        self,
        schema: Schema,
        encoding_in: Encoding,
        encoding_out: Encoding,
        compression_in: Compression,
        compression_out: Compression,
        bento: Bento,
    ):
        self.schema = schema
        self.encoding_in = encoding_in
        self.encoding_out = encoding_out
        self.compression_in = compression_in
        self.compression_out = compression_out
        self.bento = bento

        # Setup streams
        self.binary_stream = BinaryBuffer()
        self.binary_writer_in = zstandard.ZstdDecompressor().stream_writer(
            self.binary_stream,
        )

        self.bento_writer: BinaryIO = bento.writer()
        self.inner_writer = self.bento_writer
        self.text_writer_out = self.inner_writer
        self.should_close_text_writer_out = False
        if compression_in == Compression.ZSTD:
            if compression_out == Compression.NONE:
                # Wrap inner writer with zstd decompressor
                self.inner_writer = zstandard.ZstdDecompressor().stream_writer(
                    self.inner_writer,
                )
            else:
                # Wrap inner writer with zstd compressor. Here we set `closefd`
                # to False so that we may use the inner stream later.
                self.text_writer_out = zstandard.ZstdCompressor().stream_writer(
                    self.inner_writer,
                    closefd=False,
                )
                self.should_close_text_writer_out = True

        # Binary struct format
        self.binary_map = DBZ_STRUCT_MAP[schema]
        self.binary_size = np.dtype(self.binary_map).itemsize

        self.metadata_buffer: bytes = b""
        self.metadata_frame_size: int = 0

        # Header flags
        self.receiving_header = False
        self.metadata_header_received = False
        self.csv_header_written = False

    def write(self, chunk: bytes):
        """
        Write the given chunk to the stream.

        Parameters
        ----------
        chunk : bytes
            The chunk to write.

        """
        if self.encoding_in != Encoding.DBZ:
            self.inner_writer.write(chunk)
        else:
            if self.encoding_out == Encoding.DBZ:
                if self.metadata_header_received:
                    self.inner_writer.write(chunk)
                # Here we check for a known magic number. Improve this to
                # check for all valid skippable frame magic numbers.
                elif self.receiving_header or chunk.startswith(b"Q*M\x18"):
                    self.bento_writer.write(chunk)

                    if not self.receiving_header:
                        log_debug("Receiving metadata...")
                        self.metadata_frame_size = int.from_bytes(
                            bytes=chunk[4:8],
                            byteorder="little",
                        )
                        self.receiving_header = True

                    if self.metadata_frame_size > len(self.metadata_buffer) + len(
                        chunk
                    ):
                        self.metadata_buffer += chunk
                        return

                    self._decode_metadata(
                        raw=self.metadata_buffer + chunk,
                        frame_size=self.metadata_frame_size,
                        bento=self.bento,
                    )

                    self.metadata_buffer = b""
                    self.metadata_header_received = True
            else:
                if self.encoding_out == Encoding.CSV and not self.csv_header_written:
                    csv_header: bytes = CSV_HEADERS[self.schema] + b"\n"
                    self.text_writer_out.write(csv_header)
                    self.text_writer_out.flush()
                    self.csv_header_written = True

                self.binary_writer_in.write(chunk)

                # Check binary length aligns with record size
                binary_len = len(self.binary_stream)
                if binary_len == 0 or binary_len % self.binary_size > 0:
                    # Check on next chunk
                    return

                binary_buffer: bytes = self.binary_stream.raw
                text_buffer: bytes = self._decode_binary_to_text_buffer(binary_buffer)
                self.text_writer_out.write(text_buffer)

                # Clear input binary stream as it is now written
                self.binary_stream.clear()

    def close(self) -> None:
        """
        Close the stream.
        """
        self.text_writer_out.flush()
        if self.should_close_text_writer_out:
            # Finally ensure the end of the zstd frame is written
            self.text_writer_out.close()

    def _decode_metadata(self, raw: bytes, frame_size: int, bento: Bento) -> None:
        log_debug("Decoding metadata...")
        metadata = MetadataDecoder.decode_to_json(raw[8 : frame_size + 8])
        bento.set_metadata(metadata)

    def _decode_binary_to_text_buffer(self, buffer: bytes) -> bytes:
        # Unpack binary into discrete records
        binary_records: np.ndarray = np.frombuffer(buffer=buffer, dtype=self.binary_map)

        text_records: List[bytes]
        if self.encoding_out == Encoding.CSV:
            text_records = self._binary_to_csv_records(binary_records)
        elif self.encoding_out == Encoding.JSON:
            text_records = self._binary_to_json_records(binary_records)
        else:
            raise NotImplementedError(f"Cannot decode DBZ to {self.encoding_out.value}")

        return b"\n".join(text_records) + b"\n"

    def _binary_to_csv_records(self, values: np.ndarray) -> List[bytes]:
        if self.schema == Schema.MBO:
            return list(map(self._binary_mbo_to_csv_record, values))
        elif self.schema == Schema.MBP_1:
            return list(map(self._binary_mbp_1_to_csv_record, values))
        elif self.schema == Schema.MBP_10:
            return list(map(self._binary_mbp_10_to_csv_record, values))
        elif self.schema == Schema.TBBO:
            return list(map(self._binary_tbbo_to_csv_record, values))
        elif self.schema == Schema.TRADES:
            return list(map(self._binary_trades_to_csv_record, values))
        elif self.schema in (
            Schema.OHLCV_1S,
            Schema.OHLCV_1M,
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
        ):
            return list(map(self._binary_ohlcv_to_csv_record, values))
        else:
            raise NotImplementedError(
                f"{self.schema.value} schema DBZ to CSV decoding not implemented yet",
            )

    def _binary_to_json_records(self, values: np.ndarray) -> List[bytes]:
        if self.schema == Schema.MBO:
            return list(map(self._binary_mbo_to_json_record, values))
        elif self.schema == Schema.MBP_1:
            return list(map(self._binary_mbp_1_to_json_record, values))
        elif self.schema == Schema.MBP_10:
            return list(map(self._binary_mbp_10_to_json_record, values))
        elif self.schema == Schema.TBBO:
            return list(map(self._binary_tbbo_to_json_record, values))
        elif self.schema == Schema.TRADES:
            return list(map(self._binary_trades_to_json_record, values))
        elif self.schema in (
            Schema.OHLCV_1S,
            Schema.OHLCV_1M,
            Schema.OHLCV_1H,
            Schema.OHLCV_1D,
        ):
            return list(map(self._binary_ohlcv_to_json_record, values))
        else:
            raise NotImplementedError(
                f"{self.schema.value} schema DBZ to JSON decoding not implemented yet",
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
            f"{values[8] & 0xff},"  # flags
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
            f"{values[8].decode()},"  # action
            f"{values[7].decode()},"  # side
            f"{values[9] & 0xff},"  # flags
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
        return (
            f"{values[11]},"  # ts_recv
            f"{values[4]},"  # ts_event
            f"{values[12]},"  # ts_in_delta
            f"{values[2]},"  # pub_id
            f"{values[3]},"  # product_id
            f"{values[8].decode()},"  # action
            f"{values[7].decode()},"  # side
            f"{values[9] & 0xff},"  # flags
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
            f"{values[8].decode()},"  # action
            f"{values[7].decode()},"  # side
            f"{values[9] & 0xff},"  # flags
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
            f"{values[8].decode()},"  # action
            f"{values[7].decode()},"  # side
            f"{values[9] & 0xff},"  # flags
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
        record = {
            "ts_recv": int(values[12]),
            "ts_event": int(values[4]),
            "ts_in_delta": int(values[13]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "order_id": int(values[5]),
            "action": values[11].decode(),
            "side": values[10].decode(),
            "flags": int(values[8] & 0xFF),
            "price": int(values[6]),
            "size": int(values[7]),
            "sequence": int(values[14]),
        }

        return json.dumps(record).encode()

    def _binary_mbp_1_to_json_record(self, values: Tuple) -> bytes:
        record = {
            "ts_recv": int(values[11]),
            "ts_event": int(values[4]),
            "ts_in_delta": int(values[13]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "action": values[8].decode(),
            "side": values[7].decode(),
            "flags": int(values[9] & 0xFF),
            "price": int(values[5]),
            "size": int(values[6]),
            "sequence": int(values[13]),
        }
        levels = [
            {
                "bid_px": int(values[14]),
                "ask_px": int(values[15]),
                "bid_sz": int(values[16]),
                "ask_sz": int(values[17]),
                "bid_oq": int(values[18]),
                "ask_oq": int(values[19]),
            },
        ]
        record["levels"] = levels

        return json.dumps(record).encode()

    def _binary_mbp_10_to_json_record(self, values: Tuple) -> bytes:
        record = {
            "ts_recv": int(values[11]),
            "ts_event": int(values[4]),
            "ts_in_delta": int(values[12]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "action": values[8].decode(),
            "side": values[7].decode(),
            "flags": int(values[9] & 0xFF),
            "price": int(values[5]),
            "size": int(values[6]),
            "sequence": int(values[13]),
        }
        levels = [
            {
                "bid_px": int(values[14]),
                "ask_px": int(values[15]),
                "bid_sz": int(values[16]),
                "ask_sz": int(values[17]),
                "bid_oq": int(values[18]),
                "ask_oq": int(values[19]),
            },
            {
                "bid_px": int(values[20]),
                "ask_px": int(values[21]),
                "bid_sz": int(values[22]),
                "ask_sz": int(values[23]),
                "bid_oq": int(values[24]),
                "ask_oq": int(values[25]),
            },
            {
                "bid_px": int(values[26]),
                "ask_px": int(values[27]),
                "bid_sz": int(values[28]),
                "ask_sz": int(values[29]),
                "bid_oq": int(values[30]),
                "ask_oq": int(values[31]),
            },
            {
                "bid_px": int(values[32]),
                "ask_px": int(values[33]),
                "bid_sz": int(values[34]),
                "ask_sz": int(values[35]),
                "bid_oq": int(values[36]),
                "ask_oq": int(values[37]),
            },
            {
                "bid_px": int(values[38]),
                "ask_px": int(values[39]),
                "bid_sz": int(values[40]),
                "ask_sz": int(values[41]),
                "bid_oq": int(values[42]),
                "ask_oq": int(values[43]),
            },
            {
                "bid_px": int(values[44]),
                "ask_px": int(values[45]),
                "bid_sz": int(values[46]),
                "ask_sz": int(values[47]),
                "bid_oq": int(values[48]),
                "ask_oq": int(values[49]),
            },
            {
                "bid_px": int(values[50]),
                "ask_px": int(values[51]),
                "bid_sz": int(values[52]),
                "ask_sz": int(values[53]),
                "bid_oq": int(values[54]),
                "ask_oq": int(values[55]),
            },
            {
                "bid_px": int(values[56]),
                "ask_px": int(values[57]),
                "bid_sz": int(values[58]),
                "ask_sz": int(values[59]),
                "bid_oq": int(values[60]),
                "ask_oq": int(values[61]),
            },
            {
                "bid_px": int(values[62]),
                "ask_px": int(values[63]),
                "bid_sz": int(values[64]),
                "ask_sz": int(values[65]),
                "bid_oq": int(values[66]),
                "ask_oq": int(values[67]),
            },
            {
                "bid_px": int(values[68]),
                "ask_px": int(values[69]),
                "bid_sz": int(values[70]),
                "ask_sz": int(values[71]),
                "bid_oq": int(values[72]),
                "ask_oq": int(values[73]),
            },
        ]
        record["levels"] = levels

        return json.dumps(record).encode()

    def _binary_tbbo_to_json_record(self, values: Tuple) -> bytes:
        record = {
            "ts_recv": int(values[11]),
            "ts_event": int(values[4]),
            "ts_in_delta": int(values[12]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "action": values[8].decode(),
            "side": values[7].decode(),
            "flags": int(values[9] & 0xFF),
            "price": int(values[5]),
            "size": int(values[6]),
            "sequence": int(values[13]),
        }
        levels = [
            {
                "bid_px": int(values[14]),
                "ask_px": int(values[15]),
                "bid_sz": int(values[16]),
                "ask_sz": int(values[17]),
                "bid_oq": int(values[18]),
                "ask_oq": int(values[19]),
            },
        ]
        record["levels"] = levels

        return json.dumps(record).encode()

    def _binary_trades_to_json_record(self, values: Tuple) -> bytes:
        record = {
            "ts_recv": int(values[11]),
            "ts_event": int(values[4]),
            "ts_in_delta": int(values[12]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "action": values[8].decode(),
            "side": values[7].decode(),
            "flags": int(values[9] & 0xFF),
            "price": int(values[5]),
            "size": int(values[6]),
            "sequence": int(values[13]),
        }

        return json.dumps(record).encode()

    def _binary_ohlcv_to_json_record(self, values: Tuple) -> bytes:
        record = {
            "ts_event": int(values[4]),
            "pub_id": int(values[2]),
            "product_id": int(values[3]),
            "open": int(values[5]),
            "high": int(values[6]),
            "low": int(values[7]),
            "close": int(values[8]),
            "volume": int(values[9]),
        }

        return json.dumps(record).encode()
