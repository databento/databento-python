import json
import struct
from typing import Any, Dict, Optional

import zstandard
from databento.common.enums import Compression, Encoding, Schema, SType
from databento.common.parsing import (
    int_to_compression,
    int_to_encoding,
    int_to_schema,
    int_to_stype,
)


class MetadataDecoder:
    """
    Provides a decoder for Databento metadata headers.

    Fixed query and shape metadata
    ------------------------------
    version       UInt8       1   1
    dataset       Char[9]     9   10
    schema        UInt8       1   11
    stype_in      UInt8       1   12
    stype_out     UInt8       1   13
    start         UInt64      8   21
    end           UInt64      8   29
    limit         UInt64      8   37
    encoding      UInt8       1   38
    compression   UInt8       1   39
    rows          UInt64      8   47
    cols          UInt16      2   49
    padding       x          15   64

    References
    ----------
    https://github.com/facebook/zstd/wiki
    https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#skippable-frames
    """

    # 4 Bytes, little-endian ordering. Value : 0x184D2A5?, which means any value
    # from 0x184D2A50 to 0x184D2A5F. All 16 values are valid to identify a
    # skippable frame. This specification doesn't detail any specific tagging
    # for skippable frames.
    ZSTD_FIRST_MAGIC = 0x184D2A50  # 407710288
    METADATA_STRUCT_FMT = "<B9sBBBQQQBBQH15x"
    METADATA_STRUCT_SIZE = struct.calcsize(METADATA_STRUCT_FMT)

    @staticmethod
    def decode_to_json(metadata: bytes) -> Dict[str, Any]:
        """
        Decode the given metadata into a JSON object (as a Python dict).

        Parameters
        ----------
        metadata : bytes
            The metadata to decode.

        Returns
        -------
        dict[str, Any]

        """
        fixed_fmt: str = MetadataDecoder.METADATA_STRUCT_FMT
        fixed_buffer: bytes = metadata[: MetadataDecoder.METADATA_STRUCT_SIZE]
        fixed_values = struct.unpack(fixed_fmt, fixed_buffer)

        # Decode fixed values
        version: int = fixed_values[0]
        dataset: str = fixed_values[1].decode("ascii")
        schema: Schema = int_to_schema(fixed_values[2])
        stype_in: SType = int_to_stype(fixed_values[3])
        stype_out: SType = int_to_stype(fixed_values[4])
        start: int = fixed_values[5]  # UNIX nanoseconds
        end: int = fixed_values[6]  # UNIX nanoseconds

        limit_int: int = fixed_values[7]
        limit: Optional[int] = None if limit_int == 0 else limit_int

        encoding: Encoding = int_to_encoding(fixed_values[8])
        compression: Compression = int_to_compression(fixed_values[9])

        rows: int = fixed_values[10]
        cols: int = fixed_values[11]

        var_buffer: bytes = metadata[MetadataDecoder.METADATA_STRUCT_SIZE :]
        var_decompressed: bytes = zstandard.decompress(var_buffer)
        var_json: dict[str, Any] = json.loads(var_decompressed)

        json_obj = {
            "version": version,
            "dataset": dataset,
            "schema": schema.value,
            "stype_in": stype_in.value,
            "stype_out": stype_out.value,
            "start": start,
            "end": end,
            "limit": limit,
            "encoding": encoding.value,
            "compression": compression.value,
            "rows": rows,
            "cols": cols,
        }

        json_obj.update(var_json)

        return json_obj
