import json
import struct
from typing import Any, Dict

import zstandard
from databento.common.parsing import (
    int_to_compression,
    int_to_encoding,
    int_to_schema,
    int_to_stype,
)


class MetadataDecoder:
    """
    Provides Databento metadata headers in a zstd skippable frame.

    We create the skippable frames magic number by adding the bento schema
    version to the first valid skippable frame identity value. This gives us
    automatically incrementing schema versioning.

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
    METADATA_STRUCT_FMT = "<B9sBBBQQQBBQH"

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
        fixed_values = struct.unpack(MetadataDecoder.METADATA_STRUCT_FMT, metadata[:49])

        # Decode fixed values
        version = fixed_values[0]
        dataset = fixed_values[1].decode("ascii")
        schema = int_to_schema(fixed_values[2])
        stype_in = int_to_stype(fixed_values[3])
        stype_out = int_to_stype(fixed_values[4])
        start = fixed_values[5]
        end = fixed_values[6]

        limit_int = fixed_values[7]
        limit = None if limit_int == 0 else limit_int

        encoding = int_to_encoding(fixed_values[8])
        compression = int_to_compression(fixed_values[9])

        rows_int = fixed_values[10]
        cols_int = fixed_values[11]

        decompressed = zstandard.decompress(metadata[49:])
        var_json = json.loads(decompressed)

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
            "rows": rows_int,
            "cols": cols_int,
        }

        json_obj.update(var_json)

        return json_obj
