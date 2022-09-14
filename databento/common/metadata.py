from typing import Any, Dict

from databento.common.parsing import int_to_compression, int_to_schema, int_to_stype
from dbz_python import decode_metadata


class MetadataDecoder:
    """
    Provides a decoder for DBZ metadata headers.
    """

    @staticmethod
    def decode_to_json(raw_metadata: bytes) -> Dict[str, Any]:
        """
        Decode the given metadata into a JSON object (as a Python dict).

        Parameters
        ----------
        raw_metadata : bytes
            The metadata to decode.

        Returns
        -------
        Dict[str, Any]

        """

        def enum_value(fn):
            return lambda x: fn(x).value

        metadata = decode_metadata(raw_metadata)
        conversion_mapping = {
            "compression": enum_value(int_to_compression),
            "limit": lambda lim: None if lim == 0 else lim,
            "mappings": lambda m: {i["native"]: i["intervals"] for i in m},
            "schema": enum_value(int_to_schema),
            "stype_in": enum_value(int_to_stype),
            "stype_out": enum_value(int_to_stype),
        }

        for key, conv_fn in conversion_mapping.items():
            metadata[key] = conv_fn(metadata[key])

        return metadata
