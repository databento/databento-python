from typing import Dict

from databento.common.enums import Compression, Schema, SType
from dbz_python import decode_metadata


class MetadataDecoder:
    """
    Provides a decoder for DBN metadata headers.
    """

    @staticmethod
    def decode_to_json(raw_metadata: bytes) -> Dict[str, object]:
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
        metadata = decode_metadata(raw_metadata)
        conversion_mapping = {
            "compression": lambda c: str(Compression.from_int(c)),
            "limit": lambda lim: None if lim == 0 else lim,
            "mappings": lambda m: {i["native"]: i["intervals"] for i in m},
            "schema": lambda s: str(Schema.from_int(s)),
            "stype_in": lambda s_in: str(SType.from_int(s_in)),
            "stype_out": lambda s_out: str(SType.from_int(s_out)),
        }

        for key, conv_fn in conversion_mapping.items():
            metadata[key] = conv_fn(metadata[key])

        return metadata
