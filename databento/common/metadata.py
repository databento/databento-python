from typing import Dict

from databento_dbn import decode_metadata


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
            "limit": lambda lim: None if lim == 0 else lim,
        }

        metadata_dict = {}

        for field in dir(metadata):
            if field.startswith("__"):
                continue
            value = getattr(metadata, field)
            if field in conversion_mapping:
                value = conversion_mapping[field](value)
            metadata_dict[field] = value

        return metadata_dict
