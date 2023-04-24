from typing import Dict

from databento.common.enums import SType
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

        # TODO(cs): Temporary mappings until new DBN released
        stype_in = metadata_dict["stype_in"]
        stype_out = metadata_dict["stype_out"]

        if stype_in == SType.NATIVE.value:
            stype_in = SType.RAW_SYMBOL.value
        elif stype_in == SType.PRODUCT_ID.value:
            stype_in = SType.INSTRUMENT_ID.value
        else:
            stype_in = SType.SMART.value

        if stype_out == SType.NATIVE.value:
            stype_out = SType.INSTRUMENT_ID.value
        elif stype_out == SType.PRODUCT_ID.value:
            stype_out = SType.INSTRUMENT_ID.value
        else:
            stype_out = SType.SMART.value

        metadata_dict["stype_in"] = stype_in
        metadata_dict["stype_out"] = stype_out

        return metadata_dict
