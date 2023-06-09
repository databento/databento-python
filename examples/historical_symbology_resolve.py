from pprint import pprint

from databento import Historical
from databento import SType


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    response = client.symbology.resolve(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        stype_in=SType.RAW_SYMBOL,
        stype_out=SType.INSTRUMENT_ID,
        start_date="2022-06-01",
        end_date="2022-06-30",
    )

    pprint(response)
