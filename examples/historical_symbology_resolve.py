from pprint import pprint

import databento as db
from databento.common.enums import SType


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    response = client.symbology.resolve(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        stype_in=SType.NATIVE,
        stype_out=SType.PRODUCT_ID,
        start_date="2022-06-01",
        end_date="2022-06-30",
    )

    pprint(response)
