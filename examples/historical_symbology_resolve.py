from pprint import pprint

import databento as db
from databento.common.enums import SType


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    response = client.symbology.resolve(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        stype_in=SType.NATIVE,
        stype_out=SType.PRODUCT_ID,
        start="2020-12-27",
        end="2020-12-29",
    )

    pprint(response)
