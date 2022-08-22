from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    unit_prices = client.metadata.list_unit_prices(
        dataset="GLBX.MDP3",
        mode="historical-streaming",
    )

    pprint(unit_prices)
