from pprint import pprint

import databento as db
from databento import Bento


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    data: Bento = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="trades",
        start="2020-12-27T12:00",
        end="2020-12-29",
        limit=1000,  # <-- limiting response to 1000 records only
    )

    pprint(data.to_df())
