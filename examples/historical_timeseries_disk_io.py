from pprint import pprint

import databento as db
from databento import Bento


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    path = "my_data.dbn"

    # Execute request through client
    data: Bento = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- limiting response to 1000 records only
        path=path,
    )

    pprint(data.to_ndarray())
