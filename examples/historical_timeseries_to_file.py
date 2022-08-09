from pprint import pprint

import databento as db
from databento import Bento


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    data: Bento = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="trades",
        start="2020-12-28",
        end="2020-12-30",
        limit=1000,  # <-- limiting response to 1000 records only
    )  # -> MemoryBento

    path = "my_data.dbz"
    data.to_file(path=path)  # -> FileBento

    data = Bento.from_file(path=path)  # -> FileBento

    # Data now loaded into memory
    pprint(data)
