from pprint import pprint

import databento as db
from databento.historical.bento import Bento


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    data: Bento = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-27",
        end="2020-12-30",
        encoding="csv",
        compression="zstd",
        limit=1000,  # <-- limiting response to 1000 records only
    )  # -> MemoryBento

    path = "my_data.csv"
    data.to_file(path=path)  # -> FileBento

    data = db.from_file(path="my_data.csv", schema="mbo")  # -> FileBento

    # Data now loaded into memory
    pprint(data.raw)
