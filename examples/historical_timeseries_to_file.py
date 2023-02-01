from pprint import pprint

import databento as db
from databento import Bento


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    data: Bento = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="trades",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- Limiting response to 1000 records only
    )  # -> MemoryBento

    path = "my_data.dbn"
    data.to_file(path=path)  # -> FileBento

    data = Bento.from_file(path=path)  # -> FileBento

    # Data now loaded into memory
    pprint(data.to_df())
