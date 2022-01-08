import databento as db
from databento.historical.bento import BentoIOBase


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    data: BentoIOBase = client.timeseries.stream(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-28T12:00",
        end="2020-12-30",
        encoding="bin",
        compression="zstd",
        limit=1000,  # <-- limiting response to 1000 records only
    )

    print(data.to_df())
