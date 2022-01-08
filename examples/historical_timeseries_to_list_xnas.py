import databento as db
from databento.historical.bento import BentoIOBase


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    data: BentoIOBase = client.timeseries.stream(
        dataset="XNAS.ITCH",
        symbols="MSFT",
        schema="mbo",
        start="2015-04-22",
        end="2015-04-23",
        encoding="csv",
        compression="zstd",
        limit=1000,  # <-- limiting response to 1000 records only
    )

    print(data.to_list())
