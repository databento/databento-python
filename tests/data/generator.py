import databento as db
from databento.common.enums import Schema
from databento.historical.bento import BentoIOBase


if __name__ == "__main__":
    key = ""  # Change access key
    client = db.Historical(key=key)

    for schema in Schema:
        encoding = "csv"
        compression = "zstd"

        compression_ext = ".zst" if compression == "zstd" else ""
        path = f"test_data.{schema.value}.{encoding}{compression_ext}"

        # Execute request through client
        data: BentoIOBase = client.timeseries.stream(
            dataset="GLBX.MDP3",
            symbols="ESH1",
            schema=schema,
            start="2020-12-27T13:00",
            end="2020-12-28",
            encoding=encoding,
            compression=compression,
            limit=2,  # <-- limiting response to 2 records only (for test cases)
            path=path,
            overwrite=True,  # <-- will overwrite an existing file at the path
        )  # -> BentoDiskIO

        print(open(path, mode="rb").read())
        print(data.getvalue(decompress=False))
