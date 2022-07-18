import databento as db
from databento import Bento
from databento.common.enums import Schema


if __name__ == "__main__":
    key = ""  # Change access key
    client = db.Historical(key=key)

    for schema in Schema:
        if schema in (Schema.DEFINITION, Schema.STATISTICS, Schema.STATUS):
            continue

        print(schema.value)

        path = f"test_data2.{schema.value}.dbz"

        # Execute request through client
        bento: Bento = client.timeseries.stream(
            dataset="GLBX.MDP3",
            symbols=["ESH1"],
            schema=schema,
            start="2020-12-27T13:00",
            end="2020-12-29",
            limit=2,  # <-- limiting response to 2 records only (for test cases)
            path=path,
        )  # -> FileBento

        open(path, mode="rb").read()
        print(bento.raw)
