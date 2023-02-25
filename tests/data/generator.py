import databento as db
from databento import Bento
from databento.common.enums import Schema


if __name__ == "__main__":
    key = ""  # <-- Change to valid API key prior to running
    client = db.Historical(key=key)

    for schema in Schema:
        if schema in (
            Schema.STATISTICS,
            Schema.STATUS,
            Schema.GATEWAY_ERROR,
            Schema.SYMBOL_MAPPING,
        ):
            continue

        print(schema.value)

        path = f"test_data.{schema.value}.dbn.zst"

        # Execute request through client
        data: Bento = client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols=["ESH1"],
            schema=schema,
            start="2020-12-28T13:00",
            end="2020-12-29T13:00",
            limit=2,  # <-- limiting response to 2 records only (for test cases)
            path=path,
        )  # -> Bento

        written = open(path, mode="rb").read()
        print(written)
