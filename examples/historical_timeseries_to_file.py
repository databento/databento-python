from pprint import pprint

from databento import DBNStore, Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    data: DBNStore = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="trades",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- Limiting response to 1000 records only
    )  # -> DBNStore

    path = "my_data.dbn"
    data.to_file(path=path)

    data = DBNStore.from_file(path=path)

    # Data now loaded into memory
    pprint(data.to_df())
