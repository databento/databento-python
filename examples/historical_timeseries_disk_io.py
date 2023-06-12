from pprint import pprint

from databento import DBNStore
from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    path = "my_data.dbn"

    # Execute request through client
    data: DBNStore = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- Limiting response to 1000 records only
        path=path,
    )

    pprint(data.to_ndarray())
