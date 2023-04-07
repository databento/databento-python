import asyncio
from pprint import pprint

from databento import DBNStore, Historical


async def example_get_range_async() -> None:
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    data: DBNStore = await client.timeseries.get_range_async(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- Limiting response to 1000 records only
    )
    pprint(data.to_df())


if __name__ == "__main__":
    asyncio.run(example_get_range_async())
