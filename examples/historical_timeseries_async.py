import asyncio
from pprint import pprint

import databento as db
from databento import Bento


async def request_stream_async() -> None:
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    data: Bento = await client.timeseries.get_range_async(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- Limiting response to 1000 records only
    )
    pprint(data.to_df())


if __name__ == "__main__":
    asyncio.run(request_stream_async())
