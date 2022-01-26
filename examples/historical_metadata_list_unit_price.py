import databento as db
from databento.common.enums import FeedMode


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    unit_prices = client.metadata.list_unit_prices(
        dataset="GLBX.MDP3",
        mode=FeedMode.HISTORICAL_STREAMING,
    )

    print(unit_prices)
