import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    cost1 = client.metadata.get_cost(
        dataset="GLBX.MDP3",
        symbols="*",
        schema="mbo",
        start="2020-12-27T12:00",
        end="2020-12-29",
        encoding="csv",
        compression="zstd",
        # limit=100000,
    )

    print(cost1)

    cost2 = client.metadata.get_cost(
        dataset="XNAS.ITCH",
        symbols=["MSFT"],
        schema="trades",
        start="2015-04-22",
        end="2015-04-22T12:10",
        encoding="csv",
        compression="none",
        # limit=100000000,
    )

    print(cost2)
