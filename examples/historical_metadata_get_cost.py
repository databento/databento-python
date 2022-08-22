import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    cost1: float = client.metadata.get_cost(
        dataset="GLBX.MDP3",
        symbols="*",
        schema="mbo",
        start="2020-12-27T12:00",
        end="2020-12-29",
    )

    print(cost1)

    cost2: float = client.metadata.get_cost(
        dataset="XNAS.ITCH",
        symbols=["MSFT"],
        schema="trades",
        start="2015-04-22",
        end="2015-04-22T12:10",
    )

    print(cost2)
