import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_ACCESS_KEY"
    client = db.Historical(key=key)

    size: int = client.metadata.get_billable_size(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-28T12:00",
        end="2020-12-29",
        encoding="bin",
    )

    print(size)
