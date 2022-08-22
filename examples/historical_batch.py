from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    response = client.batch.timeseries_submit(
        dataset="GLBX.MDP3",
        symbols=["ESH1"],
        schema="mbo",
        start="2020-12-27T12:00",
        end="2020-12-29",
        encoding="csv",
        compression="zstd",
        delivery="download",
    )

    pprint(response)

    jobs = client.batch.list_jobs()
    pprint(jobs)
