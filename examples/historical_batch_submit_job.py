from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    response = client.batch.submit_job(
        dataset="GLBX.MDP3",
        symbols=["ESM2"],
        schema="mbo",
        start="2022-06-10T12:00",
        end="2022-06-10T14:00",
        limit=1000,  # <-- limiting batch request to 1000 records only
        encoding="csv",
        compression="zstd",
        delivery="download",
    )

    pprint(response)
