from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    response = client.batch.list_files(
        job_id="YOUR_JOB_ID",  # <-- Discover this from `.list_jobs(...)`
    )

    pprint(response)
