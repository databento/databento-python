from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    response = client.batch.list_jobs(
        states="received,queued,processing,done",  # Included states
        since=None,  # <-- Filter for jobs 'since' the given time
    )

    pprint(response)
