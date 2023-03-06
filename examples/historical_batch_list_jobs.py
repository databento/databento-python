from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    response = client.batch.list_jobs(
        states="received,queued,processing,done",  # Included states
        since=None,  # <-- Filter for jobs 'since' the given time
    )

    pprint(response)
