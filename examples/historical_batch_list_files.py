from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    response = client.batch.list_files(
        job_id="YOUR_JOB_ID",  # <-- Discover this from `.list_jobs(...)`
    )

    pprint(response)
