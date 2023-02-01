import databento as db


if __name__ == "__main__":
    db.log = "debug"  # optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    # Will download all job files to a `my_data/YOUR_JOB_ID/` directory
    client.batch.download(
        output_dir="my_data",
        job_id="YOUR_JOB_ID",  # <-- Discover this from `.list_jobs(...)`
    )
