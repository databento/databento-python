from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    # Will download all job files to a `my_data/YOUR_JOB_ID/` directory
    downloaded_files = client.batch.download(
        output_dir="my_data",
        job_id="YOUR_JOB_ID",  # <-- Discover this from `.list_jobs(...)`
    )

    for file in downloaded_files:
        print(f"Downloaded {file.name}")
