from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    condition = client.metadata.get_dataset_condition(dataset="XNAS.ITCH")

    pprint(condition)
