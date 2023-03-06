from pprint import pprint

import databento as db


if __name__ == "__main__":
    db.log = "debug"  # Optional debug logging

    key = "YOUR_API_KEY"
    client = db.Historical(key=key)

    conditions = client.metadata.list_dataset_conditions(dataset="XNAS.ITCH")

    pprint(conditions)
