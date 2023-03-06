from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    conditions = client.metadata.list_dataset_conditions(dataset="XNAS.ITCH")

    pprint(conditions)
