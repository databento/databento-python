from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    condition = client.metadata.get_dataset_condition(dataset="XNAS.ITCH")

    pprint(condition)
