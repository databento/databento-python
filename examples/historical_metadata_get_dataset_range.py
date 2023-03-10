from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    available_range = client.metadata.get_dataset_range(dataset="GLBX.MDP3")

    pprint(available_range)
