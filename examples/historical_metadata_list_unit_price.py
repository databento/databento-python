from pprint import pprint

from databento import Historical


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Historical(key=key)

    unit_prices = client.metadata.list_unit_prices(dataset="GLBX.MDP3")

    pprint(unit_prices)
