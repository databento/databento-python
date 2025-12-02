from pprint import pprint

import pandas as pd

from databento import Reference


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Reference(key=key)

    response: pd.DataFrame = client.adjustment_factors.get_range(
        symbols="TSLA",
        stype_in="raw_symbol",
        start="2020",
    )

    pprint(response.head())
