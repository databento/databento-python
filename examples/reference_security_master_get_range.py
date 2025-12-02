from pprint import pprint

import pandas as pd

from databento import Reference


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Reference(key=key)

    response: pd.DataFrame = client.security_master.get_range(
        symbols="AAPL,AMZN,MSFT,TQQQ",
        start="2024-01",
        countries="US",
    )

    pprint(response.head())
