from pprint import pprint

import pandas as pd
from databento import Reference


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Reference(key=key)

    response: pd.DataFrame = client.corporate_actions.get_range(
        dataset="XNAS.ITCH",
        symbols="AAPL,MSFT,TSLA",
        stype_in="raw_symbol",
        start_date="2023",
        end_date="2024-04",
        events="DIV,LIQ",
    )

    pprint(response.head())
