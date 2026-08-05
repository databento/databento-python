from pprint import pprint
from typing import Any

import pandas as pd

from databento import Reference


if __name__ == "__main__":
    key = "YOUR_API_KEY"
    client = Reference(key=key)

    response: pd.DataFrame = client.corporate_actions.get_range(
        symbols="AAPL,MSFT,TSLA",
        stype_in="raw_symbol",
        start="2023",
        end="2024-04",
        events="DIV,LIQ",
        countries="US",
    )

    pprint(response.head())

    event_docs: dict[str, dict[str, Any]] = client.corporate_actions.list_events()
    pprint(event_docs)

    enum_docs: dict[str, list[Any]] = client.corporate_actions.list_enums()
    pprint(enum_docs)
