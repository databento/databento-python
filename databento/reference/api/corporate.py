from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from io import StringIO

import pandas as pd
from databento_dbn import SType

from databento.common import API_VERSION
from databento.common.constants import CORPORATE_ACTIONS_DATE_COLUMNS
from databento.common.constants import CORPORATE_ACTIONS_DATETIME_COLUMNS
from databento.common.http import BentoHttpAPI
from databento.common.parsing import convert_date_columns
from databento.common.parsing import convert_datetime_columns
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_symbols_list_to_list


class CorporateActionsHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the corporate actions HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/corporate_actions"

    def get_range(
        self,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        symbols: Iterable[str] | str | None = None,
        stype_in: SType | str = "raw_symbol",
        events: Iterable[str] | str | None = None,
        us_only: bool = False,
    ) -> pd.DataFrame:
        """
        Request a new corporate actions time series from Databento.

        Makes a `POST /corporate_actions.get_range` HTTP request.

        Parameters
        ----------
        start : pd.Timestamp or date or str or int
            The start datetime of the request time range (inclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime of the request time range (exclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Values are forward filled based on the resolution provided.
            Defaults to the same value as `start`.
        symbols : Iterable[str] or str, optional
            The symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
            Use any of 'raw_symbol', 'nasdaq_symbol', 'isin', 'us_code',
            'bbg_comp_id', 'bbg_comp_ticker', 'figi', 'figi_ticker'.
        events : Iterable[str] or str, optional
            The event types to filter for.
            Takes any number of event types per request.
            If not specified then will be for **all** event types.
            See [EVENT](https://databento.com/docs/standards-and-conventions/reference-data-enums#event) enum.
        us_only : bool, default False
            If filtering for US markets only.

        Returns
        -------
        pandas.DataFrame
            The data converted into a data frame.

        """
        symbols_list = optional_symbols_list_to_list(symbols, SType.RAW_SYMBOL)

        if isinstance(events, str):
            events = events.strip().strip(",").split(",")

        data: dict[str, object | None] = {
            "start": datetime_to_string(start),
            "end": optional_datetime_to_string(end),
            "symbols": ",".join(symbols_list),
            "stype_in": stype_in,
            "events": ",".join(events) if events else None,
            "us_only": us_only,
        }

        response = self._post(
            url=self._base_url + ".get_range",
            data=data,
            basic_auth=True,
        )

        df = pd.read_json(StringIO(response.text), lines=True)
        convert_datetime_columns(df, CORPORATE_ACTIONS_DATETIME_COLUMNS)
        convert_date_columns(df, CORPORATE_ACTIONS_DATE_COLUMNS)

        return df
