from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from datetime import datetime

import pandas as pd
from databento_dbn import Compression
from databento_dbn import SType

from databento.common import API_VERSION
from databento.common.constants import CORPORATE_ACTIONS_DATE_COLUMNS
from databento.common.constants import CORPORATE_ACTIONS_DATETIME_COLUMNS
from databento.common.http import BentoHttpAPI
from databento.common.parsing import convert_date_columns
from databento.common.parsing import convert_datetime_columns
from databento.common.parsing import convert_jsonl_to_df
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_string_to_list
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
        start: pd.Timestamp | datetime | date | str | int,
        end: pd.Timestamp | datetime | date | str | int | None = None,
        index: str = "event_date",
        symbols: Iterable[str] | str | None = None,
        stype_in: SType | str = "raw_symbol",
        events: Iterable[str] | str | None = None,
        countries: Iterable[str] | str | None = None,
        exchanges: Iterable[str] | str | None = None,
        security_types: Iterable[str] | str | None = None,
        flatten: bool = True,
        pit: bool = False,
    ) -> pd.DataFrame:
        """
        Request a new corporate actions time series from Databento.

        Makes a `POST /corporate_actions.get_range` HTTP request.

        The specified `index` will be used to filter the time range and order the records.
        It will also be set as the index of the resulting data frame.

        Parameters
        ----------
        start : pd.Timestamp, datetime, date, str, or int
            The inclusive start of the request range based on `index`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp, datetime, date, str, or int, optional
            The exclusive end of the request range based on `index`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            If `None`, then will return **all** data available after `start`.
        index : str, default 'event_date'
            The index column used for filtering the `start` and `end` time range
            and for record ordering.
            Use any of 'event_date', 'ex_date' or 'ts_record'.
        symbols : Iterable[str] or str, optional
            The symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will select **all** symbols.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
            Use any of 'raw_symbol', 'nasdaq_symbol', 'isin', 'us_code',
            'bbg_comp_id', 'bbg_comp_ticker', 'figi', 'figi_ticker'.
        events : Iterable[str] or str, optional
            The event types to filter for.
            Takes any number of event types per request.
            If not specified then will select **all** event types by default.
            See [EVENT](https://databento.com/docs/standards-and-conventions/reference-data-enums#event) enum.
        countries : Iterable[str] or str, optional
            The listing countries to filter for.
            Takes any number of two letter ISO 3166-1 alpha-2 country codes per request.
            If not specified then will select **all** listing countries by default.
            See [CNTRY](https://databento.com/docs/standards-and-conventions/reference-data-enums#cntry) enum.
        exchanges : Iterable[str] or str, optional
            The (listing) exchanges to filter for.
            Takes any number of exchanges per request.
            If not specified then will select **all** exchanges by default.
            See [EXCHANGE](https://databento.com/docs/standards-and-conventions/reference-data-enums#exchange) enum.
        security_types : Iterable[str] or str, optional
            The security types to filter for.
            Takes any number of security types per request.
            If not specified then will select **all** security types by default.
            See [SECTYPE](https://databento.com/docs/standards-and-conventions/reference-data-enums#sectype) enum.
        flatten : bool, default True
            If nested JSON objects within the `date_info`, `rate_info`, and `event_info` fields
            should be flattened into separate columns in the resulting DataFrame.
        pit : bool, default False
            Determines whether to retain all historical records or only the latest records.
            If True, all historical records for each `event_unique_id` will be retained, preserving
            the complete point-in-time history.
            If False (default), the DataFrame will include only the most recent record for each
            `event_unique_id` based on the `ts_record` timestamp.

        Returns
        -------
        pandas.DataFrame
            The data converted into a data frame.

        """
        symbols_list = optional_symbols_list_to_list(symbols, SType.RAW_SYMBOL)
        events = optional_string_to_list(events)
        countries = optional_string_to_list(countries)
        exchanges = optional_string_to_list(exchanges)
        security_types = optional_string_to_list(security_types)

        data: dict[str, object | None] = {
            "start": datetime_to_string(start),
            "end": optional_datetime_to_string(end),
            "index": index,
            "symbols": ",".join(symbols_list),
            "stype_in": stype_in,
            "events": ",".join(events) if events else None,
            "countries": ",".join(countries) if countries else None,
            "security_types": ",".join(security_types) if security_types else None,
            "compression": str(Compression.ZSTD),  # Always request zstd
        }

        # Only add the `exchanges` param if it is supplied, for compatibility
        if exchanges:
            data["exchanges"] = ",".join(exchanges)

        response = self._post(
            url=self._base_url + ".get_range",
            data=data,
            basic_auth=True,
        )

        df = convert_jsonl_to_df(response.content, compressed=True)
        if df.empty:
            return df

        convert_datetime_columns(df, CORPORATE_ACTIONS_DATETIME_COLUMNS)
        convert_date_columns(df, CORPORATE_ACTIONS_DATE_COLUMNS)

        if flatten:
            # Normalize the dynamic JSON fields
            date_info_normalized = pd.json_normalize(df["date_info"]).set_index(df.index)
            rate_info_normalized = pd.json_normalize(df["rate_info"]).set_index(df.index)
            event_info_normalized = pd.json_normalize(df["event_info"]).set_index(df.index)

            # Merge normalized columns
            df = df.merge(date_info_normalized, left_index=True, right_index=True)
            df = df.merge(rate_info_normalized, left_index=True, right_index=True)
            df = df.merge(event_info_normalized, left_index=True, right_index=True)

            # Drop the original JSON columns
            df.drop(columns=["date_info", "rate_info", "event_info"], inplace=True)

        if pit:
            df.set_index(index, inplace=True)
            df.sort_index(inplace=True)
        else:
            # Filter for the latest record of each unique event
            df.sort_values("ts_record", inplace=True)
            df = df.groupby("event_unique_id").agg("last").reset_index()
            df.set_index(index, inplace=True)
            if index != "ts_record":
                df.sort_index(inplace=True)

        return df
