from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from datetime import datetime

import pandas as pd
from databento_dbn import Compression
from databento_dbn import SType

from databento.common import API_VERSION
from databento.common.constants import ADJUSTMENT_FACTORS_DATE_COLUMNS
from databento.common.constants import ADJUSTMENT_FACTORS_DATETIME_COLUMNS
from databento.common.http import BentoHttpAPI
from databento.common.parsing import convert_date_columns
from databento.common.parsing import convert_datetime_columns
from databento.common.parsing import convert_jsonl_to_df
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_string_to_list
from databento.common.parsing import optional_symbols_list_to_list


class AdjustmentFactorsHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the adjustment factors HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/adjustment_factors"

    def get_range(
        self,
        start: pd.Timestamp | datetime | date | str | int,
        end: pd.Timestamp | datetime | date | str | int | None = None,
        symbols: Iterable[str] | str | None = None,
        stype_in: SType | str = "raw_symbol",
        countries: Iterable[str] | str | None = None,
        security_types: Iterable[str] | str | None = None,
    ) -> pd.DataFrame:
        """
        Request a new adjustment factors time series from Databento.

        Makes a `POST /adjustment_factors.get_range` HTTP request.

        The `ex_date` column will be used to filter the time range and order the records.
        It will also be set as the index of the resulting data frame.

        Parameters
        ----------
        start : pd.Timestamp, datetime, date, str, or int
            The start datetime of the request time range (inclusive) based on `ex_date`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp, datetime, date, str, or int, optional
            The end datetime of the request time range (exclusive) based on `ex_date`.
            Assumes UTC as timezone unless passed a tz-aware object.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Defaults to the forward filled value of `start` based on the resolution provided.
        symbols : Iterable[str] or str, optional
            The symbols to filter for. Takes up to 2,000 symbols per request.
            If more than 1 symbol is specified, the data is merged and sorted by time.
            If 'ALL_SYMBOLS' or `None` then will select **all** symbols.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
            Use any of 'raw_symbol', 'nasdaq_symbol', 'isin', 'us_code'.
        countries : Iterable[str] or str, optional
            The listing countries to filter for.
            Takes any number of two letter ISO 3166-1 alpha-2 country codes per request.
            If not specified then will select **all** listing countries by default.
            See [CNTRY](https://databento.com/docs/standards-and-conventions/reference-data-enums#cntry) enum.
        security_types : Iterable[str] or str, optional
            The security types to filter for.
            Takes any number of security types per request.
            If not specified then will select **all** security types by default.
            See [SECTYPE](https://databento.com/docs/standards-and-conventions/reference-data-enums#sectype) enum.

        Returns
        -------
        pandas.DataFrame
            The data converted into a data frame.

        """
        symbols_list = optional_symbols_list_to_list(symbols, SType.RAW_SYMBOL)
        countries = optional_string_to_list(countries)
        security_types = optional_string_to_list(security_types)

        data: dict[str, object | None] = {
            "start": datetime_to_string(start),
            "end": optional_datetime_to_string(end),
            "symbols": ",".join(symbols_list),
            "stype_in": stype_in,
            "countries": ",".join(countries) if countries else None,
            "security_types": ",".join(security_types) if security_types else None,
            "compression": str(Compression.ZSTD),  # Always request zstd
        }

        response = self._post(
            url=self._base_url + ".get_range",
            data=data,
            basic_auth=True,
        )

        df = convert_jsonl_to_df(response.content, compressed=True)
        if df.empty:
            return df

        convert_datetime_columns(df, ADJUSTMENT_FACTORS_DATETIME_COLUMNS)
        convert_date_columns(df, ADJUSTMENT_FACTORS_DATE_COLUMNS)

        df.set_index("ex_date", inplace=True)
        df.sort_index(inplace=True)

        return df
