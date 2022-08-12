from datetime import date
from typing import List, Optional, Tuple, Union

import pandas as pd
from databento.common.enums import SType
from databento.common.parsing import enum_or_str_lowercase, maybe_symbols_list_to_string
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI
from requests import Response


class SymbologyHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the symbology HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/symbology"

    def resolve(
        self,
        dataset: str,
        symbols: Union[List[str], str],
        stype_in: Union[SType, str],
        stype_out: Union[SType, str],
        start: Union[date, str],
        end: Union[date, str],
        default_value: Optional[str] = "",
    ):
        """
        Request symbology resolution.

        `GET /v0/symbology.resolve` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols to resolve.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbol type to resolve to.
        start : pd.Timestamp or date or str or int
            The UTC start of the time range (inclusive) to resolve.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int
            The UTC end of the time range (exclusive) to resolve.
            If using an integer then this represents nanoseconds since UNIX epoch.
        default_value : str, default '' (empty string)
            The default value to return if a symbol cannot be resolved.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            A map of input symbol to output symbol across the date range.

        """
        dataset = enum_or_str_lowercase(dataset, "dataset")
        symbols = maybe_symbols_list_to_string(symbols)
        stype_in = enum_or_str_lowercase(stype_in, "stype_in")
        stype_out = enum_or_str_lowercase(stype_out, "stype_out")
        start = str(pd.to_datetime(start).date())
        end = str(pd.to_datetime(end).date())

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("symbols", symbols),
            ("stype_in", stype_in),
            ("stype_out", stype_out),
            ("start", start),
            ("end", end),
            ("default_value", default_value),
        ]

        response: Response = self._get(
            url=self._base_url + ".resolve",
            params=params,
            basic_auth=True,
        )

        return response.json()
