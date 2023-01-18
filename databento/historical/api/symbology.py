from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

from databento.common.enums import SType
from databento.common.parsing import (
    datetime_to_date_string,
    optional_symbols_list_to_string,
)
from databento.common.validation import validate_enum
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI
from requests import Response


class SymbologyHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the symbology HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/symbology"

    def resolve(
        self,
        dataset: str,
        symbols: Union[List[str], str],
        stype_in: Union[SType, str],
        stype_out: Union[SType, str],
        start_date: Union[date, str],
        end_date: Union[date, str],
        default_value: Optional[str] = "",
    ) -> Dict[str, Any]:
        """
        Request symbology mappings resolution from Databento.

        Makes a `GET /symbology.resolve` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols to resolve. Takes up to 2,000 symbols per request.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbology type to resolve to.
        start_date : date or str
            The start date (UTC) of the request time range (inclusive).
        end_date : date or str
            The end date (UTC) of the request time range (exclusive).
        default_value : str, default '' (empty string)
            The default value to return if a symbol cannot be resolved.

        Returns
        -------
        Dict[str, Any]
            A result including a map of input symbol to output symbol across a
            date range.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_string(symbols, stype_in_valid)
        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("symbols", symbols_list),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(validate_enum(stype_out, SType, "stype_out"))),
            ("start_date", datetime_to_date_string(start_date)),
            ("end_date", datetime_to_date_string(end_date)),
            ("default_value", default_value),
        ]

        response: Response = self._get(
            url=self._base_url + ".resolve",
            params=params,
            basic_auth=True,
        )

        return response.json()
