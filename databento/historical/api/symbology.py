from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from databento_dbn import SType
from requests import Response

from databento.common import API_VERSION
from databento.common.http import BentoHttpAPI
from databento.common.parsing import date_to_string
from databento.common.parsing import optional_date_to_string
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.publishers import Dataset
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string


class SymbologyHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the symbology HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/symbology"

    def resolve(
        self,
        dataset: Dataset | str,
        symbols: Iterable[str | int] | str | int,
        stype_in: SType | str,
        stype_out: SType | str,
        start_date: date | str,
        end_date: date | str | None = None,
    ) -> dict[str, Any]:
        """
        Request symbology mappings resolution from Databento.

        Makes a `POST /symbology.resolve` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
        symbols : Iterable[str | int] or str or int
            The symbols to resolve. Takes up to 2,000 symbols per request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'instrument_id'
            The output symbology type to resolve to.
        start_date : date or str
            The inclusive UTC start date of the request range.
        end_date : date or str, optional
            The exclusive UTC end date of the request range.
            Defaults to the forward filled value of `start` based on the resolution provided.

        Returns
        -------
        dict[str, Any]
            A result including a map of input symbol to output symbol across a
            date range.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        data: dict[str, object | None] = {
            "dataset": validate_semantic_string(dataset, "dataset"),
            "symbols": ",".join(symbols_list),
            "stype_in": str(stype_in_valid),
            "stype_out": str(validate_enum(stype_out, SType, "stype_out")),
            "start_date": date_to_string(start_date),
            "end_date": optional_date_to_string(end_date),
        }

        response: Response = self._post(
            url=self._base_url + ".resolve",
            data=data,
            basic_auth=True,
        )

        return response.json()
