from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
from databento_dbn import Encoding
from databento_dbn import Schema
from databento_dbn import SType
from requests import Response

from databento.common.enums import FeedMode
from databento.common.parsing import datetime_to_string
from databento.common.parsing import optional_date_to_string
from databento.common.parsing import optional_datetime_to_string
from databento.common.parsing import optional_symbols_list_to_list
from databento.common.publishers import Dataset
from databento.common.validation import validate_enum
from databento.common.validation import validate_semantic_string
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI


class MetadataHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the metadata HTTP API endpoints.
    """

    def __init__(self, key: str, gateway: str) -> None:
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/metadata"

    def list_publishers(self) -> list[dict[str, Any]]:
        """
        Request all publishers from Databento.

        Makes a `GET /metadata.list_publishers` HTTP request.

        Use this method to list the details of publishers, including their dataset and venue mappings.

        Returns
        -------
        list[dict[str, Any]]

        """
        response: Response = self._get(
            url=self._base_url + ".list_publishers",
            basic_auth=True,
        )
        return response.json()

    def list_datasets(
        self,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> list[str]:
        """
        Request all available dataset codes from Databento.

        Makes a `GET /metadata.list_datasets` HTTP request.

        Use this method to list the available dataset _codes (string identifiers), so you
        can use other methods which take the `dataset` parameter.

        Parameters
        ----------
        start_date : date or str, optional
            The start date (UTC) for the request range.
            If `None` then first date available.
        end_date : date or str, optional
            The end date (UTC) for the request range.
            If `None` then last date available.

        Returns
        -------
        list[str]

        """
        params: list[tuple[str, str | None]] = [
            ("start_date", optional_date_to_string(start_date)),
            ("end_date", optional_date_to_string(end_date)),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_datasets",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_schemas(self, dataset: Dataset | str) -> list[str]:
        """
        Request all available data schemas from Databento.

        Makes a `GET /metadata.list_schemas` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.

        Returns
        -------
        list[str]

        """
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_schemas",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_fields(
        self,
        schema: Schema | str,
        encoding: Encoding | str,
    ) -> list[dict[str, Any]]:
        """
        List all fields for a particular schema and encoding from Databento.

        Makes a `GET /metadata.list_fields` HTTP request.

        Parameters
        ----------
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'},
            The data record schema for the request.
        encoding : Encoding or str {'dbn', 'csv', 'json'}
            The data encoding.

        Returns
        -------
        list[dict[str, Any]]
            A list of field details.

        """
        params: list[tuple[str, str | Any]] = [
            ("schema", validate_enum(schema, Schema, "schema")),
            ("encoding", validate_enum(encoding, Encoding, "encoding")),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_fields",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_unit_prices(
        self,
        dataset: Dataset | str,
    ) -> list[dict[str, Any]]:
        """
        List unit prices for each feed mode and data schema in US dollars per
        gigabyte.

        Makes a `GET /metadata.list_unit_prices` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code for the request.

        Returns
        -------
        list[dict[str, Any]]
            A list of maps of feed mode to schema to unit price.

        """
        params: list[tuple[str, Dataset | str]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_unit_prices",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def get_dataset_condition(
        self,
        dataset: Dataset | str,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> dict[str, Any]:
        """
        Get the per date dataset conditions from Databento.

        Makes a `GET /metadata.get_dataset_condition` HTTP request.

        Use this method to discover data availability and quality.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code (string identifier) for the request.
        start_date : date or str, optional
            The start date (UTC) for the request range.
            If `None` then first date available.
        end_date : date or str, optional
            The end date (UTC) for the request range.
            If `None` then last date available.

        Returns
        -------
        dict[str, Any]

        """
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("start_date", optional_date_to_string(start_date)),
            ("end_date", optional_date_to_string(end_date)),
        ]

        response: Response = self._get(
            url=self._base_url + ".get_dataset_condition",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def get_dataset_range(
        self,
        dataset: Dataset | str,
    ) -> dict[str, str]:
        """
        Request the available range for the dataset from Databento.

        Makes a GET `/metadata.get_dataset_range` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code for the request.

        Returns
        -------
        dict[str, str]
            The available range for the dataset.

        """
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
        ]

        response: Response = self._get(
            url=self._base_url + ".get_dataset_range",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def get_record_count(
        self,
        dataset: Dataset | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        symbols: list[str] | str | None = None,
        schema: Schema | str = "trades",
        stype_in: SType | str = "raw_symbol",
        limit: int | None = None,
    ) -> int:
        """
        Request the count of data records from Databento.

        Makes a GET `/metadata.get_record_count` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code for the request.
        start : pd.Timestamp or date or str or int
            The start datetime for the request range (inclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (exclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Values are forward filled based on the resolution provided.
            Defaults to the same value as `start`.
        symbols : list[str | int] or str, optional
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.

        Returns
        -------
        int
            The count of records.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("symbols", ",".join(symbols_list)),
            ("schema", str(validate_enum(schema, Schema, "schema"))),
            ("start", optional_datetime_to_string(start)),
            ("end", optional_datetime_to_string(end)),
            ("stype_in", str(stype_in_valid)),
        ]

        # Optional Parameters
        if limit is not None:
            params.append(("limit", str(limit)))

        response: Response = self._get(
            url=self._base_url + ".get_record_count",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def get_billable_size(
        self,
        dataset: Dataset | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        symbols: list[str] | str | None = None,
        schema: Schema | str = "trades",
        stype_in: SType | str = "raw_symbol",
        limit: int | None = None,
    ) -> int:
        """
        Request the billable uncompressed raw binary size for historical
        streaming or batched files from Databento.

        Makes a GET `/metadata.get_billable_size` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code for the request.
        start : pd.Timestamp or date or str or int
            The start datetime for the request range (inclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (exclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Values are forward filled based on the resolution provided.
            Defaults to the same value as `start`.
        symbols : list[str | int] or str, optional
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.

        Returns
        -------
        int
            The size in number of bytes used for billing.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("start", datetime_to_string(start)),
            ("end", optional_datetime_to_string(end)),
            ("symbols", ",".join(symbols_list)),
            ("schema", str(validate_enum(schema, Schema, "schema"))),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(SType.INSTRUMENT_ID)),
        ]

        if limit is not None:
            params.append(("limit", str(limit)))

        response: Response = self._get(
            url=self._base_url + ".get_billable_size",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def get_cost(
        self,
        dataset: Dataset | str,
        start: pd.Timestamp | date | str | int,
        end: pd.Timestamp | date | str | int | None = None,
        mode: FeedMode | str = "historical-streaming",
        symbols: list[str] | str | None = None,
        schema: Schema | str = "trades",
        stype_in: SType | str = "raw_symbol",
        limit: int | None = None,
    ) -> float:
        """
        Request the cost in US dollars for historical streaming or batched
        files from Databento.

        Makes a `GET /metadata.get_cost` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset code for the request.
        start : pd.Timestamp or date or str or int
            The start datetime for the request range (inclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (exclusive).
            Assumes UTC as timezone unless otherwise specified.
            If an integer is passed, then this represents nanoseconds since the UNIX epoch.
            Values are forward filled based on the resolution provided.
            Defaults to the same value as `start`.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}, default 'historical-streaming'
            The data feed mode for the request.
        symbols : list[str | int] or str, optional
            The instrument symbols to filter for. Takes up to 2,000 symbols per request.
            If 'ALL_SYMBOLS' or `None` then will be for **all** symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        stype_in : SType or str, default 'raw_symbol'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to return. If `None` then no limit.

        Returns
        -------
        float
            The cost in US dollars.

        """
        stype_in_valid = validate_enum(stype_in, SType, "stype_in")
        symbols_list = optional_symbols_list_to_list(symbols, stype_in_valid)
        params: list[tuple[str, str | None]] = [
            ("dataset", validate_semantic_string(dataset, "dataset")),
            ("start", datetime_to_string(start)),
            ("end", optional_datetime_to_string(end)),
            ("symbols", ",".join(symbols_list)),
            ("schema", str(validate_enum(schema, Schema, "schema"))),
            ("stype_in", str(stype_in_valid)),
            ("stype_out", str(SType.INSTRUMENT_ID)),
            ("mode", validate_enum(mode, FeedMode, "mode")),
        ]

        if limit is not None:
            params.append(("limit", str(limit)))

        response: Response = self._get(
            url=self._base_url + ".get_cost",
            params=params,
            basic_auth=True,
        )

        return response.json()
