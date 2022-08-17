from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from databento.common.enums import Dataset, Encoding, FeedMode, Schema, SType
from databento.common.parsing import (
    enum_or_str_lowercase,
    maybe_date_to_string,
    maybe_datetime_to_string,
    maybe_enum_or_str_lowercase,
    maybe_symbols_list_to_string,
    values_list_to_string,
)
from databento.common.validation import validate_enum, validate_maybe_enum
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI
from requests import Response


class MetadataHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the metadata HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/metadata"

    def list_datasets(
        self,
        start_date: Optional[Union[date, str]] = None,
        end_date: Optional[Union[date, str]] = None,
    ) -> List[str]:
        """
        List all available datasets from Databento.

        Makes a `GET /metadata.list_datasets` HTTP request.

        Parameters
        ----------
        start_date : date or str, optional
            The start date (UTC) for the request range.
        end_date : date or str, optional
            The end date (UTC) for the request range.

        Returns
        -------
        List[str]

        Notes
        -----
        We use dataset names in the form `PUBLISHER.DATASET` for each dataset,
        where each publisher is represented by a 4 character code, which is
        usually its ISO 10383 [Market Identifier Code (MIC)](https://www.iso20022.org/market-identifier-codes).  # noqa
        These dataset names are also found on the [Explore]() and [Quick Download]()
        features of the Databento user portal.

        Use this method to list the _names_ of all available datasets, so you can use
        other methods which take the `dataset` parameter.

        """
        start_date = maybe_date_to_string(start_date)
        end_date = maybe_date_to_string(end_date)

        params: List[Tuple[str, str]] = [
            ("start_date", start_date),
            ("end_date", end_date),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_datasets",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_schemas(
        self,
        dataset: Union[Dataset, str],
        start_date: Optional[Union[date, str]] = None,
        end_date: Optional[Union[date, str]] = None,
    ) -> List[str]:
        """
        List all available data schemas from Databento.

        Makes a `GET /metadata.list_schemas` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        start_date : date or str, optional
            The start date (UTC) for the request range.
        end_date : date or str, optional
            The end date (UTC) for the request range.

        Returns
        -------
        List[str]

        """
        dataset = enum_or_str_lowercase(dataset, "dataset")
        start_date = maybe_date_to_string(start_date)
        end_date = maybe_date_to_string(end_date)

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("start_date", start_date),
            ("end_date", end_date),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_schemas",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_fields(
        self,
        dataset: Union[Dataset, str],
        schema: Optional[Union[Schema, str]] = None,
        encoding: Optional[Union[Encoding, str]] = None,
    ) -> Dict[str, Dict]:
        """
        List all fields for a dataset, schema and encoding from Databento.

        Makes a `GET /metadata.list_fields` HTTP request.

        The `schema` and `encoding` parameters act as optional filters. All
        metadata for that parameter is returned if they are not specified.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, optional  # noqa
            The data record schema for the request.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, optional
            The data encoding.

        Returns
        -------
        Dict[str, Any]
            A mapping of dataset to encoding to schema to field to data type.

        """
        validate_maybe_enum(schema, Schema, "schema")
        validate_maybe_enum(encoding, Encoding, "encoding")

        dataset = enum_or_str_lowercase(dataset, "dataset")
        schema = maybe_enum_or_str_lowercase(schema, "schema")
        encoding = maybe_enum_or_str_lowercase(encoding, "encoding")

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("schema", schema),
            ("encoding", encoding),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_fields",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def list_encodings(self) -> List[str]:
        """
        List all available data encodings from Databento.

        Makes a `GET /metadata.list_encodings` HTTP request.

        Returns
        -------
        List[str]

        """
        response: Response = self._get(
            url=self._base_url + ".list_encodings",
            basic_auth=True,
        )
        return response.json()

    def list_compressions(self) -> List[str]:
        """
        List all available data compression modes from Databento.

        Makes a `GET /metadata.list_compressions` HTTP request.

        Returns
        -------
        List[str]

        """
        response: Response = self._get(
            url=self._base_url + ".list_compressions",
            basic_auth=True,
        )
        return response.json()

    def list_unit_prices(
        self,
        dataset: Union[Dataset, str],
        mode: Optional[Union[FeedMode, str]] = None,
        schema: Optional[Union[Schema, str]] = None,
    ) -> Dict[str, Any]:
        """
        List data schema prices per GB unit from Databento.

        Makes a `GET /metadata.list_unit_prices` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}, optional
            The data feed mode for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, optional  # noqa
            The data record schema for the request.

        Returns
        -------
        Dict[str, Any]
            A map of dataset to feed mode to unit price.

        """
        validate_maybe_enum(schema, Schema, "schema")
        validate_maybe_enum(mode, FeedMode, "mode")

        dataset = enum_or_str_lowercase(dataset, "dataset")
        mode = maybe_enum_or_str_lowercase(mode, "mode")
        schema = maybe_enum_or_str_lowercase(schema, "schema")

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("mode", mode),
            ("schema", schema),
        ]

        response: Response = self._get(
            url=self._base_url + ".list_unit_prices",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def get_shape(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "dbz",
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> Tuple:
        """
        Get the shape of the time series data query.

        Makes a GET `/metadata.get_shape` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, optional
            The data encoding.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to include in the query.
            If ``None`` is passed then no limit.

        Returns
        -------
        Tuple
            The shape of the data expressed as size per dimension.

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(stype_in, SType, "stype_in")

        dataset = enum_or_str_lowercase(dataset, "dataset")
        symbols = maybe_symbols_list_to_string(symbols)
        schema = Schema(schema)
        start = maybe_datetime_to_string(start)
        end = maybe_datetime_to_string(end)
        encoding = Encoding(encoding)
        stype_in = SType(stype_in)

        # Build params list
        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("symbols", symbols),
            ("schema", schema.value),
            ("start", start),
            ("end", end),
            ("encoding", encoding.value),
            ("stype_in", stype_in.value),
        ]
        if limit is not None:
            params.append(("limit", str(limit)))

        response: Response = self._get(
            url=self._base_url + ".get_shape",
            params=params,
            basic_auth=True,
        )

        values = response.json()
        return tuple(values)

    def get_billable_size(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> int:
        """
        Get the raw uncompressed binary size of a historical streaming or batch
        data request, which would be used for billing.

        Makes a GET `/metadata.get_billable_size` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to include in the query.
            If ``None`` is passed then no limit.

        Returns
        -------
        int
            The size in number of bytes used for billing.

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(stype_in, SType, "stype_in")

        schema = Schema(schema)
        stype_in = SType(stype_in)

        params: List[Tuple[str, str]] = super()._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=Schema(schema),
            start=start,
            end=end,
            stype_in=SType(stype_in),
            limit=limit,
        )

        response: Response = self._get(
            url=self._base_url + ".get_billable_size",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def get_cost(
        self,
        dataset: Union[Dataset, str],
        mode: Union[FeedMode, str] = "historical-streaming",
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> float:
        """
        Get cost in US Dollars for a historical streaming or batch data request.

        Makes a `GET /metadata.get_cost` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}, default 'historical-streaming'
            The data feed mode for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range.
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        limit : int, optional
            The maximum number of records to include in the query.
            If ``None`` is passed then no limit.

        Returns
        -------
        float
            The cost in US Dollars.

        """
        validate_enum(mode, FeedMode, "mode")
        validate_enum(schema, Schema, "schema")
        validate_enum(stype_in, SType, "stype_in")

        mode = FeedMode(mode)
        schema = Schema(schema)
        stype_in = SType(stype_in)

        params: List[Tuple[str, str]] = super()._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            stype_in=stype_in,
            limit=limit,
        )

        params.append(("mode", mode.value))

        response: Response = self._get(
            url=self._base_url + ".get_cost",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def get_license_fee(
        self,
        dataset: Union[Dataset, str],
        purposes: Union[List[str], str],
    ) -> int:
        """
        Get the license fee in US Dollars for a dataset and access purposes.

        Makes a `GET /metadata.get_license_fee` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        purposes : List[str] or str, {'professional trading', 'display usage', 'non-display usage', 'external redistribution', 'internal redistribution'}  # noqa
            The licensing purposes.

        Returns
        -------
        int
            The license fee in US Dollars.

        """
        dataset = enum_or_str_lowercase(dataset, "dataset")
        purposes = values_list_to_string(purposes)

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("purposes", purposes),
        ]

        response: Response = self._get(
            url=self._base_url + ".get_license_fee",
            params=params,
            basic_auth=True,
        )

        return response.json()
