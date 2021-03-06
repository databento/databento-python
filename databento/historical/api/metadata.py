from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from databento.common.enums import (
    Compression,
    Dataset,
    Encoding,
    FeedMode,
    Schema,
    SType,
)
from databento.common.parsing import (
    enum_or_str_lowercase,
    maybe_datetime_to_string,
    maybe_enum_or_str_lowercase,
    maybe_symbols_list_to_string,
)
from databento.common.validation import validate_enum, validate_maybe_enum
from databento.historical.http import BentoHttpAPI
from requests import Response


class MetadataHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the metadata HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + "/v1/metadata"

    def list_datasets(
        self,
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
    ) -> List[str]:
        """
        Request the available datasets from the API server.

        `GET /v1/metadata.list_datasets` HTTP API endpoint.

        Parameters
        ----------
        start : pd.Timestamp or date or str or int, optional
            The UTC start datetime for the request range.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The UTC end datetime for the request range.
            If using an integer then this represents nanoseconds since UNIX epoch.

        Returns
        -------
        List[str]

        """
        start = maybe_datetime_to_string(start)
        end = maybe_datetime_to_string(end)

        params: List[Tuple[str, str]] = [
            ("start", start),
            ("end", end),
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
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
    ) -> List[str]:
        """
        Request the available record schemas from the API server.

        `GET /v1/metadata.list_schemas` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.

        Returns
        -------
        List[str]

        """
        dataset = enum_or_str_lowercase(dataset, "dataset")
        start = maybe_datetime_to_string(start)
        end = maybe_datetime_to_string(end)

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("start", start),
            ("end", end),
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
        Request the data record fields from the API server.

        `GET /v1/metadata.list_fields` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, optional  # noqa
            The data record schema for the request.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, optional
            The data output encoding.

        Returns
        -------
        Dict[str, Any]
            A map of field name-value pairs filtered on the given optional parameters.

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
        Request the available encoding options from the API server.

        `GET /v1/metadata.list_encodings` HTTP API endpoint.

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
        Request the available compression options from the API server.

        `GET /v1/metadata.list_compressions` HTTP API endpoint.

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
    ) -> Union[Dict[str, Any], float]:
        """
        Request the data prices per unit GB from the API server.

        `GET /v1/metadata.list_unit_prices` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}, optional
            The data feed mode for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, optional  # noqa
            The data record schema for the request.

        Returns
        -------
        Dict[str, Any] or float
            A map of unit prices filtered on the given optional parameters.

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
    ) -> Tuple[int, int]:
        """
        Request the shape of the timeseries data as a rows and columns tuple.

        GET `/v1/metadata.get_shape` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, optional
            The data output encoding.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        limit : int, optional
            The maximum number of records for the request.

        Returns
        -------
        Tuple[int, int]

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
        return values[0], values[1]

    def get_billable_size(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "dbz",
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> int:
        """
        Request the uncompressed binary size of the data stream (used for billing).

        GET `/v1/metadata.get_billable_size` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, default 'bin'
            The data output encoding.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        limit : int, optional
            The maximum number of records for the request.

        Returns
        -------
        int
            The uncompressed size of the data in bytes.

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")
        validate_enum(stype_in, SType, "stype_in")

        schema = Schema(schema)
        encoding = Encoding(encoding)
        stype_in = SType(stype_in)

        params: List[Tuple[str, str]] = super()._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=Schema(schema),
            start=start,
            end=end,
            encoding=Encoding(encoding),
            compression=Compression.NONE,
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
        encoding: Union[Encoding, str] = "dbz",
        compression: Optional[Union[Compression, str]] = "zstd",
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> float:
        """
        Request the expected total cost of the data stream.

        `GET /v1/metadata.get_cost` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}, default 'historical-streaming'
            The data feed mode for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, default 'dbz'
            The data output encoding.
        compression : Compression or str {'none', 'zstd'}, default 'zstd'
            The compression mode for the request.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        limit : int, optional
            The maximum number of records for the request.

        Returns
        -------
        float
            The cost for the data in US Dollars.

        """
        if compression is None:
            compression = Compression.NONE

        validate_enum(mode, FeedMode, "mode")
        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")
        validate_enum(compression, Compression, "compression")
        validate_enum(stype_in, SType, "stype_in")

        mode = FeedMode(mode)
        schema = Schema(schema)
        encoding = Encoding(encoding)
        compression = Compression(compression)
        stype_in = SType(stype_in)

        params: List[Tuple[str, str]] = super()._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            encoding=encoding,
            compression=compression,
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
