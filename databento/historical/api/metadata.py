from datetime import date
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from databento.common.enums import (
    Compression,
    Dataset,
    Encoding,
    FeedMode,
    Schema,
    SType,
)
from databento.common.parsing import enum_or_str_lowercase, maybe_datetime_to_string
from databento.common.validation import validate_enum
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
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "csv",
    ) -> Dict[str, Dict]:
        """
        Request the data record fields from the API server.

        `GET /v1/metadata.list_fields` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-5', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'status'}, optional  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'bin', 'csv', 'json'}, default 'csv'
            The data output encoding.

        Returns
        -------
        Dict[str, Dict]

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")

        dataset = enum_or_str_lowercase(dataset, "dataset")
        schema = enum_or_str_lowercase(schema, "schema")
        start = maybe_datetime_to_string(start)
        end = maybe_datetime_to_string(end)
        encoding = enum_or_str_lowercase(encoding, "encoding")

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("schema", schema),
            ("start", start),
            ("end", end),
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

    def get_unit_price(
        self,
        dataset: Union[Dataset, str],
        schema: Union[Schema, str],
        mode: Union[FeedMode, str],
    ) -> float:
        """
        Request the data price per unit GB from the API server.

        `GET /v1/metadata.unit_price` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-5', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        mode : FeedMode or str {'live', 'historical-streaming', 'historical'}
            The data feed mode for the request.

        Returns
        -------
        float

        """
        validate_enum(schema, Schema, "schema")
        validate_enum(mode, FeedMode, "mode")

        dataset = enum_or_str_lowercase(dataset, "dataset")
        schema = enum_or_str_lowercase(schema, "schema")
        mode = enum_or_str_lowercase(mode, "mode")

        params: List[Tuple[str, str]] = [
            ("dataset", dataset),
            ("schema", schema),
            ("mode", mode),
        ]

        response: Response = self._get(
            url=self._base_url + ".unit_price",
            params=params,
            basic_auth=True,
        )
        return response.json()

    def size(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "bin",
        compression: Optional[Union[Compression, str]] = "zstd",
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> int:
        """
        Request the expected size of the data stream.

        GET `/v1/metadata.size` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-5', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'bin', 'csv', 'json'}, default 'bin'
            The data output encoding.
        compression : Compression or str {'none', 'zstd'}, default 'zstd'
            The compression mode for the request.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        limit : int, optional
            The maximum number of records for the request.

        Returns
        -------
        int
            The uncompressed size of the data in bytes.

        """
        if compression is None:
            compression = Compression.NONE

        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")
        validate_enum(compression, Compression, "compression")
        validate_enum(stype_in, SType, "stype_in")

        schema = Schema(schema)
        encoding = Encoding(encoding)
        compression = Compression(compression)
        stype_in = SType(stype_in)

        params: List[Tuple[str, str]] = super()._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=Schema(schema),
            start=start,
            end=end,
            encoding=Encoding(encoding),
            compression=Compression(compression),
            stype_in=SType(stype_in),
            limit=limit,
        )

        response: Response = self._get(
            url=self._base_url + ".size",
            params=params,
            basic_auth=True,
        )

        return response.json()

    def cost(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "bin",
        compression: Optional[Union[Compression, str]] = "zstd",
        stype_in: Optional[Union[SType, str]] = "native",
        limit: Optional[int] = None,
    ) -> float:
        """
        Request the expected total cost of the data stream.

        `GET /v1/metadata.cost` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-5', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime for the request range (UTC).
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'bin', 'csv', 'json'}, default 'bin'
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

        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")
        validate_enum(compression, Compression, "compression")
        validate_enum(stype_in, SType, "stype_in")

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

        response: Response = self._get(
            url=self._base_url + ".cost",
            params=params,
            basic_auth=True,
        )

        return response.json()
