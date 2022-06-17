from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from databento.common.enums import (
    Compression,
    Dataset,
    Delivery,
    Duration,
    Encoding,
    Packaging,
    Schema,
    SType,
)
from databento.common.parsing import maybe_datetime_to_string
from databento.common.validation import validate_enum
from databento.historical.http import BentoHttpAPI


class BatchHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the batch HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + "/v1/batch"

    def timeseries_submit(
        self,
        dataset: Union[Dataset, str],
        symbols: Optional[Union[List[str], str]] = None,
        schema: Union[Schema, str] = "trades",
        start: Optional[Union[pd.Timestamp, date, str, int]] = None,
        end: Optional[Union[pd.Timestamp, date, str, int]] = None,
        encoding: Union[Encoding, str] = "dbz",
        compression: Optional[Union[Compression, str]] = "zstd",
        split_duration: Union[Duration, str] = "day",
        split_size: Optional[int] = None,
        packaging: Union[Packaging, str] = "none",
        delivery: Union[Delivery, str] = "download",
        stype_in: Union[SType, str] = "native",
        stype_out: Union[SType, str] = "product_id",
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Submit a timeseries batch data job to the Databento backend.

        `POST /v1/batch.timeseries_submit` HTTP API endpoint.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset ID for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-5', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The UTC start of the time range (inclusive) for the request.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The UTC end of the time range (exclusive) for the request.
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, default 'dbz'
            The data output encoding.
        compression : Compression or str {'none', 'zstd'}, default 'zstd'
            The data output compression.
        split_duration : Duration or str {'day', 'week', 'month', 'none'}, default 'day'
            The time duration split per data file ('week' starts on Sunday UTC).
        split_size : int, optional
            The maximum size of each data file on disk before being split.
        packaging : Packaging or str {'none', 'zip', 'tar'}, default 'none'
            The packaging method for batch data files.
        delivery : Delivery or str {'download', 's3', 'disk'}, default 'download'
            The batch data delivery mechanism.
        stype_in : SType or str, default 'native'
            The input symbol type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbol type to resolve to.
        limit : int, optional
            The maximum number of records for the request.

        Returns
        -------
        Dict[str, Any]
            The job info for submitted batch data request.

        """
        if compression is None:
            compression = Compression.NONE

        validate_enum(schema, Schema, "schema")
        validate_enum(encoding, Encoding, "encoding")
        validate_enum(compression, Compression, "compression")
        validate_enum(split_duration, Duration, "duration")
        validate_enum(packaging, Packaging, "packaging")
        validate_enum(delivery, Delivery, "delivery")
        validate_enum(stype_in, SType, "stype_in")
        validate_enum(stype_out, SType, "stype_out")

        schema = Schema(schema)
        encoding = Encoding(encoding)
        compression = Compression(compression)
        split_duration = Duration(split_duration)
        packaging = Packaging(packaging)
        delivery = Delivery(delivery)
        stype_in = SType(stype_in)
        stype_out = SType(stype_out)

        params: List[Tuple[str, str]] = BentoHttpAPI._timeseries_params(
            dataset=dataset,
            symbols=symbols,
            schema=schema,
            start=start,
            end=end,
            limit=limit,
            encoding=encoding,
            compression=compression,
            stype_in=stype_in,
            stype_out=stype_out,
        )

        params.append(("split_duration", split_duration.value))
        params.append(("packaging", packaging.value))
        params.append(("delivery", delivery.value))
        if split_size is not None:
            params.append(("split_size", str(split_size)))

        return self._post(
            url=self._base_url + ".timeseries_submit",
            params=params,
            basic_auth=True,
        ).json()

    def list_jobs(
        self,
        states: Optional[str] = "queued,processing,done",
        since: Optional[Union[pd.Timestamp, date, str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Request all batch data jobs associated with the client access key with
        the optional query filters.

        `GET /v1/batch.list_jobs` HTTP API endpoint.

        Parameters
        ----------
        states : str, optional {'queued', 'processing', 'done', 'expired'}
            The comma separated job states to include in the response. If ``None``
            will default to 'queued,processing,done' (may contain whitespace).
        since : pd.Timestamp or date or str or int, optional
            The datetime to filter from.

        Returns
        -------
        List[Dict[str, Any]]
            The matching jobs sorted in order of timestamp received.

        """
        params: List[Tuple[str, str]] = [
            ("states", states),
            ("since", maybe_datetime_to_string(since)),
        ]

        return self._get(
            url=self._base_url + ".list_jobs",
            params=params,
            basic_auth=True,
        ).json()
