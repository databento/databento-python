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
from databento.common.parsing import (
    maybe_datetime_to_string,
    maybe_values_list_to_string,
)
from databento.common.validation import validate_enum
from databento.historical.api import API_VERSION
from databento.historical.http import BentoHttpAPI


class BatchHttpAPI(BentoHttpAPI):
    """
    Provides request methods for the batch HTTP API endpoints.
    """

    def __init__(self, key, gateway):
        super().__init__(key=key, gateway=gateway)
        self._base_url = gateway + f"/v{API_VERSION}/batch"

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
        Request a new time series batch data job from Databento.

        Makes a `POST /batch.timeseries_submit` HTTP request.

        Parameters
        ----------
        dataset : Dataset or str
            The dataset name for the request.
        symbols : List[Union[str, int]] or str, optional
            The symbols for the request. If ``None`` then will be for ALL symbols.
        schema : Schema or str {'mbo', 'mbp-1', 'mbp-10', 'trades', 'tbbo', 'ohlcv-1s', 'ohlcv-1m', 'ohlcv-1h', 'ohlcv-1d', 'definition', 'statistics', 'status'}, default 'trades'  # noqa
            The data record schema for the request.
        start : pd.Timestamp or date or str or int, optional
            The start datetime of the request time range (inclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        end : pd.Timestamp or date or str or int, optional
            The end datetime of the request time range (exclusive).
            Assumes UTC as timezone unless passed a tz-aware object.
            If using an integer then this represents nanoseconds since UNIX epoch.
        encoding : Encoding or str {'dbz', 'csv', 'json'}, default 'dbz'
            The data encoding.
        compression : Compression or str {'none', 'zstd'}, default 'zstd'
            The data compression mode.
        split_duration : Duration or str {'day', 'week', 'month', 'none'}, default 'day'
            The time duration split per data file ('week' starts on Sunday UTC).
        split_size : int, optional
            The maximum size of each data file on disk before being split.
        packaging : Packaging or str {'none', 'zip', 'tar'}, default 'none'
            The packaging method for batch data files.
        delivery : Delivery or str {'download', 's3', 'disk'}, default 'download'
            The batch data delivery mechanism.
        stype_in : SType or str, default 'native'
            The input symbology type to resolve from.
        stype_out : SType or str, default 'product_id'
            The output symbology type to resolve to.
        limit : int, optional
            The maximum number of records for the request.
            If ``None`` is passed then no limit.

        Returns
        -------
        Dict[str, Any]
            The job info for submitted batch data request.

        Warnings
        --------
        Calling this method will incur a cost.

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
            stype_in=stype_in,
            stype_out=stype_out,
        )

        params.append(("encoding", encoding.value))
        params.append(("compression", compression.value))
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
        states: Optional[Union[List[str], str]] = "received,queued,processing,done",
        since: Optional[Union[pd.Timestamp, date, str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Request all batch data job details for the user account.

        The job details will be sorted in order of `ts_received`.

        Makes a `GET /batch.list_jobs` HTTP request.

        Parameters
        ----------
        states : List[str] or str, optional {'received', 'queued', 'processing', 'done', 'expired'}  # noqa
            The filter for jobs states as a list of comma separated values.
        since : pd.Timestamp or date or str or int, optional
            The filter for timestamp submitted (will not include jobs prior to this).

        Returns
        -------
        List[Dict[str, Any]]
            The batch job details.

        """
        states = maybe_values_list_to_string(states)
        since = maybe_datetime_to_string(since)

        params: List[Tuple[str, str]] = [
            ("states", states),
            ("since", since),
        ]

        return self._get(
            url=self._base_url + ".list_jobs",
            params=params,
            basic_auth=True,
        ).json()
