import os
from typing import Any, Dict, Optional, Union

from databento import Bento
from databento.common.enums import HistoricalGateway, Schema
from databento.common.logging import log_info
from databento.common.parsing import enum_or_str_lowercase
from databento.historical.api.batch import BatchHttpAPI
from databento.historical.api.metadata import MetadataHttpAPI
from databento.historical.api.symbology import SymbologyHttpAPI
from databento.historical.api.timeseries import TimeSeriesHttpAPI


class Historical:
    """
    Provides a client connection class for requesting historical data and
    metadata from the Databento API servers.

    Parameters
    ----------
    key : str, optional
        The API user access key for authentication.
        If ``None`` then the `DATABENTO_ACCESS_KEY` environment variable is used.
    gateway : HistoricalGateway or str, default HistoricalGateway.NEAREST
        The API server gateway.
        If ``None`` then the default gateway is used.

    Examples
    --------
    > import databento as db
    > client = db.Historical('YOUR_ACCESS_KEY')
    """

    def __init__(
        self,
        key: Optional[str] = None,
        gateway: Union[HistoricalGateway, str] = HistoricalGateway.NEAREST,
    ):
        if key is None:
            key = os.environ.get("DATABENTO_ACCESS_KEY")
        if key is None or not isinstance(key, str) or key.isspace():
            raise ValueError(f"invalid API access key, was {key}")

        # Configure data access gateway
        gateway = enum_or_str_lowercase(gateway, "gateway")
        if gateway in ("nearest", "bo1"):
            gateway = "https://hist.databento.com"

        self._key = key
        self._gateway = gateway

        self.batch = BatchHttpAPI(key=key, gateway=gateway)
        self.metadata = MetadataHttpAPI(key=key, gateway=gateway)
        self.symbology = SymbologyHttpAPI(key=key, gateway=gateway)
        self.timeseries = TimeSeriesHttpAPI(key=key, gateway=gateway)

        # Not logging security sensitive `key`
        log_info(f"Initialized {type(self).__name__}(gateway={self._gateway})")

    @property
    def key(self) -> str:
        """
        Return the API user access key for the client.

        Returns
        -------
        str

        """
        return self._key

    @property
    def gateway(self) -> str:
        """
        Return the API server gateway for the client.

        Returns
        -------
        str

        """
        return self._gateway

    def request_symbology(self, data: Bento) -> Dict[str, Dict[str, Any]]:
        """
        Request symbology resolution based on the metadata properties.

        Makes a `GET /v0/symbology.resolve` HTTP request.

        Current symbology mappings from the metadata are also available by
        calling the `.symbology` or `.mappings` properties.

        Parameters
        ----------
        data : Bento
            The bento to source the metadata from.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            A map of input symbol to output symbol across the date range.

        """
        return self.symbology.resolve(
            dataset=data.dataset,
            symbols=data.symbols,
            stype_in=data.stype_in,
            stype_out=data.stype_out,
            start_date=data.start.date(),
            end_date=data.end.date(),
        )

    def request_full_definitions(
        self,
        data: Bento,
        path: Optional[str] = None,
    ) -> Bento:
        """
        Request full instrument definitions based on the metadata properties.

        Makes a `GET /v0/timeseries.stream` HTTP request.

        Parameters
        ----------
        data : Bento
            The bento to source the metadata from.
        path : str, optional
            The file path to write to on disk (if provided).

        Returns
        -------
        Bento

        Warnings
        --------
        Calling this method will incur a cost.

        """
        return self.timeseries.stream(
            dataset=data.dataset,
            symbols=data.symbols,
            schema=Schema.DEFINITION,
            start=data.start,
            end=data.end,
            stype_in=data.stype_in,
            stype_out=data.stype_out,
            path=path,
        )
