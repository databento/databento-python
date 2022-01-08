import os
from typing import Optional, Union

from databento.common.enums import HistoricalGateway
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
        if gateway == "nearest":
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
    def key(self):
        """
        Return the API user access key for the client.

        Returns
        -------
        str

        """
        return self._key

    @property
    def gateway(self):
        """
        Return the API server gateway for the client.

        Returns
        -------
        str

        """
        return self._gateway
