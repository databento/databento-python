from __future__ import annotations

import logging
import os

from databento.common.enums import HistoricalGateway
from databento.common.validation import validate_gateway
from databento.reference.api.adjustment import AdjustmentFactorsHttpAPI
from databento.reference.api.corporate import CorporateActionsHttpAPI
from databento.reference.api.security import SecurityMasterHttpAPI


logger = logging.getLogger(__name__)


class Reference:
    """
    Provides a client connection class for requesting reference data from the
    Databento API servers.

    Parameters
    ----------
    key : str, optional
        The user API key for authentication.
        If `None` then the `DATABENTO_API_KEY` environment variable is used.
    gateway : HistoricalGateway or str, default HistoricalGateway.BO1
        The API server gateway.
        If `None` then the default gateway is used.

    Examples
    --------
    > import databento as db
    > client = db.Reference('YOUR_API_KEY')

    """

    def __init__(
        self,
        key: str | None = None,
        gateway: HistoricalGateway | str = HistoricalGateway.BO1,
    ):
        if key is None:
            key = os.environ.get("DATABENTO_API_KEY")
        if key is None or not isinstance(key, str) or key.isspace():
            raise ValueError(f"invalid API key, was {key}")

        try:
            gateway = HistoricalGateway(gateway)
        except ValueError:
            gateway = validate_gateway(str(gateway))

        self._key = key
        self._gateway = gateway

        self.adjustment_factors = AdjustmentFactorsHttpAPI(key=key, gateway=gateway)
        self.corporate_actions = CorporateActionsHttpAPI(key=key, gateway=gateway)
        self.security_master = SecurityMasterHttpAPI(key=key, gateway=gateway)

        # Not logging security sensitive `key`
        logger.info("Initialized %s(gateway=%s)", type(self).__name__, self.gateway)

    @property
    def key(self) -> str:
        """
        Return the user API key for the client.

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
