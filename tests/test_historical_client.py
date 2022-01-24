import databento as db
import pytest
from databento.common.enums import HistoricalGateway


class TestHistoricalClient:
    def test_key_returns_expected(self) -> None:
        # Arrange
        key = "DUMMY_ACCESS_KEY"

        # Act
        client = db.Historical(key=key)

        # Assert
        assert client.key == "DUMMY_ACCESS_KEY"

    def test_default_host_returns_expected(self) -> None:
        # Arrange, Act
        self.client = db.Historical(key="DUMMY_ACCESS_KEY")

        # Assert
        assert self.client.gateway == "https://hist.databento.com"

    @pytest.mark.parametrize(
        "gateway, expected",
        [
            [HistoricalGateway.BO1, "https://hist.databento.com"],
            [HistoricalGateway.NEAREST, "https://hist.databento.com"],
            ["bo1", "https://hist.databento.com"],
            ["nearest", "https://hist.databento.com"],
        ],
    )
    def test_gateway_nearest_and_bo1_map_to_hist_databento(
        self,
        gateway,
        expected,
    ):
        # Arrange, Act
        client = db.Historical(key="DUMMY_ACCESS_KEY", gateway=gateway)

        # Assert
        assert client.gateway == expected

    def test_custom_gateway_returns_expected(self):
        # Arrange
        ny4_gateway = "ny4.databento.com"

        # Act
        client = db.Historical(key="DUMMY_ACCESS_KEY", gateway=ny4_gateway)

        # Assert
        assert client.gateway == ny4_gateway
