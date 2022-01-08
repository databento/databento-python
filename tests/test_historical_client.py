import databento as db


class TestHistoricalClient:
    def setup(self) -> None:
        key = "DUMMY_ACCESS_KEY"
        self.client = db.Historical(key=key)

    def test_key_returns_expected(self) -> None:
        assert self.client.key == "DUMMY_ACCESS_KEY"

    def test_host_returns_expected(self) -> None:
        assert self.client.gateway == "https://hist.databento.com"
