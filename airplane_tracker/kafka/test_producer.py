import json
import pytest
from airplane_tracker.kafka.producer import fetch_adsb_data


class DummyResp:
    def __init__(self, data):
        self._data = data

    def json(self):
        return {"data": self._data}

    def raise_for_status(self):
        pass


@pytest.fixture(autouse=True)
def patch_requests(monkeypatch):
    """Redirect requests.get to our dummy response."""
    def fake_get(url, params, timeout):
        dummy = {
            "flight": {"icao": "ABC123", "iata": "IAT123"},
            "live": {"latitude": 1.0, "longitude": 2.0, "altitude": 10000},
            "departure": {"iata": "XYZ"},
        }
        return DummyResp([dummy])

    monkeypatch.setattr("airplane_tracker.kafka.producer.requests.get", fake_get)
    yield


def test_fetch_adsb_data():
    records = fetch_adsb_data()
    assert isinstance(records, list)
    assert records[0]["icao"] == "ABC123"
    assert records[0]["callsign"] == "IAT123"
    assert records[0]["latitude"] == 1.0
    assert records[0]["longitude"] == 2.0
    assert records[0]["altitude"] == 10000
    assert records[0]["country"] == "XYZ"
    assert "timestamp" in records[0]