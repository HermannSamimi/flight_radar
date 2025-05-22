import pytest
from airplane_tracker.kafka.producer import fetch_and_publish, producer, redis_client

class DummyProducer:
    def __init__(self): self.sent=[]
    def send(self,t,m): self.sent.append((t,m))
    def flush(self): pass
class DummyRedis:
    def __init__(self): self.ch=[]
    def publish(self,c,m): self.ch.append((c,m))

@pytest.fixture(autouse=True)
def patch_clients(monkeypatch):
    monkeypatch.setattr("airplane_tracker.kafka.producer.producer", DummyProducer())
    monkeypatch.setattr("airplane_tracker.kafka.producer.redis_client", DummyRedis())

def test_empty(monkeypatch):
    monkeypatch.setattr("airplane_tracker.kafka.producer.fetch_adsb_data", lambda: [])
    fetch_and_publish()

def test_publish(monkeypatch):
    data = [{"icao":"X","callsign":"FL1","latitude":0,"longitude":0,"altitude":0}]
    monkeypatch.setattr("airplane_tracker.kafka.producer.fetch_adsb_data", lambda: data)
    fetch_and_publish()
    assert producer.sent
    assert redis_client.ch