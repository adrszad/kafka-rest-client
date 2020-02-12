from kafka_rest_client import KafkaRestClient


def test_construction():
    client = KafkaRestClient()
    assert client.topics()
