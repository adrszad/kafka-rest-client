from kafka_rest_client import KafkaRestClient
from pytest import fixture


@fixture
def client():
    return KafkaRestClient()


def test_construction():
    KafkaRestClient()


def test_topics(client):
    assert client.topics()


def test_no_subscribe(client):
    assert client.subscription() == set()


def test_subscribe_topic(client):
    topics = client.topics()[:3]
    client.subscribe(topics=topics)
    subs = client.subscription()
    assert set(topics) == subs
