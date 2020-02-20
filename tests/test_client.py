from kafka_rest_client import KafkaRestClient, TopicPartition
from pytest import fixture


@fixture
def client():
    client = KafkaRestClient(auto_commit_enable=False)
    yield client
    client.close()


def test_construction():
    KafkaRestClient(auto_commit_enable=False)


def test_topics(client):
    assert client.topics()


def test_partitions_for_topic(client):
    for topic in client.topics()[:3]:
        partitions = client.partitions_for_topic(topic)
        assert isinstance(partitions, set)
        assert partitions


def topic_partitions(client, topics):
    return [TopicPartition(t, p)
            for t in topics
            for p in client.partitions_for_topic(t)]


def test_topic_offsets(client):
    partitions = topic_partitions(client, client.topics()[:3])
    bo = client.beginning_offsets(partitions)
    eo = client.end_offsets(partitions)
    for p in partitions:
        assert p in bo
        assert p in eo
        assert bo[p] <= eo[p]


def test_no_subscribe(client):
    assert client.subscription() == set()


def test_subscribe_topic(client):
    topics = client.topics()[:3]
    client.subscribe(topics=topics)
    subs = client.subscription()
    assert set(topics) == subs
