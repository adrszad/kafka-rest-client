import itertools

from pytest import fixture

from kafka_rest_client import KafkaRestClient, TopicPartition, KafkaMessage

@fixture
def topic_for_test():
    return "kafka-rest-client-test"

@fixture
def client(request):
    client = KafkaRestClient(group_id=request.function.__name__,
                             enable_auto_commit=False,
                             stop_at_end=True)
    yield client
    client.close()


@fixture
def subscribed_client(client, topic_for_test):
    nonempty = list(itertools.islice(
        nonempty_topics(client, [topic_for_test]),
        3))
    assert nonempty
    topics = [x[0] for x in nonempty]
    assert topics
    client.subscribe(topics=topics)
    partitions = [p for x in nonempty for p in x[1]]
    assert partitions
    client.seek_to_beginning(*partitions)
    yield client


def topic_partitions(client, topics):
    return [TopicPartition(t, p)
            for t in topics
            for p in client.partitions_for_topic(t)]


def nonempty_topics(client, topics):
    for topic in topics:
        partitions = topic_partitions(client, [topic])
        bo = client.beginning_offsets(partitions)
        eo = client.end_offsets(partitions)
        nonempty = set(p.topic for p in partitions if bo[p] < eo[p])
        if nonempty:
            yield topic, partitions


def test_construction():
    KafkaRestClient(enable_auto_commit=False)


def test_topics(client):
    assert client.topics()


def test_partitions_for_topic(client):
    for topic in client.topics()[:3]:
        partitions = client.partitions_for_topic(topic)
        assert isinstance(partitions, set)
        assert partitions


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

def test_empty_subscription(client):
    sub = client.subscription()
    assert not sub

def test_subscription(subscribed_client):
    sub = subscribed_client.subscription()
    assert sub

def test_poll(subscribed_client):
    msgs = subscribed_client.poll(timeout_ms=1000)
    assert msgs
    assert isinstance(msgs, dict)
    for v in msgs.values():
        assert isinstance(v, list)
        assert v
        for msg in v:
            assert isinstance(msg, KafkaMessage)


def test_iterator(subscribed_client):
    msgs = list(itertools.islice(subscribed_client, 3))
    assert msgs
    for msg in msgs:
        assert isinstance(msg, KafkaMessage)

def test_iterator_last_message(subscribed_client, topic_for_test):
    ends = subscribed_client.end_offsets(
        [TopicPartition(topic_for_test, p)
         for p in subscribed_client.partitions_for_topic(topic_for_test)])
    for tp, end in ends.items():
        subscribed_client.seek(tp, end - 1)
    msgs = list(itertools.islice(subscribed_client, 3))
    assert msgs
    for msg in msgs:
        assert isinstance(msg, KafkaMessage)
