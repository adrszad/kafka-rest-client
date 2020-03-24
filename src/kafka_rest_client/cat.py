"""
Apache Kafka rest proxy consumer tool

Usage:
  kafka-rest-cat -C -s SERVER -t TOPIC [-o OFFSET] [-c COUNT] -e
  kafka-rest-cat -L -s SERVER [-t TOPIC] [-v]
  kafka-rest-cat how how

Options:
  -C            consumer mode.
  -s SERVER     server to connect to.
  -t TOPIC      topic to consume.
  -o OFFSET     offset to start at.
  -c COUNT      number of messages to consume.
  -e            stop at end of topic.
  -v            verbose
"""

import json
from itertools import islice, takewhile
from docopt import docopt
from .client import KafkaRestClient, USER_AGENT, TopicPartition


def dumpbin(obj):
    if isinstance(obj, bytes):
        try:
            return json.loads(obj)
        except json.decoder.JSONDecodeError:
            return obj.decode(encoding='ascii')
    return obj

def consume(*, server, topic,
            offset=None, count=None,
            stop_at_end=False):
    client = KafkaRestClient(topic,
                             server=server,
                             enable_auto_commit=False)
    partitions = [TopicPartition(topic, p)
                  for p in client.partitions_for_topic(topic)]
    ends = client.end_offsets(partitions)
    starts = client.beginning_offsets(partitions)
    if offset is None:
        pass
    elif offset == "beginning":
        client.seek_to_beginning(*partitions)
    elif offset == "end":
        client.seek_to_end(*partitions)
    else:
        offset = int(offset)
        if offset < 0:
            base = ends
        else:
            base = {p:0 for p in partitions}
        for partition in partitions:
            client.seek(partition, base[partition] + offset)
    consumer = iter(client)
    if count is not None:
        count = int(count)
        assert count > 0
        consumer = islice(consumer, count)
    if stop_at_end:
        active = {p for p in partitions if ends[p] > starts[p]}
        def pending(msg):
            tp = TopicPartition(msg.topic, msg.partition)
            if ends[tp] == msg.offset + 1:
                active.discard(tp)
            return bool(active)
        consumer = takewhile(pending, consumer)
    for msg in consumer:
        print(json.dumps(msg._asdict(), default=dumpbin))
    client.unsubscribe()
    client.close()

def metadata(*, server, topic_pattern=None, verbose=False):
    client = KafkaRestClient(server=server,
                             enable_auto_commit=False)
    if topic_pattern is None:
        pred = lambda x: True
    else:
        pred = lambda x: topic_pattern in x
    topics = client.topics()
    for topic in sorted(filter(pred, topics)):
        print(topic)
        if verbose:
            partitions = [TopicPartition(topic, p)
                          for p in client.partitions_for_topic(topic)]
            ends = client.end_offsets(partitions)
            starts = client.beginning_offsets(partitions)
            for p in partitions:
                print(f'    partition {p.partition:3}'
                      f' beginning {starts[p]:8}'
                      f' end {ends[p]:8}'
                      f' count {ends[p]-starts[p]:8}')



def main():
    arguments = docopt(__doc__, version=USER_AGENT)

    if arguments["-C"]:
        consume(server=arguments["-s"],
                topic=arguments["-t"],
                offset=arguments["-o"],
                count=arguments["-c"],
                stop_at_end=arguments["-e"],
        )

    if arguments["-L"]:
        metadata(server=arguments["-s"],
                 topic_pattern=arguments["-t"],
                 verbose=arguments['-v'],
        )
