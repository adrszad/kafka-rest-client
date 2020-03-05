"""
Apache Kafka rest proxy consumer tool

Usage:
  kafka-rest-cat -C -s SERVER -t TOPIC [-o OFFSET] [-c COUNT] -e
  kafka-rest-cat how how

Options:
  -C            consumer mode.
  -s SERVER     server to connect to.
  -t TOPIC      topic to consume.
  -o OFFSET     offset to start at.
  -c COUNT      number of messages to consume.
  -e            stop at end of topic.
"""

import json
from itertools import islice, takewhile
from docopt import docopt
from .client import KafkaRestClient, USER_AGENT, TopicPartition


def dumpbin(obj):
    if isinstance(obj, bytes):
        return obj.hex()
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
    if offset == "beginning":
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

def main():
    arguments = docopt(__doc__, version=USER_AGENT)
    print(arguments)

    if arguments["-C"]:
        consume(server=arguments["-s"],
                topic=arguments["-t"],
                offset=arguments["-o"],
                count=arguments["-c"],
                stop_at_end=arguments["-e"],
        )
