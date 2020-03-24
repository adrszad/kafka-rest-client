import requests
import urllib
import uuid
import importlib_metadata
import json
import base64
import logging

from collections import namedtuple, defaultdict
from typing import List

__all__ = [
    'KafkaRestClient', 'KafkaRestClientException',
    'TopicPartition', 'KafkaMessage'
]

log = logging.getLogger(name="kafka-rest-client")

__version__ = importlib_metadata.version('kafka-rest-client')
USER_AGENT = f"kafka-rest-client/{__version__}"


TopicPartition = namedtuple('TopicPartition', "topic, partition")

KafkaMessage = namedtuple("KafkaMessage",
                          ["topic", "partition", "offset", "key", "value"])


class KafkaRestClientException(Exception):
    def __init__(self, message, *, error_code, http_code, http_message):
        super().__init__(message)
        self.error_code = error_code
        self.http_code = http_code
        self.http_message = http_message

    def __repr__(self):
        return (f"{self.message} ({self.error_code})."
                f" HTTP status {self.http_code} {self.http_message}")


class KafkaRestClient:
    """a client for kafka-rest proxy
    """

    def __init__(self, *topics: str,
                 server: str = "http://localhost:8082",
                 group_id: str = "",
                 fetch_max_bytes: int = 52428800,
                 fetch_max_wait_ms: int = 500,
                 auto_offset_reset: str = "latest",
                 enable_auto_commit: bool = True,
                 max_poll_interval_ms: int = 300000,
                 format: str = "binary"):
        """
        """
        self._server = server
        self._group_id = group_id or f"kafka-rest-client-{uuid.uuid4()}"
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        valid_reset = ("earliest", "latest")
        if auto_offset_reset not in valid_reset:
            raise ValueError(f"auto_offset_reset not in "
                             f"{valid_reset}, got {auto_offset_reset}")
        valid_format = ("json", "avro", "binary")
        if format not in valid_format:
            raise ValueError(f"format not in "
                             f"{valid_format}, got {format}")
        self._format = format
        if self._format == "binary":
            self._decode = lambda x: (base64.b64decode(x)
                                      if x is not None
                                      else None)
        else:
            self._decode = lambda x: x
        self._auto_offset_reset = auto_offset_reset
        if enable_auto_commit:
            raise RuntimeError("autocommit is not implemented yet")
        self._enable_auto_commit = enable_auto_commit
        self._max_poll_interval_ms = max_poll_interval_ms
        self._content_type = f"application/vnd.kafka.v2+json"
        self._accept = (f"application/vnd.kafka.{self._format}.v2+json,"
                        f" {self._content_type}")
        if topics:
            self.subscribe(topics=topics)
        self._observed_offsets = {}
        self._returned_offsets = {}

    def topics(self) -> List[str]:
        return self._get("topics")

    _consumer = None

    @property
    def consumer(self):
        if self._consumer is not None:
            return self._consumer
        rq = {
            "format": self._format,
            "auto.offset.reset": self._auto_offset_reset,
            "auto.commit.enable": self._enable_auto_commit,
        }
        rs = self._post("consumers", self._group_id, data=rq)
        self._consumer = self._normalize_url(rs.get("base_uri"))
        self._instance_id = rs.get("instance_id")
        return self._consumer

    def close(self, autocommit=True):
        if self._consumer is None:
            return
        if autocommit and self._enable_auto_commit:
            self.commit(self._observed_offsets)
        self._delete(self._consumer)

    def commit(self, partitions):
        raise RuntimeError("Not implemented yet")

    def commited(self, position):
        raise RuntimeError("Not implemented yet")

    def subscribe(self, *, topics: List[str] = [], pattern: str = ""):
        if all((topics, pattern)) or not any((topics, pattern)):
            raise TypeError("Subscribe() requires topics or pattern")
        if topics:
            rq = dict(topics=topics)
        else:
            rq = dict(topic_pattern=pattern)
        self._post(self.consumer, "subscription",
                   data=rq, validator=self._expect_no_content)
        next(
            self._poll_once(timeout_ms=10, max_records=2,
                            max_bytes=10000, update_offsets=False),
            None)

    def subscription(self):
        rs = self._get(self.consumer, "subscription")
        return set(rs.get("topics", []))

    def unsubscribe(self):
        self._delete(self.consumer, "subscription")

    def partitions_for_topic(self, topic):
        assert "/" not in topic
        rs = self._get('topics', topic, 'partitions')
        return set(p["partition"] for p in rs)

    def beginning_offsets(self, partitions: List[TopicPartition]):
        return dict(self._get_offsets(partitions, 'beginning_offset'))

    def end_offsets(self, partitions: List[TopicPartition]):
        return dict(self._get_offsets(partitions, 'end_offset'))

    def _check_partitions(self, partitions):
        if any(not isinstance(p, TopicPartition) for p in partitions):
            raise TypeError("partitions must be list of TopicPartition")

    def _get_offsets(self, partitions, which):
        self._check_partitions(partitions)
        for partition in partitions:
            rs = self._get("topics", partition.topic,
                           "partitions", str(partition.partition),
                           "offsets")
            yield partition, rs[which]

    def seek(self, partition, offset):
        if not isinstance(partition, TopicPartition):
            raise TypeError("partition must be TopicPartition")
        if not isinstance(offset, int):
            raise TypeError("offset must be int")

        rq = {"offsets": [{
            "topic": partition.topic,
            "partition": partition.partition,
            "offset": offset}]}
        self._post(self.consumer, "positions",
                   data=rq, validator=self._expect_no_content)

    def seek_to_beginning(self, *partitions):
        self._seek(partitions, "beginning")

    def seek_to_end(self, *partitions):
        self._seek(partitions, "end")

    def poll(self, *, timeout_ms: int = 0, max_records: int = None,
             update_offsets: bool = True):
        ro = self._returned_offsets
        self._observed_offsets.update(ro)
        ro.clear()
        msgs = self._poll_once(timeout_ms=timeout_ms,
                               max_records=max_records,
                               update_offsets=update_offsets)
        ret = defaultdict(list)
        for tp, msg in msgs:
            ret[tp].append(msg)
            ro[tp] = msg.offset
        return ret

    def _poll_once(self, *, timeout_ms: int = 0,
                   max_records: int = None,
                   max_bytes: int = None,
                   update_offsets: bool = True):
        rs = self._get(self.consumer, "records",
                       params={
                           "timeout": timeout_ms or self._fetch_max_wait_ms,
                           "max_bytes": max_bytes or self._fetch_max_bytes})
        for r in rs:
            msg = KafkaMessage(topic=r["topic"],
                               partition=r["partition"],
                               offset=r["offset"],
                               key=self._decode(r["key"]),
                               value=self._decode(r["value"]))
            tp = TopicPartition(topic=msg.topic,
                                partition=msg.partition)
            yield tp, msg

    def __iter__(self):
        oo = self._observed_offsets
        while True:
            for tp, msg in self._poll_once():
                yield msg
                oo[tp] = msg.offset

    def _seek(self, partitions, where):
        self._check_partitions(partitions)
        rq = {"partitions": [{"topic": partition.topic,
                              "partition": partition.partition}
                             for partition in partitions]}
        self._post(self.consumer, "positions", where,
                   data=rq, validator=self._expect_no_content)

    def _url(self, *url):
        return urllib.parse.urljoin(self._server, "/".join(url))

    def _get(self, *url, params=None):
        addr = self._url(*url)
        log.info("GET %s", addr)
        r = requests.get(addr, headers={
            'user-agent': USER_AGENT,
            'accept': self._accept,
        }, params=params)
        if r.status_code != requests.codes.ok:
            self._raise_response_error(r)
        return self._response(r)

    def _normalize_url(self, *url):
        addr = self._url(*url)
        log.info("HEAD %s", addr)
        r = requests.head(addr,
                          headers={
                              'user-agent': USER_AGENT,
                          },
                          allow_redirects=True)
        return r.url

    def _response(self, r):
        ret = r.json()
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Received %s", json.dumps(ret))
        return ret

    def _post(self, *url, data=None, validator=None):
        if data is None:
            assert TypeError("no data to post")
        addr = self._url(*url)
        headers = {
            'user-agent': USER_AGENT,
            'accept': self._accept,
            'content-type': self._content_type,
        }
        jdata = json.dumps(data)
        log.info("POST %s %s", addr, jdata)
        r = requests.post(addr,
                          headers=headers,
                          data=jdata)
        (validator or self._expect_ok)(r)
        if r.status_code == requests.codes.no_content:
            return None
        return self._response(r)

    def _delete(self, *url):
        headers = {
            'user-agent': USER_AGENT,
            'accept': self._accept,
            'content-type': self._content_type,
        }
        r = requests.delete(self._url(*url),
                            headers=headers)
        self._expect_no_content(r)

    def _expect_ok(self, r):
        if r.status_code != requests.codes.ok:
            self._raise_response_error(r)

    def _expect_no_content(self, r):
        if r.status_code != requests.codes.no_content:
            self._raise_response_error(r)

    def _raise_response_error(self, r):
        try:
            err = r.json()
        except ValueError:
            r.raise_for_status()
            err = {}
        exc = KafkaRestClientException(message=err.get("message"),
                                       error_code=err.get("error_code"),
                                       http_code=r.status_code,
                                       http_message=r.reason)
        raise exc
