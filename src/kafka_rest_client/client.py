import requests
import urllib
import uuid
import importlib_metadata
import json

from typing import List

__version__ = importlib_metadata.version('kafka-rest-client')
USER_AGENT = f"kafka-rest-client/{__version__}"


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
                 auto_offset_reset: str = "latest",
                 enable_auto_commit: bool = True,
                 max_poll_interval_ms: int = 300000,
                 format: str = "binary"):
        """
        """
        self._server = server
        self._group_id = group_id or f"kafka-rest-client-{uuid.uuid4()}"
        self._fetch_max_bytes = fetch_max_bytes
        valid_reset = ("earliest", "latest")
        if auto_offset_reset not in valid_reset:
            raise ValueError(f"auto_offset_reset not in "
                             f"{valid_reset}, got {auto_offset_reset}")
        valid_format = ("json", "avro", "binary")
        if format not in valid_format:
            raise ValueError(f"format not in "
                             f"{valid_format}, got {format}")
        self._format = format
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._max_poll_interval_ms = max_poll_interval_ms
        self._content_type = f"application/vnd.kafka.v2+json"
        self._accept = (f"application/vnd.kafka.{self._format}.v2+json,"
                        f" {self._content_type}")
        if topics:
            self.subscribe(topics=topics)

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
        self._consumer = rs.get("base_uri")
        self._instance_id = rs.get("instance_id")
        return self._consumer

    def subscribe(self, *, topics: List[str] = [], pattern: str = ""):
        if all((topics, pattern)) or not any((topics, pattern)):
            raise TypeError("Subscribe() requires topics or pattern")
        if topics:
            rq = dict(topics=topics)
        else:
            rq = dict(topic_pattern=pattern)
        self._post(self.consumer, "subscription",
                   data=rq, validator=self._expect_no_content)

    def subscription(self):
        rs = self._get(self.consumer, "subscription")
        return set(rs.get("topics", []))

    def _url(self, *url):
        return urllib.parse.urljoin(self._server, "/".join(url))

    def _get(self, *url):
        r = requests.get(self._url(*url), headers={
            'user-agent': USER_AGENT,
            'accept': self._accept,
        })
        if r.status_code != requests.codes.ok:
            self._raise_response_error(r)
        return r.json()

    def _post(self, *url, data=None, validator=None):
        if data is None:
            assert TypeError("no data to post")
        headers = {
            'user-agent': USER_AGENT,
            'accept': self._accept,
            'content-type': self._content_type,
        }
        r = requests.post(self._url(*url),
                          headers=headers,
                          data=json.dumps(data))
        (validator or self._expect_ok)(r)
        if r.status_code == requests.codes.no_content:
            return None
        return r.json()

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
        raise KafkaRestClientException(message=err.get("message"),
                                       error_code=err.get("error_code"),
                                       http_code=r.status_code,
                                       http_message=r.reason)
