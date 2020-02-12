import requests
import urllib
import importlib_metadata

__version__ = importlib_metadata.version('kafka-rest-client')
USER_AGENT = f"kafka-rest-client/{__version__}"


class KafkaRestClient:
    """a client for kafka-rest proxy
    """

    def __init__(self, server="http://localhost:8082"):
        """
        """
        self._server = server

    def topics(self):
        return self._get("/topics")

    def server(self, url):
        return urllib.parse.urljoin(self._server, url)

    def _get(self, url):
        r = requests.get(self.server(url), headers={
            'user-agent': USER_AGENT,
            'accept': 'application/vnd.kafka.v2+json',
        })
        if r.status_code != requests.codes.ok:
            self._raise_response_error(r)
        return r.json()
