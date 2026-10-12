import http.client
import json
import ssl
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.parse import urlencode

import run_guarded as guarded
from capacity_envelope import unique_object


MAX_RESPONSE = 8 << 20


class HTTPRejection(ValueError):
    def __init__(self, route, status):
        guarded.require(route in ("metrics", "capacity_trace") and type(status) is int and 100 <= status <= 599,
                        "collector_metrics_HTTP_failure")
        super().__init__("collector_metrics_HTTP_failure")
        self.http_rejection = {"route": route, "status": status}


class MetricsReader:
    def __init__(self, connection, factory=http.client.HTTPSConnection):
        guarded.require(connection.get("fixture_only") is True, "collector_owned_fixture_required")
        endpoint = urlsplit(connection["management_endpoint"])
        guarded.require(endpoint.scheme == "https" and endpoint.hostname and not endpoint.username
                        and not endpoint.password and endpoint.path in ("", "/") and not endpoint.query and not endpoint.fragment,
                        "collector_proper_CA_HTTPS_endpoint_required")
        self.key = connection.get("metrics_key")
        guarded.require(isinstance(self.key, str) and 16 <= len(self.key) <= 4096 and all(33 <= ord(character) <= 126 for character in self.key),
                        "collector_private_metrics_key_required")
        certificate = Path(connection["ca_file"])
        guarded.require(certificate.is_absolute() and certificate.resolve() == certificate and certificate.is_file()
                        and certificate.stat().st_size <= 1 << 20, "collector_CA_file_required")
        context = ssl.create_default_context(cafile=str(certificate))
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        guarded.require(context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED, "collector_TLS_verification_required")
        self.factory, self.host, self.port, self.context = factory, endpoint.hostname, endpoint.port or 443, context

    def read(self, path="/v1/metrics", maximum=MAX_RESPONSE):
        guarded.require(path == "/v1/metrics" or path.startswith("/v1/capacity-trace?"), "collector_read_only_path_required")
        connection = self.factory(self.host, self.port, timeout=2, context=self.context)
        try:
            connection.request("GET", path, headers={"Authorization": "Bearer " + self.key, "Accept": "application/json"})
            response = connection.getresponse()
            if response.status != 200 and type(response.status) is int and 100 <= response.status <= 599:
                raise HTTPRejection("metrics" if path == "/v1/metrics" else "capacity_trace", response.status)
            guarded.require(response.status == 200, "collector_metrics_HTTP_failure")
            length = response.getheader("Content-Length")
            guarded.require(length is None or 0 <= int(length) <= maximum, "collector_response_budget")
            deadline, body = time.monotonic() + 2, bytearray()
            while True:
                guarded.require(time.monotonic() < deadline, "collector_response_deadline")
                chunk = response.read1(min(65536, maximum + 1 - len(body)))
                if not chunk:
                    break
                body.extend(chunk)
                guarded.require(len(body) <= maximum, "collector_response_budget")
            envelope = json.loads(body, object_pairs_hook=unique_object)
            guarded.require(envelope.get("version") == 1 and envelope.get("ok") is True and envelope.get("error") is None
                            and isinstance(envelope.get("result"), dict), "collector_metrics_envelope_failure")
            return envelope["result"]
        finally:
            connection.close()

    def page(self, role, stream, after):
        parameters = {"role": "authority" if role == "authority" else "worker", "stream_id": stream,
                      "after_sequence": after, "max_events": 128}
        if role.startswith("worker:"):
            parameters["worker_id"] = role.removeprefix("worker:")
        return self.read("/v1/capacity-trace?" + urlencode(parameters), 64 << 10)
