import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
import urllib.request
from unittest.mock import Mock, patch


source = pathlib.Path(__file__).resolve().parents[1] / "dev.py"
spec = importlib.util.spec_from_file_location("peri_review_dev_protocol", source)
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


class ProtocolTests(unittest.TestCase):
    def envelope(self, **updates):
        result = {"version": 1, "request_id": "review-request", "ok": True, "result": {"count": 18446744073709551615}, "error": None}
        result.update(updates)
        return json.dumps(result).encode()

    def call(self, encoded):
        opener = Mock()
        opener.open.return_value = io.BytesIO(encoded)
        with patch.object(dev.urllib.request, "build_opener", return_value=opener) as builder:
            try:
                result = dev.request("http://127.0.0.1:19000", "synthetic-protocol-only", "/control", {"request_id": "review-request"})
            finally:
                opener.open.assert_called_once()
                builder.assert_called_once()
                handlers = builder.call_args.args
                self.assertTrue(any(isinstance(handler, dev.NoRedirect) for handler in handlers))
            request = opener.open.call_args.args[0]
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "http://127.0.0.1:19000/control")
            self.assertEqual(request.get_header("Authorization"), "Bearer synthetic-protocol-only")
            self.assertEqual(json.loads(request.data)["request_id"], "review-request")
            return result

    def test_exact_success_ack_preserves_large_count(self):
        self.assertEqual(self.call(self.envelope()), {"count": 18446744073709551615})

    def test_wrong_version_is_not_acknowledged_or_retried(self):
        for version in (0, 2, "1", None):
            with self.subTest(version=version), self.assertRaisesRegex(RuntimeError, "identity.*unknown"):
                self.call(self.envelope(version=version))

    def test_wrong_request_id_is_not_acknowledged_or_retried(self):
        for identity in ("another-request", "", None):
            with self.subTest(identity=identity), self.assertRaisesRegex(RuntimeError, "identity.*unknown"):
                self.call(self.envelope(request_id=identity))

    def test_nonboolean_success_or_error_is_not_acknowledged(self):
        for updates in ({"ok": False}, {"ok": 1}, {"ok": "true"}, {"ok": None}, {"error": {"code": "outcome_unknown"}}, {"error": "failed"}):
            with self.subTest(updates=updates), self.assertRaises(RuntimeError):
                self.call(self.envelope(**updates))

    def test_oversized_and_padded_reply_is_not_acknowledged(self):
        limit = 32 * 1024 * 1024
        valid = self.envelope()
        for encoded in (b"x" * (limit + 1), valid + b" " * (limit + 1 - len(valid))):
            with self.subTest(prefix_is_valid=encoded.startswith(b"{")), self.assertRaisesRegex(RuntimeError, "oversized.*unknown"):
                self.call(encoded)

    def test_nonobject_and_trailing_reply_is_not_acknowledged(self):
        for encoded in (b"[]", b"null", self.envelope() + b' {"extra":true}'):
            with self.subTest(encoded=encoded), self.assertRaises((RuntimeError, ValueError)):
                self.call(encoded)

    def test_duplicate_envelope_fields_are_unknown_without_retry(self):
        for field in (
            b'"version":2,"version":1',
            b'"version":1,"version":1',
            b'"request_id":"wrong","request_id":"review-request"',
            b'"ok":false,"ok":true',
            b'"error":"failed","error":null',
        ):
            encoded = self.envelope()[:-1] + b"," + field + b"}"
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "duplicate.*unknown"):
                self.call(encoded)

    def test_duplicate_nested_fields_are_unknown_without_retry(self):
        for result in (
            b'{"ok":false,"ok":true}',
            b'{"nested":{"ok":false,"ok":true}}',
            b'[{"count":1,"count":2}]',
            b'{"count":1,"\\u0063ount":2}',
        ):
            encoded = b'{"version":1,"request_id":"review-request","ok":true,"result":' + result + b',"error":null}'
            with self.subTest(result=result), self.assertRaisesRegex(RuntimeError, "duplicate.*unknown"):
                self.call(encoded)

    def test_redirect_does_not_create_credential_bearing_destination_request(self):
        request = urllib.request.Request("http://127.0.0.1:19000/control", data=b"{}", headers={"Authorization": "Bearer synthetic-protocol-only"}, method="POST")
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code):
                self.assertIsNone(dev.NoRedirect().redirect_request(request, None, code, "redirect", {}, "http://127.0.0.1:19001/other"))

    def test_redirect_response_is_unknown_without_second_dispatch(self):
        opener = Mock()
        opener.open.side_effect = urllib.error.HTTPError("http://127.0.0.1:19000/control", 302, "redirect", {}, None)
        with patch.object(dev.urllib.request, "build_opener", return_value=opener):
            with self.assertRaisesRegex(RuntimeError, "must not be blindly retried"):
                dev.request("http://127.0.0.1:19000", "synthetic-protocol-only", "/control", {"request_id": "review-request"})
        opener.open.assert_called_once()


if __name__ == "__main__":
    unittest.main()
