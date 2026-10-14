import datetime
import hashlib
import hmac
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, destination):
        return None


def signed_request(method, endpoint, bucket):
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme not in ("http", "https") or parsed.hostname not in ("127.0.0.1", "::1") or parsed.username or parsed.query or parsed.fragment:
        raise RuntimeError("only an explicit loopback fixture endpoint is supported")
    access = os.environ["S3_ACCESS_KEY_ID"]
    secret = os.environ["S3_SECRET_ACCESS_KEY"]
    region = os.environ.get("S3_REGION", "us-east-1")
    observed = datetime.datetime.now(datetime.timezone.utc)
    timestamp = observed.strftime("%Y%m%dT%H%M%SZ")
    day = observed.strftime("%Y%m%d")
    path = parsed.path.rstrip("/") + "/" + bucket
    empty_hash = hashlib.sha256(b"").hexdigest()
    headers = {"host": parsed.netloc, "x-amz-content-sha256": empty_hash, "x-amz-date": timestamp}
    if os.environ.get("S3_SESSION_TOKEN"):
        headers["x-amz-security-token"] = os.environ["S3_SESSION_TOKEN"]
    names = sorted(headers)
    canonical_headers = "".join(name + ":" + headers[name] + "\n" for name in names)
    signed_headers = ";".join(names)
    canonical = "\n".join((method, path, "", canonical_headers, signed_headers, empty_hash))
    scope = "/".join((day, region, "s3", "aws4_request"))
    signing_text = "\n".join(("AWS4-HMAC-SHA256", timestamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
    signing_key = ("AWS4" + secret).encode()
    for component in (day, region, "s3", "aws4_request"):
        signing_key = hmac.new(signing_key, component.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing_key, signing_text.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    outgoing = urllib.request.Request(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", "")), data=b"" if method == "PUT" else None, headers=headers, method=method)
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with client.open(outgoing, timeout=10) as incoming:
            return incoming.status
    except urllib.error.HTTPError as error:
        return error.code
    except (urllib.error.URLError, OSError):
        raise RuntimeError("fixture bucket transport failed") from None


def main():
    endpoint = os.environ["S3_ENDPOINT"]
    bucket = os.environ["S3_BUCKET"]
    if not re.fullmatch(r"peri-(?:test|stress|p6)-[a-z0-9-]{8,48}", bucket):
        raise RuntimeError("explicit fresh peri fixture bucket required")
    if signed_request("HEAD", endpoint, bucket) != 404:
        raise RuntimeError("bucket not confirmed absent; refusing modification")
    if signed_request("PUT", endpoint, bucket) not in (200, 204):
        raise RuntimeError("fresh fixture bucket creation failed")
    if signed_request("HEAD", endpoint, bucket) != 200:
        raise RuntimeError("created fixture bucket could not be confirmed")
    print(json.dumps({"created": True, "bucket": bucket, "scope": "new_isolated_fixture_only"}), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyError):
        raise SystemExit("isolated S3 fixture initialization failed; credentials are not logged") from None
