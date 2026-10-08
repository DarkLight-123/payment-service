"""Check the running API, consumer and webhook retry through Docker Compose."""

import argparse
import json
import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--webhook-host", default="localhost")
    args = parser.parse_args()
    notifications = []
    delivered = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            notifications.append(json.loads(body))
            accepted = len(notifications) >= 3
            self.send_response(200 if accepted else 503)
            self.end_headers()
            if accepted:
                delivered.set()

        def log_message(self, *args):
            pass

    headers = {"X-API-Key": os.getenv("API_KEY", "dev-api-key")}
    key = str(uuid4())
    payload = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Smoke test",
        "metadata": {"order_id": key},
        "webhook_url": f"http://{args.webhook_host}:9010/webhook",
    }

    def request(method, path, body=None, request_headers=None):
        encoded = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            args.base_url + path,
            data=encoded,
            method=method,
            headers={"Content-Type": "application/json", **(request_headers or {})},
        )
        try:
            response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            return response.status, json.load(response)

    server = ThreadingHTTPServer(("0.0.0.0", 9010), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert request("GET", "/health")[0] == 401
        assert request("GET", "/health", request_headers=headers)[0] == 200
        creation_headers = {**headers, "Idempotency-Key": key}
        status, created = request("POST", "/api/v1/payments", payload, creation_headers)
        assert status == 202, (status, created)
        payment_id = created["payment_id"]
        status, repeated = request("POST", "/api/v1/payments", payload, creation_headers)
        assert status == 202 and repeated["payment_id"] == payment_id
        status, _ = request(
            "POST", "/api/v1/payments", {**payload, "amount": "99.00"}, creation_headers
        )
        assert status == 409
        assert delivered.wait(timeout=45), f"Webhook attempts: {len(notifications)}"
        status, payment = request("GET", f"/api/v1/payments/{payment_id}", request_headers=headers)
        assert status == 200 and payment["status"] in {"succeeded", "failed"}
        assert len(notifications) == 3
        assert {item["payment_id"] for item in notifications} == {payment_id}
        assert {item["status"] for item in notifications} == {payment["status"]}
        assert payment["processed_at"] is not None
        print(f"OK: payment {payment_id}, status {payment['status']}, 3 webhook attempts")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
