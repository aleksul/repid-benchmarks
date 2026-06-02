"""RabbitMQ management helpers used by benchmark setup."""

from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request


def management_url_from_amqp(amqp_url: str) -> str:
    parsed = urllib.parse.urlparse(amqp_url)
    host = parsed.hostname or "localhost"
    return f"http://{host}:15672"


def _auth_header(amqp_url: str) -> str:
    parsed = urllib.parse.urlparse(amqp_url)
    user = urllib.parse.unquote(parsed.username or "user")
    password = urllib.parse.unquote(parsed.password or "testtest")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {token}"


def _request(amqp_url: str, mgmt_url: str, path: str, method: str, data: bytes | None = None) -> bytes:
    request = urllib.request.Request(
        f"{mgmt_url}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": _auth_header(amqp_url),
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read()


def delete_queue(amqp_url: str, mgmt_url: str, queue_name: str) -> None:
    try:
        _request(amqp_url, mgmt_url, f"/api/queues/%2F/{urllib.parse.quote(queue_name, safe='')}", "DELETE")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def declare_classic_queue(amqp_url: str, mgmt_url: str, queue_name: str) -> None:
    payload = json.dumps({"durable": True, "arguments": {}}).encode()
    _request(amqp_url, mgmt_url, f"/api/queues/%2F/{urllib.parse.quote(queue_name, safe='')}", "PUT", payload)


def purge_queue(amqp_url: str, mgmt_url: str, queue_name: str) -> None:
    try:
        _request(amqp_url, mgmt_url, f"/api/queues/%2F/{urllib.parse.quote(queue_name, safe='')}/contents", "DELETE")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def reset_queue(amqp_url: str, mgmt_url: str, queue_name: str) -> None:
    delete_queue(amqp_url, mgmt_url, queue_name)
    declare_classic_queue(amqp_url, mgmt_url, queue_name)
    purge_queue(amqp_url, mgmt_url, queue_name)


def queue_consumer_count(amqp_url: str, mgmt_url: str, queue_name: str) -> int:
    try:
        raw = _request(amqp_url, mgmt_url, f"/api/queues/%2F/{urllib.parse.quote(queue_name, safe='')}", "GET")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 0
        raise
    return int(json.loads(raw).get("consumers", 0))


def wait_for_consumers(amqp_url: str, mgmt_url: str, queue_name: str, minimum: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if queue_consumer_count(amqp_url, mgmt_url, queue_name) >= minimum:
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for {minimum} consumers on {queue_name}")
