from __future__ import annotations

from collections import defaultdict
import errno
import http.client
import json
from pathlib import Path
import socket
import threading
import time
from urllib.parse import urlsplit


LOCAL_ERRNOS = {
    errno.EADDRINUSE,
    errno.EADDRNOTAVAIL,
    errno.ENOBUFS,
    errno.EMFILE,
    errno.ENFILE,
    errno.ENOSPC,
}


def local_resource_error(exc: BaseException) -> bool:
    return getattr(exc, "errno", None) in LOCAL_ERRNOS or getattr(exc, "winerror", None) in (
        10048,
        10049,
        10055,
        10024,
    )


def classify(value: dict, expected_model: str) -> dict:
    """A complete HTTP error and unknown model-generation outcome are distinct."""
    result = dict(value)
    status = value.get("http_status")
    if value.get("transport_error"):
        result["status"] = "unknown" if value.get("write_started") else "infra"
        return result
    if status == 429 or (status == 503 and value.get("retry_after_seconds") is not None):
        result["status"] = "throttled"
        return result
    if status != 200:
        result.update(status="unknown", generation_outcome_unknown=True)
        return result
    body = value.get("body")
    if not isinstance(body, dict) or body.get("model") != expected_model:
        result.update(status="unknown", protocol_error="model_mismatch")
        return result
    choices = body.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        result.update(status="unknown", protocol_error="invalid_response_envelope")
        return result
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        result.update(status="unknown", protocol_error="missing_message")
        return result
    reason = choice.get("finish_reason")
    content = message.get("content")
    if reason == "length":
        kind = "length"
    elif reason == "content_filter" or message.get("refusal"):
        kind = "refused"
    elif reason != "stop" or not isinstance(content, str):
        kind = "unknown"
    else:
        kind = "ok" if content.strip() else "empty"
    result.update(
        status=kind,
        finish_reason=reason,
        content=content,
        reasoning_content=message.get("reasoning_content"),
        usage=body.get("usage") if isinstance(body.get("usage"), dict) else None,
        fingerprint=body.get("system_fingerprint"),
    )
    return result


class PreparedHTTP:
    def __init__(self, owner: HTTPTransport, endpoint, connection, connected_seconds, key):
        self.owner, self.endpoint, self.connection = owner, endpoint, connection
        self.connected_seconds = connected_seconds
        self._key = key
        self.reusable = False

    def send(self, wire: bytes, read_timeout: float) -> dict:
        connection = self.connection
        start = time.monotonic()
        phase, write_started, written = "before_write", False, False
        value = dict(connected_seconds=self.connected_seconds)
        guard = threading.Lock()
        active = True
        expired = threading.Event()
        owned_socket = connection.sock

        def abort():
            with guard:
                if active:
                    expired.set()
                    try:
                        owned_socket.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        timer = threading.Timer(read_timeout, abort)
        timer.daemon = True
        timer.start()
        self.owner.enter_request()
        try:
            endpoint = urlsplit(self.endpoint.url)
            connection.sock.settimeout(read_timeout)
            phase, write_started = "writing", True
            connection.request(
                "POST",
                endpoint.path or "/",
                body=wire,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Authorization": "Bearer " + self._key,
                },
            )
            written, phase = True, "waiting_headers"
            value["request_written_seconds"] = time.monotonic() - start
            response = connection.getresponse()
            value["headers_seconds"] = time.monotonic() - start
            value["http_status"] = response.status
            value["response_headers"] = {
                k.lower(): v
                for k, v in response.getheaders()
                if k.lower() in ("content-type", "content-length", "retry-after", "x-request-id")
            }
            retry_after = value["response_headers"].get("retry-after")
            if retry_after is not None:
                try:
                    value["retry_after_seconds"] = max(0, float(retry_after))
                except ValueError:
                    value["retry_after_seconds"] = 60
            phase = "reading_body"
            raw = response.read(self.owner.max_response_bytes + 1)
            value["raw_body"] = raw.decode("utf-8", errors="replace")
            value["body_seconds"] = time.monotonic() - start
            if len(raw) > self.owner.max_response_bytes:
                raise ValueError("Response exceeded frozen byte limit")
            if getattr(response, "length", None) not in (None, 0):
                raise http.client.IncompleteRead(raw)
            try:
                value["body"] = json.loads(raw)
            except (ValueError, UnicodeError):
                value["body"] = None
            self.reusable = not response.will_close and not expired.is_set()
        except (OSError, http.client.HTTPException, ValueError) as exc:
            value.update(
                transport_error=type(exc).__name__,
                errno=getattr(exc, "errno", None),
                winerror=getattr(exc, "winerror", None),
            )
        finally:
            with guard:
                active = False
            timer.cancel()
            self.owner.leave_request()
        value.update(
            write_started=write_started,
            request_write_completed=written,
            last_network_phase=phase,
            deadline_expired=expired.is_set(),
            seconds=time.monotonic() - start + self.connected_seconds,
        )
        return classify(value, self.endpoint.model)

    def close(self):
        self.owner.release(self.endpoint, self.connection, self.reusable)


class HTTPTransport:
    """Bounded idle keep-alive connections; one owner per checked-out connection.

    No SDK retries, environment proxies, redirects, source-port broker, or system
    network mutation. Every physical POST is controlled by the scheduler ledger.
    """

    def __init__(self, key_dir: Path, *, connect_timeout=10, max_response_bytes=4 * 1024**2):
        self.key_dir = Path(key_dir).resolve()
        self.connect_timeout, self.max_response_bytes = connect_timeout, max_response_bytes
        self.idle = defaultdict(list)
        self.limits = None
        self.lock = threading.Lock()
        self.active = self.peak = self.started = 0

    def enter_request(self):
        with self.lock:
            self.active += 1
            self.started += 1
            self.peak = max(self.peak, self.active)

    def leave_request(self):
        with self.lock:
            self.active -= 1

    def snapshot(self):
        with self.lock:
            return dict(
                network_inflight=self.active, network_peak=self.peak, network_started=self.started
            )

    def configure(self, endpoints):
        """Retire idle connections on removal/downscale; leave active ones alone."""
        with self.lock:
            self.limits = {e.identity: e.cap if e.enabled else 0 for e in endpoints}
            for identity, connections in list(self.idle.items()):
                limit = self.limits.get(identity, 0)
                while len(connections) > limit:
                    connections.pop().close()
                if not connections:
                    del self.idle[identity]

    def load_key(self, reference: str) -> str:
        path = (self.key_dir / (reference + ".txt")).resolve()
        if path.parent != self.key_dir:
            raise ValueError("Key reference escapes private directory")
        key = path.read_text(encoding="utf-8").strip()
        if not key:
            raise ValueError("Empty private key file")
        return key

    def prepare(self, endpoint) -> PreparedHTTP:
        # Key/file validation and TCP connect happen BEFORE a budget charge/write.
        key = self.load_key(endpoint.key_ref)
        with self.lock:
            connection = (
                self.idle[endpoint.identity].pop() if self.idle[endpoint.identity] else None
            )
        started = time.monotonic()
        if connection is None:
            url = urlsplit(endpoint.url)
            cls = (
                http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
            )
            connection = cls(url.hostname, url.port, timeout=self.connect_timeout)
            try:
                connection.connect()
            except BaseException:
                connection.close()
                raise
        return PreparedHTTP(self, endpoint, connection, time.monotonic() - started, key)

    def release(self, endpoint, connection, reusable):
        with self.lock:
            limit = endpoint.cap if self.limits is None else self.limits.get(endpoint.identity, 0)
            if reusable and len(self.idle[endpoint.identity]) < limit:
                self.idle[endpoint.identity].append(connection)
            else:
                connection.close()

    def close(self):
        with self.lock:
            for connections in self.idle.values():
                for connection in connections:
                    connection.close()
            self.idle.clear()
