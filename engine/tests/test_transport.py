from __future__ import annotations

import errno
import http.server
import json
import threading
from engine.config import Endpoint
from engine.transport import HTTPTransport, classify, local_resource_error


def test_http_errors_and_partial_write_are_conservative():
    assert classify(dict(http_status=500, write_started=True), "m")["status"] == "unknown"
    assert (
        classify(
            dict(transport_error="timeout", write_started=True, request_write_completed=False), "m"
        )["status"]
        == "unknown"
    )
    assert classify(dict(transport_error="timeout", write_started=False), "m")["status"] == "infra"
    assert classify(dict(http_status=503, retry_after_seconds=1), "m")["status"] == "throttled"
    assert local_resource_error(OSError(errno.EADDRINUSE, "synthetic"))


def test_model_mismatch_empty_length_and_refusal():
    def value(reason="stop", content="text", **message):
        return dict(
            http_status=200,
            body=dict(
                model="m",
                choices=[dict(finish_reason=reason, message=dict(content=content, **message))],
            ),
        )

    assert classify(value(content=""), "m")["status"] == "empty"
    assert classify(value(reason="length", content=None), "m")["status"] == "length"
    assert classify(value(refusal="denied"), "m")["status"] == "refused"
    assert classify(value(), "other")["protocol_error"] == "model_mismatch"


def test_real_loopback_http_connection_reuse_and_exact_wire(tmp_path):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        calls, clients, bodies = 0, [], []

        def do_POST(self):
            Handler.calls += 1
            Handler.clients.append(self.client_address)
            Handler.bodies.append(self.rfile.read(int(self.headers["Content-Length"])))
            data = json.dumps(
                dict(model="m", choices=[dict(finish_reason="stop", message=dict(content="ok"))])
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    (tmp_path / "test.txt").write_text("synthetic-test-key", encoding="utf-8")
    endpoint = Endpoint("a", "http://127.0.0.1:%d/test" % server.server_port, "m", "test", 2)
    transport = HTTPTransport(tmp_path)
    wire = b'{ "model" : "m", "messages" : [] }'
    try:
        for _ in range(2):
            prepared = transport.prepare(endpoint)
            result = prepared.send(wire, 2)
            assert result["status"] == "ok"
            prepared.close()
        assert Handler.calls == 2 and Handler.bodies == [wire, wire]
        assert Handler.clients[0] == Handler.clients[1]
    finally:
        transport.close()
        server.shutdown()
        server.server_close()


def test_hot_removal_closes_idle_and_late_returned_connections(tmp_path):
    class Connection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    from dataclasses import replace

    endpoint = Endpoint("a", "http://127.0.0.1/a", "m", "key", 2)
    transport = HTTPTransport(tmp_path)
    one, two, active = Connection(), Connection(), Connection()
    import time

    transport.idle[endpoint.identity] = [(one, time.monotonic()), (two, time.monotonic())]
    transport.configure([replace(endpoint, cap=1)])
    assert len(transport.idle[endpoint.identity]) == 1 and two.closed
    transport.configure([])
    assert one.closed
    transport.release(endpoint, active, True)
    assert active.closed


def test_server_closed_idle_connection_is_retired_before_any_post(tmp_path):
    import time

    closed = threading.Event()

    class Server(http.server.ThreadingHTTPServer):
        def shutdown_request(self, request):
            super().shutdown_request(request)
            closed.set()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = 0.05
        clients = []
        sessions = []

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            Handler.clients.append(self.client_address)
            Handler.sessions.append(self)
            body = b'{"model":"m","choices":[{"finish_reason":"stop","message":{"content":"ok"}}]}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = Server(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    (tmp_path / "test.txt").write_text("synthetic-key", encoding="utf-8")
    endpoint = Endpoint("a", "http://127.0.0.1:%d/test" % server.server_port, "m", "test", 2)
    transport = HTTPTransport(tmp_path)
    try:
        for n in range(2):
            prepared = transport.prepare(endpoint)
            assert prepared.send(b'{"model":"m"}', 2)["status"] == "ok"
            prepared.close()
            if n == 0:
                assert closed.wait(3), "server did not close the idle connection"
        assert len(Handler.sessions) == 2 and Handler.sessions[0] is not Handler.sessions[1]
        assert transport.started == 2
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
