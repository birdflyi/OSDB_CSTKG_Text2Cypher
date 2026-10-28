from engine import mirror


def test_publisher_reuses_one_channel_and_sends_only_newest_frames(tmp_path, monkeypatch):
    values = [
        dict(sequence=1, progress=dict(phase="运行中")),
        dict(sequence=2, progress=dict(phase="完成")),
    ]

    class Reader:
        def snapshot(self, *args):
            return values.pop(0)

    monkeypatch.setattr(mirror, "SnapshotReader", Reader)
    clients = []
    sent = []

    class Client:
        def __init__(self, *args, **kwargs):
            assert kwargs["expected_ready"] == dict(role="dashboard-publisher", hostname="owned")
            clients.append(self)
            self.closed = False

        def request(self, value, timeout):
            sent.append(value["snapshot"]["sequence"])
            return dict(status="saved")

        def close(self):
            self.closed = True

    mirror.publish(
        "plan",
        tmp_path / "run",
        ["ssh", "host"],
        "/owned/wall.json",
        interval=0,
        expected_hostname="owned",
        client_factory=Client,
    )
    assert len(clients) == 1 and clients[0].closed and sent == [1, 2]


def test_channel_failure_reconnects_without_replaying_an_old_snapshot(tmp_path, monkeypatch):
    values = [
        dict(sequence=1, progress=dict(phase="运行中")),
        dict(sequence=2, progress=dict(phase="完成")),
    ]

    class Reader:
        def snapshot(self, *args):
            return values.pop(0)

    monkeypatch.setattr(mirror, "SnapshotReader", Reader)
    clients = []
    sent = []

    class Client:
        def __init__(self, *args, **kwargs):
            clients.append(self)
            self.closed = False

        def request(self, value, timeout):
            sent.append(value["snapshot"]["sequence"])
            if len(clients) == 1:
                raise OSError("synthetic disconnect")
            return dict(status="saved")

        def close(self):
            self.closed = True

    mirror.publish(
        "plan",
        tmp_path / "run",
        ["ssh", "host"],
        "/owned/wall.json",
        interval=0,
        client_factory=Client,
    )
    assert len(clients) == 2 and all(c.closed for c in clients) and sent == [1, 2]
