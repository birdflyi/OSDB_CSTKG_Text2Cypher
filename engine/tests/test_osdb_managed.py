import pytest
from engine.workers import osdb_managed


def test_foreign_lock_is_not_removed(tmp_path,monkeypatch):
    lock=tmp_path/'managed.lock';lock.write_text('another-owner')
    monkeypatch.setattr(osdb_managed,'validate',lambda config:(tmp_path,['unused'],1))
    with pytest.raises(FileExistsError):osdb_managed.run({})
    assert lock.read_text()=='another-owner'


def test_start_failure_releases_only_own_lock(tmp_path,monkeypatch):
    class Probe:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def connect_ex(self,address):return 1
    monkeypatch.setattr(osdb_managed,'validate',lambda config:(tmp_path,['unused'],1))
    monkeypatch.setattr(osdb_managed.socket,'socket',lambda:Probe())
    def fail(*args,**kwargs):raise OSError('start failed')
    monkeypatch.setattr(osdb_managed.subprocess,'Popen',fail)
    with pytest.raises(OSError,match='start failed'):osdb_managed.run({})
    assert not (tmp_path/'managed.lock').exists()
