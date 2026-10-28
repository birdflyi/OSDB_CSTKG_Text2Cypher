"""Prepare isolated native graph directories; never start a DB or model client.

File copies and digests identify offline material, not a live immutable graph.
Startup, authentication, read-only verification and resource admission remain
separate runtime work. Old stores, old configurations and failed copies stay.
"""

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import socket

from property_graph_input_contract_dev_20261029 import encoded, digest


STORE_PARTS = ('data/databases/neo4j','transactions/neo4j')


class GraphPreparationError(ValueError):
    pass


def _owned(root):
    root = Path(root).resolve(strict=True)
    owner = json.loads((root/'execution_owner.json').read_bytes())
    if owner.get('hostname') != socket.gethostname():
        raise GraphPreparationError('Preparation root belongs to another host')
    return root


def _inside(root, path):
    path = Path(path)
    # Do not follow directory or file symlinks even when they point inside root.
    current = path.absolute()
    while current != current.parent:
        if current.is_symlink():
            raise GraphPreparationError('Preparation paths must not use symlinks')
        current = current.parent
    resolved = path.resolve()
    resolved.relative_to(root)
    return resolved


def _files(root):
    files = []
    for relative in STORE_PARTS:
        folder = root/relative
        if folder.exists():
            for path in folder.rglob('*'):
                _inside(root,path)
                if path.is_file():files.append(path)
                elif not path.is_dir():
                    raise GraphPreparationError('Only regular store files are supported')
    return sorted(files)


def _fingerprint(stat):
    return stat.st_dev,stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns


def store_snapshot(root):
    """Hash offline files, checking file identity/metadata and population stability.

    These checks detect observed changes; they are not an atomic filesystem
    snapshot or proof that no writer can modify a future running database.
    """
    root = Path(root).resolve(strict=True)
    metadata = root/'data/databases/neo4j/neostore'
    if not metadata.is_file() or metadata.stat().st_size == 0:
        raise GraphPreparationError('Missing nonempty native store metadata')
    paths = _files(root)
    records = {}
    for path in paths:
        before = path.stat()
        hasher = hashlib.sha256()
        with path.open('rb') as stream:
            if _fingerprint(os.fstat(stream.fileno())) != _fingerprint(before):
                raise GraphPreparationError('Store changed while opening')
            for block in iter(lambda:stream.read(1024*1024),b''):hasher.update(block)
            after = os.fstat(stream.fileno())
        if _fingerprint(before) != _fingerprint(after) or _fingerprint(before) != _fingerprint(path.stat()):
            raise GraphPreparationError('Store changed while hashing')
        records[path.relative_to(root).as_posix()] = dict(bytes=before.st_size,sha256=hasher.hexdigest())
    if _files(root) != paths:
        raise GraphPreparationError('Store file population changed while hashing')
    body = dict(profile='offline-native-store-tree-v1',files=records)
    return dict(body,snapshot_sha256=digest(encoded(body)),total_bytes=sum(r['bytes'] for r in records.values()))


def _save(path, value):
    raw = encoded(value)+b'\n'
    with path.open('xb') as stream:
        if stream.write(raw) != len(raw):raise OSError('Short preparation record write')
        stream.flush();os.fsync(stream.fileno())


def prepare_graph(spec, target, *, owned_root):
    """Copy only one standard database and its logs into a fresh owned directory.

    No system database, credentials, plugins or hard links are copied. Config
    ports are planned assignments, not reserved/listening ports. Authentication
    remains enabled; private setup and all startup acceptance are pending.
    """
    root = _owned(owned_root)
    source, target = _inside(root,spec['source_store']), _inside(root,target)
    if source == target or source in target.parents or target in source.parents:
        raise GraphPreparationError('Source and target directories must be disjoint')
    if spec['benchmark'] not in ('MindTheQuery','ZOGRASCOPE'):
        raise GraphPreparationError('Unknown prepared graph benchmark')
    if type(spec['graph']) is not str or not re.fullmatch('[A-Za-z0-9_-]+',spec['graph']):
        raise GraphPreparationError('Explicit bounded graph identifier required')
    for key in ('schema_sha256','source_archive_sha256'):
        if type(spec[key]) is not str or not re.fullmatch('[0-9a-f]{64}',spec[key]):
            raise GraphPreparationError('Pinned source/schema SHA256 required')
    port = spec['bolt_port']
    if type(port) is not int or not 1024 <= port <= 65535:
        raise GraphPreparationError('Explicit planned unprivileged Bolt port required')
    # Configuration paths are native absolute Linux paths; no injection/escaping
    # rules are hidden in a shell command. This routine starts no subprocess.
    if any(character in str(target) for character in '\r\n='):
        raise GraphPreparationError('Configuration path contains a line delimiter')
    schema_raw = Path(spec['schema_path']).read_bytes()
    if digest(schema_raw) != spec['schema_sha256']:
        raise GraphPreparationError('Public schema bytes differ from their pinned source')
    schema = json.loads(schema_raw)
    if type(schema) is not dict:raise GraphPreparationError('Expected a public schema object')
    before = store_snapshot(source)
    target.mkdir(parents=True,exist_ok=False)
    _save(target/'execution_owner.json',dict(hostname=socket.gethostname(),purpose='offline graph preparation only'))
    _save(target/'source_snapshot.json',before)
    for relative,record in before['files'].items():
        destination = target/relative
        destination.parent.mkdir(parents=True,exist_ok=True)
        hasher = hashlib.sha256();copied = 0
        with (source/relative).open('rb') as original,destination.open('xb') as output:
            for block in iter(lambda:original.read(1024*1024),b''):
                if output.write(block) != len(block):raise OSError('Short graph copy')
                hasher.update(block);copied += len(block)
            output.flush();os.fsync(output.fileno())
        if copied != record['bytes'] or hasher.hexdigest() != record['sha256']:
            raise GraphPreparationError('Graph changed during copy; partial target retained')
    if store_snapshot(source) != before or store_snapshot(target) != before:
        raise GraphPreparationError('Source or copied graph differs; target retained for audit')
    for name in ('conf','logs','run','plugins'):(target/name).mkdir()
    config = '\n'.join([
        'server.directories.data='+str(target/'data'),
        'server.directories.transaction.logs.root='+str(target/'transactions'),
        'server.directories.logs='+str(target/'logs'),
        'server.directories.run='+str(target/'run'),
        'server.directories.plugins='+str(target/'plugins'),
        'initial.dbms.default_database=neo4j',
        'server.default_listen_address=127.0.0.1',
        'server.bolt.listen_address=127.0.0.1:'+str(port),
        'server.bolt.advertised_address=127.0.0.1:'+str(port),
        'server.http.enabled=false','server.https.enabled=false',
        'dbms.security.auth_enabled=true','server.databases.default_to_read_only=true',
        'server.memory.heap.initial_size=256m','server.memory.heap.max_size=512m',
        'server.memory.pagecache.size=256m',''])
    with (target/'conf/neo4j.conf').open('xb') as stream:
        raw=config.encode('utf-8')
        if stream.write(raw) != len(raw):raise OSError('Short configuration write')
        stream.flush();os.fsync(stream.fileno())
    receipt = dict(kind='OFFLINE_NATIVE_GRAPH_PREPARATION_NOT_RUNNING',benchmark=spec['benchmark'],
        graph=spec['graph'],database='neo4j',source_store=str(source),runtime_directory=str(target),
        snapshot_sha256=before['snapshot_sha256'],files=len(before['files']),bytes=before['total_bytes'],
        schema_sha256=spec['schema_sha256'],schema_content_sha256=digest(encoded(schema)),
        source_archive_sha256=spec['source_archive_sha256'],
        bolt_uri='bolt://127.0.0.1:'+str(port),ports_reserved=False,
        config_sha256=digest(config.encode('utf-8')),authentication_bootstrapped=False,
        copies_use_hardlinks=False,system_database_copied=False,plugins_copied=False,
        readonly_config_requested=True,live_readonly_verified=False,live_immutability_verified=False,
        DB_server_started=False,DB_queries=0,model_calls=0,formal_admission=False)
    _save(target/'preparation.json',receipt)
    return receipt


def bind_public_task(task_id, common, receipt):
    """Bind public task/schema to prepared store metadata, without a gold read.

    The caller must use the previously verified whole-population public loader.
    This is not a complete prompt/cache key or proof of a live DB connection.
    """
    if type(task_id) is not str or not task_id or type(common) is not dict:
        raise GraphPreparationError('Verified public task and identity required')
    if set(common) != {'graph','schema','question'} or common['graph'] != receipt['graph']:
        raise GraphPreparationError('Task graph differs from prepared graph')
    if type(common['question']) is not str or not common['question'].strip():
        raise GraphPreparationError('Original public question is required')
    if digest(encoded(common['schema'])) != receipt['schema_content_sha256']:
        raise GraphPreparationError('Task public schema differs from prepared schema')
    return dict(benchmark=receipt['benchmark'],task_id=task_id,graph=common['graph'],
        common_input_sha256=digest(encoded(common)),snapshot_sha256=receipt['snapshot_sha256'],
        schema_sha256=receipt['schema_sha256'],database=receipt['database'],
        live_graph_verified=False,complete_prompt_binding_verified=False,formal_admission=False)
