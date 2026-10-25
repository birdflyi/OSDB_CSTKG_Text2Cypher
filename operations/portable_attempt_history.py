"""Copy immutable history; reuse the ORIGINAL AttemptBudget implementation.

Preparation and verified mirror primitives only. No dispatch or execution lease.
Never use a draft copy as authority without a fresh fenced cutover admission.
"""
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import shutil


class HistoryBlocked(RuntimeError):
    pass


def native_path(path):
    """Windows extended-length access only; no stored ledger paths are changed."""
    value = Path(path).resolve()
    if os.name == 'nt' and not str(value).startswith('\\\\?\\'):
        if str(value).startswith('\\\\'):
            return Path('\\\\?\\UNC\\' + str(value)[2:])
        return Path('\\\\?\\' + str(value))
    return value


def sha(path):
    return hashlib.sha256(native_path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(native_path(path).read_text(encoding='utf-8'))


def create_json(path, value):
    with native_path(path).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def copy_exact(source, target, expected):
    source, target = native_path(source), native_path(target)
    if sha(source) != expected:
        raise HistoryBlocked('Source changed before copy')
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if not target.is_file() or sha(target) != expected:
            raise HistoryBlocked('Immutable destination conflict')
        return
    # A failed/partial create must be audited, never silently overwritten.
    with source.open('rb') as src, target.open('xb') as dst:
        shutil.copyfileobj(src, dst)
        dst.flush()
        os.fsync(dst.fileno())
    if sha(target) != expected or sha(source) != expected:
        raise HistoryBlocked('Copy verification failed')


def portable_mapping(stored):
    value = PureWindowsPath(stored)
    if (not value.is_absolute() or len(value.drive) != 2 or value.drive[1] != ':' or
            '..' in value.parts):
        raise HistoryBlocked('Only explicit absolute Windows historical paths supported')
    # Parent mapping avoids deep duplicated Windows paths; original slot untouched.
    parent = str(value.parent)
    tag = hashlib.sha256(parent.casefold().encode('utf-8')).hexdigest()[:24]
    return parent, Path('history') / ('p_' + tag), value.name


def clone_history(original, destination):
    """Read-only draft snapshot; source lock/lease are deliberately not acquired.

    Double-check every byte plus the live counter. Final handover must additionally
    fence the original writer and pin the resulting manifest in an admission.
    """
    destination = native_path(destination)
    if os.name == 'nt' and len(str(destination)) + 70 > 245:
        raise HistoryBlocked('Clone root too long for unchanged original counter; select a shorter root')
    before = original.inspect()
    if before['pending']:
        raise HistoryBlocked('Pending history cannot be cloned for execution')
    if (original.ledger / '.t1-reservation.lock').exists():
        raise HistoryBlocked('Actual writer/fence exists; audit before draft snapshot')
    destination.mkdir(parents=True, exist_ok=False)
    (destination / 'ledger').mkdir()
    evidence, sources, rewrites = {}, {}, {}

    def add(source, relative):
        relative = Path(relative).as_posix()
        digest = sha(source)
        if relative in evidence and evidence[relative] != digest:
            raise HistoryBlocked('History mapping collision')
        copy_exact(source, destination / relative, digest)
        evidence[relative] = digest
        sources[str(source)] = digest

    add(original.approval, 'approval.json')
    slots = sorted(original.ledger.glob('*.json'), key=lambda p: int(p.stem))
    for slot in slots:
        record = read(slot)
        request = original._request(record['request_path']).resolve()
        parent, relative_parent, name = portable_mapping(record['request_path'])
        target = relative_parent / name
        rewrites[parent] = relative_parent.as_posix()
        add(slot, Path('ledger') / slot.name)
        add(request, target)
        add(original.terminal_path(request),
            target.with_name(original.terminal_path(request).name))
    if original.inspect() != before:
        raise HistoryBlocked('History changed during snapshot')
    for source, expected in sources.items():
        if sha(source) != expected:
            raise HistoryBlocked('Historical bytes changed during snapshot')
    manifest = {'kind': 'DRAFT_FULL_ATTEMPT_HISTORY_CLONE_V1',
                'execution_authority': False, 'reserved': before['reserved'],
                'cap': before['cap'], 'pending': [],
                'rewrites': [[source, rewrites[source]]
                             for source in sorted(rewrites, key=len, reverse=True)],
                'files': evidence}
    create_json(destination / 'manifest.json', manifest)
    cloned = open_clone(type(original), destination, sha(destination / 'manifest.json'))
    state = cloned.inspect()
    for key in ('reserved', 'cap', 'remaining', 'pending'):
        if state[key] != before[key]:
            raise HistoryBlocked('Original counter differs on copied history')
    return manifest


def open_clone(budget_class, folder, expected_manifest_sha, *, new_rewrites=()):
    folder = native_path(folder)
    if sha(folder / 'manifest.json') != expected_manifest_sha:
        raise HistoryBlocked('Unpinned manifest')
    manifest = read(folder / 'manifest.json')
    if manifest.get('kind') != 'DRAFT_FULL_ATTEMPT_HISTORY_CLONE_V1':
        raise HistoryBlocked('Not a complete historical clone')
    for relative, expected in manifest['files'].items():
        path = (folder / relative).resolve()
        if folder.resolve() not in path.parents or sha(path) != expected:
            raise HistoryBlocked('Clone missing or modified evidence')
    return budget_class(folder / 'ledger', folder / 'approval.json',
                        path_rewrites=list(new_rewrites) + [(source, str(folder / target))
                            for source, target in sorted(manifest['rewrites'],
                                                         key=lambda pair: len(pair[0]), reverse=True)])


def prepare_delta(source_budget, baseline_count, requests_root, destination):
    """Export ONLY fully terminal new physical attempts, retaining raw slots.

    Source can be remote after secure transfer, but this function opens no network.
    Unknown requests fail closed; it never re-sends or manufactures a terminal.
    """
    state = source_budget.inspect()
    if state['pending'] or type(baseline_count) is not int or not 0 <= baseline_count <= state['reserved']:
        raise HistoryBlocked('Unknown attempt or invalid baseline')
    source_root_spelling = str(Path(requests_root).resolve())
    requests_root, destination = native_path(requests_root), native_path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    slots, files = [], {}
    for path in sorted(source_budget.ledger.glob('*.json'), key=lambda p: int(p.stem)):
        number = int(path.stem)
        if number <= baseline_count:
            continue
        record = read(path)
        request = native_path(source_budget._request(record['request_path']))
        if requests_root not in request.parents:
            raise HistoryBlocked('New request outside admitted source root')
        terminal = source_budget.terminal_path(request)
        request_relative = request.relative_to(requests_root).as_posix()
        for src, target in [(path, 'ledger/' + path.name),
                            (request, 'requests/' + request_relative),
                            (terminal, 'requests/' + terminal.relative_to(requests_root).as_posix())]:
            digest = sha(src)
            copy_exact(src, destination / target, digest)
            files[target] = digest
        slots.append({'number': number, 'slot': 'ledger/' + path.name,
                      'request': 'requests/' + request_relative,
                      'terminal': 'requests/' + terminal.relative_to(requests_root).as_posix()})
    if source_budget.inspect() != state:
        raise HistoryBlocked('Live source changed during delta export')
    value = {'kind': 'TERMINAL_ATTEMPT_MIRROR_DELTA_V1', 'baseline': baseline_count,
             'total': state['reserved'], 'cap': state['cap'],
             'approval_sha256': sha(source_budget.approval),
             'source_requests_root': source_root_spelling, 'slots': slots, 'files': files}
    create_json(destination / 'manifest.json', value)
    return value


def apply_delta(budget_class, clone_root, clone_manifest_sha, delta_root, delta_sha):
    """Apply into a separate mirror, NEVER call this against active authority.

    Verify all transferred bytes first; then requests/terminals; then raw slots.
    Idempotent matching slots are allowed; conflicting evidence blocks. A caller
    must hold its execution/mirror fence; distributed release is an external gate.
    """
    clone_root, delta_root = native_path(clone_root), native_path(delta_root)
    if sha(delta_root / 'manifest.json') != delta_sha:
        raise HistoryBlocked('Unpinned transferred delta')
    delta = read(delta_root / 'manifest.json')
    target_requests = clone_root / 'incoming_attempts'
    mapping = [(delta['source_requests_root'], str(target_requests))]
    mirror = open_clone(budget_class, clone_root, clone_manifest_sha, new_rewrites=mapping)
    before = mirror.inspect()
    if (delta.get('kind') != 'TERMINAL_ATTEMPT_MIRROR_DELTA_V1' or before['pending'] or
            not delta['baseline'] <= before['reserved'] <= delta['total'] or
            delta['cap'] != before['cap'] or
            delta['approval_sha256'] != sha(mirror.approval) or
            [s['number'] for s in delta['slots']] != list(range(delta['baseline'] + 1, delta['total'] + 1))):
        raise HistoryBlocked('Mirror count, approval or sequence conflict')
    for relative, expected in delta['files'].items():
        path = (delta_root / relative).resolve()
        if delta_root.resolve() not in path.parents or sha(path) != expected:
            raise HistoryBlocked('Transferred evidence missing or modified')
    # Validate every slot against its actual request/terminal before copying slots.
    for item in delta['slots']:
        record = read(delta_root / item['slot'])
        mapped = budget_class(mirror.ledger, mirror.approval, path_rewrites=mapping)
        relative = Path(item['request']).relative_to('requests')
        expected_request = (target_requests / relative).resolve()
        if (int(Path(item['slot']).stem) != item['number'] or
                target_requests.resolve() not in expected_request.parents or
                Path(item['slot']).parent.as_posix() != 'ledger' or
                Path(item['request']).parts[0] != 'requests' or
                mapped._request(record['request_path']).resolve() != expected_request or
                record.get('request_sha256') != sha(delta_root / item['request']) or
                record.get('approval_sha256') != sha(mirror.approval) or
                record.get('approved_cap_at_reservation') != before['cap'] or
                Path(item['terminal']) != Path(item['request']).with_name('terminal.json')):
            raise HistoryBlocked('Delta slot/request binding invalid')
        terminal = read(delta_root / item['terminal'])
        if not isinstance(terminal, dict) or not any(k in terminal for k in ('http_status', 'transport_error')):
            raise HistoryBlocked('Unknown or malformed transferred terminal')
    # Staged files always precede the ledger pointer that charges them.
    for item in delta['slots']:
        for field in ('request', 'terminal'):
            relative = Path(item[field]).relative_to('requests')
            copy_exact(delta_root / item[field], target_requests / relative, delta['files'][item[field]])
    for item in delta['slots']:
        relative = item['slot']
        copy_exact(delta_root / relative, mirror.ledger / Path(relative).name, delta['files'][relative])
    after = mirror.inspect()
    if after['reserved'] != delta['total'] or after['pending']:
        raise HistoryBlocked('Mirrored cumulative count differs')
    return mirror
