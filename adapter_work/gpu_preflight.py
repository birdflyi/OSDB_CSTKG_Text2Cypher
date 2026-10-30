"""Read-only GPU/material intake. Never installs packages or loads model weights."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import shutil
import subprocess


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def inspect_model(spec):
    root = Path(spec['path'])
    problems = []
    checked = {}
    for name, expected in spec['file_sha256'].items():
        path = root / name
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('Model file must stay within model directory')
        actual = digest(path) if path.is_file() else None
        checked[name] = actual
        if actual != expected:
            problems.append('Missing or changed file: ' + name)
    index = root / 'model.safetensors.index.json'
    if index.is_file():
        shards = sorted(set(json.loads(index.read_text())['weight_map'].values()))
    else:
        shards = ['model.safetensors']
    for name in shards:
        path = root / name
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('Weight shard escapes model directory')
        if not path.is_file() or path.stat().st_size == 0:
            problems.append('Missing or empty weight shard: ' + name)
    return dict(name=spec['name'], path=str(root), metadata_hashes=checked,
                weight_shards=shards, weight_content_verified=False, problems=problems)


def inspect(spec):
    problems = []
    packages = {}
    for name, expected in spec['packages'].items():
        try:
            actual = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            actual = None
        packages[name] = actual
        if actual != expected:
            problems.append('Package version mismatch: ' + name)
    if platform.python_version() != spec['python_version']:
        problems.append('Python version mismatch')
    marker = Path(spec['shared_file']['path'])
    shared_hash = digest(marker) if marker.is_file() else None
    if shared_hash != spec['shared_file']['sha256']:
        problems.append('Shared storage marker missing or changed')
    executable = shutil.which('nvidia-smi')
    gpu = None
    if executable:
        try:
            result = subprocess.run([executable, '--query-gpu=index,name,memory.total,memory.used,utilization.gpu',
                                     '--format=csv,noheader'], capture_output=True, text=True, timeout=15)
            gpu = dict(returncode=result.returncode, devices=result.stdout.strip(), error=result.stderr.strip())
            if result.returncode or not result.stdout.strip():
                problems.append('GPU query failed or no devices')
        except (OSError, subprocess.TimeoutExpired) as error:
            problems.append('GPU query failed: ' + type(error).__name__)
    else:
        problems.append('nvidia-smi not found')
    models = [inspect_model(model) for model in spec['models']]
    problems.extend(problem for model in models for problem in model['problems'])
    return dict(hostname=platform.node(), python=platform.python_version(), packages=packages,
                shared_sha256=shared_hash, gpu=gpu, models=models, problems=problems,
                intake_passed=not problems, training_authorized=False,
                full_weight_hash_audit_complete=False, inference_protocol_verified=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = inspect(json.loads(args.config.read_text(encoding='utf-8-sig')))
    with args.output.open('x', encoding='utf8') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(dict(intake_passed=result['intake_passed'], problems=result['problems'])))
