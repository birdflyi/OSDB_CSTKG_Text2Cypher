"""Install a private CPU-only Neo4j development runtime under an owned root.

No system packages, GPU/model services, or model API calls are changed.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tarfile
from urllib.request import urlopen

NEO4J_VERSION = '5.26.32'


def download(url, target, expected_hash):
    if not target.exists():
        partial = target.with_suffix(target.suffix + '.partial')
        with urlopen(url, timeout=60) as src, partial.open('wb') as dest:
            shutil.copyfileobj(src, dest)
        partial.rename(target)
    actual = hashlib.sha256(target.read_bytes()).hexdigest()
    if actual != expected_hash:
        raise ValueError('Archive checksum mismatch: ' + target.name)
    return {'url': url, 'sha256': actual, 'bytes': target.stat().st_size}


def unpack(archive, destination):
    destination.mkdir(exist_ok=True)
    with tarfile.open(archive) as bundle:
        # Python 3.12: reject traversal and unsafe special files/links.
        bundle.extractall(destination, filter='data')


def main(root):
    root = root.resolve()
    if root.parent != Path('/mnt/data/louzehua') or root.name != 'ch7_t1_public_benchmark_20261023':
        raise ValueError('Unexpected remote experiment root')
    root.mkdir(exist_ok=True)
    owner_file = root / 'execution_owner.json'
    if owner_file.exists():
        if json.loads(owner_file.read_text())['hostname'] != socket.gethostname():
            raise ValueError('Shared directory belongs to a different execution host')
    else:
        with owner_file.open('x') as owner:
            json.dump({'hostname': socket.gethostname()}, owner)
    marker = root / 'OWNED_CH7_T1'
    marker.touch(exist_ok=True)
    downloads = root / 'downloads'
    downloads.mkdir(exist_ok=True)
    for port in (17687,):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', port))

    # Persist package metadata so subsequent runs do not silently choose a new Java.
    java_metadata = downloads / 'java21.json'
    if not java_metadata.exists():
        url = ('https://api.adoptium.net/v3/assets/latest/21/hotspot'
               '?architecture=x64&image_type=jre&os=linux&vendor=eclipse')
        with urlopen(url, timeout=60) as response:
            java_metadata.write_bytes(response.read())
    java_info = json.loads(java_metadata.read_text())[0]
    package = java_info['binary']['package']
    print('Downloading verified Java runtime', flush=True)
    java_receipt = download(package['link'], downloads / package['name'], package['checksum'])
    java_dir = root / 'java'
    if not java_dir.exists():
        unpack(downloads / package['name'], java_dir)
    java_home = next(path.parent.parent for path in java_dir.glob('*/bin/java'))

    name = 'neo4j-community-' + NEO4J_VERSION + '-unix.tar.gz'
    url = 'https://dist.neo4j.org/' + name
    checksum_file = downloads / (name + '.sha256')
    if not checksum_file.exists():
        with urlopen(url + '.sha256', timeout=60) as response:
            checksum_file.write_bytes(response.read())
    checksum = checksum_file.read_text().split()[0]
    print('Downloading verified Neo4j archive', flush=True)
    neo4j_receipt = download(url, downloads / name, checksum)
    runtime_dir = root / 'runtime'
    neo4j_home = runtime_dir / ('neo4j-community-' + NEO4J_VERSION)
    if not neo4j_home.exists():
        unpack(downloads / name, runtime_dir)
    config = '\n'.join([
        'server.default_listen_address=127.0.0.1',
        'server.bolt.enabled=true',
        'server.bolt.listen_address=127.0.0.1:17687',
        'server.bolt.advertised_address=127.0.0.1:17687',
        'server.http.enabled=false', 'server.https.enabled=false',
        'server.memory.heap.initial_size=512m', 'server.memory.heap.max_size=2g',
        'server.memory.pagecache.size=1g',
        'server.jvm.additional=-XX:ActiveProcessorCount=4',
        'db.transaction.timeout=60s', 'dbms.security.auth_enabled=true',
        'dbms.usage_report.enabled=false', '',
    ])
    (neo4j_home / 'conf/neo4j.conf').write_text(config)
    env = dict(os.environ, JAVA_HOME=str(java_home))
    private_file = root / 'db_connection.json'
    if not private_file.exists():
        credential = {'uri': 'bolt://127.0.0.1:17687', 'user': 'neo4j',
                      'password': secrets.token_urlsafe(32)}
        fd = os.open(private_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as dest:
            json.dump(credential, dest)
        subprocess.run([str(neo4j_home / 'bin/neo4j-admin'), 'dbms',
                        'set-initial-password', credential['password']],
                       env=env, check=True, capture_output=True)
    venv = root / '.venv'
    if not (venv / 'bin/python').exists():
        subprocess.run(['uv', 'venv', str(venv), '--python', 'python3'], check=True)
    package_options = []
    if (root / 'wheels').exists():
        package_options = ['--no-index', '--find-links', str(root / 'wheels')]
    subprocess.run(['uv', 'pip', 'install', '--python', str(venv / 'bin/python'),
                    *package_options, 'neo4j==5.24.0', 'pydantic==2.9.2'], check=True)
    report = {
        'status': 'RUNTIME_INSTALLED_NOT_STARTED', 'neo4j_version': NEO4J_VERSION,
        'execution_hostname': socket.gethostname(),
        'java_release': java_info['release_name'], 'java_home': str(java_home),
        'neo4j_home': str(neo4j_home), 'python': str(venv / 'bin/python'),
        'bind_address': '127.0.0.1', 'bolt_port': 17687,
        'java_download': java_receipt, 'neo4j_download': neo4j_receipt,
        'model_calls': 0, 'gpu_allocations': 0,
        'protocol_note': 'Native development deployment, not yet certified equivalent to official Docker loader.',
    }
    (root / 'runtime_receipt.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    main(parser.parse_args().root)
