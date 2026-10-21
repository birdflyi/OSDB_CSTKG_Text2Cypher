"""Bounded partial tar stream from a fixed public dataset version.

Extract only four predeclared files; no tar path is used as an output path.
Individual extracted bytes are hashed. The whole archive checksum is NOT
verified because downloading stops after the last requested member.
"""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile

import requests


CANDIDATES = {'taosdata_TDengine_2023.csv': 33068818, 'gridgain_gridgain_2023.csv': 2767766,
              'apache_iotdb_2023.csv': 33462458, 'elastic_elasticsearch_2023.csv': 78276824}
URL = 'https://zenodo.org/api/records/18817348/files/github_osdb_data-GH_CoRE-2.3.0-Pub-Alpha.tar.gz/content'


class BoundedStream:
    def __init__(self, raw, maximum):
        self.raw, self.maximum, self.bytes_read = raw, maximum, 0
        self.sha = hashlib.sha256()

    def read(self, size=-1):
        if size < 0 or self.bytes_read + size > self.maximum:
            raise ValueError('Compressed prefix budget exhausted')
        chunk = self.raw.read(size)
        self.bytes_read += len(chunk)
        self.sha.update(chunk)
        return chunk


def run(output):
    out = Path(output)
    if out.exists():
        raise FileExistsError('Do not overwrite source artifacts')
    out.mkdir(parents=True)
    manifest = {'mode': 'PRE_PREDICTION_CANDIDATE_COLLECTION', 'record': 18817348,
                'source_url': URL, 'selected_before_predictions': list(CANDIDATES),
                'selection': 'Existing archive inventory: small gridgain, medium TDengine/IoTDB, larger Elasticsearch. No candidate model outputs consulted.',
                'whole_archive_checksum_verified': False, 'files': {}}
    try:
        with requests.get(URL, stream=True, timeout=(10, 90)) as response:
            response.raise_for_status()
            stream = BoundedStream(response.raw, 64 * 1024 * 1024)
            with tarfile.open(fileobj=stream, mode='r|gz') as archive:
                for member in archive:
                    leaf = member.name.rsplit('/', 1)[-1]
                    if leaf not in CANDIDATES or not member.isfile():
                        continue
                    if member.size != CANDIDATES[leaf] or leaf in manifest['files']:
                        raise ValueError('Unexpected or duplicate archive member')
                    sha, size = hashlib.sha256(), 0
                    target = out / (leaf + '.partial')
                    with archive.extractfile(member) as source, target.open('xb') as dest:
                        while True:
                            chunk = source.read(65536)
                            if not chunk:
                                break
                            dest.write(chunk)
                            sha.update(chunk)
                            size += len(chunk)
                    if size != member.size:
                        raise ValueError('Truncated candidate file')
                    target.rename(out / leaf)
                    manifest['files'][leaf] = {'member': member.name, 'bytes': size, 'sha256': sha.hexdigest()}
                    print('EXTRACTED ' + leaf + ' ' + str(size), flush=True)
                    if len(manifest['files']) == len(CANDIDATES):
                        break
            manifest['compressed_prefix_bytes'] = stream.bytes_read
            manifest['compressed_prefix_sha256'] = stream.sha.hexdigest()
        if len(manifest['files']) != len(CANDIDATES):
            raise ValueError('Required candidate missing')
        manifest['status'] = 'COMPLETE_REQUESTED_MEMBERS'
    except Exception as exc:
        manifest['status'] = 'INCOMPLETE'
        manifest['error_type'] = type(exc).__name__
        raise
    finally:
        (out / 'source_manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True)
    run(p.parse_args().output)
