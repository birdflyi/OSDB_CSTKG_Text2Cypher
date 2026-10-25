"""SHA-only allowlist for unchanged Windows evidence paths on the server.

Never rewrites request contents, answers, queries, JSON values or frozen files.
"""
import hashlib
import json
from pathlib import Path, PureWindowsPath


class FrozenEvidenceResolver:
    def __init__(self, packet_root, freeze_path):
        self.root=Path(packet_root).resolve()
        freeze=json.loads(Path(freeze_path).read_text(encoding='utf-8'))
        self.expected=freeze['evidence_files']
        self.mapping=json.loads((self.root/'control/evidence_path_map.json').read_text(encoding='utf-8'))
        if set(self.mapping)!=set(self.expected):
            raise ValueError('Evidence allowlist differs from original freeze')
        for name in self.mapping:self.sha(name)

    def resolve(self, original):
        name=str(original)
        if name in self.mapping:
            relative=Path(self.mapping[name])
            path=(self.root/relative).resolve()
            if relative.is_absolute() or self.root not in path.parents or relative.parts[0]!='evidence':
                raise ValueError('Evidence mapping escapes admitted packet')
            return path
        if PureWindowsPath(name).is_absolute():
            raise ValueError('Unlisted Windows evidence path')
        return Path(original)

    def sha(self, original):
        actual=hashlib.sha256(self.resolve(original).read_bytes()).hexdigest()
        expected=self.expected.get(str(original))
        if expected and actual!=expected:raise ValueError('Frozen evidence hash differs')
        return actual
