"""Explicit prompt profiles independent of drive layout."""
import hashlib
import json
from pathlib import Path
from common.errors import IntegrityError

DEFAULT_PROFILES = Path(__file__).resolve().parents[1] / "prompts" / "ours.json"
DIRECT_PROFILES = DEFAULT_PROFILES.with_name("direct.json")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def compose(benchmark, payload, profile_path):
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))[benchmark]
    prefix = profile["prefix_messages"]
    if hashlib.sha256(canonical(prefix)).hexdigest() != profile["prefix_sha256"]:
        raise IntegrityError("Prompt profile changed: " + benchmark)
    if "messages" in payload:
        public = payload["messages"]
    else:
        document = {key: payload[key] for key in ("graph", "question", "schema")}
        public = [{"role": "user", "content": canonical(document).decode("utf-8")}]
    return tuple(dict(message) for message in prefix + public)


def compose_ours(benchmark, payload, profile_path=DEFAULT_PROFILES):
    return compose(benchmark, payload, profile_path)


def compose_direct(benchmark, payload, profile_path=DIRECT_PROFILES):
    return compose(benchmark, payload, profile_path)
