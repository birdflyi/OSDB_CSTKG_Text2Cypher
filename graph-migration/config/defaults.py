from __future__ import annotations

from typing import Any

PLACEHOLDER_VALUES = {"", "nan", "none", "null", "na", "n/a", "unknown"}

DEFAULT_NODE_TYPE_MAP: dict[str, str] = {
    "user": "Actor",
    "actor": "Actor",
    "developer": "Actor",
    "issue": "Issue",
    "issue_comment": "IssueComment",
    "comment": "IssueComment",
    "pull_request": "PullRequest",
    "pr": "PullRequest",
    "repo": "Repo",
    "repository": "Repo",
    "commit": "Commit",
    "object": "Object",
    "artifact": "Object",
    "url": "ExternalResource",
    "link": "ExternalResource",
}

# TODO(dataset-specific): Add source-specific terms and relation vocabulary.
DEFAULT_RELATION_RULES: list[dict[str, Any]] = [
    {"contains_any": ["open", "opened"], "maps_to": "OPENED_BY"},
    {"contains_any": ["comment", "commented"], "maps_to": "COMMENTED_ON"},
    {"contains_any": ["refer", "reference"], "maps_to": "REFERS_TO"},
    {"contains_any": ["resolve", "fix", "close"], "maps_to": "RESOLVES"},
    {"contains_any": ["belong", "in_repo", "repo"], "maps_to": "BELONGS_TO"},
    {"contains_any": ["couple", "cochange", "co-change"], "maps_to": "COUPLES_WITH"},
    {"contains_any": ["mention", "@", "cite"], "maps_to": "MENTIONS"},
]

DEFAULT_RELATION_FALLBACK = "REFERS_TO"

DEFAULT_ALLOWED_REL_VOCAB = {
    "OPENED_BY",
    "COMMENTED_ON",
    "REFERS_TO",
    "RESOLVES",
    "BELONGS_TO",
    "COUPLES_WITH",
    "MENTIONS",
}

