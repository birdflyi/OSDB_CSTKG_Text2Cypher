# Entity Aligner Interface Contract

This module provides canonical Repo/Actor alignment for user inputs.

## Frozen API

- `resolve_repo_entity_id(input_str, d_record=None, api_timeout_sec=12) -> dict`
- `resolve_actor_entity_id(input_str, d_record=None, api_timeout_sec=12) -> dict`

Both return a structured payload:

- `entity_id`: canonical id (`R_<repo_id>` or `A_<actor_id>`) or `null`
- `provenance`: one of `{direct_id, local_index, api_fallback, failed}`
- `api_called`: `true/false`
- `error`: nullable error code

## Resolution Policy

1. Canonical prefixed IDs are accepted directly.
2. Local index lookup is preferred (`repo_name_index.csv`, `actor_login_index.csv`).
3. API fallback is used only when local stage fails.

## Pattern Recognition Rule

For Repo/Actor textual forms, pattern matching prefers GH_CoRE rules:

- `GH_CoRE.data_dict_settings.re_ref_patterns`
- pattern type is passed to `get_ent_obj_in_link_text(link_pattern_type, link, d_record)`

This avoids drift from custom regex variants and keeps behavior consistent with GH_CoRE extraction semantics.

