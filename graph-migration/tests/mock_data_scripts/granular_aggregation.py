from __future__ import annotations


def granu_agg(row, repo_id):
    out = row.copy()
    out["src_entity_id_agg"] = out.get("src_entity_id")
    out["src_entity_type_agg"] = out.get("src_entity_type")
    out["tar_entity_id_agg"] = out.get("tar_entity_id")
    out["tar_entity_type_agg"] = out.get("tar_entity_type")
    out["source_repo_id"] = str(repo_id)
    return out


def set_entity_type_fine_grained(row):
    out = row.copy()
    tar_id = str(out.get("tar_entity_id_agg") or out.get("tar_entity_id") or "")
    rel = str(out.get("relation_type") or "")
    if rel != "Reference":
        out["tar_entity_type_fine_grained"] = None
        return out
    if tar_id.startswith("http"):
        out["tar_entity_type_fine_grained"] = "IssueURL"
    elif "commit:" in tar_id:
        out["tar_entity_type_fine_grained"] = "CommitSHA"
    else:
        out["tar_entity_type_fine_grained"] = "UnknownReferenceTarget"
    return out

