from __future__ import annotations


def derive_agg_fields(row, repo_id):
    return {
        "src_entity_id_agg": row.get("src_entity_id"),
        "src_entity_type_agg": row.get("src_entity_type"),
        "tar_entity_id_agg": row.get("tar_entity_id"),
        "tar_entity_type_agg": row.get("tar_entity_type"),
        "source_repo_id": str(repo_id),
    }

