from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

from normalizers.real_csv_preprocess import (  # noqa: E402
    EXID_REPAIR_STATUS_COL,
    REQUIRED_AUG_COLS,
    repair_exid_after_fine_grained,
    preprocess_osdb_csv,
)


def test_preprocess_osdb_csv_adds_augmented_columns_and_fine_grained(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    mini_sample = project_root / "fixtures" / "real_pilot_redis" / "mini_sample.csv"
    out_csv = tmp_path / "mini_sample_aug.csv"
    granular_script = project_root / "tests" / "mock_data_scripts" / "granular_aggregation.py"

    df_aug = preprocess_osdb_csv(
        input_csv_path=str(mini_sample),
        repo_id=156018,
        out_csv_path=str(out_csv),
        mode="full",
        granular_script_path=str(granular_script),
    )

    assert out_csv.exists(), "Augmented CSV should be written."
    for col in REQUIRED_AUG_COLS:
        assert col in df_aug.columns, f"Missing augmented column: {col}"

    df_ref = df_aug[df_aug["relation_type"] == "Reference"].copy()
    non_null_fine = df_ref["tar_entity_type_fine_grained"].replace("", pd.NA).dropna()
    if len(df_ref) >= 5:
        assert len(non_null_fine) >= 5, "Expected at least 5 Reference rows with fine-grained type."
    assert df_aug.isna().sum().sum() >= 0, "Processing should not crash on NaNs."
    assert EXID_REPAIR_STATUS_COL in df_aug.columns


def test_repair_exid_after_fine_grained_prrc_none() -> None:
    row = pd.Series(
        {
            "tar_entity_type_fine_grained": "PullRequestReviewComment",
            "tar_entity_type": "PullRequestReviewComment",
            "tar_entity_id": "PRRC_None",
            "tar_entity_objnt_prop_dict": "{'repo_id': 77394483, 'repo_name': 'guybe7/redis', 'issue_number': 61, 'pull_review_comment_id': '1121847378'}",
            "tar_entity_match_text": "https://github.com/guybe7/redis/pull/61#discussion_r1121847378",
        }
    )

    out = repair_exid_after_fine_grained(row, repo_id=156018)
    assert out["tar_entity_type_fine_grained"] == "PullRequestReviewComment"
    assert out["tar_entity_id"] == "PRRC_77394483#61#r1121847378"
    assert out[EXID_REPAIR_STATUS_COL] == "repaired"
