# Index Build Report

- source_csv: `C:\Users\10651\Documents\trae_projects\CSTKG_Modeling_Text2Cypher_MP\code\data_real\redis_redis_2023_aug_exidfix.csv`
- source_rows: `58241`

## repo_name_index.csv

- rows_written: `107`
- distinct_repo_id: `107`
- duplicate_repo_full_name_count: `0`
- missing_repo_id_rate: `0.4862`
- missing_repo_full_name_rate: `0.4862`

## actor_login_index.csv

- rows_written: `340`
- distinct_actor_id: `340`
- duplicate_actor_login_count: `0`
- missing_actor_id_rate: `0.6214`
- missing_actor_login_rate: `0.6214`

Notes:
- Rates are computed best-effort from parsed `tar_entity_objnt_prop_dict` records.
- Missing values in source do not fail index build; unresolved items fall back to API stage at runtime.
