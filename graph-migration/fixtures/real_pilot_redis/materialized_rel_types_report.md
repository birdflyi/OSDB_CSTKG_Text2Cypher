# Materialized Relation Types Audit Report

## A) File provenance

- edges_csv_used_abs: `C:\Users\10651\Documents\trae_projects\CSTKG_Modeling_Text2Cypher_MP\code\data_real\pilot_output\csv\edges.csv`
- edges_csv_used_rel: `data_real\pilot_output\csv\edges.csv`
- mtime: `2026-03-05T02:40:31`
- size_bytes: `42429055`
- row_count: `58241`
- detected_columns: `['source_node_uid', 'target_node_uid', 'native_rel_type', 'rel_type', 'service_rel_type', 'properties_json']`

## B) Distinct relation-type distributions (materialized set)

### B1) rel_type distribution

| rel_type     |   count |    ratio |
|:-------------|--------:|---------:|
| EVENT_ACTION |   51319 | 0.881149 |
| REFERENCE    |    6922 | 0.118851 |

### B2) raw_relation_type distribution

| raw_relation_type   |   count |    ratio |
|:--------------------|--------:|---------:|
| EventAction         |   51319 | 0.881149 |
| Reference           |    6922 | 0.118851 |

### B3) Cross-tab: raw_relation_type x rel_type (top pairs)

| raw_relation_type   | rel_type     |   count |
|:--------------------|:-------------|--------:|
| EventAction         | EVENT_ACTION |   51319 |
| Reference           | REFERENCE    |    6922 |

### B4) native_rel_type / service_rel_type distributions (if present)

| native_rel_type   |   count |    ratio |
|:------------------|--------:|---------:|
| EVENT_ACTION      |   51319 | 0.881149 |
| REFERENCE         |    6922 | 0.118851 |

| service_rel_type    |   count |       ratio |
|:--------------------|--------:|------------:|
| COMMENTED_ON_ISSUE  |    9148 | 0.157071    |
| CREATED_BY          |    7814 | 0.134167    |
| OPENED_BY           |    5652 | 0.097045    |
| STARTED_BY          |    5548 | 0.0952594   |
| CREATED_IN          |    4769 | 0.0818839   |
| REVIEWED_ON_BRANCH  |    4275 | 0.0734019   |
| REFERENCES          |    3809 | 0.0654007   |
| COMMENTED_ON_REVIEW |    3505 | 0.060181    |
| MENTIONS            |    2581 | 0.0443159   |
| ADDED_BY            |    1855 | 0.0318504   |
| ADDED_TO            |    1839 | 0.0315757   |
| OWNED_BY            |    1416 | 0.0243128   |
| CLOSED_BY           |     868 | 0.0149036   |
| PULLED_BRANCH_TO    |     596 | 0.0102333   |
| PULLED_BRANCH_FROM  |     596 | 0.0102333   |
| PULLED_REPO_TO      |     596 | 0.0102333   |
| PULLED_REPO_FROM    |     596 | 0.0102333   |
| LINKS_TO            |     532 | 0.00913446  |
| UPDATED_WITH        |     423 | 0.00726292  |
| COMMITTED_AS        |     423 | 0.00726292  |
| MERGED_BY           |     415 | 0.00712556  |
| MERGED_TO           |     415 | 0.00712556  |
| MERGED_AS           |     415 | 0.00712556  |
| REOPENED_BY         |      31 | 0.000532271 |
| CREATED_ON_COMMIT   |      22 | 0.000377741 |
| CREATED_ON_BRANCH   |      22 | 0.000377741 |
| PUBLISHED_BY        |      21 | 0.000360571 |
| PUBLISHED_ON_TAG    |      21 | 0.000360571 |
| COMMENTED_ON_COMMIT |      16 | 0.000274721 |
| BASED_ON_BRANCH     |      12 | 0.00020604  |
| DELETED_BY          |      10 | 0.0001717   |

| native_rel_type   | service_rel_type    |   count |
|:------------------|:--------------------|--------:|
| EVENT_ACTION      | COMMENTED_ON_ISSUE  |    9148 |
| EVENT_ACTION      | CREATED_BY          |    7814 |
| EVENT_ACTION      | OPENED_BY           |    5652 |
| EVENT_ACTION      | STARTED_BY          |    5548 |
| EVENT_ACTION      | CREATED_IN          |    4769 |
| EVENT_ACTION      | REVIEWED_ON_BRANCH  |    4275 |
| REFERENCE         | REFERENCES          |    3809 |
| EVENT_ACTION      | COMMENTED_ON_REVIEW |    3505 |
| REFERENCE         | MENTIONS            |    2581 |
| EVENT_ACTION      | ADDED_BY            |    1855 |
| EVENT_ACTION      | ADDED_TO            |    1839 |
| EVENT_ACTION      | OWNED_BY            |    1416 |
| EVENT_ACTION      | CLOSED_BY           |     868 |
| EVENT_ACTION      | PULLED_REPO_FROM    |     596 |
| EVENT_ACTION      | PULLED_REPO_TO      |     596 |
| EVENT_ACTION      | PULLED_BRANCH_TO    |     596 |
| EVENT_ACTION      | PULLED_BRANCH_FROM  |     596 |
| REFERENCE         | LINKS_TO            |     532 |
| EVENT_ACTION      | UPDATED_WITH        |     423 |
| EVENT_ACTION      | COMMITTED_AS        |     423 |
| EVENT_ACTION      | MERGED_TO           |     415 |
| EVENT_ACTION      | MERGED_BY           |     415 |
| EVENT_ACTION      | MERGED_AS           |     415 |
| EVENT_ACTION      | REOPENED_BY         |      31 |
| EVENT_ACTION      | CREATED_ON_COMMIT   |      22 |
| EVENT_ACTION      | CREATED_ON_BRANCH   |      22 |
| EVENT_ACTION      | PUBLISHED_ON_TAG    |      21 |
| EVENT_ACTION      | PUBLISHED_BY        |      21 |
| EVENT_ACTION      | COMMENTED_ON_COMMIT |      16 |
| EVENT_ACTION      | BASED_ON_BRANCH     |      12 |
| EVENT_ACTION      | DELETED_BY          |      10 |

## C) Classification: materialized vs not materialized (by schema sets)

Classification rules used:
- present in edges.csv and in native set -> `NATIVE_MATERIALIZED`
- present in edges.csv and in allowed service set -> `SERVICE_MATERIALIZED`
- in placeholder list and absent in edges.csv -> `PLACEHOLDER_NOT_MATERIALIZED`
- in allowed service set but absent in edges.csv -> `ALLOWED_BUT_NOT_MATERIALIZED`
- present in edges.csv but not in expected sets -> `UNEXPECTED_IN_OUTPUT`
- presence check uses `rel_type/native_rel_type` for native labels and `service_rel_type` for service labels.

| relation_label         | category                     | present_in_edges_csv   |   count_in_edges_csv | source_of_expectation   |
|:-----------------------|:-----------------------------|:-----------------------|---------------------:|:------------------------|
| LABELED_BY             | ALLOWED_BUT_NOT_MATERIALIZED | false                  |                    0 | schema_metadata         |
| MADE_PUBLIC_BY         | ALLOWED_BUT_NOT_MATERIALIZED | false                  |                    0 | schema_metadata         |
| RECEIVED_REACTION_FROM | ALLOWED_BUT_NOT_MATERIALIZED | false                  |                    0 | schema_metadata         |
| EVENT_ACTION           | NATIVE_MATERIALIZED          | true                   |                51319 | native_minimal          |
| REFERENCE              | NATIVE_MATERIALIZED          | true                   |                 6922 | native_minimal          |
| COUPLES_WITH           | PLACEHOLDER_NOT_MATERIALIZED | false                  |                    0 | placeholder_list        |
| RESOLVES               | PLACEHOLDER_NOT_MATERIALIZED | false                  |                    0 | placeholder_list        |
| ADDED_BY               | SERVICE_MATERIALIZED         | true                   |                 1855 | schema_metadata         |
| ADDED_TO               | SERVICE_MATERIALIZED         | true                   |                 1839 | schema_metadata         |
| BASED_ON_BRANCH        | SERVICE_MATERIALIZED         | true                   |                   12 | schema_metadata         |
| CLOSED_BY              | SERVICE_MATERIALIZED         | true                   |                  868 | schema_metadata         |
| COMMENTED_ON_COMMIT    | SERVICE_MATERIALIZED         | true                   |                   16 | schema_metadata         |
| COMMENTED_ON_ISSUE     | SERVICE_MATERIALIZED         | true                   |                 9148 | schema_metadata         |
| COMMENTED_ON_REVIEW    | SERVICE_MATERIALIZED         | true                   |                 3505 | schema_metadata         |
| COMMITTED_AS           | SERVICE_MATERIALIZED         | true                   |                  423 | schema_metadata         |
| CREATED_BY             | SERVICE_MATERIALIZED         | true                   |                 7814 | schema_metadata         |
| CREATED_IN             | SERVICE_MATERIALIZED         | true                   |                 4769 | schema_metadata         |
| CREATED_ON_BRANCH      | SERVICE_MATERIALIZED         | true                   |                   22 | schema_metadata         |
| CREATED_ON_COMMIT      | SERVICE_MATERIALIZED         | true                   |                   22 | schema_metadata         |
| DELETED_BY             | SERVICE_MATERIALIZED         | true                   |                   10 | schema_metadata         |
| LINKS_TO               | SERVICE_MATERIALIZED         | true                   |                  532 | schema_metadata         |
| MENTIONS               | SERVICE_MATERIALIZED         | true                   |                 2581 | schema_metadata         |
| MERGED_AS              | SERVICE_MATERIALIZED         | true                   |                  415 | schema_metadata         |
| MERGED_BY              | SERVICE_MATERIALIZED         | true                   |                  415 | schema_metadata         |
| MERGED_TO              | SERVICE_MATERIALIZED         | true                   |                  415 | schema_metadata         |
| OPENED_BY              | SERVICE_MATERIALIZED         | true                   |                 5652 | schema_metadata         |
| OWNED_BY               | SERVICE_MATERIALIZED         | true                   |                 1416 | schema_metadata         |
| PUBLISHED_BY           | SERVICE_MATERIALIZED         | true                   |                   21 | schema_metadata         |
| PUBLISHED_ON_TAG       | SERVICE_MATERIALIZED         | true                   |                   21 | schema_metadata         |
| PULLED_BRANCH_FROM     | SERVICE_MATERIALIZED         | true                   |                  596 | schema_metadata         |
| PULLED_BRANCH_TO       | SERVICE_MATERIALIZED         | true                   |                  596 | schema_metadata         |
| PULLED_REPO_FROM       | SERVICE_MATERIALIZED         | true                   |                  596 | schema_metadata         |
| PULLED_REPO_TO         | SERVICE_MATERIALIZED         | true                   |                  596 | schema_metadata         |
| REFERENCES             | SERVICE_MATERIALIZED         | true                   |                 3809 | schema_metadata         |
| REOPENED_BY            | SERVICE_MATERIALIZED         | true                   |                   31 | schema_metadata         |
| REVIEWED_ON_BRANCH     | SERVICE_MATERIALIZED         | true                   |                 4275 | schema_metadata         |
| STARTED_BY             | SERVICE_MATERIALIZED         | true                   |                 5548 | schema_metadata         |
| UPDATED_WITH           | SERVICE_MATERIALIZED         | true                   |                  423 | schema_metadata         |

## D) Notes & next-step guidance

- "Materialized" means the relation label appears in exported edge records (`rel_type`/`native_rel_type` for native; `service_rel_type` for service projection).
- Placeholders (`COUPLES_WITH`, `RESOLVES`) are expected to be absent unless injected by Chapter 5/6 derivation steps.
- `REFERS_TO` does not appear in this run.

## E) Service Track Summary

- edges_csv_used: `C:\Users\10651\Documents\trae_projects\CSTKG_Modeling_Text2Cypher_MP\code\data_real\pilot_output\csv\edges.csv`
- total_rows: `58241`

### E1) service_rel_type distribution (top 30 + NULL/empty)

| service_rel_type    |   count |       ratio |
|:--------------------|--------:|------------:|
| COMMENTED_ON_ISSUE  |    9148 | 0.157071    |
| CREATED_BY          |    7814 | 0.134167    |
| OPENED_BY           |    5652 | 0.097045    |
| STARTED_BY          |    5548 | 0.0952594   |
| CREATED_IN          |    4769 | 0.0818839   |
| REVIEWED_ON_BRANCH  |    4275 | 0.0734019   |
| REFERENCES          |    3809 | 0.0654007   |
| COMMENTED_ON_REVIEW |    3505 | 0.060181    |
| MENTIONS            |    2581 | 0.0443159   |
| ADDED_BY            |    1855 | 0.0318504   |
| ADDED_TO            |    1839 | 0.0315757   |
| OWNED_BY            |    1416 | 0.0243128   |
| CLOSED_BY           |     868 | 0.0149036   |
| PULLED_BRANCH_TO    |     596 | 0.0102333   |
| PULLED_BRANCH_FROM  |     596 | 0.0102333   |
| PULLED_REPO_TO      |     596 | 0.0102333   |
| PULLED_REPO_FROM    |     596 | 0.0102333   |
| LINKS_TO            |     532 | 0.00913446  |
| UPDATED_WITH        |     423 | 0.00726292  |
| COMMITTED_AS        |     423 | 0.00726292  |
| MERGED_BY           |     415 | 0.00712556  |
| MERGED_TO           |     415 | 0.00712556  |
| MERGED_AS           |     415 | 0.00712556  |
| REOPENED_BY         |      31 | 0.000532271 |
| CREATED_ON_COMMIT   |      22 | 0.000377741 |
| CREATED_ON_BRANCH   |      22 | 0.000377741 |
| PUBLISHED_BY        |      21 | 0.000360571 |
| PUBLISHED_ON_TAG    |      21 | 0.000360571 |
| COMMENTED_ON_COMMIT |      16 | 0.000274721 |
| BASED_ON_BRANCH     |      12 | 0.00020604  |
| (NULL_OR_EMPTY)     |       0 | 0           |

### E2) raw_relation_type ? service_rel_type crosstab (top pairs)

| raw_relation_type   | service_rel_type    |   count |
|:--------------------|:--------------------|--------:|
| EventAction         | COMMENTED_ON_ISSUE  |    9148 |
| EventAction         | CREATED_BY          |    7814 |
| EventAction         | OPENED_BY           |    5652 |
| EventAction         | STARTED_BY          |    5548 |
| EventAction         | CREATED_IN          |    4769 |
| EventAction         | REVIEWED_ON_BRANCH  |    4275 |
| Reference           | REFERENCES          |    3809 |
| EventAction         | COMMENTED_ON_REVIEW |    3505 |
| Reference           | MENTIONS            |    2581 |
| EventAction         | ADDED_BY            |    1855 |
| EventAction         | ADDED_TO            |    1839 |
| EventAction         | OWNED_BY            |    1416 |
| EventAction         | CLOSED_BY           |     868 |
| EventAction         | PULLED_REPO_FROM    |     596 |
| EventAction         | PULLED_REPO_TO      |     596 |
| EventAction         | PULLED_BRANCH_TO    |     596 |
| EventAction         | PULLED_BRANCH_FROM  |     596 |
| Reference           | LINKS_TO            |     532 |
| EventAction         | UPDATED_WITH        |     423 |
| EventAction         | COMMITTED_AS        |     423 |
| EventAction         | MERGED_TO           |     415 |
| EventAction         | MERGED_BY           |     415 |
| EventAction         | MERGED_AS           |     415 |
| EventAction         | REOPENED_BY         |      31 |
| EventAction         | CREATED_ON_COMMIT   |      22 |
| EventAction         | CREATED_ON_BRANCH   |      22 |
| EventAction         | PUBLISHED_ON_TAG    |      21 |
| EventAction         | PUBLISHED_BY        |      21 |
| EventAction         | COMMENTED_ON_COMMIT |      16 |
| EventAction         | BASED_ON_BRANCH     |      12 |

### E3) Explicit checks

- contains `RESOLVES` in service_rel_type: `false`
- contains `COUPLES_WITH` in service_rel_type: `false`
- contains legacy `REFERS_TO` in service_rel_type: `false`
