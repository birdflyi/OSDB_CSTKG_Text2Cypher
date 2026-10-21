# E8 holdout execution-data preparation

Completed the execution CSV export of both previously selected projects using the same frozen d1_typed_v1_2 projection. No model predictions were generated on either project. Raw inputs, selection profiles and projection hashes match E4. CSV validation checks unique node/edge identities, endpoints and event-time presence.

| Project | Nodes | Edges | Typed signatures | Missing event time | Dangling edges |
|---|---:|---:|---:|---:|---:|
| IoTDB | 29,425 | 140,313 | 77 | 0 | 0 |
| Elasticsearch | 130,413 | 352,591 | 94 | 0 | 0 |

Receipts and hashes: E8_PROJECTION_RESULTS.json. Full execution CSVs remain outside Git in the private continuation directory. Each project was projected/exported sequentially to limit memory demand. The complete source tar archive checksum was not verified; exact extracted members are hash-verified.

This completes data export and offline integrity checks only. Dedicated DB import, independent task bindings/references and formal transfer predictions remain pending. Do not report a cross-project accuracy score from this preparation. Retain E4 project choices; do not select replacements after seeing predictions. No schema-transfer or representative-user claim.
