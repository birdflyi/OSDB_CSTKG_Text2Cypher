# D1.3a v1 Development Regression

Role: `DEVELOPMENT_REGRESSION / NOT_HELDOUT`. This diagnostic run is post-tuning development evidence only; it does not estimate generalization. A separately authored independent v2 remains required after tuning.

Frozen baseline: {'EXECUTABLE_SEMANTIC_SUCCESS': 4, 'N_EXECUTABLE': 39, 'KNOWN_BOUNDARY_ABSTENTION': 6, 'N_ABSTENTION': 6, 'FALSE_ABSTENTION': 35, 'UNDETECTED_SEMANTIC_ERROR': 0}
D1.3a metrics: {'EXECUTABLE_SEMANTIC_SUCCESS': 12, 'N_EXECUTABLE': 39, 'KNOWN_BOUNDARY_ABSTENTION': 6, 'N_ABSTENTION': 6, 'FALSE_ABSTENTION': 27, 'UNDETECTED_SEMANTIC_ERROR': 0}
Delta: {'EXECUTABLE_SEMANTIC_SUCCESS': 8, 'KNOWN_BOUNDARY_ABSTENTION': 0, 'FALSE_ABSTENTION': -8, 'UNDETECTED_SEMANTIC_ERROR': 0}

RC1 rows moved past the old failure layer: 5.
RC3 rows moved past the old failure layer: 8.
Undetected semantic errors: 0.
Known boundary abstentions: 6/6.

The interpretation is static-only. No Neo4j runtime or held-out v2 construction/inspection occurred.
