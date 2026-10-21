# E8 execution database and source-reference readiness

Both preselected graphs were imported sequentially into new verified-empty owned loopback databases. No old database was cleared. Actual nodes/edges/unique edge UIDs match the frozen execution CSVs; endpoint identities and all source hashes are in private import receipts. IoTDB uses17477 and Elasticsearch17478.

Each new project's eight source-proxy goals passed independent CSV-versus-handwritten-Cypher reference checks, 8/8 per project. Task bindings and scopes were already frozen across all four projects, and the methods/context renderer were fixed before predictions. Runtime summary and source hashes: E8_IMPORT_RESULTS.json. This is preparation/reference conformance, not a model accuracy score.

The frozen comparison engine's original CLI only accepts the two development ports. run_selected_holdout.py invokes the same engine library after assigning the predeclared loopback endpoint, rather than editing prediction logic or copying graph data into a different existing DB. It preserves the same compact inputs, model, repair budget and scoring. Engine clean-commit/source-hash/DB-identity gates still run.

Next is model transfer evaluation using the same source coverage stratum and independently frozen complementary workloads. Service catalogs cannot express these eight source goals; measure useful completion and refusal separately and do not present coverage loss as a same-capability accuracy contest. More nodes and reference edges do not by themselves establish harder queries or new-schema generalization.
