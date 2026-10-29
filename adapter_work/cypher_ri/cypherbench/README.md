# M07-B01 cypher-ri x CypherBench

Two-turn protocol (schema slice then answer) reimplemented from the paper; weights are the anonymous Hugging Face release.

Files: adapter.py, tests/, deviations.json, paper_to_code.json, delivery.json.

Run: python -m pytest tests -q  ->  6 passed

No HTTP, no database, no model call.
