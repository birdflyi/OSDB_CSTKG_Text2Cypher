# M01-B03 direct x MindTheQuery

Same contract as M01-B01; MindTheQuery public inputs carry question+schema, so prepare composes the prompt.

Files: adapter.py, tests/, deviations.json, paper_to_code.json, delivery.json.

Run: python -m pytest tests -q  ->  6 passed

No HTTP, no database, no model call.
