# M01-B02 direct x Text2GQL

Thin adapter on the shared factory: dapter.prepare() composes from the frozen V22 public input, dapter.parse() classifies the reply, dapter.consume() delegates to common.core (save_prediction -> execute -> save_final_answer -> score).

Run: python -m pytest tests -q -> 2 passed. No HTTP, no database, no model call.
