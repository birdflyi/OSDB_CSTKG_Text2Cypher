"""Protocol-only control: original six services and compiler remain unchanged.

This changes instructions, not semantic validation or service capabilities.
Predictions still pass the original parser, contract checks and read guard.
"""
import strong_baselines as s


PROTOCOL_PROMPT = s.PROMPTS['S'] + '''
Output contract clarification:
1. Output exactly one JSON object, without prose or Markdown.
2. An answer is {"status":"ANSWER","operation":"listed_name","arguments":{...}}.
3. A refusal is {"status":"UNSUPPORTED","operation":null,"arguments":null}.
4. A missing user anchor is {"status":"NEEDS_INPUT","operation":null,"arguments":null}.
5. The service catalog is the complete executable capability boundary. Graph
   metadata is background information, not permission to add another operation.
6. If no listed operation preserves every requested condition and output,
   choose UNSUPPORTED. Do not output Cypher, rename a field, invent an operation,
   pass a prefix where a complete identifier is required, or answer a nearby task.
7. Treat null as JSON null, not a quoted string. Do not include extra keys.
'''


def prompt(arm):
    if arm == 'S':
        return s.PROMPTS['S']
    if arm == 'P':
        return PROTOCOL_PROMPT
    raise ValueError(arm)
