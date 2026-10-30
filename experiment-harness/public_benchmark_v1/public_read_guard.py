"""Public read guard: ordinary scoped subqueries are allowed; EXPLAIN must confirm read type."""
import re


class ReadOnlyViolation(ValueError):
    pass


def read_query(query):
    if not isinstance(query, str) or not query.strip() or len(query) > 50000:
        raise ReadOnlyViolation('Expected one bounded Cypher string')
    masked = []
    i = 0
    while i < len(query):
        if query.startswith('//', i):
            end = query.find('\n', i)
            i = len(query) if end < 0 else end
            masked.append(' ')
        elif query.startswith('/*', i):
            end = query.find('*/', i + 2)
            if end < 0:
                raise ReadOnlyViolation('Unclosed comment')
            i = end + 2
            masked.append(' ')
        elif query[i] in "'\"`":
            quote = query[i]
            i += 1
            while i < len(query):
                if query[i] == '\\' and quote != '`':
                    i += 2
                elif query[i] == quote:
                    i += 1
                    if i < len(query) and query[i] == quote:
                        i += 1
                    else:
                        break
                else:
                    i += 1
            else:
                raise ReadOnlyViolation('Unclosed literal/identifier')
            masked.append(' quoted ')
        else:
            masked.append(query[i])
            i += 1
    text = ''.join(masked)
    if ';' in text.strip().rstrip(';') or text.count(';') > 1:
        raise ReadOnlyViolation('Multiple statements are not allowed')
    forbidden = {'CREATE', 'MERGE', 'DELETE', 'DETACH', 'SET', 'REMOVE', 'DROP',
                 'LOAD', 'FOREACH', 'GRANT', 'DENY', 'REVOKE', 'TERMINATE',
                 'ALTER', 'START', 'STOP', 'USE'}
    tokens = list(re.finditer(r'[A-Za-z_][A-Za-z0-9_]*|[^\s]', text))
    for index, token in enumerate(tokens):
        previous = tokens[index - 1].group() if index else ''
        following = tokens[index + 1].group() if index + 1 < len(tokens) else ''
        word = token.group().upper()
        # Keywords can also be result aliases or property owners (e.g. start).
        # EXPLAIN must still confirm that the whole statement is a read.
        identifier_context = previous.upper() in ('AS', 'WITH') or previous == ',' or following == '.'
        if word in forbidden and previous not in ('.', '$', ':') and not identifier_context:
            raise ReadOnlyViolation('Write/admin clause denied')
        if word == 'CALL' and previous not in ('.', '$', ':'):
            after = text[token.end():].lstrip()
            if not after.startswith('{') and not re.match(
                r'\(\s*(?:\*|[A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)?\s*\)\s*\{', after):
                raise ReadOnlyViolation('Procedure calls not enabled in development guard')
    if not re.search(r'\bRETURN\b', text, re.IGNORECASE):
        raise ReadOnlyViolation('A returned read result is required')
    return query.strip().rstrip(';').strip()
