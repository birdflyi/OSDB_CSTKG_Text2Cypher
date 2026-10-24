"""Deterministic graph-name candidates shared by Direct and service planner.

No gold query/answer input. Candidates are suggestions, never silent rewrites.
This small lexical resolver is not an entity-linking accuracy claim.
"""
import re
import unicodedata


STOP = set('a an the of in on at for to and or by with which what who where '
           'are is was were has have had that these those their names name'.split())


def tokens(text):
    words = re.findall(r'\w+', unicodedata.normalize('NFKC', text).casefold())
    # Generic inflection heuristic is used for candidate ranking only.
    return [w[:-1] if len(w) > 3 and w.endswith('s') and not w.endswith(('ss', 'us', 'is'))
            else w for w in words if w not in STOP]


class EntityCandidates:
    def __init__(self, entities):
        self.rows = []
        for row in entities:
            name, aliases = row.get('name'), row.get('aliases', [])
            if not isinstance(name, str) or not isinstance(row.get('eid'), str):
                continue
            if not isinstance(aliases, list):
                aliases = []
            texts = [name] + [x for x in aliases if isinstance(x, str)]
            self.rows.append((row['eid'], row['label'], name,
                              [set(tokens(t)) for t in texts]))

    def find(self, question, limit=12):
        terms = set(tokens(question))
        ranked = []
        for eid, label, name, alternatives in self.rows:
            best = 0.0
            for candidate in alternatives:
                overlap = len(terms & candidate)
                if not overlap:
                    continue
                # Full phrase-token containment outranks incidental overlap.
                score = overlap / len(candidate) + overlap / max(len(terms), 1)
                best = max(best, score)
            if best:
                ranked.append((best, eid, label, name))
        ranked.sort(key=lambda row: (-row[0], row[1]))
        return [{'eid': eid, 'label': label, 'name': name, 'lexical_score': round(score, 6)}
                for score, eid, label, name in ranked[:limit]]


CANDIDATE_NOTICE = ('These are graph-derived lexical entity candidates, not reference answers. '
                    'Use the canonical name or eid only when it matches the question meaning. '
                    'Candidates may be ambiguous or incomplete. Do not broaden predicates merely '
                    'to obtain a nonempty result. Entity aliases need not be unique.')
