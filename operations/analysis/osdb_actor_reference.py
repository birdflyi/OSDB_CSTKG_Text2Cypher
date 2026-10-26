"""Snapshot opening-account reference functions, independent of Ours compiler.

Inputs are validated (typed actor key, Issue/PullRequest label, UTC microseconds).
The SQLite check is an independent relational formulation, not graph validation.
"""
from collections import defaultdict
from datetime import datetime,timezone
import re
import sqlite3

KINDS=('union_openers','both_opening_domains','observed_opening_gap','first_observed_opening')


def utc_microseconds(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z',value):
        raise ValueError('Canonical UTC instant required; no time fallback')
    body=value[:-1]
    if '.' in body:
        head,fraction=body.split('.');body=head+'.'+fraction.ljust(6,'0')
    value=datetime.fromisoformat(body+'+00:00')-datetime(1970,1,1,tzinfo=timezone.utc)
    return (value.days*86400+value.seconds)*1_000_000+value.microseconds


def validate(rows,history,cutoff,end):
    if not (type(history) is int and type(cutoff) is int and type(end) is int and history<cutoff<end):
        raise ValueError('Ordered integer UTC boundaries required')
    for actor,label,when in rows:
        if type(actor) is not str or not actor or label not in ('Issue','PullRequest') or type(when) is not int:
            raise ValueError('Validated typed identity, source label and integer instant required')


def python_answers(rows,history,cutoff,end):
    validate(rows,history,cutoff,end)
    issue=set();pr=set();times=defaultdict(list)
    for actor,label,when in rows:
        if history<=when<end:times[actor].append(when)
        if cutoff<=when<end:(issue if label=='Issue' else pr).add(actor)
    return dict(zip(KINDS,(len(issue|pr),len(issue&pr),
        sum(max(v)<cutoff for v in times.values()),
        sum(min(v)>=cutoff for v in times.values()))))


def sqlite_answers(rows,history,cutoff,end):
    validate(rows,history,cutoff,end)
    connection=sqlite3.connect(':memory:')
    try:
        connection.execute('CREATE TABLE openings(actor TEXT NOT NULL,kind TEXT NOT NULL,t INTEGER NOT NULL)')
        connection.executemany('INSERT INTO openings VALUES(?,?,?)',rows)
        queries=(
            'SELECT COUNT(DISTINCT actor) FROM openings WHERE t>=? AND t<?',
            'SELECT COUNT(*) FROM (SELECT actor FROM openings WHERE t>=? AND t<? GROUP BY actor HAVING COUNT(DISTINCT kind)=2)',
            'SELECT COUNT(*) FROM (SELECT actor FROM openings WHERE t>=? AND t<? GROUP BY actor HAVING MAX(t)<?)',
            'SELECT COUNT(*) FROM (SELECT actor FROM openings WHERE t>=? AND t<? GROUP BY actor HAVING MIN(t)>=?)')
        params=((cutoff,end),(cutoff,end),(history,end,cutoff),(history,end,cutoff))
        return {kind:connection.execute(query,p).fetchone()[0] for kind,query,p in zip(KINDS,queries,params)}
    finally:connection.close()


def manual_cypher(kind):
    """Intended Neo4j reference, NOT executed or certified by this module."""
    base="""MATCH (n)-[r:EVENT_ACTION]->(a:Actor)
WHERE (n:Issue OR n:PullRequest) AND r.service_rel_type='OPENED_BY'
  AND r.source_event_time IS NOT NULL
  AND datetime(r.source_event_time)>=datetime($history_start)
  AND datetime(r.source_event_time)<datetime($end)
"""
    if kind=='union_openers':
        return base+"AND datetime(r.source_event_time)>=datetime($cutoff)\nRETURN count(DISTINCT a.entity_id) AS account_count"
    if kind=='both_opening_domains':
        return base+"""AND datetime(r.source_event_time)>=datetime($cutoff)
WITH a.entity_id AS actor_id,
 max(CASE WHEN n:Issue THEN 1 ELSE 0 END) AS has_issue,
 max(CASE WHEN n:PullRequest THEN 1 ELSE 0 END) AS has_pr
WHERE has_issue=1 AND has_pr=1
RETURN count(*) AS account_count"""
    if kind=='observed_opening_gap':
        return base+"WITH a.entity_id AS actor_id,max(datetime(r.source_event_time)) AS last_seen\nWHERE last_seen<datetime($cutoff)\nRETURN count(*) AS account_count"
    if kind=='first_observed_opening':
        return base+"WITH a.entity_id AS actor_id,min(datetime(r.source_event_time)) AS first_seen\nWHERE first_seen>=datetime($cutoff)\nRETURN count(*) AS account_count"
    raise ValueError('Unknown task kind')
