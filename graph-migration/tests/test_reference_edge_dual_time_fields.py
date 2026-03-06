from __future__ import annotations

from mappers.node_mapper import NodeTypeMapper  # noqa: E402
from mappers.relation_mapper import RelationMapper  # noqa: E402
from normalizers.graph_normalizer import GraphNormalizer  # noqa: E402
from normalizers.placeholder import PlaceholderPolicyResolver  # noqa: E402


def test_reference_edges_keep_raw_event_time_and_normalized_source_event_time() -> None:
    normalizer = GraphNormalizer(
        node_mapper=NodeTypeMapper(),
        relation_mapper=RelationMapper(),
        placeholder_resolver=PlaceholderPolicyResolver("unknown"),
    )

    rows = [
        {
            "src_entity_id": "A_1",
            "src_entity_type": "Actor",
            "tar_entity_id": "I_1",
            "tar_entity_type": "Issue",
            "relation_type": "Reference",
            "relation_label_repr": "Issue_unknown_UnknownFromBodyRef",
            "event_type": "IssueCommentEvent",
            "event_trigger": "IssueCommentEvent::action=created",
            "event_time": "2023-03-02 21:57:09",
        }
    ]

    graph = normalizer.normalize(rows)
    assert len(graph.edges) == 1

    props = graph.edges[0].properties
    assert "event_time" in props
    assert props["event_time"] == "2023-03-02 21:57:09"
    assert "source_event_time" in props
    assert props["source_event_time"] == "2023-03-02T21:57:09Z"
