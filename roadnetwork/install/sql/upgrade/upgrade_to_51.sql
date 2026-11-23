-- Road graph checks - G06: links across the gaps of the roads
--
-- When a road is interrupted, for example by a roundabout, the last edge before the gap and the first edge
-- after it do not touch each other: only their link gives the order of the road, used to calculate the references.
-- The check G06, and its fix relink_edges, emptied such a link when it was wrong, which split the road
-- into several pieces, the following ones without references. Now:
-- * a wrong link across a gap is set to the first edge of the piece of road of the linked edge,
-- * a missing link across a gap is set to the closest piece of road starting less than the option gap_tolerance
--   (150 m by default) from the end of the edge.

-- check_graph_edge_links(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_edge_links(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between the end of a piece of road and the start of the next piece, for a missing link
    gap_tolerance real = Coalesce((_options->>'gap_tolerance')::real, 150.0);
BEGIN
    RETURN QUERY
    WITH RECURSIVE
    -- Edges of the checked roads, except the roundabouts, whose order starts at their marker,
    -- and the edges starting and ending at the same node
    road_edges AS MATERIALIZED (
        SELECT e.id, e.road_code, e.start_node, e.end_node, e.previous_edge_id, e.next_edge_id, e.geom
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r ON r.road_code = e.road_code
        WHERE r.road_type != 'roundabout'
        AND e.start_node != e.end_node
        AND (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
    ),
    -- Edges connected to each edge by the nodes: the number of edges of the road starting at its end node,
    -- and ending at its start node, and the connected edge when there is only one
    counted AS MATERIALIZED (
        SELECT
            e.*,
            (
                SELECT count(*)
                FROM road_edges AS n
                WHERE n.road_code = e.road_code AND n.start_node = e.end_node AND n.id != e.id
            ) AS next_count,
            (
                SELECT min(n.id)
                FROM road_edges AS n
                WHERE n.road_code = e.road_code AND n.start_node = e.end_node AND n.id != e.id
            ) AS first_next_id,
            (
                SELECT count(*)
                FROM road_edges AS p
                WHERE p.road_code = e.road_code AND p.end_node = e.start_node AND p.id != e.id
            ) AS previous_count,
            (
                SELECT min(p.id)
                FROM road_edges AS p
                WHERE p.road_code = e.road_code AND p.end_node = e.start_node AND p.id != e.id
            ) AS first_previous_id
        FROM road_edges AS e
    ),
    connected AS MATERIALIZED (
        SELECT
            c.*,
            CASE WHEN c.next_count = 1 THEN c.first_next_id END AS connected_next_id,
            CASE WHEN c.previous_count = 1 THEN c.first_previous_id END AS connected_previous_id
        FROM counted AS c
    ),
    -- Pieces of road: from each edge without connected previous edge (the head of the piece),
    -- the edges following each other by the nodes, until a gap or a branch
    pieces (head_id, id, depth) AS (
        SELECT c.id, c.id, 0
        FROM connected AS c
        WHERE c.previous_count = 0
        UNION ALL
        SELECT p.head_id, n.id, p.depth + 1
        FROM pieces AS p
        JOIN connected AS c ON c.id = p.id
        JOIN connected AS n ON n.id = c.connected_next_id AND n.previous_count = 1
        WHERE p.depth < 100000
    ),
    -- Ends of the pieces: the edges without any edge of the road starting at their end node
    tails AS (
        SELECT c.*, p.head_id
        FROM connected AS c
        JOIN pieces AS p ON p.id = c.id
        WHERE c.next_count = 0
    ),
    -- Edges before each end of piece, following the current previous edges: a missing link must not go back
    -- to one of them, for example a short road ending near its start
    ancestors (tail_id, id, path) AS (
        SELECT t.id, t.previous_edge_id, ARRAY[t.id]
        FROM tails AS t
        WHERE t.previous_edge_id IS NOT NULL
        UNION ALL
        SELECT a.tail_id, e.previous_edge_id, a.path || a.id
        FROM ancestors AS a
        JOIN road_edges AS e ON e.id = a.id
        WHERE e.previous_edge_id IS NOT NULL
        AND NOT e.previous_edge_id = ANY(a.path || a.id)
    ),
    -- Links across a gap given by the current wrong link: the first edge of the piece of the linked edge
    linked_gaps AS (
        SELECT DISTINCT ON (t.id) t.id AS tail_id, l.head_id AS next_head_id
        FROM tails AS t
        JOIN pieces AS l ON l.id = t.next_edge_id
        WHERE l.head_id != t.head_id
        ORDER BY t.id, l.depth
    ),
    -- Missing links across a gap: the closest piece starting near the end of the edge,
    -- the edge being also the closest end for this piece
    candidate_gaps AS (
        SELECT
            t.id AS tail_id, h.id AS next_head_id,
            row_number() OVER (PARTITION BY t.id ORDER BY ST_Distance(ST_EndPoint(t.geom), ST_StartPoint(h.geom)), h.id)
                AS tail_rank,
            row_number() OVER (PARTITION BY h.id ORDER BY ST_Distance(ST_EndPoint(t.geom), ST_StartPoint(h.geom)), t.id)
                AS head_rank
        FROM tails AS t
        JOIN connected AS h
            ON h.road_code = t.road_code
            AND h.previous_count = 0
            AND h.id != t.head_id
            AND ST_DWithin(ST_EndPoint(t.geom), ST_StartPoint(h.geom), gap_tolerance)
        WHERE NOT EXISTS (SELECT 1 FROM linked_gaps AS g WHERE g.tail_id = t.id)
        AND NOT EXISTS (SELECT 1 FROM linked_gaps AS g WHERE g.next_head_id = h.id)
        AND NOT EXISTS (SELECT 1 FROM ancestors AS a WHERE a.tail_id = t.id AND a.id = h.id)
    ),
    -- Links across the gaps: a piece of road follows only one edge, the current links being kept first
    gaps AS (
        SELECT DISTINCT ON (g.next_head_id) g.tail_id, g.next_head_id
        FROM (
            SELECT l.tail_id, l.next_head_id, 0 AS priority
            FROM linked_gaps AS l
            UNION ALL
            SELECT c.tail_id, c.next_head_id, 1
            FROM candidate_gaps AS c
            WHERE c.tail_rank = 1 AND c.head_rank = 1
        ) AS g
        ORDER BY g.next_head_id, g.priority, g.tail_id
    ),
    -- Next and previous edges contradicting the geometry:
    -- * another edge of the road is connected to the edge,
    -- * or the linked edge is connected to another edge, or connected to the edge in the opposite direction.
    -- The other links, across a gap of the road, are kept
    wrong_links AS (
        SELECT
            c.*,
            c.next_edge_id IS DISTINCT FROM c.connected_next_id AND (
                c.connected_next_id IS NOT NULL
                OR n.connected_previous_id IS NOT NULL
                OR n.connected_next_id = c.id
            ) AS is_next_wrong,
            c.previous_edge_id IS DISTINCT FROM c.connected_previous_id AND (
                c.connected_previous_id IS NOT NULL
                OR p.connected_next_id IS NOT NULL
                OR p.connected_previous_id = c.id
            ) AS is_previous_wrong
        FROM connected AS c
        LEFT JOIN connected AS n ON n.id = c.next_edge_id
        LEFT JOIN connected AS p ON p.id = c.previous_edge_id
    ),
    -- Expected links: the connected edges, the edges on the other side of a gap,
    -- else the current link if it is not wrong, or no link
    expected AS (
        SELECT
            w.*,
            CASE
                WHEN w.connected_next_id IS NOT NULL THEN w.connected_next_id
                WHEN gn.next_head_id IS NOT NULL THEN gn.next_head_id
                WHEN w.is_next_wrong THEN NULL
                ELSE w.next_edge_id
            END AS expected_next_id,
            CASE
                WHEN w.connected_previous_id IS NOT NULL THEN w.connected_previous_id
                WHEN gp.tail_id IS NOT NULL THEN gp.tail_id
                WHEN w.is_previous_wrong THEN NULL
                ELSE w.previous_edge_id
            END AS expected_previous_id
        FROM wrong_links AS w
        LEFT JOIN gaps AS gn ON gn.tail_id = w.id
        LEFT JOIN gaps AS gp ON gp.next_head_id = w.id
    ),
    -- Edges with a link different from the expected one
    issues AS (
        SELECT e.*
        FROM expected AS e
        WHERE e.next_edge_id IS DISTINCT FROM e.expected_next_id
        OR e.previous_edge_id IS DISTINCT FROM e.expected_previous_id
    )
    -- One issue for each road
    SELECT
        'G06'::text,
        'Previous and next edges inconsistent with the geometry'::text,
        'error'::text,
        i.road_code,
        array_agg(i.id ORDER BY i.id),
        NULL::integer[],
        NULL::integer[],
        jsonb_build_object(
            'edge_count', count(*),
            'links', jsonb_agg(
                jsonb_build_object(
                    'edge_id', i.id,
                    'previous_edge_id', i.previous_edge_id,
                    'next_edge_id', i.next_edge_id,
                    'expected_previous_edge_id', i.expected_previous_id,
                    'expected_next_edge_id', i.expected_next_id
                )
                ORDER BY i.id
            )
        ),
        'relink_edges'::text,
        jsonb_build_object('road_code', i.road_code),
        ST_LineInterpolatePoint((array_agg(i.geom ORDER BY i.id))[1], 0.5),
        ST_Multi(ST_Collect(i.geom ORDER BY i.id))
    FROM issues AS i
    GROUP BY i.road_code
    ORDER BY i.road_code
    ;
END;
$$;


-- FUNCTION check_graph_edge_links(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_edge_links(_road_codes text[], _options jsonb) IS 'Check G06 run by the function check_graph: find the edges whose previous or next edge contradicts the geometry.

The previous and next edges (fields previous_edge_id and next_edge_id) give the order of the edges of the road,
used to calculate the references. The next edge of an edge is wrong when it does not start at the end node
of the edge, whereas another edge of the road starts there, or when it is connected to another edge,
or to the edge in the opposite direction. The previous edge is checked the same way. The roundabouts are ignored.

When the road is interrupted, for example by a roundabout, the last edge before the gap is linked to the first edge
of the piece of road after it:

* a wrong link across the gap is replaced by a link to the first edge of the piece of the linked edge,
* a missing link is set to the closest piece of road starting less than the option gap_tolerance (150 m by default)
  from the end of the edge, if the edge is also the closest end for this piece, and if this piece is not before
  the edge in the order of the road, for example for a short road ending near its start.

The other links across a gap are kept. A gap between two close nodes is fixed by the check G03.

An issue is returned for each road. The details give the number of edges and, for each edge,
its previous and next edges and the expected ones.
The issue can be fixed with the action relink_edges, which sets the expected previous and next edges
of the road, then calculates again the references of the road.';


-- G06
UPDATE road_graph.glossary_graph_check
SET
    default_options = '{"gap_tolerance": 150}'::jsonb || default_options,
    description = 'Edges whose previous or next edge contradicts the geometry: the linked edge is not connected to the edge whereas another edge of the road is, or it is connected to another edge. When the road is interrupted, for example by a roundabout, the last edge before the gap is linked to the first edge of the piece of road after it: the linked piece for a wrong link, or the closest piece starting less than the option gap_tolerance (in meters) from the end of the edge for a missing link. The roundabouts are ignored. Fix: relink_edges sets the previous and next edges from the geometry, then calculates again the references of the road.'
WHERE code = 'G06'
;
