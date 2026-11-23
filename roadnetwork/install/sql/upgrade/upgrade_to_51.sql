-- Road graph checks - G06: links across the gaps of the roads
--
-- When a road is interrupted, for example by a roundabout, the last edge before the gap and the first edge
-- after it do not touch each other: only their link gives the order of the road, used to calculate the references.
-- The check G06, and its fix relink_edges, emptied such a link when it was wrong, which split the road
-- into several pieces, the following ones without references. Now:
-- * a wrong link across a gap is set to the first edge of the piece of road of the linked edge,
-- * a missing link across a gap is set to the closest piece of road starting less than the option gap_tolerance
--   (150 m by default) from the end of the edge.
--
-- Edges not to be cut at the intersections: their extremities do not split the edges not to be cut either,
-- and new function merge_two_edges used by the action Merge the 2 selected edges.
--
-- The start or end point of an edge being drawn or moved splits the edge under it, even if this edge must not
-- be cut at the intersections (no_intersection_cutting), for example a new access road arriving on a highway.
-- This is not done anymore when the drawn edge must not be cut itself, for example a road starting on a bridge
-- above another road: both edges stay not connected.
--
-- merge_two_edges merges two edges of the same road sharing a node. The node is kept if it is used by other edges:
-- the merged edge is then marked as not to be cut at the intersections, so that this node does not split it again.

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



-- get_or_create_edge_node(geometry, boolean)
CREATE OR REPLACE FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) RETURNS TABLE(node_id integer, node_geom geometry, is_created boolean)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Get the closest node less than 0.5 m from the given start or end point of an edge,
    -- or the node with the lowest id if several nodes are at the same distance.
    -- The closest node must be used: when an edge is modified, its start or end point
    -- must stay on its node, even if another node is less than 0.5 m from it
    SELECT n.id, n.geom
    INTO node_id, node_geom
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, _point, 0.50)
    ORDER BY n.geom <-> _point, n.id
    LIMIT 1
    ;
    is_created = False;
    IF node_id IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;

    -- Create the missing node if asked
    IF _create THEN
        -- Declare that the node is the extremity of an edge:
        -- the edges under it will be split, even if their field no_intersection_cutting is True
        -- (for example a new access road arriving in the middle of a highway edge),
        -- unless the edge itself must not be cut at the intersections (see before_edge_insert_or_update):
        -- for example a road starting on a bridge above another road
        -- The variable is used by the trigger function after_node_insert_or_update
        IF road_graph.get_current_setting('road.graph.edge.no.intersection.cutting', 'no') != 'yes' THEN
            SET LOCAL road.graph.node.edge.extremity = 'yes';
        END IF;
        WITH new_node AS (
            INSERT INTO road_graph.nodes
            (geom)
            VALUES
            (_point)
            ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
            RETURNING id
        )
        SELECT new_node.id
        FROM new_node
        INTO node_id
        ;
        SET LOCAL road.graph.node.edge.extremity = 'no';
        node_geom = _point;
        is_created = True;
    END IF;

    RETURN NEXT;
    RETURN;
END;
$$;


-- FUNCTION get_or_create_edge_node(_point geometry, _create boolean)
COMMENT ON FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) IS 'Get the node of the given start or end point of an edge: the closest node less than 0.5 m from it,
or the node with the lowest id if several nodes are at the same distance.

If there is no node and _create is True, the node is created (its id can be NULL if a node already exists at the same place).
The edges under the created node are split, even if they must not be cut at the intersections,
unless the edge of the node must not be cut itself (session variable road.graph.edge.no.intersection.cutting).
Used by the trigger function before_edge_insert_or_update.';


-- before_edge_insert_or_update()
CREATE OR REPLACE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_point geometry(point);
    end_point geometry(point);
    extremity_node record;
    references_calculated_after boolean;
    references_unchanged boolean;
    distance real;
    start_references jsonb;
    end_references jsonb;
    marker_zero record;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
    has_changed_road_code boolean;
    is_reversed boolean;
	v_sqlstate text;
	v_message text;
	v_context text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Get road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;

    -- check if edge is a part of a roundabout
    is_roundabout = (edge_road.road_type = 'roundabout');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% BEFORE edge % n° %, edge is_roundabout %,
        %',
            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, is_roundabout,
            to_json(edge_road)
        ;
    END IF;

    -- If it is a new roundabout, reverse the geometry if needed
    -- QGIS digitizing circle tool create counter-clockwise polygons
    IF is_roundabout AND ST_IsRing(NEW.geom) AND ST_IsPolygonCW(ST_MakePolygon(NEW.geom))
    THEN
        NEW.geom = ST_Reverse(NEW.geom);
    END IF;

    -- For UPDATE, check if the road_code has changed
    -- If so, we need to empty the next_edge_id and previous_edge_id to avoid wrong calculations
    has_changed_road_code = false;
    IF NOT is_roundabout
        AND TG_OP = 'UPDATE'
        AND Coalesce(OLD.road_code, '') != Coalesce(NEW.road_code, '')
    THEN
        has_changed_road_code = true;
        IF Coalesce(OLD.previous_edge_id, -1) = Coalesce(NEW.previous_edge_id, -1) THEN
            NEW.previous_edge_id = NULL;
        END IF;
        IF Coalesce(OLD.next_edge_id, -1) = Coalesce(NEW.next_edge_id, -1) THEN
            NEW.next_edge_id = NULL;
        END IF;
    END IF;

    -- For UPDATE, check if the geometry has only been reversed.
    -- ST_Equals does not take the direction into account: ST_OrderingEquals is used below
    -- to detect any change of the geometry, including its direction
    is_reversed = (
        TG_OP = 'UPDATE'
        AND ST_Equals(NEW.geom, OLD.geom) IS TRUE
        AND NOT ST_OrderingEquals(NEW.geom, OLD.geom)
    );
    -- The previous and next edges of a reversed edge must be calculated again,
    -- unless they have been given
    IF is_reversed AND NOT is_roundabout AND NOT has_changed_road_code THEN
        IF Coalesce(OLD.previous_edge_id, -1) = Coalesce(NEW.previous_edge_id, -1) THEN
            NEW.previous_edge_id = NULL;
        END IF;
        IF Coalesce(OLD.next_edge_id, -1) = Coalesce(NEW.next_edge_id, -1) THEN
            NEW.next_edge_id = NULL;
        END IF;
    END IF;

    -- Create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

    -- The nodes are also searched when the geometry has only been reversed:
    -- the start and end nodes are then swapped
    IF TG_OP = 'INSERT'
    OR (TG_OP = 'UPDATE' AND NOT ST_OrderingEquals(NEW.geom, OLD.geom) AND NEW.geom IS NOT NULL)
    THEN

        -- Get the nodes closer than 0.5 m from the start and end points, and use them.
        -- If not found, create them: only for INSERT,
        -- or for UPDATE if the start_node or end_node value has been deleted

        -- The extremities of an edge not to be cut at the intersections do not split the edges
        -- not to be cut either, for example a road starting on a bridge above another road
        -- (see get_or_create_edge_node)
        PERFORM set_config(
            'road.graph.edge.no.intersection.cutting',
            CASE WHEN Coalesce(NEW.no_intersection_cutting, False) THEN 'yes' ELSE 'no' END,
            true
        );

        -- upstream node
        SELECT INTO extremity_node *
        FROM road_graph.get_or_create_edge_node(
            start_point,
            TG_OP = 'INSERT' OR NEW.start_node IS NULL
        );
        IF extremity_node.node_id IS NOT NULL AND NOT extremity_node.is_created THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream_node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, extremity_node.node_id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, 0, extremity_node.node_geom);
            -- Update the node ID in upstream attribute
            NEW.start_node = extremity_node.node_id;
        ELSIF extremity_node.is_created THEN
            NEW.start_node = extremity_node.node_id;
            IF NEW.start_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.start_node,
                    ST_AsText(extremity_node.node_geom)
                ;
            END IF;
        END IF;

        -- downstream node
        SELECT INTO extremity_node *
        FROM road_graph.get_or_create_edge_node(
            end_point,
            TG_OP = 'INSERT' OR NEW.end_node IS NULL
        );
        IF extremity_node.node_id IS NOT NULL AND NOT extremity_node.is_created THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, extremity_node.node_id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, ST_NPoints(NEW.geom) - 1, extremity_node.node_geom);
            -- Update the node ID in downstream attribute
            NEW.end_node = extremity_node.node_id;
        ELSIF extremity_node.is_created THEN
            NEW.end_node = extremity_node.node_id;
            IF NEW.end_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.end_node,
                    ST_AsText(extremity_node.node_geom)
                ;
            END IF;
        END IF;
        PERFORM set_config('road.graph.edge.no.intersection.cutting', 'no', true);
    END IF;

    -- Calculate previous_edge_id and next_edge_id values
    -- for touching edges if the road_code has not changed
    IF NOT is_roundabout AND NOT has_changed_road_code
    THEN
        IF NEW.previous_edge_id IS NULL THEN
            -- Get the previous edge
            NEW.previous_edge_id = (
                SELECT e.id
                FROM road_graph.edges AS e
                WHERE e.road_code = NEW.road_code
                AND e.end_node = NEW.start_node
                -- not the edge itself, whose old row is still in the table, for example when it is reversed
                AND e.id != NEW.id
                ORDER BY e.id
                LIMIT 1
            );
        END IF;
        IF NEW.next_edge_id IS NULL THEN
            -- Get the next edge
            NEW.next_edge_id = (
                SELECT e.id
                FROM road_graph.edges AS e
                WHERE e.road_code = NEW.road_code
                AND e.start_node = NEW.end_node
                -- not the edge itself, whose old row is still in the table, for example when it is reversed
                AND e.id != NEW.id
                ORDER BY e.id
                LIMIT 1
            );
        END IF;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    -- The marker 0 is not moved when the geometry has only been reversed: it stays at the start of the road
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND NOT has_changed_road_code
        AND NOT is_reversed
        AND OLD.start_abscissa = 0 AND NEW.start_abscissa = 0
        AND OLD.start_cumulative = 0 AND NEW.start_cumulative = 0
        AND OLD.start_marker = 0 AND NEW.start_marker = 0
        AND OLD.road_code = NEW.road_code
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_StartPoint(OLD.geom))
    THEN
        -- Do not run the function update_edge_references
        -- on the road triggered inside the after_marker_insert_or_update_or_delete
        -- trigger since it will already been triggered with the edge geometry change
        SET LOCAL road.graph.edge.ref.calc.disabled = 'yes';
        UPDATE road_graph.markers AS m
        SET geom = ST_StartPoint(NEW.geom)
        WHERE m.road_code = NEW.road_code
        AND m.code = 0
        AND ST_Equals(m.geom, ST_StartPoint(OLD.geom))
        ;
        SET LOCAL road.graph.edge.ref.calc.disabled = 'no';
    END IF;

    -- Calculate references
    -- They are not calculated for an UPDATE:
    -- * if the trigger after_edge_insert_or_update will calculate again the references
    --   of all the edges of the road (same condition as in this trigger function):
    --   the references calculated here would be overwritten,
    -- * or if the geometry, the road and the references have not been changed, for example
    --   when only another field has been modified: the references are still right.
    -- They are always calculated for an INSERT: the start cumulative is used to order
    -- the edges of the road (see get_ordered_edges)
    references_calculated_after = False;
    references_unchanged = False;
    IF TG_OP = 'UPDATE' THEN
        references_calculated_after = (
            (
                NOT ST_OrderingEquals(OLD.geom, NEW.geom)
                OR Coalesce(OLD.previous_edge_id, -1) != NEW.previous_edge_id
                OR Coalesce(OLD.next_edge_id, -1) != NEW.next_edge_id
            ) IS TRUE
            AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
        );
        references_unchanged = (
            ST_OrderingEquals(OLD.geom, NEW.geom) IS TRUE
            AND OLD.road_code = NEW.road_code
            AND (
                NEW.start_marker, NEW.start_abscissa, NEW.start_cumulative,
                NEW.end_marker, NEW.end_abscissa, NEW.end_cumulative
            ) IS NOT DISTINCT FROM (
                OLD.start_marker, OLD.start_abscissa, OLD.start_cumulative,
                OLD.end_marker, OLD.end_abscissa, OLD.end_cumulative
            )
        );
    END IF;
    IF NOT is_roundabout
        AND NOT references_calculated_after
        AND NOT references_unchanged
    THEN
        BEGIN
            -- start point
            SELECT INTO start_references
                road_graph.get_reference_from_point(start_point, NEW.road_code) AS ref
            ;
            NEW.start_marker = (start_references->>'marker_code')::text::integer;
            NEW.start_abscissa = (start_references->>'abscissa')::text::real;
            NEW.start_cumulative = (start_references->>'cumulative')::text::real;

            -- end point
            SELECT INTO end_references
                road_graph.get_reference_from_point(end_point, NEW.road_code) AS ref
            ;
            NEW.end_marker = (end_references->>'marker_code')::text::integer;
            NEW.end_abscissa = (end_references->>'abscissa')::text::real;
            NEW.end_cumulative = (end_references->>'cumulative')::text::real;

        EXCEPTION WHEN OTHERS THEN
            IF raise_notice IN ('info', 'debug') THEN
                GET STACKED DIAGNOSTICS
                    v_sqlstate = returned_sqlstate,
                    v_message = message_text,
                    v_context = pg_exception_context
                ;
                RAISE NOTICE '% BEFORE edge % n° %, NEW references NOT calculated, error = %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP,
                    NEW.id, v_sqlstate || ' - ' || v_message
                ;
            END IF;
        END;
    END IF;

    RETURN NEW;
END;
$$;


-- FUNCTION before_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.before_edge_insert_or_update() IS 'During the creation or modification of an edge, we verify that the upstream and downstream nodes exist
within 50 cm of the start and end of the edge. If they do, we use them
and update the edge geometry so that the start and end are exactly on these nodes.
Otherwise, we create the missing nodes.
Additionally, during the creation of an edge, if the road code is provided and no marker 0 exists for this road,
we automatically create one at the start of the edge.
During the modification of an edge, if the starting point of the edge is modified and the marker 0 of the road is positioned at this starting point,
we move marker 0 to this new starting point.
When the geometry of an edge is only reversed, its start and end nodes are swapped,
its previous and next edges are calculated again, and the marker 0 is not moved.
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';


-- after_edge_insert_or_update()
CREATE OR REPLACE FUNCTION road_graph.after_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    crossing_node record;
    touching_node record;
    new_node_id integer;
    deleted_node_ids integer[];
    id_new_edge integer;
    created_nodes_at_intersection integer[];
    node_to_be_deleted integer;
    node_already_deleted integer;
    update_edge_references_result boolean;
    raise_notice text;
    cascade_edge_id integer;
    edge_road record;
    is_roundabout boolean;
    previous_ref_calc_disabled text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - Start',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    IF TG_OP = 'UPDATE' THEN
        -- UPDATE - Move related upstream and downstream nodes if needed
        IF NOT ST_Equals(ST_StartPoint(OLD.geom), ST_StartPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update upstream node if needed
            -- Declare that the node is the extremity of the edge:
            -- the edges under it will be split, even if their field no_intersection_cutting is True,
            -- unless the edge itself must not be cut at the intersections
            IF NOT Coalesce(NEW.no_intersection_cutting, False) THEN
                SET LOCAL road.graph.node.edge.extremity = 'yes';
            END IF;
            UPDATE road_graph.nodes AS n
            SET geom = ST_StartPoint(NEW.geom)
            WHERE n.id = NEW.start_node
            AND NOT ST_Equals(n.geom, ST_StartPoint(NEW.geom))
            ;
            SET LOCAL road.graph.node.edge.extremity = 'no';
        END IF;
        IF NOT ST_Equals(ST_EndPoint(OLD.geom), ST_EndPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update downstream node if needed
            -- Declare that the node is the extremity of the edge:
            -- the edges under it will be split, even if their field no_intersection_cutting is True,
            -- unless the edge itself must not be cut at the intersections
            IF NOT Coalesce(NEW.no_intersection_cutting, False) THEN
                SET LOCAL road.graph.node.edge.extremity = 'yes';
            END IF;
            UPDATE road_graph.nodes AS n
            SET geom = ST_EndPoint(NEW.geom)
            WHERE n.id = NEW.end_node
            AND NOT ST_Equals(n.geom, ST_EndPoint(NEW.geom))
            ;
            SET LOCAL road.graph.node.edge.extremity = 'no';
        END IF;

        -- Check if old referenced nodes are still referenced by edges
        -- If not, delete them
        -- Get node ID which has been manually deleted by user
        -- and must not be deleted twice
        node_already_deleted = road_graph.get_current_setting('road.graph.node.already.deleted', '-1');

        -- Upstream node
        IF OLD.start_node != NEW.start_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.start_node
                AND id != node_already_deleted
                AND NOT EXISTS (
                    SELECT id
                    FROM road_graph.edges
                    WHERE start_node = OLD.start_node
                    OR end_node = OLD.start_node
                )
                RETURNING id
            ) SELECT array_agg(id) INTO deleted_node_ids
            FROM del
            ;

            IF raise_notice IN ('info', 'debug') AND deleted_node_ids IS NOT NULL THEN
                RAISE NOTICE '% AFTER edge % n° %, unreferenced upstream nodes deleted : %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, array_to_string(deleted_node_ids, ', ')
                ;
            END IF;
        END IF;

        -- Downstream node
        IF OLD.end_node != NEW.end_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.end_node
                AND id != node_already_deleted
                AND NOT EXISTS (
                    SELECT id
                    FROM road_graph.edges
                    WHERE start_node = OLD.end_node
                    OR end_node = OLD.end_node
                )
                RETURNING id
            ) SELECT array_agg(id) INTO deleted_node_ids
            FROM del
            ;
            IF raise_notice IN ('info', 'debug') AND deleted_node_ids IS NOT NULL THEN
                RAISE NOTICE '% AFTER edge % n° %, unreferenced downstream nodes deleted : %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, array_to_string(deleted_node_ids, ', ')
                ;
            END IF;
        END IF;
    END IF;

    -- Cascade changes made on previous_edge_id and next_edge_id
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and position manually the marker 0)
    -- The cascading changes are only made when the edge is inserted or its previous or next edge has changed:
    -- the references of the road are then calculated at the end of this function.
    -- Do not calculate them for each updated neighbour edge
    previous_ref_calc_disabled = road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no');
    PERFORM set_config('road.graph.edge.ref.calc.disabled', 'yes', true);
    -- When the geometry has only been reversed, the edges of the road which referenced this edge
    -- as their next or previous edge do not follow it anymore: their reference to this edge is removed,
    -- and their trigger calculates again their next or previous edge
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND ST_Equals(NEW.geom, OLD.geom) IS TRUE
        AND NOT ST_OrderingEquals(NEW.geom, OLD.geom)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NULL
        WHERE e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.next_edge_id = NEW.id
        AND e.id IS DISTINCT FROM NEW.previous_edge_id
        ;
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NULL
        WHERE e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.previous_edge_id = NEW.id
        AND e.id IS DISTINCT FROM NEW.next_edge_id
        ;
    END IF;
    -- previous edge id
    IF (TG_OP = 'INSERT' AND NEW.previous_edge_id IS NOT NULL AND NOT is_roundabout)
        OR (TG_OP = 'UPDATE' AND NEW.previous_edge_id != Coalesce(OLD.previous_edge_id, -1) AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NEW.id
        WHERE TRUE
        AND e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.id = NEW.previous_edge_id
        AND (e.next_edge_id IS NULL OR e.next_edge_id != NEW.id)
        RETURNING e.id
        INTO cascade_edge_id
        ;
        IF raise_notice IN ('info', 'debug') AND cascade_edge_id IS NOT NULL THEN
            RAISE NOTICE '% AFTER edge % n° %, cascade changes on previous edge id : changes made on edge n° %',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                cascade_edge_id
            ;
        END IF;

    END IF;
    -- next edge id
    IF (TG_OP = 'INSERT' AND NEW.next_edge_id IS NOT NULL AND NOT is_roundabout)
        OR (TG_OP = 'UPDATE' AND NEW.next_edge_id != Coalesce(OLD.next_edge_id, -1) AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NEW.id
        WHERE TRUE
        AND e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.id = NEW.next_edge_id
        AND (e.previous_edge_id IS NULL OR e.previous_edge_id != NEW.id)
        RETURNING e.id
        INTO cascade_edge_id
        ;
        IF raise_notice IN ('info', 'debug') AND cascade_edge_id IS NOT NULL THEN
            RAISE NOTICE '% AFTER edge % n° %, cascade changes on next edge id : changes made on edge n° %',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                cascade_edge_id
            ;
        END IF;
    END IF;
    -- Set back the value
    PERFORM set_config('road.graph.edge.ref.calc.disabled', previous_ref_calc_disabled, true);

    -- For UPDATE, if the edge road_code has changed,
    -- we must also edit the previous and next edge ids of the old road edges
    IF TG_OP = 'UPDATE'
        AND OLD.road_code IS NOT NULL
        AND Coalesce(NEW.road_code, '') != OLD.road_code
    THEN
        -- Update OLD previous edge field "next_edge_id" if concerned
        UPDATE road_graph.edges AS e
        SET next_edge_id = OLD.next_edge_id
        WHERE TRUE
        -- same road
        AND e.road_code = OLD.road_code
        -- reference as the next_edge_id by the OLD edge
        AND e.next_edge_id = OLD.id
        AND e.id != NEW.id
        ;
        -- Update OLD next edge field "previous_edge_id" if concerned
        UPDATE road_graph.edges AS e
        SET previous_edge_id = OLD.previous_edge_id
        WHERE TRUE
        -- same road
        AND e.road_code = OLD.road_code
        -- reference as the previous_edge_id by the OLD edge
        AND e.previous_edge_id = OLD.id
        AND e.id != NEW.id
        ;
    END IF;

    -- For INSERT OR UPDATE
    -- Create nodes at intersection with other edges if needed
    -- This will then run the trigger after_node_insert_or_update
    -- which will eventually split the edges intersecting this NEW edge
    -- DO IT ONLY FOR GEOMETRIES INSIDE THE EDITION AREA
    -- DO IT ONLY FOR EDGES WITH THE SAME LEVEL
    created_nodes_at_intersection = ARRAY[]::integer[];
    -- Beware of the parentheses: AND has a higher precedence than OR,
    -- and the test on the variable must also be applied for INSERT
    IF (TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom)))
        -- avoid infinite loop and self-crossing
        AND road_graph.get_current_setting('road.graph.edge.crossing.node.creation.pending', 'no') != 'yes'

    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER edge % n° %, no_intersection_cutting %, crossing node test',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.no_intersection_cutting
            ;
        END IF;
        FOR crossing_node IN
            WITH new_nodes AS (
                SELECT
                    ST_Intersection(e.geom, NEW.geom) AS geom
                FROM road_graph.edges AS e
                WHERE e.id != NEW.id
                AND ST_Intersects(e.geom, NEW.geom)
                -- Do not get the intersection if one the edges has the value True in no_intersection_cutting
                AND NOT Coalesce(e.no_intersection_cutting, False)
                AND NOT Coalesce(NEW.no_intersection_cutting, False)
            ),
            -- intersection can produce multipoints
            -- if the edge crosses more than once
            -- We explode into single points
            new_nodes_dumped AS (
                SELECT (ST_Dump(c.geom)).geom AS geom
                FROM new_nodes AS c
            )
            -- Only get points not close to existing nodes
            -- If the road intersects several times the same road
            -- we should transform ST_MultiPoint into ST_Point
            SELECT DISTINCT c.geom
            FROM new_nodes_dumped AS c
            LEFT JOIN road_graph.nodes AS n
                -- check existing nodes in less than 1 m from the NEW edge
                ON ST_DWithin(n.geom, NEW.geom, 1)
                -- only nodes not already very close to the intersected node
                AND ST_DWithin(n.geom, c.geom, 0.50)
            -- this will keep only crossing nodes to use
            WHERE n.id IS NULL
            -- DO NOT USE NODE IF OUTSIDE THE EDITION AREA
            AND ST_Intersects(
                c.geom,
                (
                    SELECT es.geom
                    FROM "road_graph".editing_sessions AS es
                    LIMIT 1
                )
            )
        LOOP
            IF crossing_node.geom IS NOT NULL
                AND ST_GeometryType(crossing_node.geom) = 'ST_Point'
                -- The crossing points are calculated once, before the loop.
                -- Since then, the node could have been created by the cascading changes:
                -- the creation of the previous node has split this edge, and the new parts
                -- have created the nodes at their own intersections.
                -- These nodes are calculated from the rounded coordinates of the parts,
                -- and can be a few centimeters away from this point: check with the same tolerance
                AND NOT EXISTS (
                    SELECT 1
                    FROM road_graph.nodes AS n
                    WHERE ST_DWithin(n.geom, crossing_node.geom, 0.50)
                )
            THEN
                -- Create the missing node, which will trigger the split of the edge
                -- (see the trigger after_node_insert_or_update)
                -- Set variable to avoid infinite loop & self-crossing detection
                SET LOCAL road.graph.edge.crossing.node.creation.pending = 'yes';
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (crossing_node.geom)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT id
                FROM new_node
                INTO new_node_id
                ;
                -- revert variable back to no
                SET LOCAL road.graph.edge.crossing.node.creation.pending = 'no';

                IF new_node_id IS NOT NULL THEN
                    created_nodes_at_intersection = created_nodes_at_intersection || new_node_id;
                    IF raise_notice IN ('info', 'debug') THEN
                        RAISE NOTICE '% AFTER edge % n° %, crossing node created : % %',
                            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, new_node_id,
                            (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = new_node_id)
                        ;
                    END IF;
                END IF;
            END IF;
        END LOOP;

        -- Also split this NEW edge if it touches other nodes
        -- (but not by its start or end point)
        -- We do it by updating the already existing nodes
        -- this will run the trigger after_node_update which will split the corresponding edge
        -- WARNING : this should not be done if this node concerns only two edges already been merging
        node_to_be_deleted = road_graph.get_current_setting('road.graph.merge.edges.useless.node', '-1');

        -- RAISE NOTICE '% AFTER edge % n° %, list touching nodes, useless node = %',
        ---    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, node_to_be_deleted
        -- ;
        FOR touching_node IN
            SELECT DISTINCT
                n.*
            FROM road_graph.nodes AS n
            WHERE True
            -- Do not split the NEW edge if it has the value True in no_intersection_cutting
            AND NOT Coalesce(NEW.no_intersection_cutting, False)
            -- "intersecting" the NEW edge
            AND ST_DWithin(n.geom, NEW.geom, 0.50)
            AND n.id NOT IN (NEW.start_node, NEW.end_node)
            -- but not touching its start or end point
            AND NOT ST_DWithin(n.geom, ST_StartPoint(NEW.geom), 0.50)
            AND NOT ST_DWithin(n.geom, ST_EndPoint(NEW.geom), 0.50)
            -- node not created above from edges intersections
            -- avoid creation of useless nodes
            AND NOT (n.id = ANY(Coalesce(created_nodes_at_intersection, array[]::integer[])))
            -- node not concerned by two edges already been merging
            -- to avoid infinite loop
            -- The variable road.graph.merge.edges.useless.node is set in the function road_graph.merge_edges
            AND n.id != node_to_be_deleted::integer
            -- DO NOT USE NODE IF OUTSIDE THE EDITION AREA
            AND ST_Intersects(
                n.geom,
                (
                    SELECT es.geom
                    FROM "road_graph".editing_sessions AS es
                    LIMIT 1
                )
            )
        LOOP
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % n° % start % end %, update existing node under new edge to launch the split : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.start_node, NEW.end_node,
                    touching_node.id, ST_AsText(touching_node.geom)
                ;
            END IF;

            IF road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no') != 'yes'
            THEN
                SET LOCAL road.graph.edge.update.touching.node = 'yes';
                -- We do not create a new node (constraint on same geometry)
                -- but juste move a little bit the existing node to run
                -- the trigger after_node_insert_or_update which will run the split function
                WITH up AS (
                    UPDATE road_graph.nodes AS n
                    SET geom = ST_Translate(geom, 0.01, 0.01)
                    WHERE id = touching_node.id
                    RETURNING id
                )
                SELECT id INTO new_node_id
                FROM up
                ;
                SET LOCAL road.graph.edge.update.touching.node = 'no';
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, NEW node created in same node place: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id, new_node_id
                    ;
                END IF;
            END IF;
        END LOOP;
    END IF;

    -- Create a new marker feature with code 0 if needed
    IF TG_OP IN ('INSERT', 'UPDATE')
        AND NEW.geom IS NOT NULL
        AND NEW.previous_edge_id IS NULL
        AND NEW.road_code IS NOT NULL
        AND NOT is_roundabout
        AND Coalesce(edge_road.road_type, '') NOT IN ('roundabout')
        -- the road does not have any marker yet (new road)
        -- Do not test only the marker 0: some roads do not start with the marker 0
        AND NOT EXISTS (
            SELECT m.id
            FROM road_graph.markers AS m
            WHERE m.road_code = NEW.road_code
        )
        -- There is no edge with the same road_code and without previous_edge_id (which means the first edge of the road)
        AND NOT EXISTS (
            SELECT e.id
            FROM road_graph.edges AS e
            WHERE e.road_code = NEW.road_code
            AND e.id != NEW.id
            AND (
                e.previous_edge_id IS NULL
                OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(NEW.geom), 0.50)
            )
        )
    THEN
        INSERT INTO road_graph.markers (road_code, code, abscissa, geom)
        VALUES (NEW.road_code, 0, 0, ST_StartPoint(NEW.geom))
        ON CONFLICT DO NOTHING
        ;
    END IF;

    -- Calculate references of the edge whole road
    -- Beware to choose correct conditions
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and marker)
    IF NOT is_roundabout THEN
        IF TG_OP = 'INSERT'
        OR (
            TG_OP = 'UPDATE' AND (
                -- The references are also calculated when the geometry has only been reversed
                NOT ST_OrderingEquals(OLD.geom, NEW.geom)
                OR Coalesce(OLD.previous_edge_id, -1) != NEW.previous_edge_id
                OR Coalesce(OLD.next_edge_id, -1) != NEW.next_edge_id
            )
        )
        THEN
            IF road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
            THEN
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, update all NEW road edges references: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                        update_edge_references_result::text
                    ;
                END IF;
                -- Only from the edge, or from its previous edge, whose end point may have moved with the node
                SELECT INTO update_edge_references_result
                    road_graph.update_edge_references(
                        NEW.road_code,
                        NULL,
                        ARRAY[
                            NEW.id,
                            NEW.previous_edge_id,
                            CASE WHEN TG_OP = 'UPDATE' THEN OLD.previous_edge_id END
                        ]
                    )
                ;
            END IF;
        END IF;
    END IF;


    -- Also update old road references if road code has changed
    -- and it remains at least one edge for this old road code
    -- No need, it is done as a consequence of updating the edges
    -- of the OLD road code which referenced this edge in next_edge_id or previous_edge_id

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - End',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    RETURN NEW;
END;
$$;


-- FUNCTION after_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.after_edge_insert_or_update() IS 'Multiples opérations lancées suite à la modification d''un troncon.
Déplacement du noeud initial et terminal liés si besoin.
Suppression des noeuds orphelins si besoin.
Création des noeuds non existants à l''intersection avec les autres edges
seulement pour les edges de même niveau
';


-- merge_two_edges(integer, integer)
CREATE OR REPLACE FUNCTION road_graph.merge_two_edges(_edge_id_a integer, _edge_id_b integer) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_a record;
    edge_b record;
    node_id integer;
    kept_id integer;
    is_node_kept boolean;
BEGIN
    -- The two edges must exist, be different, have the same road and share a node,
    -- the end of one edge being the start of the other one
    SELECT e.* INTO edge_a FROM road_graph.edges AS e WHERE e.id = _edge_id_a;
    SELECT e.* INTO edge_b FROM road_graph.edges AS e WHERE e.id = _edge_id_b;
    IF edge_a.id IS NULL OR edge_b.id IS NULL OR edge_a.id = edge_b.id THEN
        RAISE EXCEPTION 'Two different existing edges must be given: % and %', _edge_id_a, _edge_id_b;
    END IF;
    IF edge_a.road_code != edge_b.road_code THEN
        RAISE EXCEPTION 'The edges % and % do not have the same road: % and %',
            edge_a.id, edge_b.id, edge_a.road_code, edge_b.road_code;
    END IF;
    IF edge_a.end_node = edge_b.start_node AND edge_a.start_node != edge_b.end_node THEN
        node_id = edge_a.end_node;
    ELSIF edge_b.end_node = edge_a.start_node AND edge_b.start_node != edge_a.end_node THEN
        node_id = edge_b.end_node;
    ELSE
        RAISE EXCEPTION 'The edges % and % must follow each other: '
            'the end node of one edge must be the start node of the other one',
            edge_a.id, edge_b.id;
    END IF;

    -- The node is kept if it is used by other edges, for example another road starting there:
    -- the merged edge must then not be cut at the intersections, else the node would split it again
    is_node_kept = EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE e.id NOT IN (edge_a.id, edge_b.id)
        AND node_id IN (e.start_node, e.end_node)
    );
    IF is_node_kept THEN
        UPDATE road_graph.edges AS e
        SET no_intersection_cutting = True
        WHERE e.id IN (edge_a.id, edge_b.id)
        AND NOT Coalesce(e.no_intersection_cutting, False)
        ;
    END IF;

    -- Merge the edges: the edge with the lowest id is kept
    PERFORM road_graph.merge_edges(edge_a.id, edge_b.id);
    kept_id = least(edge_a.id, edge_b.id);

    IF is_node_kept THEN
        RETURN format(
            'The edges %s and %s have been merged into the edge %s. The node %s is kept for the other edges: '
            'the edge %s is marked as not to be cut at the intersections',
            edge_a.id, edge_b.id, kept_id, node_id, kept_id
        );
    END IF;
    RETURN format('The edges %s and %s have been merged into the edge %s', edge_a.id, edge_b.id, kept_id);
END;
$$;


-- FUNCTION merge_two_edges(_edge_id_a integer, _edge_id_b integer)
COMMENT ON FUNCTION road_graph.merge_two_edges(_edge_id_a integer, _edge_id_b integer) IS 'Merge two edges of the same road following each other, used by the action Merge the 2 selected edges.

The edge with the lowest id is kept, and gets the geometry of the other edge (see merge_edges).
The node between the edges is kept if it is used by other edges, for example a road starting on a bridge
above the merged road: the merged edge is then marked as not to be cut at the intersections (no_intersection_cutting).
An exception is raised if the edges do not exist, do not have the same road, or do not follow each other.

It returns the message describing the merge.';
