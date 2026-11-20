-- Road graph checks - G12: duplicate edges
--
-- New check G12 finding the edges with the same end nodes and the same geometry, for example after a copy
-- of an edge, and new fix action delete_duplicate_edges deleting the duplicates of the same road.

-- check_graph_duplicate_edges(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_duplicate_edges(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between the geometries of two duplicate edges
    tolerance real = Coalesce((_options->>'tolerance')::real, 0.5);
BEGIN
    RETURN QUERY
    WITH
    -- Pairs of edges with the same end nodes, in the same or the opposite direction,
    -- and with the same geometry, less than the tolerance from each other
    -- (two joins on the nodes instead of one with OR, to use the indexes)
    same_nodes AS (
        SELECT a.id AS kept_id, b.id AS duplicate_id, a.geom AS kept_geom, b.geom AS duplicate_geom
        FROM road_graph.edges AS a
        JOIN road_graph.edges AS b
            ON b.start_node = a.start_node AND b.end_node = a.end_node AND b.id > a.id
        WHERE (_road_codes IS NULL OR a.road_code = ANY(_road_codes) OR b.road_code = ANY(_road_codes))
        UNION ALL
        SELECT a.id, b.id, a.geom, b.geom
        FROM road_graph.edges AS a
        JOIN road_graph.edges AS b
            ON b.start_node = a.end_node AND b.end_node = a.start_node AND b.id > a.id
        WHERE (_road_codes IS NULL OR a.road_code = ANY(_road_codes) OR b.road_code = ANY(_road_codes))
    ),
    pairs AS (
        SELECT s.kept_id, s.duplicate_id
        FROM same_nodes AS s
        WHERE ST_HausdorffDistance(s.kept_geom, s.duplicate_geom) <= tolerance
    ),
    -- Each duplicate is attached to the edge with the lowest id among the edges it duplicates
    duplicates AS (
        SELECT p.duplicate_id, min(p.kept_id) AS kept_id
        FROM pairs AS p
        GROUP BY p.duplicate_id
    ),
    -- One issue per kept edge, with its duplicates
    issues AS (
        SELECT
            k.id AS kept_id, k.road_code, k.start_node, k.end_node, k.geom,
            array_agg(d.id ORDER BY d.id) AS duplicate_ids,
            array_agg(DISTINCT d.road_code ORDER BY d.road_code) AS duplicate_road_codes
        FROM duplicates AS u
        JOIN road_graph.edges AS k ON k.id = u.kept_id
        JOIN road_graph.edges AS d ON d.id = u.duplicate_id
        GROUP BY k.id, k.road_code, k.start_node, k.end_node, k.geom
    )
    SELECT
        'G12'::text,
        'Duplicate edges'::text,
        'error'::text,
        -- The road code of the issue, if all the edges have the same road
        CASE WHEN i.duplicate_road_codes = ARRAY[i.road_code] THEN i.road_code END,
        ARRAY[i.kept_id] || i.duplicate_ids,
        ARRAY[i.start_node, i.end_node],
        NULL::integer[],
        jsonb_build_object(
            'kept_edge_id', i.kept_id,
            'duplicate_edge_ids', i.duplicate_ids,
            'road_codes', (
                SELECT jsonb_agg(DISTINCT c ORDER BY c)
                FROM unnest(i.road_code || i.duplicate_road_codes) AS c
            ),
            'length', round(ST_Length(i.geom)::numeric, 2)
        ),
        -- The duplicates of another road must be checked manually, for example a common section drawn twice
        CASE WHEN i.duplicate_road_codes = ARRAY[i.road_code] THEN 'delete_duplicate_edges' END,
        CASE
            WHEN i.duplicate_road_codes = ARRAY[i.road_code] THEN jsonb_build_object(
                'edge_id', i.kept_id,
                'duplicate_edge_ids', i.duplicate_ids,
                'tolerance', tolerance
            )
        END,
        ST_LineInterpolatePoint(i.geom, 0.5),
        i.geom
    FROM issues AS i
    ORDER BY i.road_code, i.kept_id
    ;
END;
$$;


-- FUNCTION check_graph_duplicate_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_duplicate_edges(_road_codes text[], _options jsonb) IS 'Check G12 run by the function check_graph: find the duplicate edges, for example after a copy of an edge.

Two edges are duplicates when they have the same end nodes, in the same or the opposite direction,
and the same geometry: their Hausdorff distance is less than the option tolerance (0.5 m by default).

An issue is returned for each kept edge, the edge with the lowest id, with its duplicates.
The details give the kept edge, the duplicate edges, their road codes and the length of the kept edge.

The issue can be fixed with the action delete_duplicate_edges, which deletes the duplicates.
The duplicates of another road have no fix action: they must be checked manually.';


-- fix_graph_delete_duplicate_edges(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_delete_duplicate_edges(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    kept_edge record;
    duplicate_id integer;
    deleted_ids integer[];
    tolerance real = Coalesce((_params->>'tolerance')::real, 0.5);
BEGIN
    -- The kept edge may have been removed or modified by a previous fix
    SELECT e.*
    INTO kept_edge
    FROM road_graph.edges AS e
    WHERE e.id = (_params->>'edge_id')::integer
    ;
    IF kept_edge.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s does not exist anymore', _params->>'edge_id')
        );
    END IF;

    -- Delete the duplicates which are still duplicates of the kept edge, one by one:
    -- the trigger after_edge_delete links the previous and next edges of each deleted edge
    FOR duplicate_id IN
        SELECT d.id
        FROM road_graph.edges AS d
        WHERE d.id IN (SELECT jsonb_array_elements_text(_params->'duplicate_edge_ids')::integer)
        AND d.road_code = kept_edge.road_code
        AND (
            (d.start_node = kept_edge.start_node AND d.end_node = kept_edge.end_node)
            OR (d.start_node = kept_edge.end_node AND d.end_node = kept_edge.start_node)
        )
        AND ST_HausdorffDistance(d.geom, kept_edge.geom) <= tolerance
        ORDER BY d.id DESC
    LOOP
        DELETE FROM road_graph.edges AS e
        WHERE e.id = duplicate_id
        ;
        deleted_ids = deleted_ids || duplicate_id;
    END LOOP;

    IF deleted_ids IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s has no duplicate anymore', kept_edge.id)
        );
    END IF;
    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The edges %s duplicating the edge %s have been deleted',
            array_to_string(ARRAY(SELECT unnest(deleted_ids) ORDER BY 1), ', '), kept_edge.id
        )
    );
END;
$$;


-- FUNCTION fix_graph_delete_duplicate_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_delete_duplicate_edges(_params jsonb) IS 'Action delete_duplicate_edges run by the function fix_graph_issue, to fix the issues of the check G12:
delete the duplicates of an edge.

The parameters give the kept edge, the duplicate edges and the tolerance:
{"edge_id": 12, "duplicate_edge_ids": [13, 14], "tolerance": 0.5}.
Only the edges which are still duplicates of the kept edge, with the same road code, are deleted, one by one:
the trigger after_edge_delete links the previous and next edges of each deleted edge.

It returns a JSON object with the keys status (success, skipped if there is no duplicate anymore, or error) and message.';


-- G12
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G12', 'Duplicate edges', 'error', 'check_graph_duplicate_edges', '{delete_duplicate_edges}', '{"tolerance": 0.5}', 'Edges with the same end nodes and the same geometry, for example after a copy of an edge. Option: tolerance, in meters, the maximum Hausdorff distance between the two edges. Fix: delete_duplicate_edges deletes the duplicates of the edge with the lowest id. The duplicates of another road have no fix action: check them manually, for example a common section of two roads drawn twice.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
