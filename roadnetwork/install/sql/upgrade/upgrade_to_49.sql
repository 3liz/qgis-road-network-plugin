-- Road graph checks - G12: duplicate edges
--
-- New check G12 finding the edges with the same end nodes and the same geometry, for example after a copy
-- of an edge, and new fix action delete_duplicate_edges deleting the duplicates of the same road.
--
-- merge_editing_session_data: the table of the issues of the editing session is emptied after the merge.

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


-- merge_editing_session_data(integer)
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'road_graph', 'public'
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    road_codes text[];
    impacts jsonb;
    managed_object record;
    updated_objects_number integer;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    IF editing_session_record.status != 'edited' THEN
        RAISE EXCEPTION 'The given id % does not correspond to editing session with status ''edited'' !', _editing_session_id;
    END IF;

    -- Disable topology triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(FALSE)
        INTO _toggle_triggers;

    -- Get the road codes concerned by the editing session
    SELECT INTO road_codes
        "road_graph".get_updated_roads_from_editing_session(_editing_session_id)
    ;

    -- Get the impacts of the editing session on the roads, to update only the managed objects
    -- which may be impacted by the modifications.
    -- It must be calculated before the merge, since it compares the production and editing data.
    -- Only the roads whose edges geometries or markers have changed are impacted:
    -- the other edges have only been updated by the calculation of their references
    SELECT INTO impacts
        "road_graph".get_editing_session_impacts(_editing_session_id)
    ;
    SELECT INTO road_codes
        array_agg(i->>'road_code')
    FROM jsonb_array_elements(Coalesce(impacts, '[]'::jsonb)) AS i
    ;

    -- Create SQL to run for each edited data
    FOR sql_record IN
        SELECT
            sql_text
        FROM "road_graph".create_queries_from_editing_session(_editing_session_id)
    LOOP
       -- RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    -- Update the managed objects concerned by the editing session changes
    IF road_codes IS NOT NULL AND array_length(road_codes, 1) > 0 THEN
        -- Disable the trigger (to avoid calculate_managed_object_references to be executed)
        SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
        -- Loop for each managed_object tables
        FOR managed_object IN
            SELECT o.*
            FROM "road_graph".managed_objects AS o
        LOOP
            SELECT INTO updated_objects_number
                road_graph.update_managed_objects_on_graph_change(
                    managed_object.schema_name,
                    managed_object.table_name,
                    road_codes,
                    impacts
                )
            ;
            RAISE NOTICE 'Number of updated managed objects for "%"."%": %',
                managed_object.schema_name,
                managed_object.table_name,
                Coalesce(updated_objects_number, 0)
            ;
        END LOOP;
        -- Re-enable triggers
        SELECT set_config('road.graph.disable.trigger', '0'::text, true)
        INTO _set_config;

    ELSE
        -- No object has been updated: empty the list of the objects updated by the previous merge
        UPDATE "road_graph".managed_objects
        SET (
            last_updated_objects_ids,
            last_update
        ) = (
            ARRAY[]::integer[],
            now()::timestamp(0)
        );

    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;
    TRUNCATE road_graph_editing.graph_issues RESTART IDENTITY;

    -- Delete merged editing session
    DELETE FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Refresh the materialized view of the last updated managed objects
    REFRESH MATERIALIZED VIEW CONCURRENTLY road_graph.vm_managed_objects;

    RETURN True;
END;
$$;


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';
