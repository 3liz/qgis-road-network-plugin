
-- after_edge_delete()
CREATE OR REPLACE FUNCTION road_graph.after_edge_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    has_changed boolean;
    test_edges record;
    merge_result boolean;
    node_already_deleted integer;
    update_edge_references_result boolean;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN OLD;
    END IF;

    -- Raise notice ?
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Get node ID which has been manually deleted by user
    -- and must not be deleted twice
    node_already_deleted = road_graph.get_current_setting('road.graph.node.already.deleted', '-1');

    -- Check if old referenced nodes are still referenced by edges
    -- If not, delete them
    -- Upstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.start_node
    AND id != node_already_deleted
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.start_node
        OR end_node = OLD.start_node
    )
    ;
    -- Downstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.end_node
    AND id != node_already_deleted
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.end_node
        OR end_node = OLD.end_node
    );

    -- Merge edges which were linked to the deleted node
    -- ONLY if
    -- there are only 2 edges linked to the node
    -- AND if these edges have the same road code
    -- AND the road_code is not the same as the deleted edge
    -- to avoid merging the remaining edge with the next one which has the same road code
    -- a/ Node A - OLD.end_node
    -- RAISE NOTICE 'Node A - %', OLD.end_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes,
        max(t.road_code) AS road_code
    FROM road_graph.edges AS t
    WHERE TRUE
    AND (OLD.end_node = t.end_node OR OLD.end_node = t.start_node)
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    -- And if road_code is not the same as the deleted edge
    IF
        test_edges.nb = 2
        AND test_edges.nb_road_codes = 1
        -- prevent to merge the remaining edge with the next one which has the same road code as the deleted edge
        AND test_edges.road_code != OLD.road_code
    THEN
        IF raise_notice IN ('yes', 'info', 'debug') THEN
            RAISE NOTICE 'after_edge_delete % OLD.end_node - We must merge - % ',
                OLD.id, array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- b/ Node B - OLD.start_node
    -- RAISE NOTICE 'Node B - %', OLD.start_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes,
        max(t.road_code) AS road_code
    FROM road_graph.edges AS t
    WHERE TRUE
    AND (OLD.start_node = t.end_node OR OLD.start_node = t.start_node)
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF
        test_edges.nb = 2
        AND test_edges.nb_road_codes = 1
        AND test_edges.road_code != OLD.road_code
    THEN
        IF raise_notice IN ('yes', 'info', 'debug') THEN
            RAISE NOTICE 'after_edge_delete % OLD.start_node - We must merge - % ',
                OLD.id, array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- Update road references if there is at least one edge left
    -- for the road
    IF (
        SELECT count(e.*)
        FROM road_graph.edges AS e
        WHERE e.road_code = OLD.road_code
        AND e.id != OLD.id
    ) > 0
    THEN
        -- First update previous and next edges
        -- previous
        SET road.graph.edge.ref.calc.disabled = 'yes';

        UPDATE road_graph.edges AS e
        SET next_edge_id = (
            SELECT s.id
            FROM road_graph.edges AS s
            WHERE s.id = OLD.next_edge_id
        )
        WHERE e.next_edge_id = OLD.id
        ;
        -- next
        UPDATE road_graph.edges AS e
        SET previous_edge_id = (
            SELECT s.id
            FROM road_graph.edges AS s
            WHERE s.id = OLD.previous_edge_id
        )
        WHERE e.previous_edge_id = OLD.id
        ;

        -- Set back the value
        SET road.graph.edge.ref.calc.disabled = 'no';

        -- Calculate references
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER EDGE % N° %, update all road edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL)
        ;

    END IF;

    RETURN OLD;
END;
$$;


-- FUNCTION after_edge_delete()
COMMENT ON FUNCTION road_graph.after_edge_delete() IS 'Lors de la suppression d''un tronçon, on supprime les Nodes qui étaient rattachés aux sommets amont et aval du tronçon,
uniquement si ces derniers ne sont plus rattachés à aucun autre tronçon.
On fusionne aussi les tronçons collés plus liés par les possibles noeuds supprimés
';



-- split_edge_by_node(record, record, real, real)
CREATE OR REPLACE FUNCTION road_graph.split_edge_by_node(edge_record record, node_record record, minimum_distance_from_linestring real, maximum_distance_from_start_end_points real) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    source_fields text;
    start_geom geometry;
    end_geom geometry;
    id_new_object integer;
    new_edge_record record;
    sql_text text;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
BEGIN
    -- log level
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = edge_record.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    -- Check point is close enough to the given edge
    IF ST_Distance(node_record.geom, edge_record.geom) > minimum_distance_from_linestring THEN
        RAISE NOTICE 'No edge found close enough to the node : Distance % m > % m ',
            ST_Distance(node_record.geom, edge_record.geom), minimum_distance_from_linestring
        ;
        RETURN NULL;
    END IF;

    -- Check the edge is not already cut
    IF node_record.id IN (edge_record.start_node, edge_record.end_node)
        OR ST_DWithin(node_record.geom, ST_StartPoint(edge_record.geom), 0.50)
        OR ST_DWithin(node_record.geom, ST_EndPoint(edge_record.geom), 0.50)
    THEN
        RAISE NOTICE 'Given edge is already cut for this node which corresponds to its start or end point'
        ;
        RETURN NULL;
    END IF;

    -- Create new geometries
    start_geom := ST_LineSubstring(
        edge_record.geom,
        0,
        ST_LineLocatePoint(edge_record.geom, node_record.geom)
    );
    end_geom := ST_LineSubstring(
        edge_record.geom,
        ST_LineLocatePoint(edge_record.geom, node_record.geom),
        1
    );

    -- Check the cut point is not to close to the start or end point of the given linestring
    IF ST_length(start_geom) < maximum_distance_from_start_end_points
        OR ST_length(end_geom) < maximum_distance_from_start_end_points
    THEN-- Set variable to avoid infinite loops
        RAISE NOTICE 'Abort splitting the edge: given point too close from the edge end points ( < %m)', maximum_distance_from_start_end_points;
        RETURN NULL;
    END IF;

    -- Modify the initial edge :
    -- OA----------(O)----------OB becomes  OA----------(O)
    -- we must get the new edge id beforehand to use it as next_edge_id
    -- For roundabout, do not calculate previous and next edge id
    -- To let the user decide
    SET search_path TO road_graph, public;
    id_new_object = nextval(pg_get_serial_sequence('edges', 'id'));
    RESET search_path;

    UPDATE road_graph.edges
    SET
        -- Set the geometry
        geom = start_geom,
        -- We must also set the downstream node id column
        end_node = node_record.id,
        -- next edge_id
        next_edge_id = CASE WHEN is_roundabout THEN NULL ELSE id_new_object END,
        -- end_cumulative
        end_cumulative = (end_cumulative - ST_Length(end_geom))
    WHERE id = edge_record.id
    ;

    -- Get the object fields minus the PK, upstream node & geometry column
    WITH fields AS (
        SELECT json_object_keys(to_json(edge_record.*)) AS field
    )
    SELECT concat ('"', string_agg(field, '", "'), '"')
    INTO source_fields
    FROM fields
    WHERE field NOT IN (
        'id', 'start_node', 'geom',
        'previous_edge_id', 'next_edge_id',
        'start_cumulative', 'uid'
    )
    ;

    -- Check if the new edge geometry does not exists
    -- This avoid to create 2 superimposed edges
    -- especially for roundabouts.
    SELECT INTO new_edge_record
        e.id
    FROM road_graph.edges AS e
    WHERE
        -- use ST_DWithin and not ST_Intersects to use spatial indexing
        -- but keep a tolerance to avoid precision issues
        ST_DWithin(e.geom, end_geom, 1)
        AND (
            ST_Equals(e.geom, end_geom)
            OR ST_Equals(e.geom, ST_ReducePrecision(end_geom, 0.10))
        )
    LIMIT 1
    ;

    -- Create the new edge:
    -- (O)----------OB
    -- Get the values from the original linestring
    -- Only if the geometry does not exists
    IF new_edge_record.id IS NULL
    THEN
        sql_text = format(
            $$
                INSERT INTO road_graph.edges (
                    id,
                    %1$s,
                    start_node,
                    geom,
                    previous_edge_id,
                    next_edge_id,
                    start_cumulative,
                    uid
                )
                WITH source AS (
                    SELECT e.*, r.road_type
                    FROM road_graph.edges AS e
                    JOIN road_graph.roads AS r
                        ON r.road_code = e.road_code
                    WHERE e.id = %2$s::integer
                    LIMIT 1
                )
                SELECT
                    %5$s::integer AS id,
                    %1$s,
                    %3$s::integer AS start_node,
                    '%4$s'::geometry(LINESTRING) AS geom,
                    CASE WHEN road_type = 'roundabout' THEN NULL ELSE %2$s::integer END AS previous_edge_id,
                    CASE WHEN road_type = 'roundabout' THEN NULL ELSE(Nullif(%6$s, -1))::integer END AS next_edge_id,
                    %7$s::real AS start_cumulative,
                    uuid_generate_v4()::text
                FROM source
                LIMIT 1
                RETURNING id
            $$,
            source_fields,
            edge_record.id,
            node_record.id,
            end_geom,
            id_new_object,
            Coalesce(edge_record.next_edge_id, -1),
            Coalesce(edge_record.end_cumulative, 0)
        );

        EXECUTE sql_text
        INTO id_new_object
        ;

        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% split_edge_by_node - node n° % %- splitting edge n° % : created edge n° %',
               repeat('    ', pg_trigger_depth()::integer), node_record.id, ST_AsText(node_record.geom), edge_record.id, id_new_object
            ;
        END IF;
    ELSE
        id_new_object = -1;
    END IF;

    -- Return True
    RETURN id_new_object;

END;
$_$;

COMMENT ON FUNCTION road_graph.split_edge_by_node(record, record, real, real)
IS 'Split an edge by a node. The node must be close enough to the edge and not too close from the edge start or end point.
The function returns the id of the new created edge or null if the edge has not been split.';
