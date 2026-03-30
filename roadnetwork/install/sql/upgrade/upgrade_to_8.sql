-- Rename sequence
ALTER SEQUENCE road_graph.glossary_road_scale_id_seq RENAME TO glossary_road_class_id_seq;
ALTER TABLE road_graph.glossary_road_class RENAME CONSTRAINT glossary_road_scale_code_key TO glossary_road_class_code_key;
ALTER TABLE road_graph.glossary_road_class RENAME CONSTRAINT glossary_road_scale_label_key TO glossary_road_class_label_key;
ALTER TABLE road_graph.glossary_road_class RENAME CONSTRAINT glossary_road_scale_pkey TO glossary_road_class_pkey;


-- copy_data_to_editing_session(integer)
CREATE OR REPLACE FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    _set_config text;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;
    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    IF editing_session_record.status != 'created' THEN
        RAISE EXCEPTION 'The given id % does not correspond to editing session with status ''created'' !', _editing_session_id;
    END IF;

    -- Disable triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- Truncate tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;
    TRUNCATE editing_session.glossary_road_class CASCADE;

    -- Re-insert glossary
    INSERT INTO editing_session.glossary_road_class
    SELECT *
    FROM road_graph.glossary_road_class
    ;

    -- Get roads to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    roads AS (
        SELECT DISTINCT r.*
        FROM road_graph.roads AS r
        JOIN edges_roads AS e
            ON r.road_code = e.road_code
    )
    INSERT INTO editing_session.roads
    SELECT *
    FROM roads
    ;

    -- Get nodes to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.start_node, e.end_node
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    ),
    nodes AS (
        SELECT DISTINCT n.*
        FROM road_graph.nodes AS n
        JOIN edges AS e
            ON n.id IN (e.start_node, e.end_node)
    )
    INSERT INTO editing_session.nodes
    SELECT *
    FROM nodes
    ;

    -- Get edges to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.*
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    )
    INSERT INTO editing_session.edges
    SELECT *
    FROM edges
    ;

    -- Get markers to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    markers AS (
        SELECT DISTINCT m.*
        FROM road_graph.markers AS m
        JOIN edges_roads AS er
            ON m.road_code = er.road_code
    )
    INSERT INTO editing_session.markers
    SELECT *
    FROM markers
    ;

    -- Replace identities by road_schema sequences to get correct ids
    BEGIN
        ALTER TABLE editing_session.roads ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.nodes ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.markers ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.edges ALTER COLUMN id DROP IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Cannot drop identity in editing_session tables';
    END;
    -- roads
    ALTER TABLE editing_session.roads
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.roads', 'id'));
    -- nodes
    ALTER TABLE editing_session.nodes
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.nodes', 'id'));
    -- markers
    ALTER TABLE editing_session.markers
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.markers', 'id'));
    -- edges
    ALTER TABLE editing_session.edges
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.edges', 'id'));

    -- Set ids in the editing_sessions
    UPDATE road_graph.editing_sessions
    SET cloned_ids = jsonb_build_object(
        'roads', (SELECT jsonb_agg(id::text) FROM editing_session.roads),
        'nodes', (SELECT jsonb_agg(id::text) FROM editing_session.nodes),
        'markers', (SELECT jsonb_agg(id::text) FROM editing_session.markers),
        'edges', (SELECT jsonb_agg(id::text) FROM editing_session.edges)
    ),
    "status" = 'cloned'
    WHERE id = _editing_session_id
    ;

    -- In the table editing_session.editing_sessions
    -- keep only the current editing session
    -- This table will be used by the trigger editing_survey
    -- to collect changes in the editing_sessions tables
    TRUNCATE editing_session.editing_sessions RESTART IDENTITY;
    INSERT INTO editing_session.editing_sessions
    SELECT * FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Audit trigger on editing_session tables
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.roads;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.roads
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.nodes;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.nodes
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.markers;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.markers
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.edges;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.edges
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;

    RETURN True;
END;
$$;


-- FUNCTION copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the editing_session schema corresponding to the given editing session ID.';




-- before_edge_insert_or_update()
CREATE OR REPLACE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_point geometry(point);
    end_point geometry(point);
    upstream_node record;
    downstream_node record;
    distance real;
    start_references jsonb;
    end_references jsonb;
    marker_zero record;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
    has_changed_road_code boolean;
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
    is_roundabout = (edge_road.road_type = 'roundabout' AND ST_IsRing(NEW.geom));
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% BEFORE edge % n° %, edge is_roundabout %,
        %',
            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, is_roundabout,
            to_json(edge_road)
        ;
    END IF;

    -- If it is a new roundabout, reverse the geometry if needed
    -- QGIS digitizing circle tool create counter-clockwise polygons
    IF is_roundabout AND ST_IsPolygonCW(ST_MakePolygon(NEW.geom))
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

    -- Create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

    IF TG_OP = 'INSERT'
    OR (TG_OP = 'UPDATE' AND NOT ST_Equals(NEW.geom, OLD.geom) AND NEW.geom IS NOT NULL)
    THEN

        -- Get first nodes < 0.5 m - If found, edit NEW geom
        -- upstream
        SELECT INTO upstream_node
            n.id, n.geom
        FROM road_graph.nodes AS n
        WHERE ST_DWithin(n.geom, start_point, 0.50)
        ORDER BY n.id, n.geom <-> start_point
        LIMIT 1
        ;

        -- upstream - create node or just update value
        IF upstream_node IS NOT NULL THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream_node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, upstream_node.id
                ;
            END IF;

            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, 0, upstream_node.geom);
            -- Update the node ID in upstream attribute
            NEW.start_node = upstream_node.id;
        ELSE
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream node IS NULL -> create it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
                ;
            END IF;

            -- Create the missing node
            -- only for INSERT
            -- or for UPDATE if the start_node value has been deleted
            IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.start_node IS NULL) THEN
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (start_point)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT new_node.id
                FROM new_node
                INTO NEW.start_node
                ;
                IF NEW.start_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% BEFORE edge % n° %, upstream node created : % %',
                        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.start_node,
                        (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.start_node)
                    ;
                END IF;
            END IF;
        END IF;

        -- get downstream node
        SELECT INTO downstream_node
            n.id, n.geom
        FROM road_graph.nodes AS n
        WHERE ST_DWithin(n.geom, end_point, 0.50)
        ORDER BY n.id, n.geom <-> end_point
        LIMIT 1
        ;
        -- downstream node - create or update value
        IF downstream_node IS NOT NULL THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, downstream_node.id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, ST_NPoints(NEW.geom) - 1, downstream_node.geom);
            -- Update the node ID in downstream attribute
            NEW.end_node = downstream_node.id;
        ELSE
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node IS NULL -> create it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
                ;
            END IF;

            -- Create the missing node
            -- only for INSERT
            -- or for UPDATE if the end_node value has been deleted
            IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.end_node IS NULL) THEN
                -- Create the missing node
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (end_point)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT new_node.id
                FROM new_node
                INTO NEW.end_node
                ;
                IF NEW.end_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% BEFORE edge % n° %, downstream node created : % %',
                        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.end_node,
                        (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.end_node)
                    ;
                END IF;
            END IF;
        END IF;
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
            );
        END IF;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND NOT has_changed_road_code
        AND OLD.start_abscissa = 0 AND NEW.start_abscissa = 0
        AND OLD.start_cumulative = 0 AND NEW.start_cumulative = 0
        AND OLD.start_marker = 0 AND NEW.start_marker = 0
        AND OLD.road_code = NEW.road_code
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_StartPoint(OLD.geom))
    THEN
        -- Do not run the function update_edge_references
        -- on the road triggered inside the after_marker_insert_or_update_or_delete
        -- trigger since it will already been triggered with the edge geometry change
        SET road.graph.edge.ref.calc.disabled = 'yes';
        UPDATE road_graph.markers AS m
        SET geom = ST_StartPoint(NEW.geom)
        WHERE m.road_code = NEW.road_code
        AND m.code = 0
        AND ST_Equals(m.geom, ST_StartPoint(OLD.geom))
        ;
        SET road.graph.edge.ref.calc.disabled = 'no';
    END IF;

    -- Calculate references
    IF NOT is_roundabout
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
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';
