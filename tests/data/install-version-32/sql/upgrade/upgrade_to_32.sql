-- Rename the schema editing_session to road_graph_editing
--
-- This migration is idempotent: it can be run several times.
-- It is also run on the editing schema itself by the algorithm "Upgrade database structure",
-- with "road_graph." replaced by "road_graph_editing.".
-- The table road_graph.editing_sessions and the functions *_editing_session* keep their names.

-- SCHEMA: editing_session -> road_graph_editing
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'editing_session') THEN
        IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'road_graph_editing') THEN
            RAISE EXCEPTION 'The schemas editing_session and road_graph_editing both exist: the schema editing_session cannot be renamed';
        END IF;
        ALTER SCHEMA editing_session RENAME TO road_graph_editing;
    END IF;
END
$$;

-- The functions bodies are stored as text and are not updated by the schema renaming:
-- replace the schema name in the functions which reference the editing schema

-- FUNCTION: road_graph.copy_data_to_editing_session(integer)
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
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;
    TRUNCATE road_graph_editing.glossary_road_class CASCADE;

    -- Re-insert glossary
    INSERT INTO road_graph_editing.glossary_road_class
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
    INSERT INTO road_graph_editing.roads
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
    INSERT INTO road_graph_editing.nodes
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
    INSERT INTO road_graph_editing.edges
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
    INSERT INTO road_graph_editing.markers
    SELECT *
    FROM markers
    ;

    -- Replace identities by road_schema sequences to get correct ids
    BEGIN
        ALTER TABLE road_graph_editing.roads ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE road_graph_editing.nodes ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE road_graph_editing.markers ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE road_graph_editing.edges ALTER COLUMN id DROP IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Cannot drop identity in road_graph_editing tables';
    END;
    -- roads
    ALTER TABLE road_graph_editing.roads
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.roads', 'id'));
    -- nodes
    ALTER TABLE road_graph_editing.nodes
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.nodes', 'id'));
    -- markers
    ALTER TABLE road_graph_editing.markers
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.markers', 'id'));
    -- edges
    ALTER TABLE road_graph_editing.edges
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.edges', 'id'));

    -- Set ids in the editing_sessions
    UPDATE road_graph.editing_sessions
    SET cloned_ids = jsonb_build_object(
        'roads', (SELECT jsonb_agg(id::text) FROM road_graph_editing.roads),
        'nodes', (SELECT jsonb_agg(id::text) FROM road_graph_editing.nodes),
        'markers', (SELECT jsonb_agg(id::text) FROM road_graph_editing.markers),
        'edges', (SELECT jsonb_agg(id::text) FROM road_graph_editing.edges)
    ),
    "status" = 'cloned'
    WHERE id = _editing_session_id
    ;

    -- In the table road_graph_editing.editing_sessions
    -- keep only the current editing session
    -- This table will be used by the trigger editing_survey
    -- to collect changes in the editing_sessions tables
    TRUNCATE road_graph_editing.editing_sessions RESTART IDENTITY;
    INSERT INTO road_graph_editing.editing_sessions
    SELECT * FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Audit trigger on road_graph_editing tables
    DROP TRIGGER IF EXISTS trg_editing_survey ON road_graph_editing.roads;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON road_graph_editing.roads
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON road_graph_editing.nodes;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON road_graph_editing.nodes
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON road_graph_editing.markers;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON road_graph_editing.markers
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON road_graph_editing.edges;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON road_graph_editing.edges
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;

    RETURN True;
END;
$$;

-- FUNCTION: road_graph.create_queries_from_editing_session(integer)
CREATE OR REPLACE FUNCTION road_graph.create_queries_from_editing_session(_editing_session_id integer) RETURNS TABLE(sql_text text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    editing_session_record record;
BEGIN
    RETURN QUERY
    WITH
    items AS (
        SELECT unnest(array['nodes', 'roads', 'edges', 'markers']) AS item
    ),
    ids AS (
        SELECT item, logged_ids->item AS item_ids
        FROM items, "road_graph".editing_sessions
        WHERE status = 'edited'
        AND id = _editing_session_id
    ),
    item_cols AS (
        SELECT item, string_agg(c.column_name, ', ' ORDER BY ordinal_position) AS cols
        FROM items, information_schema.columns c
        WHERE table_schema = 'road_graph'
        AND table_name = item
        AND c.column_name != 'id'
        GROUP BY item
    ),
    queries AS (
        SELECT item, "key"::integer AS id, "value" AS op
        FROM ids, jsonb_each_text(item_ids)
    )
    SELECT concat(
        CASE
            WHEN op = 'I' THEN 'INSERT INTO'
            WHEN op = 'U' THEN 'UPDATE'
            WHEN op = 'D' THEN 'DELETE FROM'
        END, ' ',
        '"road_graph".', q.item, ' AS t ',
        CASE
            WHEN op = 'I' THEN 'SELECT * FROM road_graph_editing.' || q.item || ' AS s '
            WHEN op = 'U' THEN concat(
                ' SET (', c.cols, ') = (',
                's.', replace(c.cols, ', ', ', s.'),
                ') FROM road_graph_editing.',  q.item, ' AS s '
            )
            WHEN op = 'D' THEN ''
        END, ' ',
        CASE
            WHEN op = 'I' THEN ' WHERE s.id = ' || id::text
            WHEN op = 'U' THEN ' WHERE s.id = t.id AND t.id = ' || id::text
            WHEN op = 'D' THEN ' WHERE t.id = ' || id::text
        END,
        CASE
            WHEN op = 'I' THEN ' ON CONFLICT DO NOTHING '
            ELSE ''
        END,
        ';'
    ) AS sql
    FROM queries AS q, item_cols AS c
    WHERE q.item = c.item
    ;

END;
$$;

-- FUNCTION: road_graph.drop_editing_session(integer)
CREATE OR REPLACE FUNCTION road_graph.drop_editing_session(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    _set_config text;
    _set_val bigint;
BEGIN
    -- Get editing session record
    -- We set the search path to avoid using the schema in the request
    -- since we need to get information from the road_graph schema,
    -- not the copy
    SET search_path TO road_graph, public;
    SELECT INTO editing_session_record
        *
    FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Raise exception if no record found
    IF editing_session_record.id IS NULL THEN
        RESET search_path;
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;

    -- Set the sequences back to the maximum id in the main tables
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('roads', 'id'), (SELECT max(id) FROM roads))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('markers', 'id'), (SELECT max(id) FROM markers))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('edges', 'id'), (SELECT max(id) FROM edges))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('nodes', 'id'), (SELECT max(id) FROM nodes))
    ;

    -- Delete merged editing session
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Reset the search path
    RESET search_path;

    RETURN True;
END;
$$;

-- FUNCTION: road_graph.editing_survey()
CREATE OR REPLACE FUNCTION road_graph.editing_survey() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    object_geom geometry;
    object_id integer;
    editing_session_geom geometry;
    original_edge_object record;
    new_logged_ids jsonb;
BEGIN

    -- Do nothing if the table is inside the production road_graph schema
    IF TG_TABLE_SCHEMA = 'road_graph' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NEW;
        END IF;
    END IF;

    -- Do nothing if the object has not changed for an update
    IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN
        RETURN NEW;
    END IF;

    -- Do nothing if data are the same as the road_graph data
    -- Since UPDATEs can occur on newly created object in the road_graph_editing schema
    -- we must check if a corresponding object exists in the road_graph schema
    -- before checking if they are equal
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'edges' THEN
        -- We need to defined the search_path to avoid using syntax
        -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
        -- when creating an editing_session_xxx schema
        SET search_path TO road_graph, public;

        -- Get original edge
        SELECT INTO original_edge_object
            *
        FROM edges
        WHERE id = NEW.id
        ;
        IF
            original_edge_object IS NOT NULL
            AND md5(to_json(NEW.*)::text) = md5(to_json(original_edge_object.*)::text)
        THEN
            -- Reset the search path
            RESET search_path;
            RETURN NEW;
        END IF;
        -- Reset the search path
        RESET search_path;
    END IF;

    -- 1/ Check if geometry is inside the editing session geometry
    -- Get editing session geometry
    IF to_jsonb(NEW) ? 'geom' THEN
        SELECT INTO editing_session_geom
            geom
        FROM road_graph_editing.editing_sessions
        LIMIT 1
        ;

        -- Get edited object geometry
        IF TG_OP = 'DELETE' THEN
            object_geom = OLD.geom;
        ELSE
            object_geom = NEW.geom;
        END IF;

        -- Check if the inserted, updated or deleted geometry is inside the editing session polygon
        -- The actual check depends on the operation
        IF TG_OP IN ('INSERT', 'DELETE') THEN
            -- INSERT AND DELETE must concern geometries which are fully inside the polygon
            -- NB : We must be less rigid since split actions can insert or update
            -- an edge which is not fully in the polygon
            -- IF NOT ST_Within(object_geom, editing_session_geom) THEN
            IF NOT ST_Intersects(object_geom, editing_session_geom) THEN
                RAISE EXCEPTION
                    -- 'The object geometry must be strictly inside the editing session polygon
                    -- object = %, polygon = %',
                    'The object geometry must intersect the editing session polygon
                    object = %, polygon = %',
                    ST_AsText(object_geom),
                    ST_AsText(editing_session_geom)
                ;
            END IF;
        END IF;

        -- UPDATE must modify only the part of the geometry which is inside the polygon
        IF TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom) THEN
            IF (
                -- Easy for points
                TG_TABLE_NAME != 'edges'
                AND NOT ST_Within(object_geom, editing_session_geom)
            )
            OR (
                -- For edges (linestrings) we must check the user did not change
                -- the geometry outside the polygon
                -- TODO : issue when deleting node which launches the merge of 2 edges
                -- the edge which was outside the polygon has been merged which modifies its geometry
                -- We do not enforce this constraint
                TG_TABLE_NAME = 'edges'
                /*
                AND NOT ST_Equals(
                    ST_SymDifference(OLD.geom, editing_session_geom),
                    ST_SymDifference(NEW.geom, editing_session_geom)
                )
                */
                AND NOT ST_Intersects(object_geom, editing_session_geom)
            )
            THEN
                -- RAISE EXCEPTION 'The updated geometry must be strictly inside the editing session polygon';
                RAISE EXCEPTION 'The updated geometry must intersect the editing session polygon';
            END IF;
        END IF;
    END IF;

    -- 2/ Log editing inside the road_graph_editing table*
    -- edited object id
    object_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.id
        ELSE NEW.id
    END
    ;

    -- Update log in the editing session
    -- We need to defined the search_path to avoid using syntax
    -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
    -- when creating an editing_session_xxx schema
    SET search_path TO road_graph, public;
    SELECT INTO new_logged_ids
        logged_ids
    FROM editing_sessions
    WHERE unique_code = (
        SELECT unique_code
        FROM road_graph_editing.editing_sessions
        LIMIT 1
    )
    ;

    IF new_logged_ids[TG_TABLE_NAME] IS NULL THEN
        new_logged_ids[TG_TABLE_NAME] = '{}'::jsonb;
        new_logged_ids[TG_TABLE_NAME][object_id::text] = format('"%s"', substr(TG_OP, 1, 1))::jsonb;
    ELSE
        IF new_logged_ids[TG_TABLE_NAME][object_id::text] IS NULL THEN
            new_logged_ids[TG_TABLE_NAME][object_id::text] = format('"%s"', substr(TG_OP, 1, 1))::jsonb;
        ELSE
            -- DELETE
            IF TG_OP = 'DELETE' THEN
                -- If the DELETE comes after an INSERT, remove it completely
                -- The user has just deleted an object which has been inserted in the editing session
                IF new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"'::jsonb THEN
                    new_logged_ids = new_logged_ids #- (format('{%s,%s}', TG_TABLE_NAME, object_id::text))::text[];
                ELSE
                    new_logged_ids[TG_TABLE_NAME][object_id::text] = '"D"';
                END IF;
            -- UPDATE
            ELSIF TG_OP = 'UPDATE' THEN
                -- If the UPDATE comes after an INSERT, keep the I
                -- The object could be updated several times afterwards
                -- we must keep the track of its insert
                IF new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"'::jsonb THEN
                     new_logged_ids = new_logged_ids;
                ELSE
                    new_logged_ids[TG_TABLE_NAME][object_id::text] = '"U"';
                END IF;
            -- INSERT
            ELSE
                new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"';
            END IF;
        END IF;
    END IF;

    UPDATE editing_sessions
    SET
        -- Add the id of the object
        logged_ids = new_logged_ids,
        "status" = 'edited'
    WHERE unique_code = (
        SELECT unique_code
        FROM road_graph_editing.editing_sessions
        LIMIT 1
    )
    ;
    -- Reset the search path
    RESET search_path;

    -- Return object
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;
$$;

-- FUNCTION: road_graph.get_updated_roads_from_editing_session(integer)
CREATE OR REPLACE FUNCTION road_graph.get_updated_roads_from_editing_session(_editing_session_id integer) RETURNS TABLE(road_codes text[])
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    WITH
    items AS (
        SELECT unnest(array['edges', 'markers']) AS item
    ),
    ids AS (
        SELECT item, logged_ids->item AS item_ids
        FROM items, "road_graph".editing_sessions
        WHERE status = 'edited'
        AND id = _editing_session_id
    ),
    queries AS (
        SELECT item, "key"::integer AS id, "value" AS op
        FROM ids, jsonb_each_text(item_ids)
    ),
    objects_ids AS (
        SELECT
            item,
            array_agg(id) AS o_ids
        FROM queries
        GROUP BY item
    ),
    codes AS (
        SELECT
            DISTINCT e.road_code
        FROM
            road_graph_editing.edges AS e,
            objects_ids AS o
        WHERE o.item = 'edges' AND e.id = ANY(o.o_ids)
        UNION
        SELECT
            DISTINCT m.road_code
        FROM
            road_graph_editing.markers AS m,
            objects_ids AS o
        WHERE o.item = 'markers' AND m.id = ANY(o.o_ids)
    )
    SELECT
        array_agg(DISTINCT road_code) AS road_codes
    FROM codes
    ;

END;
$$;

-- FUNCTION: road_graph.merge_editing_session_data(integer)
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    road_codes text[];
    managed_object record;
    updated_objects_number integer;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SET search_path TO road_graph, public;
    SELECT INTO editing_session_record
    *
    FROM editing_sessions
    WHERE id = _editing_session_id
    ;
    RESET search_path;

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
    -- no need to prefix the function by the schema name because we set it in the search path
    SET search_path TO road_graph, public;
    SELECT INTO road_codes
        get_updated_roads_from_editing_session(_editing_session_id)
    ;

    -- Create SQL to run for each edited data
    -- no need to prefix the function by the schema name because we set it in the search path
    FOR sql_record IN
        SELECT
            sql_text
        FROM create_queries_from_editing_session(_editing_session_id)
    LOOP
       -- RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;
    RESET search_path;

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
                    road_codes
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

    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;

    -- Delete merged editing session
    SET search_path TO road_graph, public;
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;
    RESET search_path;

    -- Refresh the materialized view of the last updated managed objects
    REFRESH MATERIALIZED VIEW CONCURRENTLY road_graph.vm_managed_objects;

    RETURN True;
END;
$$;

-- COMMENT: road_graph.before_editing_sessions_update()
COMMENT ON FUNCTION road_graph.before_editing_sessions_update() IS 'Prevent from updating an editing session geometry if there is data inside the road_graph_editing schema';

-- COMMENT: road_graph.copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the road_graph_editing schema corresponding to the given editing session ID.';

-- COMMENT: road_graph.drop_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.drop_editing_session(_editing_session_id integer) IS 'Delete the given editing session and truncate all the table of the road_graph_editing schema';

-- COMMENT: road_graph.toggle_foreign_key_constraints(_toggle boolean)
COMMENT ON FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) IS 'Deactivate or reactivate the foreign key constraints to ease
the merging road_graph_editing data into road_graph schema.
It is also used when importing data from templates.

Under the hood, it drops or drops & creates the constraint
depending on the value of the _toggle parameter
';

-- Replace the schema name in the bodies of the other functions still referencing the old schema name,
-- for example the copies of the road_graph functions stored in the editing schema
DO $$
DECLARE
    old_schema text := 'editing_session';
    new_schema text := 'road_graph_editing';
    function_record record;
BEGIN
    FOR function_record IN
        SELECT p.oid, n.nspname, p.proname
        FROM pg_proc AS p
        JOIN pg_namespace AS n
            ON n.oid = p.pronamespace
        WHERE n.nspname IN ('road_graph', 'road_graph_editing')
        AND p.prokind IN ('f', 'p')
        AND p.prosrc ~ ('\m' || old_schema || '\.')
    LOOP
        RAISE NOTICE 'Replace the schema % by % in the function %.%',
            old_schema, new_schema, function_record.nspname, function_record.proname
        ;
        EXECUTE regexp_replace(
            pg_get_functiondef(function_record.oid),
            '\m' || old_schema || '\.',
            new_schema || '.',
            'g'
        );
    END LOOP;
END
$$;
