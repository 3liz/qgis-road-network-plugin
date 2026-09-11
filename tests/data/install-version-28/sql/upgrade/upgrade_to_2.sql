-- Create editing session from given polygon
CREATE OR REPLACE FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    editing_session_record record;
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
    SET session_replication_role = replica;

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
    -- roads
    ALTER TABLE editing_session.roads
        ALTER COLUMN id DROP IDENTITY;
    ALTER TABLE editing_session.roads
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.roads', 'id'));
    -- nodes
    ALTER TABLE editing_session.nodes
        ALTER COLUMN id DROP IDENTITY;
    ALTER TABLE editing_session.nodes
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.nodes', 'id'));
    -- markers
    ALTER TABLE editing_session.markers
        ALTER COLUMN id DROP IDENTITY;
    ALTER TABLE editing_session.markers
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.markers', 'id'));
    -- edges
    ALTER TABLE editing_session.edges
        ALTER COLUMN id DROP IDENTITY;
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
    SET session_replication_role = DEFAULT;

    RETURN True;
END;
$$;



CREATE OR REPLACE FUNCTION road_graph.editing_survey()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    object_geom geometry;
    object_id integer;
    editing_session_geom geometry;
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

    -- 1/ Check if geometry is inside the editing session geometry
    -- Get editing session geometry
    IF to_jsonb(NEW) ? 'geom' THEN
        SELECT INTO editing_session_geom
            geom
        FROM editing_session.editing_sessions
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
            IF NOT ST_Within(object_geom, editing_session_geom) THEN
                RAISE EXCEPTION
                    'The object geometry must be strictly inside the editing session polygon
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
                -- TODO : issue when deleting node which launch the merge of 2 edges
                TG_TABLE_NAME = 'edges'
                AND NOT ST_Equals(
                    ST_SymDifference(OLD.geom, editing_session_geom),
                    ST_SymDifference(NEW.geom, editing_session_geom)
                )
                AND FALSE -- TODO corriger
            )
            THEN
                RAISE EXCEPTION 'The updated geometry must be strictly inside the editing session polygon';
            END IF;
        END IF;
    END IF;

    -- 2/ Log editing inside the editing_session table*
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
        FROM editing_session.editing_sessions
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
        FROM editing_session.editing_sessions
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
$BODY$;

COMMENT ON FUNCTION road_graph.editing_survey()
    IS 'Logs the modifications done inside the editing session schema tables.
It also check that the edited geometries are inside the editing session polygon.';
