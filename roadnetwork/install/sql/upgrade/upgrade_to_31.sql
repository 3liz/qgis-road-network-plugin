-- TABLE: road_graph.managed_objects
-- Add a column to choose for which tables the references trigger must be created
ALTER TABLE road_graph.managed_objects
ADD COLUMN IF NOT EXISTS add_trigger_on_geom_change boolean DEFAULT false NOT NULL;

COMMENT ON COLUMN road_graph.managed_objects.add_trigger_on_geom_change
IS 'If True, the function road_graph.add_managed_objects_triggers will create the trigger
trg_calculate_managed_object_references on this table, which calculates the references
of the features on insert or on geometry update.';

-- FUNCTION: road_graph.add_managed_objects_triggers()

DROP FUNCTION IF EXISTS road_graph.add_managed_objects_triggers();
CREATE OR REPLACE FUNCTION road_graph.add_managed_objects_triggers()
    RETURNS TABLE(
        schema_name text,
        table_name text,
        trigger_status text
    )
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    managed_object record;
    table_regclass regclass;
BEGIN
    FOR managed_object IN
        SELECT o.schema_name, o.table_name, o.add_trigger_on_geom_change
        FROM road_graph.managed_objects AS o
        ORDER BY o.id
    LOOP
        schema_name = managed_object.schema_name;
        table_name = managed_object.table_name;

        -- Only add the trigger if it has been asked for this table
        IF NOT managed_object.add_trigger_on_geom_change THEN
            trigger_status = 'not requested';
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- Check the table exists
        table_regclass = to_regclass(format('%I.%I', schema_name, table_name));
        IF table_regclass IS NULL THEN
            trigger_status = 'table not found';
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- Check the table has a geom column, used by the trigger
        IF NOT EXISTS (
            SELECT 1
            FROM pg_attribute AS a
            WHERE a.attrelid = table_regclass
            AND a.attname = 'geom'
            AND NOT a.attisdropped
        ) THEN
            trigger_status = 'geom column not found';
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- Do not create the trigger if it already exists
        IF EXISTS (
            SELECT 1
            FROM pg_trigger AS t
            WHERE t.tgrelid = table_regclass
            AND t.tgname = 'trg_calculate_managed_object_references'
            AND NOT t.tgisinternal
        ) THEN
            trigger_status = 'already exists';
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- Create the trigger
        EXECUTE format(
            $SQL$
            CREATE TRIGGER trg_calculate_managed_object_references
            BEFORE INSERT OR UPDATE OF geom
            ON %1$I.%2$I
            FOR EACH ROW EXECUTE PROCEDURE road_graph.calculate_managed_object_references();
            $SQL$,
            schema_name,
            table_name
        );
        trigger_status = 'created';
        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$BODY$;

COMMENT ON FUNCTION road_graph.add_managed_objects_triggers()
IS 'Create the trigger trg_calculate_managed_object_references for each table listed in road_graph.managed_objects
with the column add_trigger_on_geom_change set to True, if it does not already exist.
This trigger calculates the references of the inserted or updated features
with the function road_graph.calculate_managed_object_references.
It returns one line per managed object with its schema name, table name and trigger status:
created, already exists, not requested, table not found or geom column not found.';

-- FUNCTION: road_graph.get_merged_geom_from_table_and_ids(text, text, integer[])
-- Set the function search_path, needed to create and refresh the materialized view vm_managed_objects:
-- since PostgreSQL 17, materialized views are refreshed with a restricted search_path (pg_catalog, pg_temp)
-- and the PostGIS types and functions used by this function would not be found
ALTER FUNCTION road_graph.get_merged_geom_from_table_and_ids(text, text, integer[]) SET search_path TO public;

-- MATERIALIZED VIEW: road_graph.vm_managed_objects
-- Materialized version of the view v_managed_objects, which can be slow to compute.
-- It is refreshed at the end of the function merge_editing_session_data
DROP MATERIALIZED VIEW IF EXISTS road_graph.vm_managed_objects;
CREATE MATERIALIZED VIEW road_graph.vm_managed_objects AS
SELECT
    v.id,
    v.schema_name,
    v.table_name,
    v.geometry_type,
    v.update_policy_on_graph_change,
    v.last_update,
    v.last_updated_objects_ids,
    v.geom::geometry(MultiPolygon, 2154) AS geom
FROM road_graph.v_managed_objects AS v
;

CREATE UNIQUE INDEX vm_managed_objects_id_idx ON road_graph.vm_managed_objects (id);
CREATE INDEX vm_managed_objects_geom_idx ON road_graph.vm_managed_objects USING GIST (geom);

COMMENT ON MATERIALIZED VIEW road_graph.vm_managed_objects
IS 'Materialized view based on the view v_managed_objects, showing the merged geometries of each managed table
for the last editing session merge. It is refreshed at the end of the function merge_editing_session_data.';

-- FUNCTION: road_graph.merge_editing_session_data(integer)
-- Refresh the materialized view vm_managed_objects at the end of the merge
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

    -- Truncate editing_session tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;

    -- Delete merged editing session
    SET search_path TO road_graph, public;
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;
    RESET search_path;

    -- Refresh the materialized view of the last updated managed objects
    REFRESH MATERIALIZED VIEW road_graph.vm_managed_objects;

    RETURN True;
END;
$$;

COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';
