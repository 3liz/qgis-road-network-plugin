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
