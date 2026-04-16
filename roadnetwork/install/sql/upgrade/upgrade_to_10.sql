-- Modify the managed_objects table
COMMENT ON TABLE road_graph.managed_objects
IS 'Table listing all managed objects in the road graph system';
ALTER TABLE road_graph.managed_objects DROP COLUMN IF EXISTS object_type;
COMMENT ON COLUMN road_graph.managed_objects.schema_name IS 'The schema name of the managed object table';
COMMENT ON COLUMN road_graph.managed_objects.table_name IS 'The table name of the managed object table';
COMMENT ON COLUMN road_graph.managed_objects.geometry_type
    IS 'The geometry type of the managed object table. It can be POINT or LINESTRING or MULTILINESTRING.';
COMMENT ON COLUMN road_graph.managed_objects.update_policy_on_graph_change
    IS 'The update policy to apply on the managed objects when the road graph changes.
It can be "geometry" to update only the geometries based on the references,
"references" to update only the references based on the geometries
or "none" to not update anything.';
ALTER TABLE road_graph.managed_objects DROP CONSTRAINT IF EXISTS check_geometry_type;
ALTER TABLE road_graph.managed_objects ADD CONSTRAINT check_geometry_type
CHECK (lower(geometry_type) = ANY(ARRAY['point', 'linestring', 'multilinestring']) );

-- Add a column to store the update time and the updated objects ids
ALTER TABLE road_graph.managed_objects
    ADD COLUMN IF NOT EXISTS last_update timestamp without time zone
    DEFAULT now()::timestamp(0)
;
COMMENT ON COLUMN road_graph.managed_objects.last_update
IS 'The date and time of the last update of table objects based on the road graph.'
;
ALTER TABLE road_graph.managed_objects
    ADD COLUMN IF NOT EXISTS last_updated_objects_ids integer[]
;
COMMENT ON COLUMN road_graph.managed_objects.last_updated_objects_ids
IS 'The list of the last updated objects ids based on the road graph.'
;

-- Update the records geometries of the given table schema and name
-- for the given list or road codes
-- based on the references stored in the table.
-- It returns a JSONB containing the number of updated features
-- and the list of primary key values of these features
-- and stores the records ids in the road_graph.managed_objects table
CREATE OR REPLACE FUNCTION road_graph.update_table_geometries_from_references(
    _schema_name text,
    _table_name text,
    _road_codes text[]
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    road_code text;
    update_count integer;
    sql_text text;
    table_exists boolean;
    primary_key_field text;
    needed_fields text[];
    managed_object record;
    table_cols text[];
    updated_stats record;
BEGIN

    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Get the table primary key field
    sql_text = format(
        $SQL$
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '%1$I.%2$I'::regclass AND i.indisprimary
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO primary_key_field
    ;
    IF primary_key_field IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a primary key !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check the table contains the needed fields based on the geometry type of the managed object
    sql_text = format(
        $SQL$
        SELECT array_agg(column_name) AS cols
        FROM information_schema.columns
        WHERE table_schema = '%1$s' AND table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_cols
    ;
    -- For point geometry type, we need road_code, marker_code and abscissa fields
    needed_fields = ARRAY[
        'road_code', 'marker_code', 'abscissa'
    ]::text[];
    -- For line geometry type, we also need start_marker_code, start_abscissa, end_marker_code and end_abscissa fields
    IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring')
    THEN
        needed_fields = ARRAY[
            'road_code',
            'start_marker_code', 'start_abscissa',
            'end_marker_code', 'end_abscissa'
        ]::text[];
    END IF;
    IF NOT (table_cols @> needed_fields)
        THEN
            RAISE NOTICE 'The table "%"."%" does not contain the necessary fields to update geometries from references !', _schema_name, _table_name;
            RETURN NULL;
    END IF;

    -- Update geometries based on references
    -- and geometry type of the managed object
    -- We filter the objects to update based on the given road codes
    -- We must check if the road_graph function returns a valid geometry
    sql_text = format(
        $SQL$
            WITH get_geom AS (
                SELECT
                    mo.%1$I AS id,
                    road_graph.%8$s(
                        mo.road_code,
                        %9$s,
                        %6$s,
                        %7$s
                    )->'geom' AS geom
                FROM
                    %2$I.%3$I AS mo
                WHERE (
                    mo.road_code = ANY(string_to_array('%4$s', ',')::text[])
                    OR '%4$s' = ''
                )
            ),
            updated_objects AS (
                SELECT
                    g.id,
                    CASE
                        WHEN g.geom = 'null'::jsonb THEN NULL
                        ELSE ST_GeomFromGeoJSON(g.geom)
                    END::geometry(%5$s, 2154) AS geom
                FROM get_geom AS g
            ),
            run_update AS (
                UPDATE %2$I.%3$I AS mo
                SET
                    geom = u.geom
                FROM updated_objects AS u
                WHERE mo.%1$I = u.id
                AND u.geom IS NOT NULL
                AND (mo.geom IS NULL OR NOT ST_Equals(mo.geom, u.geom))
                RETURNING mo.*
            )
            SELECT
                count(r.*) AS nb,
                array_agg(r.%1$I) AS last_updated_objects_ids
            FROM run_update AS r
        $SQL$,
        primary_key_field,
        _schema_name,
        _table_name,
        Coalesce(array_to_string(_road_codes::text[], ','), ''),
        managed_object.geometry_type,
        -- Use default values for side and offset if columns does not exists
        CASE WHEN 'offset' = ANY(table_cols) THEN 'mo."offset"' ELSE '0.0' END,
        CASE WHEN 'side' = ANY(table_cols) THEN 'mo.side' ELSE 'right' END,
        -- used function
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'get_road_point_from_reference'
            ELSE 'get_road_substring_from_references'
        END,
        -- used fields
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'mo.marker_code, mo.abscissa'
            ELSE 'mo.start_marker_code, mo.start_abscissa, mo.end_marker_code, mo.end_abscissa'
        END
    );
    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$$;


-- Update the records references of the given table schema and name
-- for the given list or road codes
-- based on the objects geometries
-- It returns a JSONB containing the number of updated features
-- and the list of primary key values of these features
-- and stores the update records ids in the road_graph.managed_objects table
CREATE OR REPLACE FUNCTION road_graph.update_table_references_from_geometries(
    _schema_name text,
    _table_name text,
    _road_codes text[]
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    road_code text;
    update_count integer;
    sql_text text;
    table_exists boolean;
    primary_key_field text;
    needed_fields text[];
    managed_object record;
    table_cols text[];
    updated_stats record;
BEGIN

    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Get the table primary key field
    sql_text = format(
        $SQL$
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '%1$I.%2$I'::regclass AND i.indisprimary
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO primary_key_field
    ;
    IF primary_key_field IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a primary key !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check the table contains the needed fields based on the geometry type of the managed object
    sql_text = format(
        $SQL$
        SELECT array_agg(column_name) AS cols
        FROM information_schema.columns
        WHERE table_schema = '%1$s' AND table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_cols
    ;
    -- For point geometry type, we need road_code, marker_code and abscissa fields
    needed_fields = ARRAY[
        'road_code', 'marker_code', 'abscissa'
    ]::text[];
    -- For line geometry type, we also need start_marker_code, start_abscissa, end_marker_code and end_abscissa fields
    IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring')
    THEN
        needed_fields = ARRAY[
            'road_code',
            'start_marker_code', 'start_abscissa',
            'end_marker_code', 'end_abscissa'
        ]::text[];
    END IF;
    IF NOT (table_cols @> needed_fields)
        THEN
            RAISE NOTICE 'The table "%"."%" does not contain the necessary fields to update references from geometries !', _schema_name, _table_name;
            RETURN NULL;
    END IF;

    -- Update references based on geometries
    -- and geometry type of the managed object
    -- We filter the objects to update based on the given road codes
    IF lower(managed_object.geometry_type) = 'point' THEN
        sql_text = format(
            $SQL$
                WITH
                objects AS (
                    SELECT
                        mo.%1$I AS id,
                        mo.road_code,
                        mo.geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            o.geom, o.road_code
                        ) AS ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.ref->>'road_code',
                        -- cumulative if present
                        %5$s
                        -- offset if present
                        %6$s
                        -- side if present
                        %7$s
                        -- marker code and abscissa put here to avoid errors with commas
                        marker_code = (r.ref->>'marker_code')::integer,
                        abscissa = (r.ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.id = r.id
                    --AND r.ref->'road_code' != 'null'::jsonb
                    AND (%8$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'cumulative' = ANY(table_cols) THEN $STR$cumulative = (r.ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE WHEN 'offset' = ANY(table_cols) THEN $STR$"offset" = (r.ref->>'offset')::real, $STR$ ELSE '' END,
            -- 7 / add update for side if the columns exists in the target table
            CASE WHEN 'side' = ANY(table_cols) THEN $STR$side = (r.ref->>'side')::text, $STR$ ELSE '' END,
            -- 8 / Detect if we need to update or not
            concat(
                $STR$
                (r.ref->>'road_code') != mo.road_code
                OR (r.ref->>'marker_code')::integer != mo.marker_code::integer
                OR (r.ref->>'abscissa')::real != mo.abscissa::real
                $STR$,
                CASE WHEN 'cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.ref->>'cumulative')::integer != mo.cumulative::real $STR$ ELSE ''
                END,
                CASE WHEN 'offset' = ANY(table_cols)
                    THEN $STR$ OR (r.ref->>'offset')::real != mo.offset::real $STR$ ELSE ''
                END,
                CASE WHEN 'side' = ANY(table_cols)
                    THEN $STR$ OR (r.ref->>'side')::text != mo.side::text $STR$ ELSE ''
                END
            )
        );

    ELSIF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring') THEN
        sql_text = format(
            $SQL$
                WITH
                objects AS (
                    SELECT
                        mo.%1$I AS id,
                        mo.road_code,
                        mo.geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            ST_StartPoint(o.geom), o.road_code
                        ) AS start_ref,
                        road_graph.get_reference_from_point(
                            ST_EndPoint(o.geom), o.road_code
                        ) AS end_ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.start_ref->>'road_code',
                        -- start_cumulative if present
                        %5$s
                        -- start_offset if present
                        %6$s
                        -- start_side if present
                        %7$s
                        -- end_cumulative if present
                        %8$s
                        -- end_offset if present
                        %9$s
                        -- end_side if present
                        %10$s
                        -- marker code and abscissa put here to avoid errors with commas
                        start_marker_code = (r.start_ref->>'marker_code')::integer,
                        start_abscissa = (r.start_ref->>'abscissa')::real,
                        end_marker_code = (r.end_ref->>'marker_code')::integer,
                        end_abscissa = (r.end_ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.id = r.id
                    -- Do not UPDATE if no changes must be made (values already are the same)
                    AND (%11$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'start_cumulative' = ANY(table_cols) THEN $STR$cumulative = (r.ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE WHEN 'start_offset' = ANY(table_cols) THEN $STR$"offset" = (r.ref->>'offset')::real, $STR$ ELSE '' END,
            -- 7 / add update for side if the columns exists in the target table
            CASE WHEN 'start_side' = ANY(table_cols) THEN $STR$side = (r.ref->>'side')::text, $STR$ ELSE '' END,
            -- 8 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'end_cumulative' = ANY(table_cols) THEN $STR$cumulative = (r.ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 9 / add update for offset if the columns exists in the target table
            CASE WHEN 'end_offset' = ANY(table_cols) THEN $STR$"offset" = (r.ref->>'offset')::real, $STR$ ELSE '' END,
            -- 10 / add update for side if the columns exists in the target table
            CASE WHEN 'end_side' = ANY(table_cols) THEN $STR$side = (r.ref->>'side')::text, $STR$ ELSE '' END,
            -- 11 / Detect if we need to update or not
            concat(
                $STR$
                (r.start_ref->>'road_code') != mo.road_code
                OR (r.start_ref->>'marker_code')::integer != mo.start_marker_code::integer
                OR (r.start_ref->>'abscissa')::real != mo.start_abscissa::real
                OR (r.end_ref->>'marker_code')::integer != mo.end_marker_code::integer
                OR (r.end_ref->>'abscissa')::real != mo.end_abscissa::real
                $STR$,
                CASE WHEN 'start_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.start_ref->>'cumulative')::integer != mo.start_cumulative::real $STR$ ELSE ''
                END,
                CASE WHEN 'start_offset' = ANY(table_cols)
                    THEN $STR$ OR (r.start_ref->>'offset')::real != mo.start_offset::real $STR$ ELSE ''
                END,
                CASE WHEN 'start_side' = ANY(table_cols)
                    THEN $STR$ OR (r.start_ref->>'side')::text != mo.start_side::text $STR$ ELSE ''
                END,
                CASE WHEN 'end_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.end_ref->>'cumulative')::integer != mo.end_cumulative::real $STR$ ELSE ''
                END,
                CASE WHEN 'end_offset' = ANY(table_cols)
                    THEN $STR$ OR (r.end_ref->>'offset')::real != mo.end_offset::real $STR$ ELSE ''
                END,
                CASE WHEN 'end_side' = ANY(table_cols)
                    THEN $STR$ OR (r.end_ref->>'side')::text != mo.end_side::text $STR$ ELSE ''
                END
            )
        );
    END IF;

    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$$;


-- update_managed_objects_on_graph_change(text, text, integer[])
DROP FUNCTION IF EXISTS road_graph.update_managed_objects_on_graph_change(text, text, integer[]);
CREATE OR REPLACE FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[])
RETURNS integer
LANGUAGE plpgsql
AS $_$
DECLARE
    sql_text text;
    table_exists boolean;
    managed_object record;
    updated_stats jsonb;
    update_count integer;
BEGIN
    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Update objects
    IF managed_object.update_policy_on_graph_change = 'geometry'
    THEN
        -- Update geometries based on references
        SELECT road_graph.update_table_geometries_from_references(
            _schema_name,
            _table_name,
            _road_codes
        )
        INTO updated_stats
        ;

    ELSIF managed_object.update_policy_on_graph_change = 'references' THEN
        SELECT road_graph.update_table_references_from_geometries(
            _schema_name,
            _table_name,
            _road_codes
        )
        INTO updated_stats
        ;
    END IF;

    -- Check results
    IF updated_stats IS NULL THEN
        RAISE NOTICE 'No object updated for %.%', _schema_name, _table_name;
        RETURN NULL;
    END IF;
    IF (updated_stats->>'nb')::integer = 0 THEN
        RAISE NOTICE 'No object updated for %.%', _schema_name, _table_name;
        RETURN NULL;
    END IF;
    -- Update the managed_objects table
    EXECUTE format(
        $SQL$
            UPDATE road_graph.managed_objects AS o
            SET (
                last_updated_objects_ids,
                last_update
            ) = (
                string_to_array('%3$s', ','),
                now()::timestamp(0)
            )
            WHERE o.schema_name = %1$I
            AND o.table_name = %2$I
        $SQL$,
        _schema_name,
        _table_name,
        -- convert json array to string like 1,3,10
        (
            SELECT array_to_string(array_agg(j)::text[], ',')
            FROM jsonb_array_elements(updated_stats->last_updated_objects_ids) AS j
        )
    );

    -- Return the number of updated features
    RETURN updated_stats.nb;

END;
$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _ids integer[])
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[])
IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table'
;
