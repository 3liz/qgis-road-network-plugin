-- FUNCTION: road_graph.merge_table_features(text, text, integer, integer)

DROP FUNCTION IF EXISTS road_graph.merge_table_linestring_features(text, text, integer, integer);
CREATE OR REPLACE FUNCTION road_graph.merge_table_linestring_features(
	_schema_name text,
	_table_name text,
	_feature_id_a integer,
	_feature_id_b integer
)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    primary_key_field text;
    geometry_column_data record;
    feature_a record;
    feature_b record;
    feature_to_delete record;
    sql_text text;
    target_geometry geometry(MULTILINESTRING, 2154);
    update_result record;
BEGIN
    -- Check the given feature ids
    IF Coalesce(_feature_id_a, 0) = Coalesce(_feature_id_b, 0) THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('The given feature ids are equals: %L and %L', _feature_id_a, _feature_id_b)
        );
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
        RETURN json_build_object(
            'status', 'error',
            'message', format('The table %I.%I does not have a primary key !', _schema_name, _table_name)
        );
    END IF;

    -- Get geometry column name
    sql_text = format(
        $SQL$
        SELECT f_geometry_column AS geom_name, "type" AS geom_type
        FROM geometry_columns
        WHERE f_table_schema = '%1$s' AND f_table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO geometry_column_data
    ;
    IF geometry_column_data.geom_name IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('The table %I.%I does not have a geometry column !', _schema_name, _table_name)
        );
    END IF;

    -- Get both features
    -- feature A
    sql_text = format(
        $SQL$
            SELECT *
            FROM %1$I.%2$I
            WHERE %3$I = %4$s
            LIMIT 1
        $SQL$,
        _schema_name,
        _table_name,
        primary_key_field,
        _feature_id_a
    );
    EXECUTE sql_text
    INTO feature_a
    ;
    IF feature_a.geom IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('No object found with id = %L ', _feature_id_a)
        );
    END IF;

    -- Check road_code values
    IF NOT to_jsonb(feature_a) ? 'road_code' THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'The given feature does not have the mandatory column road_code !'
        );
    END IF;

    -- feature B
    sql_text = format(
        $SQL$
            SELECT *
            FROM %1$I.%2$I
            WHERE %3$I = %4$s
            LIMIT 1
        $SQL$,
        _schema_name,
        _table_name,
        primary_key_field,
        _feature_id_b
    );
    EXECUTE sql_text
    INTO feature_b
    ;
    IF feature_b.geom IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('No object found with id = %L ', _feature_id_b)
        );
    END IF;

    -- Prevent from merging if features does not share the same road_code
    IF Coalesce(feature_a.road_code, '') != Coalesce(feature_b.road_code, '') THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format(
                'The given features do not share the same road_code: %L VS %L',
                feature_a.road_code,
                feature_b.road_code
            )
        );
    END IF;

    -- Merge features: keep the one with the smallest start_cumulative
    -- and add geometry from the other one
    -- Decide which feature must be deleted
    feature_to_delete = feature_b;
    IF
        Coalesce(feature_b.start_marker_code * 10000 + feature_b.start_abscissa, 0.0)
        < Coalesce(feature_a.start_marker_code * 10000 + feature_a.start_abscissa, 0.0)
    THEN
        feature_to_delete = feature_a;
    END IF;

    -- Compute merged geometry
    target_geometry = road_graph.reorder_multilinestring_parts(
        ST_Multi(
            ST_Union(
                ST_GeomFromGeoJSON((to_json(feature_a))->>geometry_column_data.geom_name),
                ST_GeomFromGeoJSON((to_json(feature_b))->>geometry_column_data.geom_name)
            )
        ),
        feature_a.road_code
    );

    -- Delete and UPDATE in the same request
    sql_text = format(
        $SQL$
            WITH del AS (
                DELETE FROM %1$I.%2$I
                WHERE %3$I = %4$s
                RETURNING *
            ),
            up AS (
                UPDATE %1$I.%2$I
                SET %5$I = ST_GeomFromGeoJSON('%6$s')
                WHERE True
                AND %3$I IN (%7$s, %8$s)
                AND %3$I != %4$s
                RETURNING *
            )
            SELECT *
            FROM up
        $SQL$,
        _schema_name,
        _table_name,
        primary_key_field,
        (to_jsonb(feature_to_delete))->>primary_key_field,
        geometry_column_data.geom_name,
        ST_AsGeoJSON(target_geometry),
        _feature_id_a, _feature_id_b
    );
    EXECUTE sql_text
    INTO update_result
    ;

    RETURN json_build_object(
        'status', 'success',
        'message', 'The two features have been successfully merged.'
    );

END;
$BODY$;

COMMENT ON FUNCTION road_graph.merge_table_linestring_features(text, text, integer, integer)
    IS 'Merge two linestring features of the same table. The kept one is the one with the lower start_cumulative. The other one is deleted.';


-- Splot
DROP FUNCTION IF EXISTS road_graph.split_table_linestring_feature_by_point(text, text, integer, geometry);
CREATE OR REPLACE FUNCTION road_graph.split_table_linestring_feature_by_point(
	_schema_name text,
	_table_name text,
    _feature_id integer,
    _point geometry(POINT, 2154)
)
RETURNS json
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    primary_key_field text;
    geometry_column_data record;
    feature record;
    feature_geom geometry;
    source_fields text;
    closest_point geometry(point, 2154);
    closest_point_references jsonb;
    feature_start_point geometry(point, 2154);
    feature_end_point geometry(point, 2154);
    start_references jsonb;
    end_references jsonb;
    geom_1 geometry;
    geom_2 geometry;
    sql_text text;
    update_result record;
    insert_result record;
    v_sqlstate text;
    v_message text;
    v_context text;
BEGIN

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
        RETURN json_build_object(
            'status', 'error',
            'message', format('The table %I.%I does not have a primary key !', _schema_name, _table_name)
        );
    END IF;

    -- Get geometry column name
    sql_text = format(
        $SQL$
        SELECT f_geometry_column AS geom_name, "type" AS geom_type
        FROM geometry_columns
        WHERE f_table_schema = '%1$s' AND f_table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO geometry_column_data
    ;
    IF geometry_column_data.geom_name IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('The table %I.%I does not have a geometry column !', _schema_name, _table_name)
        );
    END IF;

    -- Get feature
    sql_text = format(
        $SQL$
            SELECT *
            FROM %1$I.%2$I
            WHERE %3$I = %4$s
            LIMIT 1
        $SQL$,
        _schema_name,
        _table_name,
        primary_key_field,
        _feature_id
    );
    EXECUTE sql_text
    INTO feature
    ;
    IF feature.geom IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('No feature could be found with %L = %L ', primary_key_field, _feature_id)
        );
    END IF;

    -- Check road_code
    IF nullif(trim(to_jsonb(feature)->>'road_code'), '') IS NULL THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'The given feature does not have a valid road code !'
        );
    END IF;

    -- Prevent splitting if the point is not close enough to the given feature
    IF ST_Distance(feature.geom, _point) > 5 THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format('The point is not close enough to the feature (distance %L > 5m)', ST_Distance(feature.geom, _point)::int)
        );
    END IF;

    -- Get feature start and end point
    feature_start_point = CASE
        WHEN lower(GeometryType(feature.geom)) = 'linestring'
            THEN ST_StartPoint(feature.geom)
        -- hopefully the last part is really the end part of the multilinestring
        ELSE ST_StartPoint(ST_GeometryN(feature.geom, 1))
    END;
    feature_end_point = CASE
        WHEN lower(GeometryType(feature.geom)) = 'linestring'
            THEN ST_EndPoint(feature.geom)
        -- hopefully the last part is really the end part of the multilinestring
        ELSE ST_EndPoint(ST_GeometryN(feature.geom, ST_NumGeometries(feature.geom)))
    END;

    -- Check position of the cutting point agains these start & end points
    closest_point = ST_ClosestPoint(
        feature.geom,
        _point
    );
    IF ST_DWithin(closest_point, feature_start_point, 1.0) THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format(
                'The given point is too close to the start point of the given feature (%L m).',
                Round(ST_Distance(closest_point, feature_start_point)::numeric, 1)
            )
        );
    END IF;
    IF ST_DWithin(closest_point, feature_end_point, 1.0) THEN
        RETURN json_build_object(
            'status', 'error',
            'message', format(
                'The given point is too close to the end point of the given feature (%L m).',
                Round(ST_Distance(closest_point, feature_end_point)::numeric, 1)
            )
        );
    END IF;

    -- Get the references of the closet point, start, end points
    start_references = road_graph.get_reference_from_point(
        feature_start_point,
        Nullif(
            trim(to_jsonb(feature)->>'road_code'),
            ''
        )
    );
    end_references = road_graph.get_reference_from_point(
        feature_end_point,
        Nullif(
            trim(to_jsonb(feature)->>'road_code'),
            ''
        )
    );
    closest_point_references = road_graph.get_reference_from_point(
        closest_point,
        Nullif(
            trim(to_jsonb(feature)->>'road_code'),
            ''
        )
    );

    -- Build the new geometries
    geom_1 = ((road_graph.get_road_substring_from_references(
        to_jsonb(feature)->>'road_code',
        (start_references->>'marker_code')::integer, (start_references->>'abscissa')::real,
        (closest_point_references->>'marker_code')::integer, (closest_point_references->>'abscissa')::real,
        (start_references->>'offset')::real, start_references->>'side'
    ))->>'geom')::geometry(MultiLinestring, 2154);
    geom_2 = ((road_graph.get_road_substring_from_references(
        to_jsonb(feature)->>'road_code',
        (closest_point_references->>'marker_code')::integer, (closest_point_references->>'abscissa')::real,
        (end_references->>'marker_code')::integer, (end_references->>'abscissa')::real,
        (start_references->>'offset')::real, start_references->>'side'
    ))->>'geom')::geometry(MultiLinestring, 2154);

    -- Transform geometries into multigeometries if needed
    IF geometry_column_data.geom_type ILIKE 'multi%' THEN
        geom_1 = ST_Multi(geom_1);
        geom_2 = ST_Multi(geom_2);
    END IF;

    -- Update the feature and insert the new one
    -- Use a try/catch to avoid doing half the job
    BEGIN
        -- Update the feature with the new geometry
        sql_text = format(
            $SQL$
                WITH up AS (
                    UPDATE %1$I.%2$I
                    SET %3$I = ST_GeomFromGeoJSON('%4$s')
                    WHERE True
                    AND %5$I = %6$s
                    RETURNING *
                )
                SELECT *
                FROM up
            $SQL$,
            _schema_name,
            _table_name,
            geometry_column_data.geom_name,
            ST_AsGeoJSON(geom_1),
            primary_key_field,
            (to_jsonb(feature))->>primary_key_field
        );
        EXECUTE sql_text
        INTO update_result
        ;

        -- Get the object fields minus the PK, geometry column, start_cumulative
        WITH fields AS (
            SELECT json_object_keys(to_json(feature.*)) AS field
        )
        SELECT concat ('"', string_agg(field, '", "'), '"')
        INTO source_fields
        FROM fields
        WHERE field NOT IN (
            primary_key_field,
            geometry_column_data.geom_name,
            'start_marker', 'start_abscissa', 'start_cumulative',
            'uid', 'longueur'
        )
        ;

        -- Create the new feature
        sql_text = format(
            $SQL$
                INSERT INTO %1$I.%2$I (
                        -- geometry
                        %3$I,
                        -- fields
                        %4$s
                )
                WITH source AS (
                    SELECT s.*
                    FROM %1$I.%2$I AS s
                    WHERE %6$I = %7$L
                    LIMIT 1
                )
                SELECT
                    -- geometry
                    ST_GeomFromGeoJSON('%5$s'),
                    -- fields
                    %4$s
                FROM source
                RETURNING *
            $SQL$,
            _schema_name,
            _table_name,
            geometry_column_data.geom_name,
            source_fields,
            ST_AsGeoJSON(geom_2),
            primary_key_field,
            (to_jsonb(feature))->>primary_key_field
        );
        EXECUTE sql_text
        INTO insert_result
        ;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_sqlstate = returned_sqlstate,
            v_message = message_text,
            v_context = pg_exception_context
        ;
        RETURN json_build_object(
            'status', 'error',
            'message', format('Error while splitting the feature: %s', v_sqlstate || ' - ' || v_message)
        );
    END;


    RETURN json_build_object(
        'status', 'success',
        'message', 'The given feature has been successfully splitted by the given point.'
    );

END;
$BODY$;

COMMENT ON FUNCTION road_graph.split_table_linestring_feature_by_point(text, text, integer, geometry)
IS 'Splits a table linestring feature by a given point.'
;


-- Fix search path
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

    RETURN True;
END;
$$;


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';


-- Trigger which update references of an inserted or updated managed object
CREATE OR REPLACE FUNCTION road_graph.calculate_managed_object_references() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    new_jsonb jsonb;
    sql_text text;
    managed_object record;
    point_references jsonb;
    start_references jsonb;
    end_references jsonb;
BEGIN
    -- Trigger disabled by session variable
    -- This variable can be set for example when merging the editing session schema
    -- The managed object would be updated and the trigger must not be run
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    new_jsonb = to_jsonb(NEW);

    -- Check road_code values
    IF NOT new_jsonb ? 'road_code' THEN
        RAISE NOTICE 'The given feature does not have the mandatory column road_code !';
        RETURN NEW;
    END IF;

    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = %1$L
            AND table_name = %2$L
        LIMIT 1;
        $SQL$,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NEW;
    END IF;

    IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring')
    THEN
        -- LINESTRING
        -- Get the references of the start & end points
        start_references = road_graph.get_reference_from_point(
            CASE
                WHEN lower(GeometryType(NEW.geom)) = 'linestring'
                    THEN ST_StartPoint(NEW.geom)
                -- hopefully the last part is really the end part of the multilinestring
                ELSE ST_StartPoint(ST_GeometryN(NEW.geom, 1))
            END,
            Nullif(
                trim(new_jsonb->>'road_code'),
                ''
            )
        );
        end_references = road_graph.get_reference_from_point(
            CASE
                WHEN lower(GeometryType(NEW.geom)) = 'linestring'
                    THEN ST_EndPoint(NEW.geom)
                -- hopefully the last part is really the end part of the multilinestring
                ELSE ST_EndPoint(ST_GeometryN(NEW.geom, ST_NumGeometries(NEW.geom)))
            END,
            Nullif(
                trim(new_jsonb->>'road_code'),
                ''
            )
        );
        -- Update the NEW object with the given references

        -- road_code - keep object road_code intact if it is not empty
        NEW.road_code =
        CASE
            WHEN Coalesce(NEW.road_code, '') != ''
                THEN NEW.road_code
            ELSE start_references->>'road_code'
        END;

        -- start_marker_code
        IF new_jsonb ? 'start_marker_code' THEN
            NEW.start_marker_code = (start_references->>'marker_code')::int;
        END IF;

        -- start_abscissa
        IF new_jsonb ? 'start_abscissa' THEN
            NEW.start_abscissa = (start_references->>'abscissa')::real;
        END IF;

        -- start_cumulative
        IF new_jsonb ? 'start_cumulative' THEN
            NEW.start_cumulative = (start_references->>'cumulative')::real;
        END IF;

        -- end_marker_code
        IF new_jsonb ? 'end_marker_code' THEN
            NEW.end_marker_code = (end_references->>'marker_code')::int;
        END IF;

        -- end_abscissa
        IF new_jsonb ? 'end_abscissa' THEN
            NEW.end_abscissa = (end_references->>'abscissa')::real;
        END IF;

        -- end_cumulative
        IF new_jsonb ? 'end_cumulative' THEN
            NEW.end_cumulative = (end_references->>'cumulative')::real;
        END IF;

        -- offset
        IF new_jsonb ? 'offset' THEN
            NEW.offset = Coalesce((start_references->>'offset')::real, 0.0::real);
        END IF;

        -- side
        IF new_jsonb ? 'side' THEN
            NEW.side = Coalesce(start_references->>'side', 'right');
        END IF;
    ELSE
        -- POINT
        point_references = road_graph.get_reference_from_point(
            NEW.geom,
            Nullif(
                trim(new_jsonb->>'road_code'),
                ''
            )
        );

        -- Update the NEW object with the given references
        -- road_code - keep object road_code intact if it is not empty
        NEW.road_code =
        CASE
            WHEN Coalesce(NEW.road_code, '') != ''
                THEN NEW.road_code
            ELSE point_references->>'road_code'
        END;

        -- marker_code
        IF new_jsonb ? 'marker_code' THEN
            NEW.marker_code = (point_references->>'marker_code')::int;
        END IF;

        -- abscissa
        IF new_jsonb ? 'abscissa' THEN
            NEW.abscissa = (point_references->>'abscissa')::real;
        END IF;

        -- cumulative
        IF new_jsonb ? 'cumulative' THEN
            NEW.cumulative = (point_references->>'cumulative')::real;
        END IF;

        -- offset
        IF new_jsonb ? 'offset' THEN
            NEW.offset = Coalesce((point_references->>'offset')::real, 0.0::real);
        END IF;

        -- side
        IF new_jsonb ? 'side' THEN
            NEW.side = Coalesce(point_references->>'side', 'right');
        END IF;

    END IF;

    RETURN NEW;
END;
$$;


-- FUNCTION aa_before_geometry_insert_or_update()
COMMENT ON FUNCTION road_graph.calculate_managed_object_references()
IS 'Trigger function allowing to update the references of an inserted or update managed object feature';
