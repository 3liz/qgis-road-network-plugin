-- Split of a managed object linestring by a point (Lizmap action split_line_by_point)
--
-- * split_table_linestring_feature_by_point: the references were not updated.
--   The feature kept the end references of the whole line, and the new feature
--   kept the start marker of the whole line, with an empty start abscissa.
--   The references of the cut point now become the end references of the feature
--   and the start references of the new feature,
--   if the table has the fields start_marker_code, start_abscissa, end_marker_code and end_abscissa.
-- * split_table_linestring_feature_by_point: a feature digitized in the opposite direction of the road
--   could not be split (error raised by get_road_substring_from_references).
--   The road substrings are calculated in the road direction, then reversed to keep the feature direction.
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION and COMMENT.


-- FUNCTION: road_graph.split_table_linestring_feature_by_point
CREATE OR REPLACE FUNCTION road_graph.split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry) RETURNS json
    LANGUAGE plpgsql
    AS $_$
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
    table_cols text[];
    has_references boolean;
    update_references_sql text;
    insert_references_fields text;
    insert_references_values text;
    is_reversed boolean;
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

    -- Check if the table has the references fields of the managed objects
    sql_text = format(
        $SQL$
        SELECT array_agg(column_name::text) AS cols
        FROM information_schema.columns
        WHERE table_schema = '%1$s' AND table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_cols
    ;
    has_references = Coalesce(
        table_cols @> ARRAY['start_marker_code', 'start_abscissa', 'end_marker_code', 'end_abscissa']::text[],
        False
    );

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
    -- geom_1: from the feature start point to the cut point
    -- geom_2: from the cut point to the feature end point
    -- The feature can be digitized in the opposite direction of the road:
    -- the road substrings must be calculated in the road direction,
    -- then reversed to keep the direction of the feature
    is_reversed = (start_references->>'cumulative')::real > (end_references->>'cumulative')::real;
    IF NOT is_reversed THEN
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
    ELSE
        geom_1 = ((road_graph.get_road_substring_from_references(
            to_jsonb(feature)->>'road_code',
            (closest_point_references->>'marker_code')::integer, (closest_point_references->>'abscissa')::real,
            (start_references->>'marker_code')::integer, (start_references->>'abscissa')::real,
            (start_references->>'offset')::real, start_references->>'side'
        ))->>'geom')::geometry(MultiLinestring, 2154);
        geom_2 = ((road_graph.get_road_substring_from_references(
            to_jsonb(feature)->>'road_code',
            (end_references->>'marker_code')::integer, (end_references->>'abscissa')::real,
            (closest_point_references->>'marker_code')::integer, (closest_point_references->>'abscissa')::real,
            (start_references->>'offset')::real, start_references->>'side'
        ))->>'geom')::geometry(MultiLinestring, 2154);
        -- Reverse the parts and their order
        geom_1 = (
            SELECT ST_Multi(ST_Collect(ST_Reverse(d.geom) ORDER BY d.path DESC))
            FROM ST_Dump(geom_1) AS d
        );
        geom_2 = (
            SELECT ST_Multi(ST_Collect(ST_Reverse(d.geom) ORDER BY d.path DESC))
            FROM ST_Dump(geom_2) AS d
        );
    END IF;

    -- Transform geometries into multigeometries if needed
    IF geometry_column_data.geom_type ILIKE 'multi%' THEN
        geom_1 = ST_Multi(geom_1);
        geom_2 = ST_Multi(geom_2);
    END IF;

    -- References of the cut point:
    -- they become the end references of the feature and the start references of the new feature.
    -- The start references of the feature are kept, and the new feature gets the original end references.
    -- The new feature is created from the updated feature: its end references are set explicitly
    update_references_sql = '';
    insert_references_fields = '';
    insert_references_values = '';
    IF has_references THEN
        update_references_sql = format(
            ', end_marker_code = %1$L::integer, end_abscissa = %2$L::real',
            closest_point_references->>'marker_code',
            closest_point_references->>'abscissa'
        );
        insert_references_fields = ', start_marker_code, start_abscissa, end_marker_code, end_abscissa';
        insert_references_values = format(
            ', %1$L::integer, %2$L::real, %3$L::integer, %4$L::real',
            closest_point_references->>'marker_code',
            closest_point_references->>'abscissa',
            to_jsonb(feature)->>'end_marker_code',
            to_jsonb(feature)->>'end_abscissa'
        );
    END IF;

    -- Update the feature and insert the new one
    -- Use a try/catch to avoid doing half the job
    BEGIN
        -- Update the feature with the new geometry
        sql_text = format(
            $SQL$
                WITH up AS (
                    UPDATE %1$I.%2$I
                    SET %3$I = ST_GeomFromGeoJSON('%4$s')%7$s
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
            (to_jsonb(feature))->>primary_key_field,
            update_references_sql
        );
        EXECUTE sql_text
        INTO update_result
        ;

        -- Get the object fields minus the PK, geometry column, start_cumulative
        -- and the references, set explicitly
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
        AND NOT (has_references AND field IN ('start_marker_code', 'end_marker_code', 'end_abscissa'))
        ;

        -- Create the new feature
        sql_text = format(
            $SQL$
                INSERT INTO %1$I.%2$I (
                        -- geometry
                        %3$I,
                        -- fields
                        %4$s
                        -- start references
                        %8$s
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
                    -- start references
                    %9$s
                FROM source
                RETURNING *
            $SQL$,
            _schema_name,
            _table_name,
            geometry_column_data.geom_name,
            source_fields,
            ST_AsGeoJSON(geom_2),
            primary_key_field,
            (to_jsonb(feature))->>primary_key_field,
            insert_references_fields,
            insert_references_values
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
$_$;

COMMENT ON FUNCTION road_graph.split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry) IS 'Splits a table linestring feature by a given point.
The feature keeps the first part, and a new feature is created with the second part.
If the table has the fields start_marker_code, start_abscissa, end_marker_code and end_abscissa,
the references of the cut point become the end references of the feature and the start references of the new feature.
The feature can be digitized in the direction of the road or in the opposite direction: the new geometries keep its direction.';
