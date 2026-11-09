-- Managed objects: the references of the lines follow the road direction (start before end)
--
-- split_table_linestring_feature_by_point (Lizmap action split_line_by_point):
-- for a feature digitized in the opposite direction of the road, the references of the two parts
-- were written in the direction of the digitization, and the start references of the feature were kept
-- from the stored values: each part could get the references of the other part, or start after its end.
-- The geometries calculated from the references (update policy "geometry") then swapped the two parts,
-- or could not be calculated. Now:
-- * the two new geometries follow the road direction, like the geometries calculated from the references,
-- * the four references of the two features are calculated from the geometries, in the road direction,
-- * the start and end cumulative distances are also set if the table has these fields.
--
-- calculate_managed_object_references (trigger trg_calculate_managed_object_references):
-- the start and end references of a line digitized in the opposite direction of the road are inverted,
-- like in the function update_table_references_from_geometries.
--
-- merge_table_linestring_features (Lizmap action merge_selected_lines):
-- the kept feature is now the feature A, chosen first by the user, instead of the feature
-- with the lowest start references. Its references, and cumulative distances, are calculated
-- from the merged geometry, in the road direction: before, it kept its own references
-- if the table had no trigger calculating them.

-- calculate_managed_object_references()
CREATE OR REPLACE FUNCTION road_graph.calculate_managed_object_references() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
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

        -- The references of the managed lines follow the road direction (start before end),
        -- like in the function update_table_references_from_geometries:
        -- invert start and end if the line is digitized in the opposite direction of the road
        IF (
            (start_references->>'marker_code')::integer,
            (start_references->>'abscissa')::real
        ) > (
            (end_references->>'marker_code')::integer,
            (end_references->>'abscissa')::real
        ) THEN
            SELECT end_references, start_references
            INTO start_references, end_references;
        END IF;

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
$_$;


-- FUNCTION calculate_managed_object_references()
COMMENT ON FUNCTION road_graph.calculate_managed_object_references() IS 'Trigger function allowing to update the references of an inserted or update managed object feature.
For a line, the start and end references follow the road direction (start before end),
even if the line is digitized in the opposite direction of the road.';



-- split_table_linestring_feature_by_point(text, text, integer, geometry)
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

    -- The references of the managed lines follow the road direction (start before end),
    -- like in the functions update_table_references_from_geometries and get_road_substring_from_references.
    -- If the feature is digitized in the opposite direction of the road,
    -- its start and end references are swapped: the two new geometries follow the road direction
    is_reversed = (
        (start_references->>'marker_code')::integer,
        (start_references->>'abscissa')::real
    ) > (
        (end_references->>'marker_code')::integer,
        (end_references->>'abscissa')::real
    );
    IF is_reversed THEN
        SELECT end_references, start_references
        INTO start_references, end_references;
    END IF;

    -- Build the new geometries, in the road direction
    -- geom_1: from the start references to the cut point
    -- geom_2: from the cut point to the end references
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

    -- References of the two features, set explicitly instead of keeping the stored values,
    -- which could be in the opposite order or not correspond to the geometry:
    -- the feature goes from the start references to the cut point,
    -- and the new feature goes from the cut point to the end references.
    -- The cumulative distances are also set if the table has these fields
    update_references_sql = '';
    insert_references_fields = '';
    insert_references_values = '';
    IF has_references THEN
        update_references_sql = format(
            ', start_marker_code = %1$L::integer, start_abscissa = %2$L::real'
            ', end_marker_code = %3$L::integer, end_abscissa = %4$L::real',
            start_references->>'marker_code',
            start_references->>'abscissa',
            closest_point_references->>'marker_code',
            closest_point_references->>'abscissa'
        );
        insert_references_fields = ', start_marker_code, start_abscissa, end_marker_code, end_abscissa';
        insert_references_values = format(
            ', %1$L::integer, %2$L::real, %3$L::integer, %4$L::real',
            closest_point_references->>'marker_code',
            closest_point_references->>'abscissa',
            end_references->>'marker_code',
            end_references->>'abscissa'
        );
        IF 'start_cumulative' = ANY(table_cols) THEN
            update_references_sql = concat(
                update_references_sql,
                format(', start_cumulative = %1$L::real', start_references->>'cumulative')
            );
            insert_references_fields = concat(insert_references_fields, ', start_cumulative');
            insert_references_values = concat(
                insert_references_values,
                format(', %1$L::real', closest_point_references->>'cumulative')
            );
        END IF;
        IF 'end_cumulative' = ANY(table_cols) THEN
            update_references_sql = concat(
                update_references_sql,
                format(', end_cumulative = %1$L::real', closest_point_references->>'cumulative')
            );
            insert_references_fields = concat(insert_references_fields, ', end_cumulative');
            insert_references_values = concat(
                insert_references_values,
                format(', %1$L::real', end_references->>'cumulative')
            );
        END IF;
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
        AND NOT (has_references AND field IN ('start_marker_code', 'end_marker_code', 'end_abscissa', 'end_cumulative'))
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


-- FUNCTION split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry)
COMMENT ON FUNCTION road_graph.split_table_linestring_feature_by_point(_schema_name text, _table_name text, _feature_id integer, _point geometry) IS 'Splits a table linestring feature by a given point.
The two new geometries follow the road direction, even if the feature is digitized in the opposite direction,
like the geometries calculated from the references.
The feature keeps the first part in the road direction, and a new feature is created with the second part.
If the table has the fields start_marker_code, start_abscissa, end_marker_code and end_abscissa,
the references of the two features are calculated in the road direction (start before end):
the references of the cut point become the end references of the feature and the start references of the new feature.
The fields start_cumulative and end_cumulative are also set if they exist.';


-- merge_table_linestring_features(text, text, integer, integer)
CREATE OR REPLACE FUNCTION road_graph.merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer) RETURNS json
    LANGUAGE plpgsql
    AS $_$
DECLARE
    primary_key_field text;
    geometry_column_data record;
    feature_a record;
    feature_b record;
    sql_text text;
    target_geometry geometry(MULTILINESTRING, 2154);
    table_cols text[];
    start_references jsonb;
    end_references jsonb;
    update_references_sql text;
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

    -- Merge features: keep the feature A, chosen first by the user, with its field values,
    -- and add the geometry of the feature B, which is deleted

    -- Compute merged geometry
    -- The parts are ordered and oriented in the road direction
    target_geometry = road_graph.reorder_multilinestring_parts(
        ST_Multi(
            ST_Union(
                ST_GeomFromGeoJSON((to_json(feature_a))->>geometry_column_data.geom_name),
                ST_GeomFromGeoJSON((to_json(feature_b))->>geometry_column_data.geom_name)
            )
        ),
        feature_a.road_code
    );

    -- Get the table columns, to update the references if the table has the references fields
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

    -- References of the merged geometry, in the road direction (start before end),
    -- set explicitly since the feature A only kept its own references,
    -- and the table may have no trigger calculating them
    update_references_sql = '';
    IF table_cols @> ARRAY['start_marker_code', 'start_abscissa', 'end_marker_code', 'end_abscissa']::text[] THEN
        start_references = road_graph.get_reference_from_point(
            ST_StartPoint(ST_GeometryN(target_geometry, 1)),
            Nullif(trim(feature_a.road_code), '')
        );
        end_references = road_graph.get_reference_from_point(
            ST_EndPoint(ST_GeometryN(target_geometry, ST_NumGeometries(target_geometry))),
            Nullif(trim(feature_a.road_code), '')
        );
        -- Invert start and end if needed
        IF (
            (start_references->>'marker_code')::integer,
            (start_references->>'abscissa')::real
        ) > (
            (end_references->>'marker_code')::integer,
            (end_references->>'abscissa')::real
        ) THEN
            SELECT end_references, start_references
            INTO start_references, end_references;
        END IF;
        update_references_sql = format(
            ', start_marker_code = %1$L::integer, start_abscissa = %2$L::real'
            ', end_marker_code = %3$L::integer, end_abscissa = %4$L::real',
            start_references->>'marker_code',
            start_references->>'abscissa',
            end_references->>'marker_code',
            end_references->>'abscissa'
        );
        -- Cumulative distances if the table has these fields
        IF 'start_cumulative' = ANY(table_cols) THEN
            update_references_sql = concat(
                update_references_sql,
                format(', start_cumulative = %1$L::real', start_references->>'cumulative')
            );
        END IF;
        IF 'end_cumulative' = ANY(table_cols) THEN
            update_references_sql = concat(
                update_references_sql,
                format(', end_cumulative = %1$L::real', end_references->>'cumulative')
            );
        END IF;
    END IF;

    -- Delete the feature B and update the feature A in the same request
    sql_text = format(
        $SQL$
            WITH del AS (
                DELETE FROM %1$I.%2$I
                WHERE %3$I = %4$s
                RETURNING *
            ),
            up AS (
                UPDATE %1$I.%2$I
                SET %5$I = ST_GeomFromGeoJSON('%6$s')%8$s
                WHERE %3$I = %7$s
                RETURNING *
            )
            SELECT *
            FROM up
        $SQL$,
        _schema_name,
        _table_name,
        primary_key_field,
        _feature_id_b,
        geometry_column_data.geom_name,
        ST_AsGeoJSON(target_geometry),
        _feature_id_a,
        update_references_sql
    );
    EXECUTE sql_text
    INTO update_result
    ;

    RETURN json_build_object(
        'status', 'success',
        'message', 'The two features have been successfully merged.'
    );

END;
$_$;


-- FUNCTION merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer)
COMMENT ON FUNCTION road_graph.merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer) IS 'Merge two linestring features of the same table.
The feature A, chosen first by the user, is kept with its field values and gets the merged geometry, in the road direction.
The feature B is deleted. If the table has the references fields, the references of the feature A
(and its cumulative distances if the fields exist) are calculated from the merged geometry, in the road direction.
@param _schema_name Schema of the table
@param _table_name Name of the table
@param _feature_id_a Primary key value of the feature to keep
@param _feature_id_b Primary key value of the feature to merge into the feature A, then deleted
@return JSON object with the keys status (success or error) and message';

