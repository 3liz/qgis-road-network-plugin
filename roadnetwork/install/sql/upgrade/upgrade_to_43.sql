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
--
-- update_managed_object_linked_geometries: new trigger function updating the lines of a managed table
-- which were connected to the start or the end of an updated line: their references and geometries
-- are calculated again to stay connected to the line (shortened or extended).
-- It is disabled by the session variable road.graph.disable.linked.geometries, set by the functions
-- split_table_linestring_feature_by_point, merge_table_linestring_features
-- and update_table_geometries_from_references.
--
-- add_managed_objects_triggers: also creates the trigger trg_update_managed_object_linked_geometries
-- on the line tables, if it does not already exist. It now returns one line per trigger, with the new column trigger_name.
--
-- search_path: the functions called from outside the plugin, which only read data, get the property
-- SET search_path TO 'public', so that PostGIS is found even if the search_path of the caller does not contain public.
-- The SECURITY DEFINER functions copy_data_to_editing_session, drop_editing_session and toggle_foreign_key_constraints
-- get SET search_path TO 'public', 'pg_temp' (security recommendation of PostgreSQL).
-- The schema road_graph is not added: the functions are also installed in the editing schema road_graph_editing.
-- merge_editing_session_data: the objects of the production schema are prefixed by "road_graph".

-- calculate_managed_object_references()
CREATE OR REPLACE FUNCTION road_graph.calculate_managed_object_references() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public'
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

    -- Disable the trigger updating the linked lines: the line following the feature
    -- must not be moved to the cut point, since the new feature is inserted between them
    PERFORM set_config('road.graph.disable.linked.geometries', '1', true);

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
        -- Re-enable the trigger updating the linked lines
        PERFORM set_config('road.graph.disable.linked.geometries', '0', true);
        RETURN json_build_object(
            'status', 'error',
            'message', format('Error while splitting the feature: %s', v_sqlstate || ' - ' || v_message)
        );
    END;

    -- Re-enable the trigger updating the linked lines
    PERFORM set_config('road.graph.disable.linked.geometries', '0', true);

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
The fields start_cumulative and end_cumulative are also set if they exist.

The lines connected to the feature are not modified by the trigger trg_update_managed_object_linked_geometries.';


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

    -- Disable the trigger updating the lines linked to the feature A:
    -- the merged line covers the feature B, and its other linked lines must not be modified
    PERFORM set_config('road.graph.disable.linked.geometries', '1', true);

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

    -- Re-enable the trigger updating the linked lines
    PERFORM set_config('road.graph.disable.linked.geometries', '0', true);

    RETURN json_build_object(
        'status', 'success',
        'message', 'The two features have been successfully merged.'
    );

END;
$_$;


-- FUNCTION merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer)
COMMENT ON FUNCTION road_graph.merge_table_linestring_features(_schema_name text, _table_name text, _feature_id_a integer, _feature_id_b integer) IS 'Merge two linestring features of the same table.

The feature A, chosen first by the user, is kept with its field values
and gets the merged geometry, in the road direction. The feature B is deleted.

If the table has the references fields, the references of the feature A
(and its cumulative distances if the fields exist) are calculated from the merged geometry, in the road direction.

The lines connected to the features are not modified by the trigger trg_update_managed_object_linked_geometries.

It returns a JSON object with the keys status (success or error) and message.';


-- update_table_geometries_from_references(text, text, text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb DEFAULT NULL::jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
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
                    road_graph.get_managed_object_geometry_from_references(
                        mo.road_code::text,
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
                AND (%8$s)
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
                    geom = ST_ReducePrecision(u.geom, 0.01)
                FROM updated_objects AS u
                WHERE mo.%1$I = u.id
                AND u.geom IS NOT NULL
                AND (
                    mo.geom IS NULL
                    OR NOT ST_Equals(
                        ST_ReducePrecision(mo.geom, 0.01),
                        ST_ReducePrecision(u.geom, 0.01)
                    )
                )
                RETURNING mo.*
            )
            SELECT
                count(r.*) AS nb,
                array_agg(r.%1$I) AS last_updated_objects_ids
            FROM run_update AS r
        $SQL$,
        -- 1
        primary_key_field,
        -- 2
        _schema_name,
        -- 3
        _table_name,
        -- 4 : list of road codes
        Coalesce(array_to_string(_road_codes::text[], ','), ''),
        -- 5 : geometry type
        managed_object.geometry_type,
        -- Use default values for side and offset if columns does not exists or value is NULL
        -- else we need to cast to the expected function parameter formats
        -- 6
        CASE WHEN 'offset' = ANY(table_cols) THEN 'Coalesce(mo."offset"::real, 0.0::real)' ELSE '0.0::real' END,
        -- 7
        CASE WHEN 'side' = ANY(table_cols) THEN 'Coalesce(mo.side::text, ''right''::text) ' ELSE '''right''::text' END,
        -- 8 : filter of the impacted objects
        CASE
            WHEN _impacts IS NULL THEN 'TRUE'
            ELSE road_graph.get_managed_objects_impacts_filter(managed_object.geometry_type, table_cols, 'geom')
        END,
        -- used fields: we need to cast the values taken from the managed table
        -- The end references are NULL for the points
        -- 9
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'mo.marker_code::integer, mo.abscissa::real, NULL::integer, NULL::real'
            ELSE 'mo.start_marker_code::integer, mo.start_abscissa::real, mo.end_marker_code::integer, mo.end_abscissa::real'
        END
    );
    RAISE NOTICE 'sql = %', sql_text;
    -- Disable the trigger updating the linked lines:
    -- all the objects are calculated from their own references
    PERFORM set_config('road.graph.disable.linked.geometries', '1', true);
    EXECUTE sql_text
    INTO updated_stats
    USING _impacts;
    -- Re-enable the trigger updating the linked lines
    PERFORM set_config('road.graph.disable.linked.geometries', '0', true);

    RETURN to_jsonb(updated_stats);

END;
$_$;


-- FUNCTION update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb) IS 'Update the given table geometries based on the references. This function needs the table to be listed in the table road_graph.managed_objects.
The objects whose geometry cannot be calculated are not updated, and a warning is raised.
The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.';


-- update_managed_object_linked_geometries()
CREATE OR REPLACE FUNCTION road_graph.update_managed_object_linked_geometries() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    new_jsonb jsonb;
    old_jsonb jsonb;
    managed_object record;
    primary_key_field text;
    old_end_points geometry;
    linked record;
    linked_jsonb jsonb;
    linked_start record;
    linked_end record;
    start_marker_code integer;
    start_abscissa real;
    end_marker_code integer;
    end_abscissa real;
    cumulative_sql text;
    substring_jsonb jsonb;
    linked_geom geometry;
    sql_text text;
    updated_number integer = 0;
    -- Maximum distance between the end points of two lines considered as connected
    tolerance constant real = 0.05;
BEGIN
    -- Trigger disabled by session variable, for example when merging the editing session,
    -- when the managed objects are updated from the road graph, when splitting or merging lines,
    -- or when this trigger updates the linked lines (to avoid a cascade of updates)
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
        OR road_graph.get_current_setting('road.graph.disable.linked.geometries', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Only for the updates of the geometry
    IF TG_OP != 'UPDATE' OR NEW.geom IS NULL OR OLD.geom IS NULL
        OR ST_Equals(NEW.geom, OLD.geom)
    THEN
        RETURN NEW;
    END IF;

    new_jsonb = to_jsonb(NEW);
    old_jsonb = to_jsonb(OLD);

    -- Check the table has the needed reference fields
    IF NOT new_jsonb ?& ARRAY['road_code', 'start_marker_code', 'start_abscissa', 'end_marker_code', 'end_abscissa'] THEN
        RAISE NOTICE 'The table "%"."%" does not have the reference fields needed to update the linked lines !',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NEW;
    END IF;

    -- Only for the lines registered as managed objects
    SELECT o.*
    INTO managed_object
    FROM road_graph.managed_objects AS o
    WHERE o.schema_name = TG_TABLE_SCHEMA
    AND o.table_name = TG_TABLE_NAME
    LIMIT 1
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !',
            TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NEW;
    END IF;
    IF lower(managed_object.geometry_type) NOT IN ('linestring', 'multilinestring') THEN
        RETURN NEW;
    END IF;

    -- The linked lines follow the edited line only if it stays on the same road
    IF Coalesce(trim(new_jsonb->>'road_code'), '') = ''
        OR new_jsonb->>'road_code' IS DISTINCT FROM old_jsonb->>'road_code'
    THEN
        RETURN NEW;
    END IF;

    -- Nothing to do if the references have not changed.
    -- The references of NEW must have been calculated before, for example by the trigger
    -- trg_calculate_managed_object_references (BEFORE trigger, run before this AFTER trigger)
    IF (new_jsonb->>'start_marker_code', new_jsonb->>'start_abscissa')
        IS NOT DISTINCT FROM (old_jsonb->>'start_marker_code', old_jsonb->>'start_abscissa')
        AND (new_jsonb->>'end_marker_code', new_jsonb->>'end_abscissa')
        IS NOT DISTINCT FROM (old_jsonb->>'end_marker_code', old_jsonb->>'end_abscissa')
    THEN
        RETURN NEW;
    END IF;

    -- Get the table primary key field
    SELECT a.attname
    INTO primary_key_field
    FROM pg_index AS i
    JOIN pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = TG_RELID AND i.indisprimary
    LIMIT 1
    ;
    IF primary_key_field IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a primary key !', TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN NEW;
    END IF;

    -- Start and end points of the line before its modification
    old_end_points = ST_Collect(
        ST_StartPoint(ST_GeometryN(ST_Multi(OLD.geom), 1)),
        ST_EndPoint(ST_GeometryN(ST_Multi(OLD.geom), ST_NumGeometries(ST_Multi(OLD.geom))))
    );

    -- Disable the triggers on the updated linked lines:
    -- their references are set here, and they must not update their own linked lines
    PERFORM set_config('road.graph.disable.trigger', '1', true);
    PERFORM set_config('road.graph.disable.linked.geometries', '1', true);

    -- Loop over the other lines of the same road
    -- whose start or end point touched the start or end point of the line before its modification
    sql_text = format(
        $SQL$
        SELECT to_jsonb(t) AS feature
        FROM %1$I.%2$I AS t
        WHERE t.%3$I::text != $1
        AND t.road_code = $2
        AND ST_DWithin(t.geom, $3, $4)
        AND ST_DWithin(
            ST_Collect(
                ST_StartPoint(ST_GeometryN(ST_Multi(t.geom), 1)),
                ST_EndPoint(ST_GeometryN(ST_Multi(t.geom), ST_NumGeometries(ST_Multi(t.geom))))
            ),
            $3, $4
        )
        $SQL$,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        primary_key_field
    );
    FOR linked IN EXECUTE sql_text
        USING old_jsonb->>primary_key_field, new_jsonb->>'road_code', old_end_points, tolerance
    LOOP
        linked_jsonb = linked.feature;

        -- Current references of the linked line, in the road direction
        SELECT
            (linked_jsonb->>'start_marker_code')::integer AS marker_code,
            (linked_jsonb->>'start_abscissa')::real AS abscissa
        INTO linked_start;
        SELECT
            (linked_jsonb->>'end_marker_code')::integer AS marker_code,
            (linked_jsonb->>'end_abscissa')::real AS abscissa
        INTO linked_end;
        IF linked_start.marker_code IS NULL OR linked_start.abscissa IS NULL
            OR linked_end.marker_code IS NULL OR linked_end.abscissa IS NULL
        THEN
            CONTINUE;
        END IF;

        start_marker_code = linked_start.marker_code;
        start_abscissa = linked_start.abscissa;
        end_marker_code = linked_end.marker_code;
        end_abscissa = linked_end.abscissa;
        cumulative_sql = '';

        IF (linked_start.marker_code, linked_start.abscissa) < (
            (old_jsonb->>'start_marker_code')::integer, (old_jsonb->>'start_abscissa')::real
        ) THEN
            -- The linked line is upstream: it ended at the start of the line.
            -- Its end must follow the new start of the line
            IF (new_jsonb->>'start_marker_code', new_jsonb->>'start_abscissa')
                IS NOT DISTINCT FROM (old_jsonb->>'start_marker_code', old_jsonb->>'start_abscissa')
            THEN
                CONTINUE;
            END IF;
            end_marker_code = (new_jsonb->>'start_marker_code')::integer;
            end_abscissa = (new_jsonb->>'start_abscissa')::real;
            IF linked_jsonb ? 'end_cumulative' AND new_jsonb ? 'start_cumulative' THEN
                cumulative_sql = format(', end_cumulative = %L::real', new_jsonb->>'start_cumulative');
            END IF;
        ELSIF (linked_end.marker_code, linked_end.abscissa) > (
            (old_jsonb->>'end_marker_code')::integer, (old_jsonb->>'end_abscissa')::real
        ) THEN
            -- The linked line is downstream: it started at the end of the line.
            -- Its start must follow the new end of the line
            IF (new_jsonb->>'end_marker_code', new_jsonb->>'end_abscissa')
                IS NOT DISTINCT FROM (old_jsonb->>'end_marker_code', old_jsonb->>'end_abscissa')
            THEN
                CONTINUE;
            END IF;
            start_marker_code = (new_jsonb->>'end_marker_code')::integer;
            start_abscissa = (new_jsonb->>'end_abscissa')::real;
            IF linked_jsonb ? 'start_cumulative' AND new_jsonb ? 'end_cumulative' THEN
                cumulative_sql = format(', start_cumulative = %L::real', new_jsonb->>'end_cumulative');
            END IF;
        ELSE
            -- The linked line overlaps the line: it is not modified
            CONTINUE;
        END IF;

        -- The line has been extended over the whole linked line: it cannot be calculated
        IF (start_marker_code, start_abscissa) >= (end_marker_code, end_abscissa) THEN
            RAISE NOTICE 'The linked line "%"."%" % = % is not modified: its new start references % + % are not before its new end references % + %',
                TG_TABLE_SCHEMA, TG_TABLE_NAME, primary_key_field, linked_jsonb->>primary_key_field,
                start_marker_code, start_abscissa, end_marker_code, end_abscissa
            ;
            CONTINUE;
        END IF;

        -- Build the new geometry of the linked line from its new references,
        -- keeping its offset and side
        BEGIN
            substring_jsonb = road_graph.get_road_substring_from_references(
                linked_jsonb->>'road_code',
                start_marker_code, start_abscissa,
                end_marker_code, end_abscissa,
                Coalesce((linked_jsonb->>'offset')::real, 0.0::real),
                Coalesce(linked_jsonb->>'side', 'right')
            );
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'The geometry of the linked line "%"."%" % = % cannot be calculated: %',
                TG_TABLE_SCHEMA, TG_TABLE_NAME, primary_key_field, linked_jsonb->>primary_key_field, SQLERRM
            ;
            CONTINUE;
        END;
        linked_geom = ST_GeomFromGeoJSON(substring_jsonb->>'geom');
        IF linked_geom IS NULL OR ST_IsEmpty(linked_geom) THEN
            CONTINUE;
        END IF;
        linked_geom = ST_SetSRID(linked_geom, 2154);

        -- Adapt the geometry to the type of the table
        IF lower(managed_object.geometry_type) = 'linestring' THEN
            linked_geom = ST_LineMerge(linked_geom);
            IF GeometryType(linked_geom) != 'LINESTRING' THEN
                RAISE NOTICE 'The geometry of the linked line "%"."%" % = % is not a single linestring',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, primary_key_field, linked_jsonb->>primary_key_field
                ;
                CONTINUE;
            END IF;
        ELSE
            linked_geom = ST_Multi(linked_geom);
        END IF;

        -- Update the geometry and the references of the linked line
        EXECUTE format(
            $SQL$
            UPDATE %1$I.%2$I
            SET
                geom = $1,
                start_marker_code = $2, start_abscissa = $3,
                end_marker_code = $4, end_abscissa = $5
                %4$s
            WHERE %3$I::text = $6
            $SQL$,
            TG_TABLE_SCHEMA,
            TG_TABLE_NAME,
            primary_key_field,
            cumulative_sql
        )
        USING
            linked_geom,
            start_marker_code, start_abscissa,
            end_marker_code, end_abscissa,
            linked_jsonb->>primary_key_field
        ;
        updated_number = updated_number + 1;
    END LOOP;

    -- Re-enable the triggers
    PERFORM set_config('road.graph.disable.linked.geometries', '0', true);
    PERFORM set_config('road.graph.disable.trigger', '0', true);

    IF updated_number > 0 THEN
        RAISE NOTICE 'Number of linked lines updated in "%"."%": %', TG_TABLE_SCHEMA, TG_TABLE_NAME, updated_number;
    END IF;

    RETURN NEW;
END;
$_$;


-- FUNCTION update_managed_object_linked_geometries()
COMMENT ON FUNCTION road_graph.update_managed_object_linked_geometries() IS 'Trigger function updating the lines of a managed object table
which were connected to the start or the end of an updated line.

The linked lines are the other lines of the same road whose start or end point touched (5 cm tolerance)
the start or end point of the line before its modification:

* an upstream linked line gets the new start references of the line as end references,
* a downstream linked line gets the new end references of the line as start references,
* the cumulative distances are also updated if the fields exist.

Their geometry is built again with the function get_road_substring_from_references,
keeping their offset and side: they are shortened or extended to stay connected.
A linked line is not modified if its new references are not ordered (the line has been extended over it).

It must be used in an AFTER UPDATE OF geom trigger, so that the references of the updated line
are already calculated, for example by the trigger trg_calculate_managed_object_references.
The function road_graph.add_managed_objects_triggers creates this trigger, named trg_update_managed_object_linked_geometries.

The table must be registered in road_graph.managed_objects with a line geometry type, and have the fields
road_code, start_marker_code, start_abscissa, end_marker_code and end_abscissa.

The trigger is disabled by the session variables road.graph.disable.trigger
or road.graph.disable.linked.geometries set to 1.';


-- add_managed_objects_triggers()
-- The returned columns have changed: the function must be dropped first
DROP FUNCTION IF EXISTS road_graph.add_managed_objects_triggers();
CREATE OR REPLACE FUNCTION road_graph.add_managed_objects_triggers() RETURNS TABLE(schema_name text, table_name text, trigger_name text, trigger_status text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    managed_object record;
    table_regclass regclass;
BEGIN
    FOR managed_object IN
        SELECT o.schema_name, o.table_name, o.geometry_type, o.add_trigger_on_geom_change
        FROM road_graph.managed_objects AS o
        ORDER BY o.id
    LOOP
        schema_name = managed_object.schema_name;
        table_name = managed_object.table_name;
        trigger_name = NULL;

        -- Only add the triggers if it has been asked for this table
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

        -- Check the table has a geom column, used by the triggers
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

        -- Trigger calculating the references of the inserted or updated features
        -- It is not created again if it already exists
        trigger_name = 'trg_calculate_managed_object_references';
        IF EXISTS (
            SELECT 1
            FROM pg_trigger AS t
            WHERE t.tgrelid = table_regclass
            AND t.tgname = trigger_name
            AND NOT t.tgisinternal
        ) THEN
            trigger_status = 'already exists';
        ELSE
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
        END IF;
        RETURN NEXT;

        -- Trigger updating the lines connected to the updated line, only for the lines
        -- It is not created again if it already exists
        IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring') THEN
            trigger_name = 'trg_update_managed_object_linked_geometries';
            IF EXISTS (
                SELECT 1
                FROM pg_trigger AS t
                WHERE t.tgrelid = table_regclass
                AND t.tgname = trigger_name
                AND NOT t.tgisinternal
            ) THEN
                trigger_status = 'already exists';
            ELSE
                EXECUTE format(
                    $SQL$
                    CREATE TRIGGER trg_update_managed_object_linked_geometries
                    AFTER UPDATE OF geom
                    ON %1$I.%2$I
                    FOR EACH ROW EXECUTE PROCEDURE road_graph.update_managed_object_linked_geometries();
                    $SQL$,
                    schema_name,
                    table_name
                );
                trigger_status = 'created';
            END IF;
            RETURN NEXT;
        END IF;
    END LOOP;

    RETURN;
END;
$_$;


-- FUNCTION add_managed_objects_triggers()
COMMENT ON FUNCTION road_graph.add_managed_objects_triggers() IS 'Create the managed objects triggers on each table listed in road_graph.managed_objects
with the column add_trigger_on_geom_change set to True.

Each trigger is only created if it does not already exist:

* trg_calculate_managed_object_references, for all the geometry types:
  it calculates the references of the inserted or updated features
  with the function road_graph.calculate_managed_object_references,
* trg_update_managed_object_linked_geometries, only for the linestring and multilinestring tables:
  it updates the lines connected to an updated line
  with the function road_graph.update_managed_object_linked_geometries.

It returns one line per trigger with the schema name, table name, trigger name and trigger status:
created or already exists.

If no trigger can be created for a managed object, it returns one line with an empty trigger name
and the status: not requested, table not found or geom column not found.';


-- managed_objects.add_trigger_on_geom_change
COMMENT ON COLUMN road_graph.managed_objects.add_trigger_on_geom_change IS 'If True, the function road_graph.add_managed_objects_triggers will create the trigger
trg_calculate_managed_object_references on this table, which calculates the references
of the features on insert or on geometry update, and for the line tables the trigger
trg_update_managed_object_linked_geometries, which updates the lines connected to an updated line.';


-- search_path of the functions called from outside the plugin, which only read data
-- calculate_managed_object_references gets it with its CREATE OR REPLACE above
ALTER FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text, _use_cache boolean) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text, _from_first_edge_start boolean, _to_last_edge_end boolean) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_snapped_line_from_track(_track geometry, _densify_distance real, _max_distance real, _road_change_penalty real) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_road_sections_from_snapped_line(_line geometry, _tolerance real) SET search_path TO 'public';
ALTER FUNCTION road_graph.get_spatial_road(_road_code text) SET search_path TO 'public';

-- search_path of the SECURITY DEFINER functions
ALTER FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) SET search_path TO 'public', 'pg_temp';
ALTER FUNCTION road_graph.drop_editing_session(_editing_session_id integer) SET search_path TO 'public', 'pg_temp';
ALTER FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) SET search_path TO 'public', 'pg_temp';


-- merge_editing_session_data(integer)
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'road_graph', 'public'
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    road_codes text[];
    impacts jsonb;
    managed_object record;
    updated_objects_number integer;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

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
    SELECT INTO road_codes
        "road_graph".get_updated_roads_from_editing_session(_editing_session_id)
    ;

    -- Get the impacts of the editing session on the roads, to update only the managed objects
    -- which may be impacted by the modifications.
    -- It must be calculated before the merge, since it compares the production and editing data.
    -- Only the roads whose edges geometries or markers have changed are impacted:
    -- the other edges have only been updated by the calculation of their references
    SELECT INTO impacts
        "road_graph".get_editing_session_impacts(_editing_session_id)
    ;
    SELECT INTO road_codes
        array_agg(i->>'road_code')
    FROM jsonb_array_elements(Coalesce(impacts, '[]'::jsonb)) AS i
    ;

    -- Create SQL to run for each edited data
    FOR sql_record IN
        SELECT
            sql_text
        FROM "road_graph".create_queries_from_editing_session(_editing_session_id)
    LOOP
       -- RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;

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
                    road_codes,
                    impacts
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

    ELSE
        -- No object has been updated: empty the list of the objects updated by the previous merge
        UPDATE "road_graph".managed_objects
        SET (
            last_updated_objects_ids,
            last_update
        ) = (
            ARRAY[]::integer[],
            now()::timestamp(0)
        );

    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;

    -- Delete merged editing session
    DELETE FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Refresh the materialized view of the last updated managed objects
    REFRESH MATERIALIZED VIEW CONCURRENTLY road_graph.vm_managed_objects;

    RETURN True;
END;
$$;


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';
