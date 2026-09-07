

-- get_road_point_from_reference(text, integer, real, real, text)
CREATE OR REPLACE FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    get_downstream_multilinestring_from_reference record;
    _closest_marker_abscissa real;
    _downstream_road geometry(MULTILINESTRING, 2154);
    result_point geometry(POINT, 2154);
    raise_notice text;
BEGIN

    -- Notice
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Add default values for offset and side if they are NULL
    _offset = Coalesce(_offset, 0.0);
    _side = Coalesce(_side, 'right');

    -- Get downstream MULTILINESTRING
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code, _marker_code, _abscissa, _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO get_downstream_multilinestring_from_reference
    ;

    _closest_marker_abscissa = get_downstream_multilinestring_from_reference.closest_marker_abscissa;
    _downstream_road = get_downstream_multilinestring_from_reference.downstream_road;

    -- Return point
    SELECT INTO result_point
    ST_ReducePrecision(
        -- extract first point of resulting multipoint from ST_LocateAlong
        ST_GeometryN(
            -- Remove M measure from the result multipoint
            ST_Force2D(
                -- Locate the point on the measured downstream road part
                ST_LocateAlong(
                    -- Add measure to the downstream road part from 0 to length
                    ST_AddMeasure(_downstream_road, 0, ST_Length(_downstream_road)),
                    -- Abscissa cannot be above length
                    -- We must also add the marker abscissa
                    CASE
                        WHEN "_abscissa" - _closest_marker_abscissa >= ST_Length(_downstream_road)
                            THEN ST_Length(_downstream_road)
                        ELSE "_abscissa" - _closest_marker_abscissa
                    END,
                    -- JSON helper to choose side and offset
                    ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
                )
            )
            , 1
        ),
        0.01
    )::geometry(POINT, 2154)
    ;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'marker_code', _marker_code,
        'abscissa', _abscissa,
        'offset', _offset,
        'side', _side,
        'closest_marker_abscissa', _closest_marker_abscissa,
        'geom', result_point
    );

END;
$$;


-- FUNCTION get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the geometry of the corresponding point';



-- reorder_multilinestring_parts(geometry, text)
CREATE OR REPLACE FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text) RETURNS geometry
    LANGUAGE plpgsql
    AS $$
DECLARE
    _result_multilinestring geometry(MULTILINESTRING, 2154);
BEGIN

    -- Return untouched geometry if road code is not given
    IF _road_code IS NULL OR trim(_road_code) = '' THEN
        RETURN _multilinestring;
    END IF;

    -- Return untouched geometry if NULL or not a MultiLineString
    IF Coalesce(ST_GeometryType(_multilinestring), '') != 'ST_MultiLineString' THEN
        RETURN _multilinestring;
    END IF;

    -- Return untouched geometry if there is only one part
    IF ST_NumGeometries(_multilinestring) <= 1 THEN
        RETURN _multilinestring;
    END IF;

    BEGIN
        -- Reorder the geometry parts
        WITH
        -- Split the multilinestring into parts
        d AS (
            SELECT ST_Dump(_multilinestring) AS dump
        ),
        -- Get the part geometries
        parties AS (
            SELECT (dump).geom
            FROM d
        ),
        -- Calculate the references of the parts start & end points
        get_refs AS (
            SELECT
                geom,
                road_graph.get_reference_from_point(
                    ST_StartPoint(geom),
                    _road_code
                ) AS start_refs,
                road_graph.get_reference_from_point(
                    ST_EndPoint(geom),
                    _road_code
                ) AS end_refs
            FROM parties
        ),
        -- Extract the references and calculate a cost which will be used to order the parts
        extract_values AS (
            SELECT
                geom,
                (start_refs->>'marker_code')::int AS start_marker_code,
                (start_refs->>'abscissa')::real AS start_abscissa,
                (start_refs->>'marker_code')::int * 10000 + (start_refs->>'abscissa')::real AS start_part_order,
                (end_refs->>'marker_code')::int AS end_marker_code,
                (end_refs->>'abscissa')::real AS end_abscissa,
                (end_refs->>'marker_code')::int * 10000 + (end_refs->>'abscissa')::real AS end_part_order
            FROM get_refs
        )
        -- Reassemble the parts order by the calculated cost
        SELECT INTO _result_multilinestring
            ST_Multi(
                ST_Collect(
                    -- Revert the geometry if needed
                    CASE
                        WHEN start_part_order > end_part_order
                            THEN ST_Reverse(geom)
                        ELSE geom
                    END
                    -- Order the parts based on the parts order
                    ORDER BY end_part_order
                )
            )
        FROM extract_values
        ;
    EXCEPTION WHEN OTHERS THEN
        _result_multilinestring = _multilinestring;
    END;

    RETURN _result_multilinestring;

END;
$$;


-- FUNCTION reorder_multilinestring_parts(_multilinestring geometry, _road_code text)
COMMENT ON FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text)
IS 'Reorder the parts of the given road MULTILINESTRING based on the graph.
For each part, the references of the start & end points is calculated, which helps to reorder the parts.
We also reverse the geometry if needed (since the ST_OffsetCurve sometimes does not respect the node order)
';



-- update_table_geometries_from_references(text, text, text[])
CREATE OR REPLACE FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[]) RETURNS jsonb
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
                    road_graph.%8$s(
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
        CASE WHEN 'side' = ANY(table_cols) THEN 'Coalesce(mo.side::text, ''right''::text) ' ELSE 'right::text' END,
        -- used function
        -- 8
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'get_road_point_from_reference'
            ELSE 'get_road_substring_from_references'
        END,
        -- used fields: we need to cast the values taken from the managed table
        -- 9
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'mo.marker_code::integer, mo.abscissa::real'
            ELSE 'mo.start_marker_code::integer, mo.start_abscissa::real, mo.end_marker_code::integer, mo.end_abscissa::real'
        END
    );
    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$_$;



-- get_road_substring_from_references(text, integer, real, integer, real, real, text)
CREATE OR REPLACE FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    _start_multilinestring record;
    _end_multilinestring record;
    _start_closest_marker_abscissa real;
    _end_closest_marker_abscissa real;
    _start_downstream_road geometry(MULTILINESTRING, 2154);
    _start_downstream_road_m geometry(MULTILINESTRINGM, 2154);
    _end_downstream_road geometry(MULTILINESTRING, 2154);
    _end_downstream_road_m geometry(MULTILINESTRINGM, 2154);
    _start_substring geometry(MULTILINESTRING, 2154);
    _end_substring geometry(MULTILINESTRING, 2154);
    result_multilinestring_a geometry(MULTILINESTRING, 2154);
    result_multilinestring_b geometry(MULTILINESTRING, 2154);
    result_multilinestring geometry(MULTILINESTRING, 2154);
    raise_notice text;
BEGIN

    -- Notice
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    IF _road_code IS NULL OR trim(_road_code) = '' THEN
        RAISE EXCEPTION 'The road code must be given';
    END IF;

    -- Tests
    IF _start_marker_code > _end_marker_code THEN
        RAISE EXCEPTION 'The start marker code cannot be greater than the end marker code';
    END IF;
    IF _start_marker_code = _end_marker_code
        AND _start_marker_abscissa >= _end_marker_abscissa
    THEN
        RAISE EXCEPTION 'The start abscissa cannot be equal or greater than the end abscissa when the start and end marker have the same code';
    END IF;

    -- Add default values for offset and side if they are NULL
    _offset = Coalesce(_offset, 0.0);
    _side = Coalesce(_side, 'right');

    -- Get downstream start MULTILINESTRING from start marker to end of the road
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code,
                _start_marker_code, _start_marker_abscissa,
                _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO _start_multilinestring
    ;
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_start_multilinestring  closest_marker_abscissa %', _start_multilinestring.closest_marker_abscissa;
        RAISE NOTICE '_start_multilinestring  downstream_road %', ST_AsText(_start_multilinestring.downstream_road);
    END IF;

    _start_closest_marker_abscissa = _start_multilinestring.closest_marker_abscissa;
    _start_downstream_road = _start_multilinestring.downstream_road;
    -- Add measure to the downstream road part from 0 to length
    _start_downstream_road_m = ST_AddMeasure(_start_downstream_road, 0, ST_Length(_start_downstream_road));

    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_start_downstream_road_m %', ST_AsText(_start_downstream_road_m);
    END IF;
    -- Get downstream end MULTILINESTRING from end marker to end of the road
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code,
                _end_marker_code, _end_marker_abscissa,
                _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO _end_multilinestring
    ;
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_end_multilinestring closest_marker_abscissa %', _end_multilinestring.closest_marker_abscissa;
        RAISE NOTICE '_end_multilinestring downstream_road %', ST_AsText(_end_multilinestring.downstream_road);
    END IF;

    _end_closest_marker_abscissa = _end_multilinestring.closest_marker_abscissa;
    _end_downstream_road = _end_multilinestring.downstream_road;
    -- Add measure to the downstream road part from 0 to length
    _end_downstream_road_m = ST_AddMeasure(_end_downstream_road, 0, ST_Length(_end_downstream_road));

    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_end_downstream_road_m %', ST_AsText(_end_downstream_road_m);
    END IF;
    -- Create substring lines
    -- start
    _start_substring = ST_CollectionExtract(
        ST_Force2D(ST_LocateBetween(
            _start_downstream_road_m,
            CASE
                WHEN _start_marker_abscissa - _start_closest_marker_abscissa >= ST_Length(_start_downstream_road)
                    THEN ST_Length(_start_downstream_road)
                ELSE _start_marker_abscissa - _start_closest_marker_abscissa
            END,
            ST_Length(_start_downstream_road)
        ))
        , 2
    )
    ;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE '_start_substring  %', ST_AsText(_start_substring);
    END IF;

    -- end
    _end_substring = ST_CollectionExtract(
        ST_Force2D(ST_LocateBetween(
            _end_downstream_road_m,
            CASE
                WHEN _end_marker_abscissa - _end_closest_marker_abscissa >= ST_Length(_end_downstream_road)
                    THEN ST_Length(_end_downstream_road)
                ELSE _end_marker_abscissa - _end_closest_marker_abscissa
            END,
            ST_Length(_end_downstream_road)
        ))
        , 2
    )
    ;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE '_end_substring  %', ST_AsText(_end_substring);
    END IF;

    -- Return multilinestring between given references
    -- First we do the difference between start and end substrings to remove the common part between them
    result_multilinestring :=
        ST_Difference(
            -- start
            _start_substring,
            --end
            _end_substring,
            -- we must use a tolerance to be sure the 2 lines are considered as equal
            -- when they are very close but not exactly equal due to digitizing or calculation precision issues
            0.01
        )
    ;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'result_multilinestring ST_Difference  %', ST_AsText(result_multilinestring);
    END IF;

    -- Then we must merge the touching lines to avoid the offset curve function to produce gaps or crossing lines
    result_multilinestring_a = ST_LineMerge(
        result_multilinestring
        -- , True
    );
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'result_multilinestring ST_LineMerge  %', ST_AsText(result_multilinestring);
    END IF;

    -- Then we apply the offset curve on the result
    -- BEWARE: The ST_OffsetCurve does not respect the initial linestring node order !
    -- The function road_graph.reorder_multilinestring_parts will do the job
    result_multilinestring_b :=
    ST_Multi(
        ST_LineMerge(
            ST_OffsetCurve(
                ST_Multi(
                    result_multilinestring_a
                )::geometry(MULTILINESTRING, 2154),
                -- The offset value is multiplied by +1 or -1 depending on the side (right or left)
                ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
            )
            -- ,
            -- True
        )
    )
    ;

    -- Reorder the multilinestring parts if needed
    -- This method also ensure that the lines are orientations follows the references
    result_multilinestring_b := road_graph.reorder_multilinestring_parts(
        result_multilinestring_b,
        _road_code
    );

    -- Raise notice with the final result
    -- We reduce the precision, but not too much
    result_multilinestring = ST_ReducePrecision(
        result_multilinestring_b,
        0.01
    );
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE 'result_multilinestring  %', ST_AsText(result_multilinestring);
    END IF;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'start_marker_code', _start_marker_code,
        'start_marker_abscissa', _start_marker_abscissa,
        'end_marker_code', _end_marker_code,
        'end_marker_abscissa', _end_marker_abscissa,
        'offset', _offset,
        'side', _side,
        'start_geom', _start_substring,
        'end_geom', _end_substring,
        'geom', result_multilinestring
    );

END;
$$;


-- FUNCTION get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the geometry of the built linestring. The produced multilinestring geometry has been reordered based on the graph if it contains more than one part';



-- update_table_references_from_geometries(text, text, text[], boolean)
CREATE OR REPLACE FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean DEFAULT true) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
DECLARE
    road_code text;
    update_count integer;
    sql_text text;
    table_exists boolean;
    primary_key_field text;
    geometry_column text;
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

    -- Get geometry column name
    sql_text = format(
        $SQL$
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_schema = '%1$s' AND f_table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO geometry_column
    ;
    IF geometry_column IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a geometry column !', _schema_name, _table_name;
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
                        trim(mo.road_code)::text AS road_code,
                        mo.%9$I AS geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code::text = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            o.geom,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code::text,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.ref->>'road_code',
                        -- cumulative if present
                        %5$s
                        -- offset if present and if _update_offset_and_side is True
                        %6$s
                        -- side if present and if _update_offset_and_side is True
                        %7$s
                        -- marker code and abscissa put here to avoid errors with commas
                        marker_code = (r.ref->>'marker_code')::integer,
                        abscissa = (r.ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.%1$I = r.id
                    --AND r.ref->'road_code' != 'null'::jsonb
                    AND (%8$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I ORDER BY r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'cumulative' = ANY(table_cols) THEN $STR$cumulative = (r.ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE
                WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"offset" = Coalesce((r.ref->>'offset')::real, 0.0::real), $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$side = Coalesce((r.ref->>'side')::text, 'right'), $STR$
                ELSE ''
            END,
            -- 8 / Detect if we need to update or not
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.ref->>'road_code'), '') != '' )
                )
                OR (r.ref->>'marker_code')::integer != Coalesce(mo.marker_code, -1)::integer
                OR (r.ref->>'abscissa')::real != Coalesce(mo.abscissa, -1)::real
                $STR$,
                CASE WHEN 'cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.ref->>'cumulative')::real != Coalesce(mo.cumulative, -1)::real $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.ref->>'offset')::real, 0.0::real) != Coalesce(mo.offset, 0.0)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.ref->>'side')::text, 'right') != Coalesce(mo.side, 'right')::text $STR$
                    ELSE ''
                END
            ),
            -- 9 / geometry_column
            geometry_column
        );

    ELSIF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring') THEN
        sql_text = format(
            $SQL$
                WITH
                objects AS (
                    SELECT
                        mo.%1$I AS id,
                        trim(mo.road_code)::text AS road_code,
                        mo.%10$I AS geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code::text = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            -- ST_StartPoint could return NULL for a MULTILINESTRING
                            CASE
                                WHEN lower(GeometryType(o.geom)) = 'linestring'
                                    THEN ST_StartPoint(o.geom)
                                -- hopefully the last part is really the end part of the multilinestring
                                ELSE ST_StartPoint(ST_GeometryN(o.geom, 1))
                            END,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS start_ref,
                        road_graph.get_reference_from_point(
                            -- ST_EndPoint could return NULL for a MULTILINESTRING
                            CASE
                                WHEN lower(GeometryType(o.geom)) = 'linestring'
                                    THEN ST_EndPoint(o.geom)
                                -- hopefully the last part is really the end part of the multilinestring
                                ELSE ST_EndPoint(ST_GeometryN(geom, ST_NumGeometries(geom)))
                            END,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS end_ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.start_ref->>'road_code',
                        -- start_cumulative if present
                        %5$s
                        -- offset if present
                        %6$s
                        -- side if present
                        %7$s
                        -- end_cumulative if present
                        %8$s
                        -- marker code and abscissa put here to avoid errors with commas
                        start_marker_code = (r.start_ref->>'marker_code')::integer,
                        start_abscissa = (r.start_ref->>'abscissa')::real,
                        end_marker_code = (r.end_ref->>'marker_code')::integer,
                        end_abscissa = (r.end_ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.%1$I = r.id
                    -- Do not UPDATE if no changes must be made (values already are the same)
                    AND (%9$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I ORDER BY r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'start_cumulative' = ANY(table_cols) THEN $STR$"start_cumulative" = (r.start_ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE
                WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"offset" = Coalesce((r.start_ref->>'offset')::real, 0.0::real), $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"side" = Coalesce((r.start_ref->>'side')::text, 'right'::text), $STR$
                ELSE ''
            END,
            -- 8 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'end_cumulative' = ANY(table_cols) THEN $STR$"end_cumulative" = (r.end_ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 9 / Detect if we need to update or not
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.start_ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.start_ref->>'road_code'), '') != '' )
                )
                OR (r.start_ref->>'marker_code')::integer != Coalesce(mo.start_marker_code, -1)::integer
                OR (r.start_ref->>'abscissa')::real != Coalesce(mo.start_abscissa, -1)::real
                OR (r.end_ref->>'marker_code')::integer != Coalesce(mo.end_marker_code, -1)::integer
                OR (r.end_ref->>'abscissa')::real != Coalesce(mo.end_abscissa, -1)::real
                $STR$,
                CASE WHEN 'start_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.start_ref->>'cumulative')::real != Coalesce(mo.start_cumulative, -1)::real $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.start_ref->>'offset')::real, 0.0::real) != Coalesce(mo.offset, 0.0)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.start_ref->>'side')::text, 'right') != Coalesce(mo.side, 'right')::text $STR$
                    ELSE ''
                END,
                CASE WHEN 'end_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.end_ref->>'cumulative')::real != Coalesce(mo.end_cumulative, -1)::real $STR$ ELSE ''
                END
            ),
            -- 10 / geometry_column
            geometry_column
        );
    END IF;

    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$_$;


-- FUNCTION update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean)
COMMENT ON FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean) IS 'Update the given table references based on the geometries. This function needs the table to be listed in the table road_graph.managed_objects.
The given columns must exists:
* for points: road_code, marker_code, abscissa. Optional columns: offset & side,
* road_code, start_marker_code, start_abscissa, end_marker_code, end_abscissa. Optional columns: start_cumulative, end_cumulative, offset & side

The parameter _update_offset_and_side allows to not update the offset and side columns of the target table.
It is useful when used before updating the table geometries from the references
(to keep the object in the same start and end places but adapt the geometry)
';


-- update_managed_objects_on_graph_change(text, text, text[])
CREATE OR REPLACE FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[]) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    sql_text text;
    table_exists boolean;
    managed_object record;
    updated_stats_geometry jsonb;
    updated_stats_references jsonb;
    merged_last_updated_objects_ids integer[];
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

    -- CHOIX IMPORTANT
    -- Lorsqu'on demande la modification de la géométrie,
    -- il faut toujours au préalable recalculer d'abord les références
    -- de début et de fin à partir du nouveau graphe,
    -- pour ensuite adapter la géométrie (si jamais les edges support ont été modifiés)
    -- Cela assure que le début et la fin des lignes des données métiers restent
    -- les "mêmes" sur le terrain (ex: début à cette maison, et fin à ce carrefour)
    -- Par contre on ne calcule pas les offset et side (pour respecter cette donnée)

    -- Update objects references in both cases
    -- But when we do it before updating geometries, we should not modify the offset and side values
    SELECT road_graph.update_table_references_from_geometries(
        _schema_name,
        _table_name,
        _road_codes,
        CASE
            WHEN managed_object.update_policy_on_graph_change = 'geometry'
                THEN False
            ELSE True
        END
    )
    INTO updated_stats_references
    ;
    IF updated_stats_references IS NULL THEN
        updated_stats_references = jsonb_build_object(
            'nb', 0,
            'last_updated_objects_ids', ARRAY[]::integer[]
        );
    END IF;
    updated_stats = updated_stats_references;

    IF managed_object.update_policy_on_graph_change = 'geometry' THEN
        -- Update geometries based on references
        SELECT road_graph.update_table_geometries_from_references(
            _schema_name,
            _table_name,
            _road_codes
        )
        INTO updated_stats_geometry
        ;
        IF updated_stats_geometry IS NULL THEN
            updated_stats_geometry = jsonb_build_object(
                'nb', 0,
                'last_updated_objects_ids', ARRAY[]::integer[]
            );
        END IF;
        -- Get combined stats
        -- RAISE NOTICE '--------------';
        -- RAISE NOTICE 'refs, %', updated_stats_references::json;
        -- RAISE NOTICE 'geoms, %', updated_stats_geometry::json;
        merged_last_updated_objects_ids = (
            WITH a AS (
                SELECT t FROM jsonb_array_elements(
                        CASE
                        WHEN jsonb_typeof(updated_stats_references->'last_updated_objects_ids') = 'array'
                        THEN updated_stats_references->'last_updated_objects_ids'
                        ELSE '[]'
                        END
                ) AS t
                UNION
                SELECT t FROM jsonb_array_elements(
                        CASE
                        WHEN jsonb_typeof(updated_stats_geometry->'last_updated_objects_ids') = 'array'
                        THEN updated_stats_geometry->'last_updated_objects_ids'
                        ELSE '[]'
                        END
                ) AS t
            )
            SELECT array_agg(t ORDER BY t)
            FROM a
        );
        -- RAISE NOTICE 'merged_last_updated_objects_ids, %', merged_last_updated_objects_ids;
        -- RAISE NOTICE '--------------';


        updated_stats = jsonb_build_object(
            'nb',
            array_length(merged_last_updated_objects_ids, 1),
            'last_updated_objects_ids',
            merged_last_updated_objects_ids
        )
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
                ARRAY[%3$s]::integer[],
                now()::timestamp(0)
            )
            WHERE o.schema_name = '%1$I'
            AND o.table_name = '%2$I'
        $SQL$,
        _schema_name,
        _table_name,
        -- convert json array to string like 1,3,10
        (
            SELECT array_to_string(array_agg(j)::text[], ',')
            FROM jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(updated_stats->'last_updated_objects_ids') = 'array'
                        THEN updated_stats->'last_updated_objects_ids'
                    ELSE '[]'
                END
            ) AS j
        )
    );

    -- Return the number of updated features
    RETURN (updated_stats->>'nb')::integer;

END;
$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[])
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[]) IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table';
