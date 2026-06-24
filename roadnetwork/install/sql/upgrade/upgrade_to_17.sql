CREATE OR REPLACE FUNCTION road_graph.reorder_multilinestring_parts(
    _multilinestring geometry(MULTILINESTRING, 2154),
    _road_code text
)
RETURNS geometry(MULTILINESTRING, 2154)
LANGUAGE plpgsql
AS $_$
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
        -- Calculate the references of the parts end points
        get_refs AS (
            SELECT
                geom, ST_EndPoint(geom) AS end_point,
                road_graph.get_reference_from_point(
                    ST_EndPoint(geom),
                    _road_code
                ) AS refs
            FROM parties
        ),
        -- Extract the references and calculate a cost which will be used to order the parts
        extract_values AS (
            SELECT
                geom,
                (refs->>'marker_code')::int AS marker_code,
                (refs->>'abscissa')::real AS abscissa,
                (refs->>'marker_code')::int * 10000 + (refs->>'abscissa')::real AS part_order
            FROM get_refs
        )
        -- Reassemble the parts order by the calculated cost
        SELECT INTO _result_multilinestring
            ST_Multi(
                ST_Collect(geom ORDER BY part_order)
            )
        FROM extract_values
        ;
    EXCEPTION WHEN OTHERS THEN
        _result_multilinestring = _multilinestring;
    END;

    RETURN _result_multilinestring;

END;
$_$;


COMMENT ON FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text)
IS 'Reorder the parts of the given road MULTILINESTRING based on the graph.
For each part, the references of the end point is calculated, which helps to reorder the parts
';



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
    _start_substring =
        ST_Force2D(ST_LocateBetween(
            _start_downstream_road_m,
            CASE
                WHEN _start_marker_abscissa - _start_closest_marker_abscissa >= ST_Length(_start_downstream_road)
                    THEN ST_Length(_start_downstream_road)
                ELSE _start_marker_abscissa - _start_closest_marker_abscissa
            END,
            ST_Length(_start_downstream_road)
        ))
    ;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE '_start_substring  %', ST_AsText(_start_substring);
    END IF;

    -- end
    _end_substring =
        ST_Force2D(ST_LocateBetween(
            _end_downstream_road_m,
            CASE
                WHEN _end_marker_abscissa - _end_closest_marker_abscissa >= ST_Length(_end_downstream_road)
                    THEN ST_Length(_end_downstream_road)
                ELSE _end_marker_abscissa - _end_closest_marker_abscissa
            END,
            ST_Length(_end_downstream_road)
        ))
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
    result_multilinestring = ST_LineMerge(result_multilinestring);
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'result_multilinestring ST_LineMerge  %', ST_AsText(result_multilinestring);
    END IF;

    -- Then we apply the offset curve on the result
    result_multilinestring :=
    ST_Multi(
        ST_OffsetCurve(
            ST_Multi(
                result_multilinestring
            )::geometry(MULTILINESTRING, 2154),
            -- The offset value is multiplied by +1 or -1 depending on the side (right or left)
            ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
        )
    )
    ;

    -- Reorder the multilinestring parts if needed
    result_multilinestring = road_graph.reorder_multilinestring_parts(
        result_multilinestring,
        _road_code
    );

    -- Raise notice with the final result
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
