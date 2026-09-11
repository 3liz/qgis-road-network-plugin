
-- get_road_substring_from_references(text, integer, real, integer, real, real, text)
CREATE OR REPLACE FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    _road_marker_code_min_max record;
    _roundabout_data record;
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

    -- Automatically change start marker code and end marker from the road
    -- depending on the given values
    -- Get min and max marker codes for the road
    SELECT INTO _road_marker_code_min_max
        min(code) AS min_code, max(code) AS max_code
    FROM road_graph.markers
    WHERE road_code = _road_code
    ;
    IF _start_marker_code < _road_marker_code_min_max.min_code THEN
        _start_marker_code = _road_marker_code_min_max.min_code;
        -- Use 0 to be at the beginning of the road
        _start_marker_abscissa = 0;
    END IF;
    IF _end_marker_code > _road_marker_code_min_max.max_code THEN
        _end_marker_code = _road_marker_code_min_max.max_code;
        -- Add 2000m to the end abscissa to go to the end of the line
        _end_marker_abscissa = _end_marker_abscissa + 2000;
    END IF;

    -- For roundabouts, fix issues preventing from calculating the geometry
    -- Useful when processing external data before integrating them into the managed objects
    IF (
        SELECT (r.road_type = 'roundabout') AS test
        FROM road_graph.roads AS r
        WHERE r.road_code = _road_code
    ) IS TRUE THEN
        -- We set the start and end marker code to 0 (the only marker code for roundabouts)
        _start_marker_code = 0;
        _end_marker_code = 0;
        -- Get needed information
        SELECT INTO _roundabout_data
            max(end_cumulative) AS max_cumulative
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        GROUP BY e.road_code
        ;
        -- If the start point has an abscissa of 0+1M or less than 1 meter, we set it to 0
        IF _start_marker_abscissa <= 1.0 THEN
            _start_marker_abscissa = 0.0;
        END IF;
        -- If the start point has an abscissa less than 1 meter from the max cumulative, we set it to 0
        IF abs(_roundabout_data.max_cumulative - _start_marker_abscissa) <= 1.0 THEN
            _start_marker_abscissa = 0.0;
        END IF;
        -- If the end point has an abscissa lower than 1 meter, we set it to the max cumulative
        IF _end_marker_abscissa <= 1.0 THEN
            _end_marker_abscissa = _roundabout_data.max_cumulative;
        END IF;
        -- If the end point has an abscissa close to the max cumulative, we set it to the max cumulative
        IF abs(_roundabout_data.max_cumulative - _end_marker_abscissa) <= 1 THEN
            _end_marker_abscissa = _roundabout_data.max_cumulative;
        END IF;
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
