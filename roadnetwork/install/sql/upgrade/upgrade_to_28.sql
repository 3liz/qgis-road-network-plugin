

-- get_downstream_multilinestring_from_reference(text, integer, real, real, text)
DROP FUNCTION IF EXISTS road_graph.get_downstream_multilinestring_from_reference(text, integer, real, real, text);
CREATE OR REPLACE FUNCTION road_graph.get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    marker record;
    closest_edge record;
    merged_downstream_edges geometry(MULTILINESTRING, 2154);
    downstream_road geometry(MULTILINESTRING, 2154);
    raise_notice text;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Get marker
    -- We must control the marker abscissa
    -- and compare it with the desired abscissa
    -- to be able to manage the virtual markers
    SELECT INTO marker
        m.id, m.abscissa, m.geom
    FROM
        road_graph.markers AS m
    WHERE True
    -- same road
    AND m.road_code = _road_code
    -- marker code
    AND m.code = _marker_code
    -- marker abscissa must be below given abscissa
    -- so that it is before the given reference
    AND m.abscissa <= _abscissa
    -- get the marker with bigger abscissa
    -- to keep the closest marker before the given reference
    ORDER BY m.abscissa DESC
    LIMIT 1
    ;

    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Marker found with code % for road % = id %', _marker_code, _road_code, marker.id;
    END IF;

    IF marker IS NULL THEN
        IF raise_notice = 'yes' THEN
            RAISE NOTICE 'Marker cannot be found with code % for road %', _marker_code, _road_code;
        END IF;
        RETURN NULL;
    END IF;

    -- find closest edge from marker and keep only the downstream part
    -- from the projected point
    WITH get_closest_edge AS (
        SELECT
            e.*,
            e.geom <-> marker.geom AS distance,
            -- BEWARE: it can be a point e.g if the marker is at the end of the edge
            -- it why we do not cast with
            -- It could also be an empty geometry
            ST_LineSubstring(
                e.geom,
                ST_LineLocatePoint(e.geom, marker.geom),
                1
            )
            AS sub_geom
        FROM
            road_graph.edges AS e
        WHERE True
        -- same road
        AND e.road_code = _road_code
        ORDER BY distance, start_cumulative
        LIMIT 1
    )
    SELECT INTO closest_edge
        e.id, e.road_code, e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.geom, e.previous_edge_id, e.next_edge_id,
        distance,
        (CASE
            WHEN GeometryType(sub_geom) = 'POINT'
                THEN ST_MakeLine(sub_geom, sub_geom)::geometry(LINESTRING, 2154)
            ELSE
                CASE
                    -- If the geometry is empty, we take the closest edge end point
                    WHEN ST_IsEmpty(sub_geom)
                        THEN ST_MakeLine(ST_EndPoint(e.geom), ST_EndPoint(e.geom))::geometry(LINESTRING, 2154)
                    ELSE sub_geom
                END
        END)::geometry(LINESTRING, 2154) AS sub_geom
    FROM get_closest_edge AS e
    ;
    IF closest_edge IS NULL THEN
        IF raise_notice = 'yes' THEN
            RAISE NOTICE 'Closest edge from given marker % cannot be found for road %', _marker_code, _road_code;
        END IF;
        RETURN NULL;
    END IF;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Closest edge found: id = %', closest_edge.id;
        RAISE NOTICE 'Closest edge found: sub_geom = %', ST_AsText(closest_edge.sub_geom);
    END IF;

    -- Merge all linestrings for the given road
    -- after the closest edge
    -- ordered by edge order (we use the next_edge_id to calculate the edges order)
    WITH ordered_ids AS (
        SELECT *
        FROM road_graph.get_ordered_edges(_road_code, closest_edge.id, 'downstream')
    )
    SELECT INTO merged_downstream_edges
        -- ids
        -- array_agg(e.id ORDER BY o.edge_order) AS ids,
        -- geometries of downstream edges
        ST_Collect(e.geom ORDER BY o.edge_order) AS geom
    FROM
        road_graph.edges AS e
    INNER JOIN ordered_ids AS o ON e.id = o.id
    WHERE True
    -- same road code
    AND e.road_code = _road_code
    -- not the closest edge
    AND e.id != closest_edge.id
    -- downstream compared to closest edge
    AND e.start_marker >= closest_edge.end_marker
    AND e.start_cumulative >= closest_edge.end_cumulative
    GROUP BY e.road_code
    ;

    -- Build the linestring
    IF merged_downstream_edges IS NULL THEN
        -- There is no edges after the closest edge
        -- We just need to use the splitted closest edge geometry
        -- RAISE NOTICE 'Downstream edges cannot be found for given marker %', _marker_code;
        downstream_road = ST_Multi(closest_edge.sub_geom);
    ELSE
        -- There are some edges after the closest edge
        -- We must merge the splitted closest edge and the downstream edges
        SELECT INTO downstream_road
            --ST_LineMerge(
            -- ST_LineMerge cannot be used as it creates a valid multilinestring
            -- but will not preserve order of the parts.
            -- So the final geometry could be reversed when adding measure in the next step
            -- even with the new "directed" parameter which concerns segments and not parts
            ST_CollectionExtract(
                -- even if ST_Collect collects 2 multiilnestrings
                -- we must extract only 2 to be able to get a multilinestring as result
                ST_Collect(geom ORDER BY merge_order),
                --extract lines
                2
            )
            AS geom
        FROM
        (
            SELECT
                1::int AS merge_order,
                -- We need to transform sub geom into multilinestring
                -- to have only multilinestrings in the collected result
                ST_Multi(closest_edge.sub_geom) AS geom
            UNION ALL
            SELECT
                2::int AS merge_order,
                merged_downstream_edges AS geom
        ) AS foo
        ;
    END IF;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Merge downstream edges  %', ST_AsText(downstream_road);
        RAISE NOTICE 'downstream_road length = %, _abscissa = %', ST_Length(downstream_road), _abscissa;
    END IF;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'marker_code', _marker_code,
        'abscissa', _abscissa,
        'closest_marker_abscissa', marker.abscissa,
        'downstream_road', downstream_road
    );

END;
$$;

COMMENT ON FUNCTION road_graph.get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real)
IS 'Returns a JSON object with the given references and the MULTILINESTRING downstream road from given references to the road end.';


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
                _road_code, _marker_code, _abscissa
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
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



-- get_road_substring_from_references(text, integer, real, integer, real, real, text)
DROP FUNCTION IF EXISTS road_graph.get_road_substring_from_references(text, integer, real, integer, real, real, text);
CREATE OR REPLACE FUNCTION road_graph.get_road_substring_from_references(
    _road_code text,
    _start_marker_code integer, _start_marker_abscissa real,
    _end_marker_code integer, _end_marker_abscissa real,
    _offset real, _side text,
    _from_first_edge_start boolean DEFAULT False,
    _to_last_edge_end boolean DEFAULT False
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    _road_edges_min_max_values record;
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
        RAISE EXCEPTION 'The road code must be given. It is NULL or empty';
    END IF;

    -- Automatically change start marker code and end marker from the road
    -- depending on the given values
    -- Get min and max marker codes for the road
    SELECT INTO _road_edges_min_max_values
        min(start_marker) AS min_code, max(end_marker) AS max_code,
        min(start_cumulative) AS min_cumulative,
        max(end_cumulative) AS max_cumulative,
        min(start_abscissa) AS min_abscissa,
        max(end_abscissa) AS max_abscissa
    FROM road_graph.edges
    WHERE road_code = _road_code
    ;
    IF _start_marker_code < _road_edges_min_max_values.min_code THEN
        _start_marker_code = _road_edges_min_max_values.min_code;
        -- Use 0 to be at the beginning of the road
        _start_marker_abscissa = _road_edges_min_max_values.min_abscissa;
    END IF;
    IF _end_marker_code > _road_edges_min_max_values.max_code THEN
        _end_marker_code = _road_edges_min_max_values.max_code;
        -- Add 2000m to the end abscissa to go to the end of the line
        _end_marker_abscissa = _road_edges_min_max_values.max_abscissa;
    END IF;
    IF _end_marker_code = _road_edges_min_max_values.max_code
        AND _end_marker_abscissa > _road_edges_min_max_values.max_abscissa THEN
        _end_marker_abscissa = _road_edges_min_max_values.max_abscissa;
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
        -- If the start point has an abscissa of 0+1M or less than 1 meter, we set it to 0
        IF _start_marker_abscissa <= 1.0 THEN
            _start_marker_abscissa = 0.0;
        END IF;
        -- If the start point has an abscissa less than 1 meter from the max cumulative, we set it to 0
        IF abs(_road_edges_min_max_values.max_abscissa - _start_marker_abscissa) <= 1.0 THEN
            _start_marker_abscissa = 0.0;
        END IF;
        -- If the end point has an abscissa lower than 1 meter, we set it to the max cumulative
        IF _end_marker_abscissa <= 1.0 THEN
            _end_marker_abscissa = _road_edges_min_max_values.max_abscissa;
        END IF;
        -- If the end point has an abscissa close to the max cumulative, we set it to the max cumulative
        IF abs(_road_edges_min_max_values.max_abscissa - _end_marker_abscissa) <= 1 THEN
            _end_marker_abscissa = _road_edges_min_max_values.max_abscissa;
        END IF;
    END IF;

    -- Tests
    IF _start_marker_code > _end_marker_code THEN
        RAISE EXCEPTION 'The start marker code (%) cannot be greater than the end marker code (%)',
            _start_marker_code, _end_marker_code
        ;
    END IF;
    IF _start_marker_code = _end_marker_code
        AND _start_marker_abscissa >= _end_marker_abscissa
    THEN
        RAISE EXCEPTION 'The start abscissa (%) cannot be equal or greater than the end abscissa (%) when the start and end marker have the same code (%)',
            _start_marker_abscissa, _end_marker_abscissa, _start_marker_code
        ;
    END IF;

    -- Add default values for offset and side if they are NULL
    _offset = Coalesce(_offset, 0.0);
    _side = Coalesce(_side, 'right');


    -- Then we may add the first edge (touching the start point)
    IF _from_first_edge_start IS TRUE THEN
        -- We get the edges closest to the given start marker
        -- (its start point is before the marker)
        SELECT e.start_marker, e.start_abscissa
        INTO _start_marker_code, _start_marker_abscissa
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        AND e.start_marker::int * 10000 + e.start_abscissa <= _start_marker_code * 10000 + _start_marker_abscissa
        ORDER BY e.start_cumulative DESC
        LIMIT 1
        ;
    END IF;
    -- Then we may add the last edge (touching the end point)
    IF _to_last_edge_end IS TRUE THEN
        -- We get the edges closest to the given end marker
        -- (its end is after the marker)
        SELECT e.end_marker, e.end_abscissa
        INTO _end_marker_code, _end_marker_abscissa
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        AND e.end_marker * 10000 + e.end_abscissa >= _end_marker_code * 10000 + _end_marker_abscissa
        ORDER BY e.end_cumulative ASC
        LIMIT 1
        ;
    END IF;

    -- Get downstream start MULTILINESTRING from start marker to end of the road
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code, _start_marker_code, _start_marker_abscissa
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
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
                _road_code, _end_marker_code, _end_marker_abscissa
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
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
COMMENT ON FUNCTION road_graph.get_road_substring_from_references(
    _road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text,
    _from_first_edge_start boolean, _to_edge_end boolean
)
IS 'Returns a JSON object with the given references and the geometry of the built linestring. The produced multilinestring geometry has been reordered based on the graph if it contains more than one part.
The parameters _from_first_edge_start & _to_last_edge_end allows to respectively add the edge under the result linestring start point & the edge under the result linestring end point.';
