
-- get_road_previous_marker_from_point(text, geometry, boolean)
CREATE OR REPLACE FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean DEFAULT false) RETURNS TABLE(id integer, road_code text, code integer, abscissa real, geom geometry, road_linestring_from_marker_to_point geometry, road_linestring_from_start_to_point geometry, closing_multilinestring geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    _road_info record;
    _run_build_cache boolean;
BEGIN
    -- If there is less than 2 edges, we do not use cache
    -- since it will not be useful and will raise an exception ( by function get_ordered_edges)
    SELECT INTO _use_cache
        CASE
            WHEN (SELECT COUNT(*) FROM road_graph.edges  AS e WHERE e.road_code = _road_code) < 2 THEN False
            ELSE _use_cache
        END
    ;

    -- Retrieve cached objects such as road simple linestring and closing multilinestring
    -- and also the road markers locations agains the simple road linestring
    -- from the temporary table generated beforehand (see update_edge_references)
    IF _use_cache THEN

        -- Get given _point location against the road simple linestring
        -- And retrieve data from the temporary table road_cache
        SELECT INTO _road_info
            r.simple_linestring,
            r.closing_multilinestring,
            r.marker_locations,
            ST_LineLocatePoint(r.simple_linestring, _point) AS point_location
        FROM road_cache AS r
        WHERE r.road_code = _road_code
        ;

        -- Get the previous marker
        -- which is the one with the location just before the _point location
        -- We also compute the linestring from the marker to the end of the road
        -- since we can use it to simplify the calcution of the reference from the point
        RETURN QUERY
        SELECT
            m.id, m.road_code, m.code, m.abscissa, m.geom,
            -- Linestring (with no gaps) between the marker and the point
            ST_LineSubstring(
                _road_info.simple_linestring,
                (_road_info.marker_locations->>(m.id::text))::float8,
                _road_info.point_location
            ) AS road_linestring_from_marker_to_point,
            -- Linestring (with no gaps) between the start of the road and the point
            ST_LineSubstring(
                _road_info.simple_linestring,
                0,
                _road_info.point_location
            ) AS road_linestring_from_start_to_point,
            -- MultiLinestring of the linestrings generated to close the gaps between edges
            -- used in other functions to remove them from the road linestring parts
            _road_info.closing_multilinestring
            --,
            -- road simple linestring
            --_road_info.simple_linestring AS road_simple_linestring
        FROM
            road_graph.markers AS m
        WHERE True
        AND (_road_info.marker_locations->>(m.id::text))::float8 <= _road_info.point_location
        ORDER BY (_road_info.marker_locations->>(m.id::text))::float8 DESC
        LIMIT 1
        ;
    ELSE
        -- Use plain query to avoid creating temporary tables
        RETURN QUERY
        WITH
        -- get road ordered edges (use previous and next edge ids)
        ordered_edges AS (
            SELECT DISTINCT o.id, o.road_code, o.edge_order, o.geom
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
        ),
        touching_edges AS (
            -- For each edge, add a line at the end of it
            -- from the previous edge (lag) end point
            -- to the edge start_point
            SELECT
                o.id, o.road_code, o.edge_order,
                -- closing lines between last end point and edge start point
                ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                ) AS closing_line,
                -- Merge closing lines and edge geometry
                ST_LineMerge(ST_Union(
                    ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                    ),
                    o.geom
                )) AS geom
            FROM ordered_edges AS o
            ORDER BY o.edge_order
        ),
        road_line AS (
            SELECT
                a.*,
                -- Calculate the location of the given point against this generated linestring
                ST_LineLocatePoint(a.geom, _point) AS point_location
            FROM (
                -- Create a single linestring by merging all edge augmented linestrings
                SELECT
                    max(t.road_code) AS road_code,
                    ST_MakeLine(t.geom ORDER BY t.edge_order) AS geom,
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Multi(ST_Collect(t.closing_line ORDER BY t.edge_order))
                        ),
                        2
                    ) AS closing_multilinestring
                FROM touching_edges AS t
            ) AS a
        ),
        marker_position AS (
            -- For each marker of the road, get its fractionnal location
            -- against the single merged linestring
            -- and all other needed columns values
            SELECT
                m.id, m.road_code, m.code, m.abscissa, m.geom,
                ST_LineLocatePoint(l.geom, m.geom) AS marker_location,
                l.geom AS road_simple_linestring
            FROM
                road_line as l,
                road_graph.markers AS m
            WHERE True
            AND m.road_code = _road_code
            -- No need to add filter 'AND m.road_code = l.road_code' since there is only one linestring
            -- No need to order data either
            -- ORDER BY m.code, m.abscissa
        )
        -- Get the previous marker
        -- which is the one with the location just before the _point location
        -- We also compute the linestring from the marker to the end of the road
        -- since we can use it to simplify the calcution of the reference from the point
        SELECT
            m.id, m.road_code, m.code, m.abscissa, m.geom,
            -- Linestring (with no gaps) between the marker and the point
            ST_LineSubstring(
                l.geom,
                m.marker_location,
                point_location
            ) AS road_linestring_from_marker_to_point,
            -- Linestring (with no gaps) between the start of the road and the point
            ST_LineSubstring(
                l.geom,
                0,
                point_location
            ) AS road_linestring_from_start_to_point,
            -- MultiLinestring of the linestrings generated to close the gaps between edges
            -- used in other functions to remove them from the road linestring parts
            l.closing_multilinestring
            --,
            -- road simple linestring
            --l.geom AS road_simple_linestring
        FROM
            marker_position AS m,
            road_line AS l
        WHERE True
        AND m.marker_location <= l.point_location
        ORDER BY m.marker_location DESC
        LIMIT 1
        ;

    END IF;
END;
$$;


-- FUNCTION get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean) IS 'Get the closest upstream marker for the given road from a given point.
This function can use roads cached objects generated beforehand via function build_road_cached_objects.
Or use a full SQL query with no use of temporary tables, depending of the paramter _use_cache

Illustration
|m0----m1----|  |-m2-m2b----m3----|   |--------m4---|
                 p0    p1               p2
p0 -> marker is m1
p1 -> marker is m2b (virtual marker with a non-null abscissa)
p2 -> marker is m3
The function also returns
* the simple linestring (no gaps) made by merging all edges linestrings from the marker to the point
* the simple linestring (no gaps) made by merging all edges linestrings from the start to the point
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
';



-- update_edge_references(text, integer[])
CREATE OR REPLACE FUNCTION road_graph.update_edge_references(_road_code text DEFAULT NULL::text, _edge_ids integer[] DEFAULT NULL::integer[]) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    _run_build_cache boolean;
    _set_config text;
    _use_cache boolean;
    raise_notice text;
BEGIN
    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Deactivate triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- If there is less than 2 edges, we do not use cache
    -- since it will not be useful and will raise an exception ( by function get_ordered_edges)
    SELECT INTO _use_cache
        CASE
            WHEN (SELECT COUNT(*) FROM road_graph.edges AS e WHERE e.road_code = _road_code) < 2 THEN False
            ELSE True
        END
    ;
    IF raise_notice IN ('info', 'debug') IS NOT NULL THEN
        RAISE NOTICE '% update_edge_references road n° % - Use cache %',
            repeat('    ', pg_trigger_depth()::integer), _road_code, _use_cache
        ;
    END IF;

    -- Build road & marker objects cache for speeding up linear referencing
    -- only if it exists at least one edge for the road
    IF _use_cache THEN
        SELECT road_graph.build_road_cached_objects(_road_code)
        INTO _run_build_cache;
    END IF;

    -- Get edges references
    WITH s AS (
        SELECT
            e.road_code, e.id,
            x.*
        FROM
            road_graph.edges AS e,
            json_to_record(
                -- 2nd parameter is TRUE so that we use precomputed object cache
                road_graph.get_edge_references(e.id, _use_cache)
            ) AS x (
                start_marker integer, start_abscissa real, start_cumulative real,
                end_marker integer, end_abscissa real, end_cumulative real
            )
        WHERE True
        AND (_road_code IS NULL OR e.road_code = _road_code)
        AND (_edge_ids IS NULL OR e.id = ANY (_edge_ids))
    )
    UPDATE road_graph.edges AS e
    SET (
        start_marker, start_abscissa, start_cumulative,
        end_marker, end_abscissa, end_cumulative
    ) = (
        s.start_marker, s.start_abscissa, s.start_cumulative,
        s.end_marker, s.end_abscissa, s.end_cumulative
    )
    FROM s
    WHERE s.id = e.id
    ;

    -- Go back to original setting
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;

    -- Return boolean
    RETURN True
    ;

END;
$$;


-- FUNCTION update_edge_references(_road_code text, _edge_ids integer[])
COMMENT ON FUNCTION road_graph.update_edge_references(_road_code text, _edge_ids integer[]) IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges
';

-- get_road_substring_from_references(text, integer, real, integer, real, real, text)
CREATE OR REPLACE FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) RETURNS jsonb
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


-- get_reference_from_point(geometry, text, boolean)
CREATE OR REPLACE FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text DEFAULT NULL::text, _use_cache boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    closest_edge record;
    closest_edge_marker record;
    previous_marker record;
    merged_upstream_edges record;
    upstream_road_from_marker record;
    upstream_road_from_start record;
    found_road_code text;
    found_marker_code integer;
    found_abscissa real;
    found_offset real;
    found_side text;
    found_cumulative real;
    raise_notice text;
BEGIN
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% get_reference_from_point - _point = % & _road_code = %',
            REPEAT('    ', pg_trigger_depth()::INTEGER),
            ST_AsText(_point),
            _road_code
        ;
    END IF;

    -- Get the splitted closest road depending on given road_code
    -- we keep only the edge part between start point and given _point
    IF _road_code IS NOT NULL THEN
        WITH ordered_ids AS (
            SELECT *
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream')
        )
        SELECT INTO closest_edge
            e.*,
            -- Calculate the distance between the edge and the point
            -- Do not use e.geom <-> _point AS distance
            -- since it can lead to some PostGIS errors if used in the ORDER BY
            -- such as "ERROR:  index returned tuples in wrong order"
            ST_Distance(e.geom, _point) AS distance,
            -- create the line portion from edge start point to the given point
            -- BEWARE: it can be a point e.g if the _point is edge start point
            -- it why we do not cast with ::geometry(LINESTRING, 2154)
            ST_LineSubstring(
                e.geom,
                0,
                ST_LineLocatePoint(e.geom, _point)
            ) AS sub_geom,
            -- calculate the measure for the point on this edge
            ST_LineLocatePoint(e.geom, _point) AS point_measure
        FROM
            road_graph.edges AS e
        INNER JOIN ordered_ids AS o ON e.id = o.id
        -- Limit search for the given road code
        WHERE e.road_code = _road_code
        -- Limit search to 50m
        AND ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance, o.edge_order, e.start_cumulative --, ST_X(ST_Centroid(e.geom)), ST_Y(ST_Centroid(e.geom))
        -- Get only the closest edge, with the least edge_order -- start cumulative
        LIMIT 1
        ;
    ELSE
        SELECT INTO closest_edge
            e.*,
            -- Calculate the distance between the edge and the point
            -- Do not use e.geom <-> _point AS distance
            -- since it can lead to some PostGIS errors if used in the ORDER BY
            -- such as "ERROR:  index returned tuples in wrong order"
            ST_Distance(e.geom, _point) AS distance,
            -- create the line portion from edge start point to the given point
            -- BEWARE: it can be a point e.g if the _point is edge start point
            -- it why we do not cast with ::geometry(LINESTRING, 2154)
            ST_LineSubstring(
                e.geom,
                0,
                ST_LineLocatePoint(e.geom, _point)
            ) AS sub_geom,
            -- calculate the measure for the point on this edge
            ST_LineLocatePoint(e.geom, _point) AS point_measure
        FROM
            road_graph.edges AS e
            -- LIMIT search without road_code TO 50m
        WHERE ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance --, ST_X(ST_Centroid(e.geom)), ST_Y(ST_Centroid(e.geom))
        -- Get only the closest edge, with the least edge_order, start cumulative
        LIMIT 1
        ;
    END IF;
    IF closest_edge IS NULL THEN
        IF raise_notice = 'debug' THEN
            RAISE NOTICE 'CLOSEST_EDGE is NULL';
        END IF;
        RETURN NULL;
    END IF;
    IF raise_notice = 'debug' THEN
        RAISE NOTICE 'CLOSEST_EDGE %', to_json(closest_edge);
        RAISE NOTICE 'CLOSEST_EDGE subgeom %', ST_AsText(closest_edge.sub_geom);
    END IF;

    -- Get road_code
    found_road_code = closest_edge.road_code;

    WITH
    get_previous_marker AS (
        SELECT *
        FROM road_graph.get_road_previous_marker_from_point(
            found_road_code,
            _point,
            _use_cache
        )
    ),
    marker_data AS (
        SELECT
            -- marker data
            m.*,
            -- Multilinestring from the marker to the point
            ST_Difference(
                -- linestring between the marker and _point
                m.road_linestring_from_marker_to_point,
                -- multilinestring made with connectors between edges end points and start points
                m.closing_multilinestring,
                -- grid size to avoid rounding issues
                0.01
            ) AS upstream_road_from_marker,
            -- Multilinestring from the road start to the point
            ST_Difference(
                -- linestring between the road start and the point
                m.road_linestring_from_start_to_point,
                -- multilinestring made with connectors between edges end points and start points
                m.closing_multilinestring,
                -- grid size to avoid rounding issues
                0.01
            ) AS upstream_road_from_start
        FROM
            get_previous_marker AS m
    )
    SELECT INTO closest_edge_marker
        m.*
    FROM marker_data AS m
    ;

    IF raise_notice = 'debug' THEN
        RAISE NOTICE 'CLOSEST_EDGE_MARKER %', to_json(closest_edge_marker);
        RAISE NOTICE '-';
    END IF;

    -- Calculate values to return based on the generated geometries
    found_marker_code = closest_edge_marker.code;
    found_abscissa = ST_Length(closest_edge_marker.upstream_road_from_marker) + closest_edge_marker.abscissa;
    found_cumulative = Coalesce(ST_Length(closest_edge_marker.upstream_road_from_start), 0);
    found_offset = closest_edge.distance;
    found_side = (
        CASE
            WHEN ST_Contains(
                ST_Buffer(
                    closest_edge.geom,
                    closest_edge.distance +1,
                    'side=left'
                ), _point
            ) THEN 'left' ELSE 'right'
        END
    );

    -- Build the JSON to return
    RETURN json_build_object(
        'road_code', found_road_code,
        'marker_code', found_marker_code,
        'abscissa', round(found_abscissa::numeric, 2),
        'cumulative', round(found_cumulative::numeric, 2),
        'offset', round(found_offset::numeric, 2),
        'side', found_side
    );

END;
$$;

COMMENT ON FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text, _use_cache boolean)
IS 'Calculate the references for the given point. The second parameter _road_code allows to narrow the search to the specified road.
Since this method is heavily used and the calculation is costly, we can pass a third parameter _use_cache which allows to use a pre-generated cache (to be build before using this function).'
;



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

    -- Get road information


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
                        -- Do no change the road code if no references have been found
                        -- for the given road (meaning the object is too far)
                        road_code =
                        CASE
                            -- keep object road_code intact if it is not empty
                            WHEN Coalesce(mo.road_code, '') != '' THEN mo.road_code
                            ELSE r.ref->>'road_code'
                        END,
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
                road_info AS (
                    SELECT r.road_code, r.road_type,
                        min(e.start_marker) AS min_code,
                        max(e.end_marker) AS max_code,
                        min(e.start_cumulative) AS min_cumulative,
                        max(e.end_cumulative) AS max_cumulative,
                        min(e.start_abscissa) AS min_abscissa,
                        max(e.end_abscissa) AS max_abscissa
                    FROM road_graph.edges AS e
                    JOIN road_graph.roads AS r
                        USING (road_code)
                    WHERE e.road_code IN (
                        SELECT DISTINCT o.road_code
                        FROM objects AS o
                    )
                    GROUP BY r.road_code, r.road_type
                ),
                refs AS (
                    SELECT
                        o.id, o.road_code,
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
                processed_refs AS (
                    SELECT
                        r.id,
                        r.road_code,
                        -- start_ref
                        CASE
                            -- For roundabout, if the start and end values are equal
                            -- use 0+0 for start & 0+max_cumulative for end
                            WHEN i.road_type = 'roundabout'
                            AND Coalesce((r.start_ref->>'abscissa')::real, 0) = Coalesce((r.end_ref->>'abscissa')::real, 0)
                                THEN jsonb_build_object(
                                    'road_code', r.start_ref->>'road_code',
                                    'marker_code', 0,
                                    'abscissa', 0.0,
                                    'cumulative', 0.0,
                                    'offset', r.start_ref->>'offset',
                                    'side', r.start_ref->'side'
                                )
                            -- for other roads, invert start and end if start < end
                            WHEN
                            Coalesce((r.start_ref->>'marker_code')::int * 10000 + (r.start_ref->>'abscissa')::real, 0)
                            >
                            Coalesce((r.end_ref->>'marker_code')::int * 10000 + (r.end_ref->>'abscissa')::real, 0)
                                THEN end_ref
                            ELSE r.start_ref
                        END AS start_ref,
                        -- end_ref
                        CASE
                            -- For roundabout, if the start and end values are equal
                            -- use 0+0 for start & 0+max_cumulative for end
                            WHEN i.road_type = 'roundabout'
                            AND Coalesce((r.start_ref->>'abscissa')::real, 0) = Coalesce((r.end_ref->>'abscissa')::real, 0)
                                THEN jsonb_build_object(
                                    'road_code', r.end_ref->>'road_code',
                                    'marker_code', 0,
                                    'abscissa', i.max_abscissa,
                                    'cumulative', i.max_cumulative,
                                    'offset', r.end_ref->>'offset',
                                    'side', r.end_ref->'side'
                                )
                            -- for other roads, invert start and end if start < end
                            WHEN
                            Coalesce((r.start_ref->>'marker_code')::int * 10000 + (r.start_ref->>'abscissa')::real, 0)
                            >
                            Coalesce((r.end_ref->>'marker_code')::int * 10000 + (r.end_ref->>'abscissa')::real, 0)
                                THEN start_ref
                            ELSE r.end_ref
                        END AS end_ref
                    FROM refs AS r
                    JOIN road_info AS i
                        USING (road_code)
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        -- Do no change the road code if no references have been found
                        -- for the given road (meaning the object is too far)
                        road_code =
                        CASE
                            -- keep object road_code intact if it is not empty
                            WHEN Coalesce(mo.road_code, '') != '' THEN mo.road_code
                            ELSE r.start_ref->>'road_code'
                        END,

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
                    FROM processed_refs AS r
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
