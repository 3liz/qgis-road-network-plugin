

-- build_road_cached_objects(text)
CREATE OR REPLACE FUNCTION road_graph.build_road_cached_objects(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN

    DROP TABLE IF EXISTS road_cache;
    CREATE TEMPORARY TABLE IF NOT EXISTS road_cache
    ON COMMIT DROP AS (
        WITH
        ordered_edges AS (
            -- get road ordered edges (use previous and next edge ids)
            SELECT o.id, o.road_code, o.edge_order, o.geom
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
                )) AS geom,
                o.geom AS edge_geom
            FROM ordered_edges AS o
            ORDER BY o.edge_order
        ),
        road_line AS (
            -- Create a single linestring by merging all edge augmented linestrings
            SELECT
                max(t.road_code) AS road_code,
                ST_MakeLine(t.geom ORDER BY t.edge_order) AS geom,
                ST_CollectionExtract(
                    ST_MakeValid(
                        ST_Multi(ST_Collect(t.closing_line ORDER BY t.edge_order))
                    ),
                    2
                ) AS closing_multilinestring,
                ST_CollectionExtract(
                    ST_MakeValid(
                        ST_Multi(ST_Collect(t.edge_geom ORDER BY t.edge_order))
                    ),
                    2
                )  AS edges_multilinestring
            FROM touching_edges AS t
        )
        SELECT
            _road_code AS road_code,
            l.geom AS simple_linestring,
            l.closing_multilinestring,
            l.edges_multilinestring,
            jsonb_object_agg(
                m.id,
                ST_LineLocatePoint(l.geom, m.geom)
            ) AS marker_locations
        FROM
            road_line as l,
            road_graph.markers AS m
        WHERE TRUE
        AND m.road_code = _road_code
        AND l.road_code = _road_code
        GROUP BY l.geom, l.closing_multilinestring, l.edges_multilinestring
    );

    RETURN TRUE;

END;
$$;


-- FUNCTION build_road_cached_objects(_road_code text)
COMMENT ON FUNCTION road_graph.build_road_cached_objects(_road_code text) IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)

Cached data is store in a temporary table living only until COMMIT
';



-- get_road_previous_marker_from_point(text, geometry, boolean)
DROP FUNCTION IF EXISTS road_graph.get_road_previous_marker_from_point(text, geometry, boolean);
CREATE OR REPLACE FUNCTION road_graph.get_road_previous_marker_from_point(
    _road_code text,
    _point geometry,
    _use_cache boolean DEFAULT false
) RETURNS TABLE(
    id integer,
    road_code text, code integer,
    abscissa real, geom geometry,
    road_linestring_from_marker_to_point geometry,
    road_linestring_from_start_to_point geometry,
    closing_multilinestring geometry,
    is_next_edge_start_marker boolean,
    cumulative real
)
    LANGUAGE plpgsql
    AS $$
DECLARE
    _road_info record;
    _run_build_cache boolean;
    _previous_marker record;
    _point_is_between_edges boolean;
    _previous_edge_record record;
    _next_edge_record record;
    -- timing variables
   _timing1  timestamptz;
   _start_ts timestamptz;
   _end_ts   timestamptz;
   _overhead numeric;     -- in ms
BEGIN

    -- timing
    _timing1  := clock_timestamp();
    _start_ts := clock_timestamp();
    _end_ts   := clock_timestamp();
    -- take minimum duration as conservative estimate
    _overhead := 1000 * extract(epoch FROM LEAST(_start_ts - _timing1, _end_ts   - _start_ts));
    _start_ts := clock_timestamp();

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
            r.edges_multilinestring,
            r.marker_locations,
            ST_LineLocatePoint(r.simple_linestring, _point) AS point_location
        FROM road_cache AS r
        WHERE r.road_code = _road_code
        ;

    -- RAISE NOTICE '% = %' , 'get road info from cache', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

        -- Get the previous marker
        -- which is the one with the location just before the _point location
        -- We also compute the linestring from the marker to the end of the road
        -- since we can use it to simplify the calcution of the reference from the point
        SELECT
        INTO _previous_marker
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
            _road_info.closing_multilinestring,
            -- MultiLinestring of the edges (with gaps between edges)
            _road_info.edges_multilinestring,
            -- road simple linestring : the single linestring (with no gaps) made by merging all edges linestrings and closing linestrings
            _road_info.simple_linestring AS road_simple_linestring,
            False AS is_next_edge_start_marker,
            NULL::real as cumulative
        FROM
            road_graph.markers AS m
        WHERE True
        AND (_road_info.marker_locations->>(m.id::text))::float8 <= _road_info.point_location
        ORDER BY (_road_info.marker_locations->>(m.id::text))::float8 DESC
        LIMIT 1
        ;
    -- RAISE NOTICE '% = %' , 'get previous marker from cache', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
    ELSE
        -- Use plain query to avoid creating temporary tables
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
                )) AS geom,
                o.geom AS edge_geom
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
                    ) AS closing_multilinestring,
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Multi(ST_Collect(t.edge_geom ORDER BY t.edge_order))
                        ),
                        2
                    )  AS edges_multilinestring
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
        INTO _previous_marker
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
            l.closing_multilinestring,
            -- MultiLinestring of the edges (with gaps between edges)
            l.edges_multilinestring,
            -- road simple linestring : the single linestring (with no gaps) made by merging all edges linestrings and closing linestrings
            l.geom AS road_simple_linestring,
            False AS is_next_edge_start_marker,
            NULL::real as cumulative
        FROM
            marker_position AS m,
            road_line AS l
        WHERE True
        AND m.marker_location <= l.point_location
        ORDER BY m.marker_location DESC
        LIMIT 1
        ;
    -- RAISE NOTICE '% = %' , 'get road info without cache', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
    END IF;

    -- Check if the _point is between 2 real edges or close to the edges
    _point_is_between_edges =
    CASE
        WHEN ST_Distance(
                _point,
                _previous_marker.road_simple_linestring
            ) != ST_Distance(
                _point,
                _previous_marker.edges_multilinestring
            )
            THEN True
        ELSE False
    END
    ;
    -- RAISE NOTICE '% = %' , 'check point between edges', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
    -- RAISE NOTICE '_point_is_between_edges: %', _point_is_between_edges;

    -- If true, we should check if the next edge start_marker_code is the same as the found previous marker code
    -- If so, do nothing,
    -- If not we should preferably use the marker code of the edge start point
    IF _point_is_between_edges IS TRUE THEN
        -- first we need to find the previous edge
        SELECT INTO _previous_edge_record
            e.id, e.next_edge_id
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        AND e.end_marker <= _previous_marker.code
        ORDER BY e.end_cumulative DESC
        LIMIT 1
        ;
    RAISE NOTICE '% = %' , 'get previous edge record', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
        IF _previous_edge_record.next_edge_id IS NOT NULL THEN
            -- Get the next edge start marker code
            SELECT INTO _next_edge_record
                e.id, e.start_marker, e.start_abscissa,
                m.id AS marker_id, m.geom AS marker_geom,
                e.start_cumulative
            FROM road_graph.edges AS e
            JOIN road_graph.markers AS m
                ON m.code = e.start_marker AND m.road_code = e.road_code
            WHERE e.id = _previous_edge_record.next_edge_id
            ;
    RAISE NOTICE '% = %' , 'get next edge record', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
            IF _next_edge_record.start_marker IS NOT NULL
                AND _next_edge_record.start_marker > _previous_marker.code THEN
                -- RAISE NOTICE 'We should use the next edge start_marker instead of the previous marker code';
                _previous_marker.id := _next_edge_record.marker_id;
                _previous_marker.code := Coalesce(_next_edge_record.start_marker, 0);
                _previous_marker.abscissa := Coalesce(_next_edge_record.start_abscissa, 0.0);
                _previous_marker.geom := _next_edge_record.marker_geom;
                -- Linestring (with no gaps) between the marker and the point
                -- In this case, we adapt and return the same geometry as the one between the start and the point
                _previous_marker.road_linestring_from_marker_to_point := road_linestring_from_start_to_point;
                -- Declare we have not taken the previous marker, but the start marker of the first next edge
                _previous_marker.is_next_edge_start_marker = True;
                -- we also need the start_cumulative
                _previous_marker.cumulative = _next_edge_record.start_cumulative;
            END IF;
        END IF;
    END IF;


    -- Return the previous marker data
    RETURN QUERY
    SELECT
        _previous_marker.id, _previous_marker.road_code,
        _previous_marker.code, _previous_marker.abscissa,
        _previous_marker.geom,
        _previous_marker.road_linestring_from_marker_to_point,
        _previous_marker.road_linestring_from_start_to_point,
        _previous_marker.closing_multilinestring,
        _previous_marker.is_next_edge_start_marker,
        _previous_marker.cumulative
    ;

END;
$$;


-- FUNCTION get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean) IS 'Get the closest upstream marker for the given road from a given point.
This function can use roads cached objects generated beforehand via function build_road_cached_objects.
Or use a full SQL query with no use of temporary tables, depending of the paramter _use_cache

Illustration
|m0----m1----|  |-m2-m2b----m3----|    p3   m4  |--------m5---|
                 p0     p1                         p2
p0 -> marker is m1
p1 -> marker is m2b (virtual marker with a non-null abscissa)
p2 -> marker is m3
p3 -> marker is m4, since it is the start marker of the first next edge and p3 is not on a real previous edge.

The function also returns:
* the simple linestring (no gaps) made by merging all edges linestrings from the marker to the point
* the simple linestring (no gaps) made by merging all edges linestrings from the start to the point
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
* if the found marker is the start marker of the next edge (for p3-like cases when the point is between two edges)
';


-- get_reference_from_point(geometry, text, boolean)
CREATE OR REPLACE FUNCTION road_graph.get_reference_from_point(
    _point geometry,
    _road_code text DEFAULT NULL::text,
    _use_cache boolean DEFAULT false
) RETURNS jsonb
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
    -- If the marker is the next edge start marker, because the point
    -- was not on an edge but between two unconnected edges,
    -- we should take information from this edge.
    found_marker_code = closest_edge_marker.code;
    found_abscissa =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN ST_Length(closest_edge_marker.upstream_road_from_marker) + closest_edge_marker.abscissa
            ELSE closest_edge_marker.abscissa
        END
    ;
    found_cumulative =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN Coalesce(ST_Length(closest_edge_marker.upstream_road_from_start), 0)
            ELSE closest_edge_marker.cumulative
        END
    ;
    found_offset =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN closest_edge.distance
            ELSE 0.0
        END
    ;
    found_side = (
        CASE
            WHEN ST_Contains(
                ST_Buffer(
                    closest_edge.geom,
                    closest_edge.distance +1,
                    'side=left'
                ), _point
            ) THEN 'left'
            ELSE 'right'
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


-- FUNCTION get_reference_from_point(_point geometry, _road_code text, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text, _use_cache boolean) IS 'Calculate the references for the given point. The second parameter _road_code allows to narrow the search to the specified road.
Since this method is heavily used and the calculation is costly, we can pass a third parameter _use_cache which allows to use a pre-generated cache (to be build before using this function).';
