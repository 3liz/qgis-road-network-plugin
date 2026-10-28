-- Performance of the linear referencing functions
--
-- * get_road_previous_marker_from_point: with the road cache, only search the markers of the road
--   (all the markers of the database were read)
-- * build_road_cached_objects & get_reference_from_point: store the edge orders in the road cache
--   to avoid calling get_ordered_edges for each point
-- * get_ordered_edges: a road with only one edge returned this edge twice
-- * update_edge_references: the notice was always raised
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION.

-- FUNCTION: road_graph.get_road_previous_marker_from_point
-- Cached branch: only use the markers of the road (the query was reading all the markers of the database).
-- Fix the linestring returned when the point is between two edges
CREATE OR REPLACE FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean DEFAULT false) RETURNS TABLE(id integer, road_code text, code integer, abscissa real, geom geometry, road_linestring_from_marker_to_point geometry, road_linestring_from_start_to_point geometry, closing_multilinestring geometry, is_next_edge_start_marker boolean, cumulative real)
    LANGUAGE plpgsql
    AS $$
DECLARE
    _road_info record;
    _run_build_cache boolean;
    _previous_marker record;
    _point_is_between_edges boolean;
    _closest_edge_at_5m record;
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
        -- Only the markers of the road
        AND m.road_code = _road_code
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

    -- If the point is between two edges (floating in the air),
    -- we get the closest edge start, and replace the return references if needed
    --  |---ID:5-----(4)---------|     there is a gap    X |(7+60)----ID:10-----------|
    --                         4+180                      7+60
    -- the point X should have the references 7+60 if it is close enough to the edge ID 10
    IF _point_is_between_edges IS TRUE THEN
        -- Get the closest edge (tolerance 5m)
        SELECT INTO _closest_edge_at_5m
            e.id,
            e.start_marker, e.start_abscissa, e.start_cumulative,
            _point <-> ST_StartPoint(e.geom) AS distance,
            ST_StartPoint(e.geom) AS geom
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        AND ST_DWithin(ST_StartPoint(e.geom), _point, 5)
        ORDER BY distance
        LIMIT 1
        ;

        IF _closest_edge_at_5m.id IS NOT NULL
            AND Coalesce(_closest_edge_at_5m.start_marker * 10000 + _closest_edge_at_5m.start_abscissa, 0.0)
                > Coalesce(_previous_marker.code * 10000 + _previous_marker.abscissa, 0.0)
        THEN
            -- RAISE NOTICE 'We should use the next edge start_marker instead of the previous marker code';
            _previous_marker.id := _closest_edge_at_5m.id;
            _previous_marker.code := Coalesce(_closest_edge_at_5m.start_marker, 0);
            _previous_marker.abscissa := Coalesce(_closest_edge_at_5m.start_abscissa, 0.0);
            _previous_marker.geom := _closest_edge_at_5m.geom;
            -- Linestring (with no gaps) between the marker and the point
            -- In this case, we adapt and return the same geometry as the one between the start and the point
            _previous_marker.road_linestring_from_marker_to_point := _previous_marker.road_linestring_from_start_to_point;
            -- Declare we have not taken the previous marker, but the start marker of the first next edge
            _previous_marker.is_next_edge_start_marker = True;
            -- we also need the start_cumulative
            _previous_marker.cumulative = _closest_edge_at_5m.start_cumulative;
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


-- FUNCTION: road_graph.get_ordered_edges
-- Exit after RETURN QUERY: a road with one edge returned this edge twice
CREATE OR REPLACE FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer DEFAULT '-1'::integer, _direction text DEFAULT 'downstream'::text) RETURNS TABLE(id integer, edge_order integer, road_code text, geom geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    initial_edge record;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- If there is only on edge for this road, return it directly
    IF (
        SELECT count(*)
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
    ) = 1
    THEN
        RETURN QUERY
        SELECT
            e.id, 1 AS edge_order, e.road_code, e.geom
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        ;
        -- RETURN QUERY does not exit the function
        RETURN;
    END IF;

    -- Get initial edge. If not given, get the first road edge
    IF _initial_id = -1 THEN
        IF _direction = 'downstream' THEN
            SELECT into initial_edge
                e.id, e.start_node, e.end_node
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code
            AND previous_edge_id IS NULL
            ORDER BY start_cumulative, e.id
            LIMIT 1
            ;
            _initial_id = initial_edge.id;
        ELSE
            SELECT INTO initial_edge
                e.id, e.start_node, e.end_node
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code
            AND next_edge_id IS NULL
            ORDER BY start_cumulative DESC, e.id DESC
            LIMIT 1
            ;
            _initial_id = initial_edge.id;
        END IF;
        IF _initial_id IS NULL THEN
            RAISE NOTICE 'No edge found for the road %s initial edge',
                _road_code
            ;
            -- Do not raise any exception
            -- To allow wrong road_code in managed data
            -- to be silently used
            RETURN;
        END IF;
    ELSE
        SELECT INTO initial_edge
            e.id, e.start_node, e.end_node
        FROM road_graph.edges AS e
        WHERE e.id = _initial_id
        ;
    END IF;
    IF raise_notice in ('info', 'debug') THEN
        RAISE NOTICE 'get_ordered_edges - _initial_id = %',
            _initial_id
        ;
    END IF;

    IF _direction = 'downstream' THEN
        -- Get downstream edges
        RETURN QUERY
        WITH RECURSIVE ordered_edges
        AS(
            -- anchor member
            SELECT
                e.id, next_edge_id,
                -- add id to the list of processed ids
                ARRAY[e.id] AS ids,
                1 AS edge_order,
                e.start_node, e.end_node,
                e.geom --, 0.0::numeric AS distance
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code AND e.id = _initial_id
            UNION ALL
            -- recursive term
            SELECT
                e.id, e.next_edge_id,
                -- add id to the list of processed ids
                array_append(o.ids, e.id) AS ids,
                o.edge_order + 1 AS edge_order,
                e.start_node, e.end_node,
                e.geom --, ST_Distance(ST_StartPoint(e.geom), ST_EndPoint(o.geom))::numeric AS distance
            FROM road_graph.edges AS e
            JOIN ordered_edges AS o
                ON e.id = o.next_edge_id
                -- We try to get the next edge which can be far
                -- limit search radius to 200m
                -- NOT EFFICIENT FOR BIG ROADS
                -- OR ST_DWithin(ST_StartPoint(e.geom), ST_EndPoint(o.geom), 200)
                -- for roundabout, when previous and next edge ids are not set
                OR ST_DWithin(ST_StartPoint(e.geom), ST_EndPoint(o.geom), 0.20)
            WHERE e.road_code = _road_code
        	-- avoir infinite loop
            AND NOT (ARRAY[e.id] && o.ids)
        )
        SELECT --DISTINCT ON (o.edge_order)
            o.id, o.edge_order, _road_code AS road_code, o.geom
        FROM ordered_edges AS o
        ORDER BY o.edge_order --, distance
        ;
    ELSE
        RETURN QUERY
        WITH RECURSIVE ordered_edges
        AS(
            -- anchor member
            SELECT
                e.id, previous_edge_id,
                -- add id to the list of processed ids
                ARRAY[e.id] AS ids,
                1 AS edge_order,
                e.start_node, e.end_node,
                e.geom --, 0.0::numeric AS distance
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code AND e.id = _initial_id
            UNION ALL
            -- recursive term
            SELECT
                e.id, e.previous_edge_id,
                -- add id to the list of processed ids
                array_append(o.ids, e.id) AS ids,
                o.edge_order + 1 AS edge_order,
                e.start_node, e.end_node,
                e.geom --, ST_Distance(ST_EndPoint(e.geom), ST_StartPoint(o.geom))::numeric AS distance
            FROM road_graph.edges AS e
            JOIN ordered_edges AS o
                ON e.id = o.previous_edge_id
                -- We try to get the next edge which can be far
                -- limit search radius to 200m
                -- OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(o.geom), 200)
                -- for roundabout, when previous and next edge ids are not set
                OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(o.geom), 0.20)
            WHERE e.road_code = _road_code
        	-- avoir infinite loop
            AND NOT (ARRAY[e.id] && o.ids)
        )
        SELECT --DISTINCT ON (o.edge_order)
            o.id, o.edge_order, _road_code AS road_code, o.geom
        FROM ordered_edges AS o
        ORDER BY o.edge_order--, distance
        ;
    END IF;
END;
$$;


-- FUNCTION: road_graph.build_road_cached_objects
-- Add the edge orders in the road cache.
-- Do not raise a notice when the cache table does not exist yet
CREATE OR REPLACE FUNCTION road_graph.build_road_cached_objects(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN

    -- Drop the cache of the previous road
    -- Test before to avoid the notice raised by DROP TABLE IF EXISTS
    IF to_regclass('road_cache') IS NOT NULL THEN
        DROP TABLE road_cache;
    END IF;
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
            ) AS marker_locations,
            -- Order of the edges, used to choose between two edges at the same distance from a point
            (
                SELECT jsonb_object_agg(eo.id, eo.edge_order)
                FROM (
                    SELECT o.id, min(o.edge_order) AS edge_order
                    FROM ordered_edges AS o
                    GROUP BY o.id
                ) AS eo
            ) AS edge_orders
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

COMMENT ON FUNCTION road_graph.build_road_cached_objects(_road_code text) IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
* the order of the edges, used by get_reference_from_point to avoid calculating it again for each point

Cached data is store in a temporary table living only until COMMIT
';


-- FUNCTION: road_graph.get_reference_from_point
-- Use the edge orders from the road cache instead of calling get_ordered_edges for each point.
-- Remove the unused calculation of the edge substring
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
    cached_edge_orders jsonb;
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

    -- Get the edge order of the road from the cache, if it has been built beforehand
    -- (see build_road_cached_objects)
    IF _use_cache AND _road_code IS NOT NULL AND to_regclass('road_cache') IS NOT NULL THEN
        SELECT INTO cached_edge_orders r.edge_orders
        FROM road_cache AS r
        WHERE r.road_code = _road_code
        ;
    END IF;

    -- Get the closest edge depending on given road_code
    IF _road_code IS NOT NULL THEN
        WITH ordered_ids AS (
            -- Edge order calculated only if not found in the cache
            SELECT o.id, o.edge_order
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
            WHERE cached_edge_orders IS NULL
            UNION ALL
            SELECT j.key::integer AS id, j.value::integer AS edge_order
            FROM jsonb_each_text(cached_edge_orders) AS j
        )
        SELECT INTO closest_edge
            e.*,
            -- Calculate the distance between the edge and the point
            -- Do not use e.geom <-> _point AS distance
            -- since it can lead to some PostGIS errors if used in the ORDER BY
            -- such as "ERROR:  index returned tuples in wrong order"
            ST_Distance(e.geom, _point) AS distance
        FROM
            road_graph.edges AS e
        INNER JOIN ordered_ids AS o ON e.id = o.id
        -- Limit search for the given road code
        WHERE e.road_code = _road_code
        -- Limit search to 50m
        AND ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance, o.edge_order, e.start_cumulative
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
            ST_Distance(e.geom, _point) AS distance
        FROM
            road_graph.edges AS e
            -- LIMIT search without road_code TO 50m
        WHERE ST_DWithin(e.geom, _point, 50)
        ORDER BY distance
        -- Get only the closest edge
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


-- FUNCTION: road_graph.update_edge_references
-- Fix the condition of the notice, which was always raised
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
    IF raise_notice IN ('info', 'debug') THEN
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
