-- Performance of the linear referencing functions
--
-- * get_road_previous_marker_from_point: with the road cache, only search the markers of the road
--   (all the markers of the database were read)
-- * build_road_cached_objects & get_reference_from_point: store the edge orders in the road cache
--   to avoid calling get_ordered_edges for each point
-- * get_ordered_edges: a road with only one edge returned this edge twice
-- * update_edge_references: the notice was always raised
-- * build_road_cached_objects: the cache can contain several roads (one row per road).
--   Each build replaces the row of the road, so the cache of a road is rebuilt after a graph modification
--   (update_edge_references) and before its use (update_table_references_from_geometries)
-- * get_road_previous_marker_from_point: fall back to the calculation without cache if the road is not in the cache
-- * update_table_references_from_geometries: build the cache of the roads of the objects, then use it
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION.


-- FUNCTION: road_graph.get_road_previous_marker_from_point
-- Cached branch: only use the markers of the road (the query was reading all the markers of the database).
-- Fix the linestring returned when the point is between two edges.
-- Fall back to the calculation without cache if the road is not in the cache
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

    -- The road must be in the cache (see build_road_cached_objects).
    -- If not, the calculation is done without cache
    -- Two separate tests: the query on road_cache cannot be planned if the table does not exist
    IF _use_cache AND to_regclass('road_cache') IS NULL THEN
        _use_cache = False;
    END IF;
    IF _use_cache THEN
        IF NOT EXISTS (SELECT 1 FROM road_cache AS r WHERE r.road_code = _road_code) THEN
            _use_cache = False;
        END IF;
    END IF;

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
-- Cache with one row per road, replaced at each build. Add the edge orders
CREATE OR REPLACE FUNCTION road_graph.build_road_cached_objects(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN

    -- The cache contains one row per road. It is dropped at the end of the transaction.
    -- Test before to avoid the notice raised by CREATE TABLE IF NOT EXISTS
    IF to_regclass('road_cache') IS NULL THEN
        CREATE TEMPORARY TABLE road_cache (
            road_code text PRIMARY KEY,
            simple_linestring geometry,
            closing_multilinestring geometry,
            edges_multilinestring geometry,
            marker_locations jsonb,
            edge_orders jsonb
        ) ON COMMIT DROP;
    END IF;

    -- Remove the previous cache of the road, which could be outdated
    -- if the road graph has been modified since in the same transaction
    DELETE FROM road_cache AS r
    WHERE r.road_code = _road_code
    ;

    -- The cache is not used for roads with less than 2 edges
    IF (SELECT count(*) FROM road_graph.edges AS e WHERE e.road_code = _road_code) < 2 THEN
        RETURN TRUE;
    END IF;

    INSERT INTO road_cache (
        road_code, simple_linestring, closing_multilinestring, edges_multilinestring,
        marker_locations, edge_orders
    )
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
    ;

    RETURN TRUE;

END;
$$;

COMMENT ON FUNCTION road_graph.build_road_cached_objects(_road_code text) IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
* the order of the edges, used by get_reference_from_point to avoid calculating it again for each point

Cached data is stored in the temporary table road_cache, with one row per road, living only until COMMIT.
Each call replaces the row of the given road: the cache of a road must be built again after any modification
of its edges or markers in the same transaction, before using it.
The functions using the cache fall back to the calculation without cache if the road is not in the cache.
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
-- Always rebuild the cache of the road. Fix the condition of the notice, which was always raised
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
    -- It is always built again, since the road graph has been modified:
    -- it also removes the outdated cache of a road which has now less than 2 edges
    IF _road_code IS NOT NULL THEN
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


-- FUNCTION: road_graph.update_table_references_from_geometries
-- Build the cache of the roads of the objects, then use it
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
    cached_road_codes text[];
    cached_road_code text;
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

    -- Build the road cache for the roads of the objects to update
    -- It is always built again here, even if it already exists for a road,
    -- since the road graph could have been modified since its last build in the same transaction
    sql_text = format(
        $SQL$
        SELECT array_agg(DISTINCT trim(mo.road_code)::text)
        FROM %1$I.%2$I AS mo
        WHERE Coalesce(trim(mo.road_code), '') != ''
        AND (
            mo.road_code::text = ANY(string_to_array('%3$s', ',')::text[])
            OR '%3$s' = ''
        )
        $SQL$,
        _schema_name,
        _table_name,
        Coalesce(array_to_string(_road_codes::text[], ','), '')
    );
    EXECUTE sql_text
    INTO cached_road_codes
    ;
    FOREACH cached_road_code IN ARRAY Coalesce(cached_road_codes, ARRAY[]::text[]) LOOP
        PERFORM road_graph.build_road_cached_objects(cached_road_code);
    END LOOP;

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
                            -- Use the road cache built before
                            TRUE
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
                            -- Use the road cache built before
                            TRUE
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
                            -- Use the road cache built before
                            TRUE
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
