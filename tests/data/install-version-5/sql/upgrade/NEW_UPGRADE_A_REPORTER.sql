-- Division du temps par 2 sans cache (27 ms au lieu de 60)
-- par 20 avec cache (2ms !)

-- FUNCTION: editing_session.get_road_previous_marker_from_point(text, geometry, boolean)

DROP FUNCTION IF EXISTS editing_session.get_road_previous_marker_from_point(text, geometry);
DROP FUNCTION IF EXISTS editing_session.get_road_previous_marker_from_point(text, geometry, boolean);

CREATE OR REPLACE FUNCTION editing_session.get_road_previous_marker_from_point(
	_road_code text,
	_point geometry,
	_use_cache boolean DEFAULT false)
    RETURNS TABLE(id integer, road_code text, code integer, abscissa real, geom geometry, road_linestring_from_marker_to_point geometry, road_linestring_from_start_to_point geometry, closing_multilinestring geometry)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    _road_info record;
    _run_build_cache boolean;
BEGIN
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
                (_road_info.marker_locations->>(m.id))::float8,
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
            editing_session.markers AS m
        WHERE True
        AND m.marker_location <= _road_info.point_location
        ORDER BY m.marker_location DESC
        LIMIT 1
        ;
    ELSE
        -- Use plain query to avoid creating temporary tables
        RETURN QUERY
        WITH
        -- get road ordered edges (use previous and next edge ids)
        ordered_edges AS (
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
$BODY$;


COMMENT ON FUNCTION editing_session.get_road_previous_marker_from_point(text, geometry, boolean)
    IS 'Get the closest upstream marker for the given road from a given point.
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



-- FUNCTION: editing_session.get_reference_from_point(geometry, text, boolean)

DROP FUNCTION IF EXISTS editing_session.get_reference_from_point(geometry, text, boolean);
DROP FUNCTION IF EXISTS editing_session.get_reference_from_point(geometry, text, boolean);

CREATE OR REPLACE FUNCTION editing_session.get_reference_from_point(
	_point geometry,
	_road_code text DEFAULT NULL::text,
	_use_cache boolean DEFAULT false)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
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
    -- timing variables
   _timing1  timestamptz;
   _start_ts timestamptz;
   _end_ts   timestamptz;
   _overhead numeric;     -- in ms
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% get_reference_from_point - _point = % & _road_code = %',
            REPEAT('    ', pg_trigger_depth()::INTEGER),
            ST_AsText(_point),
            _road_code
        ;
    END IF;
    -- timing
    _timing1  := clock_timestamp();
    _start_ts := clock_timestamp();
    _end_ts   := clock_timestamp();
    -- take minimum duration as conservative estimate
    _overhead := 1000 * extract(epoch FROM LEAST(_start_ts - _timing1, _end_ts   - _start_ts));
    _start_ts := clock_timestamp();
    RAISE NOTICE 'START get_reference_from_point';

    -- Get the splitted closest road depending on given road_code
    -- we keep only the edge part between start point and given _point
    IF _road_code IS NOT NULL THEN
        WITH ordered_ids AS (
            SELECT *
            FROM editing_session.get_ordered_edges(_road_code, -1, 'downstream')
        )
        SELECT INTO closest_edge
            e.*,
            e.geom <-> _point AS distance,
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
            editing_session.edges AS e
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
            e.geom <-> _point AS distance,
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
            editing_session.edges AS e
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

    RAISE NOTICE '% = %' , 'CLOSEST_EDGE', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Get road_code
    found_road_code = closest_edge.road_code;

    WITH
    get_previous_marker AS (
        SELECT *
        FROM editing_session.get_road_previous_marker_from_point(
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
    RAISE NOTICE '% = %' , 'CLOSEST_EDGE_MARKER', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

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
    RAISE NOTICE '% = %' , 'VALUES', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
    RAISE NOTICE 'END';

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
$BODY$;


-- FUNCTION: editing_session.get_edge_references(integer, boolean)

DROP FUNCTION IF EXISTS editing_session.get_edge_references(integer);
DROP FUNCTION IF EXISTS editing_session.get_edge_references(integer, boolean);
CREATE OR REPLACE FUNCTION editing_session.get_edge_references(
	_edge_id integer,
	_use_cache boolean DEFAULT false)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    edge record;
    start_references jsonb;
    end_references jsonb;
    -- timing variables
   _timing1  timestamptz;
   _start_ts timestamptz;
   _end_ts   timestamptz;
   _overhead numeric;     -- in ms
BEGIN
    -- Notice used for big imports
    -- RAISE NOTICE 'get_edge_references - %', _edge_id;

    -- timing
    _timing1  := clock_timestamp();
    _start_ts := clock_timestamp();
    _end_ts   := clock_timestamp();
    -- take minimum duration as conservative estimate
    _overhead := 1000 * extract(epoch FROM LEAST(_start_ts - _timing1, _end_ts   - _start_ts));
    _start_ts := clock_timestamp();

    -- Get edge
    SELECT INTO edge
        e.*, r.road_type
    FROM editing_session.edges AS e
    JOIN editing_session.roads AS r
        ON e.road_code = r.road_code
    WHERE e.id = _edge_id
    ;
    IF edge IS NULL THEN
        RETURN NULL::json;
    END IF;

   -- RAISE NOTICE '% = %' , 'get edge', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Check road code
    IF edge.road_code IS NULL THEN
        RETURN NULL::json;
    END IF;

    -- start point
    SELECT INTO start_references
        editing_session.get_reference_from_point(
            ST_StartPoint(edge.geom),
            edge.road_code,
            _use_cache
        ) AS ref
    ;
   -- RAISE NOTICE '% = %' , 'get start point', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- end point
    -- we do not use ST_EndPoint for roundabout because this corresponds
    -- also to the ST_STartPoint
    IF edge.road_type = 'roundabout' THEN
        -- Use ST_LineInterpolatePoint(a_linestring, a_fraction);
        SELECT INTO end_references
            editing_session.get_reference_from_point(
                ST_LineInterpolatePoint(edge.geom, 0.99999),
                edge.road_code,
                _use_cache
            ) AS ref
        ;
    ELSE
        -- Use St_EndPoint(geom)
        SELECT INTO end_references
            editing_session.get_reference_from_point(
                ST_EndPoint(edge.geom),
                edge.road_code,
                _use_cache
            ) AS ref
        ;
    END IF;
   -- RAISE NOTICE '% = %' , 'get end references', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Return calculated data
    RETURN json_build_object(
        'start_marker', (start_references->>'marker_code')::text::integer,
        'start_abscissa', (start_references->>'abscissa')::text::real,
        'start_cumulative', (start_references->>'cumulative')::text::real,
        'end_marker', (end_references->>'marker_code')::text::integer,
        'end_abscissa', (end_references->>'abscissa')::text::real,
        'end_cumulative', (end_references->>'cumulative')::text::real
    )
    ;

END;
$BODY$;

COMMENT ON FUNCTION editing_session.get_edge_references(integer, boolean)
    IS 'Return the references of the given edge start and end point.
Could be used to UPDATE the edges of a road.
By default does not use road object cache store in a temporary table.
This behaviour can be overriden by setting _use_cache TO True;
';

-- FUNCTION: editing_session.update_edge_references(text, integer[])

DROP FUNCTION IF EXISTS editing_session.update_edge_references(text, integer[]);
CREATE OR REPLACE FUNCTION editing_session.update_edge_references(
	_road_code text DEFAULT NULL::text,
	_edge_ids integer[] DEFAULT NULL::integer[])
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    edge record;
    _run_build_cache boolean;
    _set_config text;
BEGIN
    -- Deactivate triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, false)
    INTO _set_config;

    -- Build road & marker objects cache for speeding up linear referencing
    SELECT editing_session.build_road_cached_objects(_road_code)
    INTO _run_build_cache;

    -- Get edges references
    WITH s AS (
        SELECT
            e.road_code, e.id,
            x.*
        FROM
            editing_session.edges AS e,
            json_to_record(
                -- 2nd parameter is TRUE so that we use precomputed object cache
                editing_session.get_edge_references(e.id, true)
            ) AS x (
                start_marker integer, start_abscissa real, start_cumulative real,
                end_marker integer, end_abscissa real, end_cumulative real
            )
        WHERE True
        AND (_road_code IS NULL OR e.road_code = _road_code)
        AND (_edge_ids IS NULL OR e.id = ANY (_edge_ids))
    )
    UPDATE editing_session.edges AS e
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
    SELECT set_config('road.graph.disable.trigger', '0'::text, false)
    INTO _set_config;

    -- Return boolean
    RETURN True
    ;

END;
$BODY$;


COMMENT ON FUNCTION editing_session.update_edge_references(text, integer[])
    IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges
';
