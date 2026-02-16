-- Remove useless function
DROP FUNCTION IF EXISTS road_graph.get_point_from_reference(text, integer, real, real, text);

-- DROP TRIGGERS
DROP TRIGGER IF EXISTS trg_aa_before_geometry_insert_or_update
ON road_graph.edges;
DROP TRIGGER IF EXISTS trg_aa_before_geometry_insert_or_update
ON road_graph.editing_sessions;
DROP TRIGGER IF EXISTS trg_aa_before_geometry_insert_or_update
ON road_graph.markers;
DROP TRIGGER IF EXISTS trg_aa_before_geometry_insert_or_update
ON road_graph.nodes;
DROP TRIGGER IF EXISTS trg_after_edge_delete
ON road_graph.edges;
DROP TRIGGER IF EXISTS trg_after_edge_insert_or_update
ON road_graph.edges;
DROP TRIGGER IF EXISTS trg_after_marker_insert_or_update_or_delete
ON road_graph.markers;
DROP TRIGGER IF EXISTS trg_after_node_insert_or_update
ON road_graph.nodes;
DROP TRIGGER IF EXISTS trg_before_edge_insert_or_update
ON road_graph.edges;



-- Get a PostgreSQL setting specific to road_graph
DROP FUNCTION IF EXISTS road_graph.get_current_setting(text, text, text);
CREATE FUNCTION road_graph.get_current_setting(setting_name text, default_value text, value_type text DEFAULT 'text')
RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    setting_value text;
BEGIN
    -- Get current setting value, if not set return default value
    setting_value = coalesce(current_setting(setting_name, true), default_value);
    -- Try to cast the setting value to the expected type, if it fails return the default value
    BEGIN
        IF value_type = 'integer' THEN
            RETURN setting_value::integer;
        ELSIF value_type = 'boolean' THEN
            RETURN  setting_value::boolean;
        ELSIF value_type = 'real' THEN
            RETURN  setting_value::real;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        IF value_type = 'integer' THEN
            RETURN default_value::integer;
        ELSIF value_type = 'boolean' THEN
            RETURN  default_value::boolean;
        ELSIF value_type = 'real' THEN
            RETURN  default_value::real;
        END IF;
    END;

    RETURN setting_value;
END;
$$;
COMMENT ON FUNCTION road_graph.get_current_setting(text, text, text)
IS 'Get a PostgreSQL current setting, with a default value if the setting is not set or is invalid.
The function is used to avoid repeating the coalesce(current_setting(...))::TYPE, 0) = 1
and to have a single point of maintenance for getting settings.'
;


-- aa_before_geometry_insert_or_update()
DROP FUNCTION IF EXISTS road_graph.aa_before_geometry_insert_or_update();
CREATE OR REPLACE FUNCTION road_graph.aa_before_geometry_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    is_roundabout boolean;
    _road_type text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Do not modify the geometry if geom field has not been changed
    IF TG_OP = 'UPDATE' AND (
            ST_Equals(OLD.geom, NEW.geom)
            OR
            ST_Equals(NEW.geom, ST_ReducePrecision(NEW.geom, 0.10))
        )
    THEN
        RETURN NEW;
    END IF;

    -- Do not modifiy for roundabout, else we have weird aspect
    IF TG_TABLE_NAME = 'edges'
    THEN
        -- Get edge road
        SELECT INTO _road_type
            r.road_type
        FROM road_graph.roads AS r
        WHERE r.road_code = NEW.road_code
        ;
        IF _road_type = 'roundabout' THEN
            RETURN NEW;
        END IF;
    END IF;

    -- Reduce geometry precision
    NEW.geom = ST_ReducePrecision(NEW.geom, 0.10);

    RETURN NEW;
END;
$$;


-- FUNCTION aa_before_geometry_insert_or_update()
COMMENT ON FUNCTION road_graph.aa_before_geometry_insert_or_update()
IS 'Fonction qui arrondit la précision des coordonnées à 0.1 soit 10cm si ce n''est pas déjà fait
lors d''une création ou d''une modification de géométrie.
Elle est préfixée par aa_ pour être lancée avant les autres trigger,
car l''ordre alphabétique compte.
';



-- FUNCTION: road_graph.build_road_cached_objects(text)

DROP FUNCTION IF EXISTS road_graph.build_road_cached_objects(text);
CREATE OR REPLACE FUNCTION road_graph.build_road_cached_objects(_road_code text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
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
                )) AS geom
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
                ) AS closing_multilinestring
            FROM touching_edges AS t
        )
        SELECT
            _road_code AS road_code,
            l.geom AS simple_linestring,
            l.closing_multilinestring,
            jsonb_agg(
                jsonb_build_object(
                    m.id,
                    ST_LineLocatePoint(l.geom, m.geom)
                )
            ) AS marker_locations
        FROM
            road_line as l,
            road_graph.markers AS m
        WHERE TRUE
        AND m.road_code = _road_code
        AND l.road_code = _road_code
        GROUP BY l.geom, l.closing_multilinestring
    );

    RETURN TRUE;

END;
$BODY$;

COMMENT ON FUNCTION road_graph.build_road_cached_objects(text)
    IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)

Cached data is store in a temporary table living only until COMMIT
';


-- FUNCTION: road_graph.get_road_previous_marker_from_point(text, geometry, boolean)
DROP FUNCTION IF EXISTS road_graph.get_road_previous_marker_from_point(text, geometry);
DROP FUNCTION IF EXISTS road_graph.get_road_previous_marker_from_point(text, geometry, boolean);
CREATE OR REPLACE FUNCTION road_graph.get_road_previous_marker_from_point(
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
            road_graph.markers AS m
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

COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(text, geometry, boolean)
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


-- FUNCTION: road_graph.get_reference_from_point(geometry, text, boolean)
DROP FUNCTION IF EXISTS road_graph.get_reference_from_point(geometry, text);
DROP FUNCTION IF EXISTS road_graph.get_reference_from_point(geometry, text, boolean);
CREATE OR REPLACE FUNCTION road_graph.get_reference_from_point(
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
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');
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
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream')
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

    RAISE NOTICE '% = %' , 'CLOSEST_EDGE', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

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


-- FUNCTION: road_graph.get_edge_references(integer, boolean)

DROP FUNCTION IF EXISTS road_graph.get_edge_references(integer);
DROP FUNCTION IF EXISTS road_graph.get_edge_references(integer, boolean);
CREATE OR REPLACE FUNCTION road_graph.get_edge_references(
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
    FROM road_graph.edges AS e
    JOIN road_graph.roads AS r
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
        road_graph.get_reference_from_point(
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
            road_graph.get_reference_from_point(
                ST_LineInterpolatePoint(edge.geom, 0.99999),
                edge.road_code,
                _use_cache
            ) AS ref
        ;
    ELSE
        -- Use St_EndPoint(geom)
        SELECT INTO end_references
            road_graph.get_reference_from_point(
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


COMMENT ON FUNCTION road_graph.get_edge_references(integer, boolean)
    IS 'Return the references of the given edge start and end point.
Could be used to UPDATE the edges of a road.
By default does not use road object cache store in a temporary table.
This behaviour can be overriden by setting _use_cache TO True;
';


-- FUNCTION: road_graph.update_edge_references(text, integer[])
DROP FUNCTION IF EXISTS road_graph.update_edge_references(text, integer[]);
CREATE OR REPLACE FUNCTION road_graph.update_edge_references(
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
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- Build road & marker objects cache for speeding up linear referencing
    SELECT road_graph.build_road_cached_objects(_road_code)
    INTO _run_build_cache;

    -- Get edges references
    WITH s AS (
        SELECT
            e.road_code, e.id,
            x.*
        FROM
            road_graph.edges AS e,
            json_to_record(
                -- 2nd parameter is TRUE so that we use precomputed object cache
                road_graph.get_edge_references(e.id, true)
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
$BODY$;


COMMENT ON FUNCTION road_graph.update_edge_references(text, integer[])
    IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges
';




-- copy_data_to_editing_session(integer)
DROP FUNCTION IF EXISTS road_graph.copy_data_to_editing_session(integer);
CREATE OR REPLACE FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
    editing_session_record record;
    _set_config text;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;
    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    IF editing_session_record.status != 'created' THEN
        RAISE EXCEPTION 'The given id % does not correspond to editing session with status ''created'' !', _editing_session_id;
    END IF;

    -- Disable triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- Truncate tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;

    -- Get roads to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    roads AS (
        SELECT DISTINCT r.*
        FROM road_graph.roads AS r
        JOIN edges_roads AS e
            ON r.road_code = e.road_code
    )
    INSERT INTO editing_session.roads
    SELECT *
    FROM roads
    ;

    -- Get nodes to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.start_node, e.end_node
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    ),
    nodes AS (
        SELECT DISTINCT n.*
        FROM road_graph.nodes AS n
        JOIN edges AS e
            ON n.id IN (e.start_node, e.end_node)
    )
    INSERT INTO editing_session.nodes
    SELECT *
    FROM nodes
    ;

    -- Get edges to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.*
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    )
    INSERT INTO editing_session.edges
    SELECT *
    FROM edges
    ;

    -- Get markers to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    markers AS (
        SELECT DISTINCT m.*
        FROM road_graph.markers AS m
        JOIN edges_roads AS er
            ON m.road_code = er.road_code
    )
    INSERT INTO editing_session.markers
    SELECT *
    FROM markers
    ;

    -- Replace identities by road_schema sequences to get correct ids
    BEGIN
        ALTER TABLE editing_session.roads ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.nodes ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.markers ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.edges ALTER COLUMN id DROP IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Cannot drop identity in editing_session tables';
    END;
    -- roads
    ALTER TABLE editing_session.roads
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.roads', 'id'));
    -- nodes
    ALTER TABLE editing_session.nodes
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.nodes', 'id'));
    -- markers
    ALTER TABLE editing_session.markers
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.markers', 'id'));
    -- edges
    ALTER TABLE editing_session.edges
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.edges', 'id'));

    -- Set ids in the editing_sessions
    UPDATE road_graph.editing_sessions
    SET cloned_ids = jsonb_build_object(
        'roads', (SELECT jsonb_agg(id::text) FROM editing_session.roads),
        'nodes', (SELECT jsonb_agg(id::text) FROM editing_session.nodes),
        'markers', (SELECT jsonb_agg(id::text) FROM editing_session.markers),
        'edges', (SELECT jsonb_agg(id::text) FROM editing_session.edges)
    ),
    "status" = 'cloned'
    WHERE id = _editing_session_id
    ;

    -- In the table editing_session.editing_sessions
    -- keep only the current editing session
    -- This table will be used by the trigger editing_survey
    -- to collect changes in the editing_sessions tables
    TRUNCATE editing_session.editing_sessions RESTART IDENTITY;
    INSERT INTO editing_session.editing_sessions
    SELECT * FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Audit trigger on editing_session tables
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.roads;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.roads
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.nodes;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.nodes
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.markers;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.markers
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.edges;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.edges
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;

    RETURN True;
END;
$$;


-- FUNCTION copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the editing_session schema corresponding to the given editing session ID.';



-- merge_edges(integer, integer)
DROP FUNCTION IF EXISTS road_graph.merge_edges(integer, integer);
CREATE OR REPLACE FUNCTION road_graph.merge_edges(id_edge_a integer, id_edge_b integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_a record;
    edge_b record;
    edge_to_delete record;
    node_to_be_deleted integer;
    _set_config text;
BEGIN

    -- Get both edges
    -- edge A
    SELECT INTO edge_a
    *
    FROM road_graph.edges
    WHERE id = id_edge_a;
    IF edge_a.id IS NULL THEN
        RAISE EXCEPTION 'No edge found with id = % ', id_edge_a;
    END IF;

    -- edge B
    SELECT INTO edge_b
    *
    FROM road_graph.edges
    WHERE id = id_edge_b;
    IF edge_b.id IS NULL THEN
        RAISE EXCEPTION 'No edge found with id = % ', id_edge_b;
    END IF;

    -- edge A and B touches
    IF NOT ST_Touches(edge_a.geom, edge_b.geom) THEN
        RAISE NOTICE 'Edges does not touch';
        RETURN FALSE;
    END IF;

    -- RETURN FALSE;
    -- Merge edges: keep the oldest one,
    -- and add geometry from the other one
    edge_to_delete = edge_a;
    IF edge_a.id < edge_b.id OR edge_a.id < edge_b.id THEN
        edge_to_delete = edge_b;
    END IF;
    RAISE NOTICE 'merge_edges - % and %. Delete %', id_edge_a, id_edge_b, edge_to_delete.id
    ;

    -- Get node to be deleted
    node_to_be_deleted = -1;
    IF edge_a.start_node = edge_b.end_node THEN
        node_to_be_deleted = edge_a.start_node;
    END IF;
    IF edge_b.start_node = edge_a.end_node THEN
        node_to_be_deleted = edge_b.start_node;
    END IF;
    SELECT set_config('road.graph.merge.edges.useless.node', node_to_be_deleted::text, true)
    INTO _set_config
    ;
    RAISE NOTICE 'merge_edges -  % and %. node to be deleted = %', id_edge_a, id_edge_b, node_to_be_deleted
    ;

    -- Delete and UPDATE in the same request
    WITH del AS (
        DELETE FROM road_graph.edges
        WHERE id = edge_to_delete.id
        RETURNING *
    )
    UPDATE road_graph.edges
    SET geom = ST_LineMerge(ST_Union(geom, edge_to_delete.geom))
    WHERE True
    AND id IN (id_edge_a, id_edge_b)
    AND id != edge_to_delete.id
    ;
    SELECT set_config('road.graph.merge.edges.useless.node', '-1', true)
    INTO _set_config
    ;

    -- Return True
    RETURN TRUE;

END;
$$;

COMMENT ON FUNCTION road_graph.merge_edges(id_edge_a integer, id_edge_b integer)
IS 'Merge two edges by their geometry.
The merged edge is the one with the lowest id. The other edge is deleted.
The node between the two edges is not deleted but its id is stored
in the session variable ''road.graph.merge.edges.useless.node'' for further use if needed.
';


-- merge_editing_session_data(integer)
DROP FUNCTION IF EXISTS road_graph.merge_editing_session_data(integer);
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SET search_path TO road_graph, public;
    SELECT INTO editing_session_record
    *
    FROM editing_sessions
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

    -- Create SQL to run for each edited data
    FOR sql_record IN
        SELECT
            sql_text
        FROM create_queries_from_editing_session(_editing_session_id)
    LOOP
       RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    -- Truncate editing_session tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;

    -- Delete merged editing session
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Reset the search path
    RESET search_path;

    RETURN True;
END;
$$;


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer)
IS 'Copy data from the given editing session into the road_graph schema.';



-- after_edge_delete()
DROP FUNCTION IF EXISTS road_graph.after_edge_delete();
CREATE OR REPLACE FUNCTION road_graph.after_edge_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    has_changed boolean;
    test_edges record;
    merge_result boolean;
    update_edge_references_result boolean;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN OLD;
    END IF;

    -- Raise notice ?
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Check if old referenced nodes are still referenced by edges
    -- If not, delete them
    -- Upstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.start_node
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.start_node
        OR end_node = OLD.start_node
    )
    ;
    -- Downstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.end_node
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.end_node
        OR end_node = OLD.end_node
    );

    -- Merge edges which were linked to the deleted node
    -- Node A - OLD.end_node
    -- RAISE NOTICE 'Node A - %', OLD.end_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes
    FROM road_graph.edges AS t
    WHERE OLD.end_node = t.end_node OR OLD.end_node = t.start_node
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF test_edges.nb = 2 AND test_edges.nb_road_codes = 1 THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE 'We must merge - % ',
                array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- Node B - OLD.start_node
    -- RAISE NOTICE 'Node B - %', OLD.start_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes
    FROM road_graph.edges AS t
    WHERE OLD.start_node = t.end_node OR OLD.start_node = t.start_node
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF test_edges.nb = 2 AND test_edges.nb_road_codes = 1 THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE 'We must merge - % ',
                array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- Update road references if there is at least one edge left
    -- for the road
    IF (
        SELECT count(e.*)
        FROM road_graph.edges AS e
        WHERE e.road_code = OLD.road_code
        AND e.id != OLD.id
    ) > 0
    THEN
        -- First update previous and next edges
        -- previous
        SET road.graph.edge.ref.calc.disabled = 'yes';

        UPDATE road_graph.edges AS e
        SET next_edge_id = (
            SELECT s.id
            FROM road_graph.edges AS s
            WHERE s.id = OLD.next_edge_id
        )
        WHERE e.next_edge_id = OLD.id
        ;
        -- next
        UPDATE road_graph.edges AS e
        SET previous_edge_id = (
            SELECT s.id
            FROM road_graph.edges AS s
            WHERE s.id = OLD.previous_edge_id
        )
        WHERE e.previous_edge_id = OLD.id
        ;

        -- Set back the value
        SET road.graph.edge.ref.calc.disabled = 'no';

        -- Calculate references
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER EDGE % N° %, update all road edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL)
        ;

    END IF;

    RETURN OLD;
END;
$$;


-- FUNCTION after_edge_delete()
COMMENT ON FUNCTION road_graph.after_edge_delete() IS 'Lors de la suppression d''un tronçon, on supprime les Nodes qui étaient rattachés aux sommets amont et aval du tronçon,
uniquement si ces derniers ne sont plus rattachés à aucun autre tronçon.
On fusionne aussi les tronçons collés plus liés par les possibles noeuds supprimés
';



-- after_edge_insert_or_update()
DROP FUNCTION IF EXISTS road_graph.after_edge_insert_or_update();
CREATE OR REPLACE FUNCTION road_graph.after_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    crossing_node record;
    touching_node record;
    new_node_id integer;
    deleted_node_ids integer[];
    id_new_edge integer;
    created_nodes_at_intersection integer[];
    node_to_be_deleted integer;
    update_edge_references_result boolean;
    raise_notice text;
    cascade_edge_id integer;
    edge_road record;
    is_roundabout boolean;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - Start',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    IF TG_OP = 'UPDATE' THEN
        -- UPDATE - Move related upstream and downstream nodes if needed
        IF NOT ST_Equals(ST_StartPoint(OLD.geom), ST_StartPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update upstream node if needed
            UPDATE road_graph.nodes AS n
            SET geom = ST_StartPoint(NEW.geom)
            WHERE n.id = NEW.start_node
            AND NOT ST_Equals(n.geom, ST_StartPoint(NEW.geom))
            ;
        END IF;
        IF NOT ST_Equals(ST_EndPoint(OLD.geom), ST_EndPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update downstream node if needed
            UPDATE road_graph.nodes AS n
            SET geom = ST_EndPoint(NEW.geom)
            WHERE n.id = NEW.end_node
            AND NOT ST_Equals(n.geom, ST_EndPoint(NEW.geom))
            ;
        END IF;

        -- Check if old referenced nodes are still referenced by edges
        -- If not, delete them
        -- Upstream node
        IF OLD.start_node != NEW.start_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.start_node
                AND NOT EXISTS (
                    SELECT id
                    FROM road_graph.edges
                    WHERE start_node = OLD.start_node
                    OR end_node = OLD.start_node
                )
                RETURNING id
            ) SELECT array_agg(id) INTO deleted_node_ids
            FROM del
            ;

            IF raise_notice IN ('info', 'debug') AND deleted_node_ids IS NOT NULL THEN
                RAISE NOTICE '% AFTER edge % n° %, unreferenced upstream nodes deleted : %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, array_to_string(deleted_node_ids, ', ')
                ;
            END IF;
        END IF;

        -- Downstream node
        IF OLD.start_node != NEW.start_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.end_node
                AND NOT EXISTS (
                    SELECT id
                    FROM road_graph.edges
                    WHERE start_node = OLD.end_node
                    OR end_node = OLD.end_node
                )
                RETURNING id
            ) SELECT array_agg(id) INTO deleted_node_ids
            FROM del
            ;
            IF raise_notice IN ('info', 'debug') AND deleted_node_ids IS NOT NULL THEN
                RAISE NOTICE '% AFTER edge % n° %, unreferenced downstream nodes deleted : %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, array_to_string(deleted_node_ids, ', ')
                ;
            END IF;
        END IF;
    END IF;

    -- Cascade changes made on previous_edge_id and next_edge_id
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and position manually the marker 0)
    -- previous edge id
    IF (TG_OP = 'INSERT' AND NEW.previous_edge_id IS NOT NULL AND NOT is_roundabout)
        OR (TG_OP = 'UPDATE' AND NEW.previous_edge_id != OLD.previous_edge_id AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NEW.id
        WHERE e.id != NEW.id
        AND e.id = NEW.previous_edge_id
        AND (e.next_edge_id IS NULL OR e.next_edge_id != NEW.id)
        RETURNING e.id
        INTO cascade_edge_id
        ;
        IF raise_notice IN ('info', 'debug') AND cascade_edge_id IS NOT NULL THEN
            RAISE NOTICE '% AFTER edge % n° %, cascade changes on previous edge id : changes made on edge n° %',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                cascade_edge_id
            ;
        END IF;

    END IF;
    -- next edge id
    IF (TG_OP = 'INSERT' AND NEW.next_edge_id IS NOT NULL AND NOT is_roundabout)
        OR (TG_OP = 'UPDATE' AND NEW.next_edge_id != OLD.next_edge_id AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NEW.id
        WHERE e.id != NEW.id
        AND e.id = NEW.next_edge_id
        AND (e.previous_edge_id IS NULL OR e.previous_edge_id != NEW.id)
        RETURNING e.id
        INTO cascade_edge_id
        ;
        IF raise_notice IN ('info', 'debug') AND cascade_edge_id IS NOT NULL THEN
            RAISE NOTICE '% AFTER edge % n° %, cascade changes on next edge id : changes made on edge n° %',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                cascade_edge_id
            ;
        END IF;
    END IF;

    -- For INSERT OR UPDATE
    -- Create nodes at intersection with other edges if needed
    -- This will then run the trigger after_node_insert_or_update
    -- which will eventually split the edges intersecting this NEW edge
    created_nodes_at_intersection = ARRAY[]::integer[];
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom))
        -- avoid infinite loop and self-crossing
        AND road_graph.get_current_setting('road.graph.edge.crossing.node.creation.pending', 'no', 'text') != 'yes'

    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER edge % n° %, crossing node test',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;
        FOR crossing_node IN
            WITH new_nodes AS (
                SELECT
                    ST_Intersection(e.geom, NEW.geom) AS geom
                FROM road_graph.edges AS e
                WHERE e.id != NEW.id
                AND ST_Intersects(e.geom, NEW.geom)
            ),
            -- intersection can produce multipoints
            -- if the edge crosses more than once
            -- We explode into single points
            new_nodes_dumped AS (
                SELECT (ST_Dump(c.geom)).geom AS geom
                FROM new_nodes AS c
            )
            -- Only get points not close to existing nodes
            -- If the road intersects several times the same road
            -- we should transform ST_MultiPoint into ST_Point
            SELECT DISTINCT c.geom
            FROM new_nodes_dumped AS c
            LEFT JOIN road_graph.nodes AS n
                -- check existing nodes in less than 1 m from the NEW edge
                ON ST_DWithin(n.geom, NEW.geom, 1)
                -- only nodes not already very close to the intersected node
                AND ST_DWithin(n.geom, c.geom, 0.50)
            -- this will keep only crossing nodes to use
            WHERE n.id IS NULL
        LOOP
            IF crossing_node.geom IS NOT NULL
                AND ST_GeometryType(crossing_node.geom) = 'ST_Point'
            THEN
                -- Create the missing node, which will trigger the split of the edge
                -- (see the trigger after_node_insert_or_update)
                -- Set variable to avoid infinite loop & self-crossing detection
                SET road.graph.edge.crossing.node.creation.pending = 'yes';
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (crossing_node.geom)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT id
                FROM new_node
                INTO new_node_id
                ;
                -- revert variable back to no
                SET road.graph.edge.crossing.node.creation.pending = 'no';

                IF new_node_id IS NOT NULL THEN
                    created_nodes_at_intersection = created_nodes_at_intersection || new_node_id;
                    IF raise_notice IN ('info', 'debug') THEN
                        RAISE NOTICE '% AFTER edge % n° %, crossing node created : % %',
                            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, new_node_id,
                            (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = new_node_id)
                        ;
                    END IF;
                END IF;
            END IF;
        END LOOP;

        -- Also split this NEW edge if it touches other nodes
        -- (but not by its start or end point)
        -- We do it by updating the already existing nodes
        -- this will run the trigger after_node_update which will split the corresponding edge
        -- WARNING : this should not be done if this node concerns only two edges already been merging
        node_to_be_deleted = road_graph.get_current_setting('road.graph.merge.edges.useless.node', '-1', 'integer');

        -- RAISE NOTICE '% AFTER edge % n° %, list touching nodes, useless node = %',
        ---    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, node_to_be_deleted
        -- ;
        FOR touching_node IN
            SELECT DISTINCT
                n.*
            FROM road_graph.nodes AS n
            WHERE True
            -- "intersecting" the NEW edge
            AND ST_DWithin(n.geom, NEW.geom, 0.50)
            AND n.id NOT IN (NEW.start_node, NEW.end_node)
            -- but not touching its start or end point
            AND NOT ST_DWithin(n.geom, ST_StartPoint(NEW.geom), 0.50)
            AND NOT ST_DWithin(n.geom, ST_EndPoint(NEW.geom), 0.50)
            -- node not created above from edges intersections
            -- avoid creation of useless nodes
            AND NOT (n.id = ANY(Coalesce(created_nodes_at_intersection, array[]::integer[])))
            -- node not concerned by two edges already been merging
            -- to avoid infinite loop
            -- The variable road.graph.merge.edges.useless.node is set in the function road_graph.merge_edges
            AND n.id != node_to_be_deleted
        LOOP
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % n° % start % end %, update existing node under new edge to launch the split : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.start_node, NEW.end_node,
                    touching_node.id, ST_AsText(touching_node.geom)
                ;
            END IF;

            IF road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no', 'text') != 'yes'
            THEN
                SET road.graph.edge.update.touching.node = 'yes';
                -- We do not create a new node (constraint on same geometry)
                -- but juste move a little bit the existing node to run
                -- the trigger after_node_insert_or_update which will run the split function
                WITH up AS (
                    UPDATE road_graph.nodes AS n
                    SET geom = ST_Translate(geom, 0.01, 0.01)
                    WHERE id = touching_node.id
                    RETURNING id
                )
                SELECT id INTO new_node_id
                FROM up
                ;
                SET road.graph.edge.update.touching.node = 'no';
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, NEW node created in same node place: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id, new_node_id
                    ;
                END IF;
            END IF;
        END LOOP;
    END IF;

    -- Calculate references of the edge whole road
    -- Beware to choose correct conditions
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and marker)
    IF (TG_OP = 'INSERT' AND NOT is_roundabout) OR (
        TG_OP = 'UPDATE' AND (
            NOT ST_Equals(OLD.geom, NEW.geom)
            OR OLD.previous_edge_id != NEW.previous_edge_id
            OR OLD.next_edge_id != NEW.next_edge_id
        ) AND NOT is_roundabout
    )
    THEN
        IF road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
        THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % N° %, update all road edges references: %',
                    REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                    update_edge_references_result::text
                ;
            END IF;
            SELECT INTO update_edge_references_result
                road_graph.update_edge_references(NEW.road_code, NULL)
            ;
        END IF;
    END IF;

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - End',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    RETURN NEW;
END;
$$;


-- FUNCTION after_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.after_edge_insert_or_update() IS 'Multiples opérations lancées suite à la modification d''un troncon.
Déplacement du noeud initial et terminal liés si besoin.
Suppression des noeuds orphelins si besoin.
Création des noeuds non existants à l''intersection avec les autres edges';



-- after_marker_insert_or_update_or_delete()
DROP FUNCTION IF EXISTS road_graph.after_marker_insert_or_update_or_delete();
CREATE OR REPLACE FUNCTION road_graph.after_marker_insert_or_update_or_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    is_roundabout boolean;
    edge_road record;
    initial_roundabout_node integer;
    update_edge_neighbours boolean;
    update_edge_references_result boolean;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    -- Get log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this marker does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    -- Check if only a marker 0 is used
    IF is_roundabout AND NEW.code != 0 AND TG_OP != 'DELETE' THEN
        RAISE EXCEPTION 'The value of the roundabout marker code must be 0 !';
    END IF;

    -- Update : Do nothing if geometry has not changed
    IF TG_OP = 'UPDATE' AND ST_Equals(NEW.geom, OLD.geom) THEN
        RETURN NEW;
    END IF;

    -- For roundabout, calculate edges previous and next ids
    -- anytime geometry is modified
    IF is_roundabout
    THEN
        SELECT INTO initial_roundabout_node
            n.id
        FROM road_graph.nodes AS n
        WHERE ST_DWithin(n.geom, NEW.geom, 0.5)
        AND n.id IN (
            SELECT start_node FROM road_graph.edges WHERE road_code = edge_road.road_code
            UNION ALL
            SELECT end_node FROM road_graph.edges WHERE road_code = edge_road.road_code
        )
        ORDER BY n.id
        LIMIT 1;

        IF initial_roundabout_node IS NOT NULL
        THEN
            SELECT INTO update_edge_neighbours
                road_graph.update_road_edges_neighbours(
                    NEW.road_code,
                    initial_roundabout_node
                )
            ;
        ELSE
        -- force inserted or updated roundabout marker
        -- to be on top of a roundabout edge node
            RAISE EXCEPTION 'The marker must be positionned at the start node of an existing roundabout edge !';
        END IF;
    END IF;

    -- INSERT OR UPDATE - Update road references
    IF TG_OP != 'DELETE'
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                NEW.road_code,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(NEW.road_code, NULL)
        ;
    END IF;

    -- Also update old road if marker road has changed
    IF (
        (TG_OP = 'UPDATE' AND NEW.road_code != OLD.road_code)
            OR (TG_OP = 'DELETE')
        )
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                OLD.road_code,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL)
        ;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        RETURN NEW;
    ELSE
        RETURN OLD;
    END IF;
END;
$$;


-- FUNCTION after_marker_insert_or_update_or_delete()
COMMENT ON FUNCTION road_graph.after_marker_insert_or_update_or_delete() IS 'Update road edges references after a marker has been created, updated or deleted.';



-- after_node_insert_or_update()
DROP FUNCTION IF EXISTS road_graph.after_node_insert_or_update();
CREATE OR REPLACE FUNCTION road_graph.after_node_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_under_node record;
    id_new_edge integer;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Do nothing if geometry has not changed
    -- except road.graph.edge.update.touching.node equals yes
    IF
        TG_OP = 'UPDATE'
        AND ST_Equals(NEW.geom, OLD.geom)
        AND road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no', 'text') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER node % n° %, OLD & NEW geometries are equal, return',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;
        RETURN NEW;
    END IF;

    -- Update all upstream edges referenced ids
    UPDATE road_graph.edges AS t
    SET start_node = NEW.id
    WHERE ST_DWithin(NEW.geom, ST_StartPoint(t.geom), 0.50)
    AND start_node != NEW.id
    ;

    -- Update all downstream edges referenced ids
    UPDATE road_graph.edges AS t
    SET end_node = NEW.id
    WHERE ST_DWithin(NEW.geom, ST_EndPoint(t.geom), 0.50)
    AND end_node != NEW.id
    ;

    -- Update geometry of all upstream edges linked to the node
    -- We replace the first edge node by the node NEW geometry
    -- We deactivate the creation of crossing nodes for these updated edges
    SET road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, 0, NEW.geom)
    WHERE TRUE
    AND start_node = NEW.id
    AND NOT ST_Intersects(ST_StartPoint(geom), NEW.geom)
    ;
    SET road.graph.edge.crossing.node.creation.pending = 'no';

    -- Update geometry of all downstream edges linked to the node
    -- We replace the last edge node but the node NEW geometry
    -- We deactivate the creation of crossing nodes for these updated edges
    SET road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, ST_NPoints(geom) - 1, NEW.geom)
    WHERE True
    AND end_node = NEW.id
    AND NOT ST_Intersects(ST_EndPoint(geom), NEW.geom)
    ;
    SET road.graph.edge.crossing.node.creation.pending = 'no';

    -- Split edges under the node if needed
    FOR edge_under_node IN
        SELECT
            -- we must pass all the fields
            -- to be able to copy/paste attributes data from the original edge
            -- when spliting it
            t.*
        FROM road_graph.edges AS t
        WHERE True
        -- edge is close to the node
        AND ST_DWithin(t.geom, NEW.geom, 0.50)
        -- but node is far enough from the start and end points
        AND NOT ST_DWithin(ST_StartPoint(t.geom), NEW.geom, 0.50)
        AND NOT ST_DWithin(ST_EndPoint(t.geom), NEW.geom, 0.50)
        ORDER BY t.geom <-> NEW.geom
    LOOP
        IF edge_under_node.id IS NOT NULL THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER NODE % n° % %, found nearby edge % -> should split it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, ST_AsText(NEW.geom), edge_under_node.id
                ;
            END IF;
            IF raise_notice IN ('debug') THEN
                RAISE NOTICE 'EDGE TO BE SPLIT';
                RAISE NOTICE '%', to_json(edge_under_node);
            END IF;

            SELECT INTO id_new_edge
                split_edge_by_node
            FROM road_graph.split_edge_by_node(
                -- edge
                edge_under_node,
                -- node
                NEW,
                -- minimum distance from line
                0.50::real,
                -- maximum distance from start & end points
                0.50::real
            );

            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER NODE % n° % %, New edge created from split: %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, ST_AsText(NEW.geom), id_new_edge
                ;
            END IF;
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$;


-- FUNCTION after_node_insert_or_update()
COMMENT ON FUNCTION road_graph.after_node_insert_or_update() IS '1. Lors d''une création de Node (automatique via une action sur les tronçons) ou de la modification de la position d''un Node
(donc lorsque la géométrie change), on met à jour la géométrie des tronçons dont un des sommets (amont ou aval) est rattaché à ce Node,
et on renseigne les champs start_node et end_node (id des Nodes amont et aval) des tronçons concernés.

2. Lors de la création ou d''une modification de la géométrie d''un tronçon,
si un des sommets (amont ou aval) du tronçon accroche le segment d''un tronçon existant :
Ce tronçon existant est coupé en deux (la géométrie du tronçon A est tronquée, un nouveau tronçon B est créé, on récupère les attributs du tronçon A dans le tronçon B).
Rq :
- La fonction split_edge_by_node a été créée et est appelée à cet effet
- La création du Node à cette nouvelle intersection est gérée séparément dans un trigger sur les tronçons
';


-- before_edge_insert_or_update()
DROP FUNCTION IF EXISTS road_graph.before_edge_insert_or_update();
CREATE OR REPLACE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_point geometry(point);
    end_point geometry(point);
    upstream_node record;
    downstream_node record;
    distance real;
    start_references jsonb;
    end_references jsonb;
    marker_zero record;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Get road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;

    -- check if edge is a part of a roundabout
    is_roundabout = (edge_road.road_type = 'roundabout' AND ST_IsRing(NEW.geom));
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% BEFORE edge % n° %, edge is_roundabout %,
        %',
            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, is_roundabout,
            to_json(edge_road)
        ;
    END IF;

    -- If it is a new roundabout, reverse the geometry if needed
    -- QGIS digitizing circle tool create counter-clockwise polygons
    IF is_roundabout AND ST_IsPolygonCW(ST_MakePolygon(NEW.geom))
    THEN
        NEW.geom = ST_Reverse(NEW.geom);
    END IF;

    -- Do nothing if geometry has not changed
    IF TG_OP = 'UPDATE' AND ST_Equals(NEW.geom, OLD.geom) THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, NEW and OLD geom are equal',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;

        RETURN NEW;
    END IF;

    -- INSERT - create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

    -- Get first nodes < 0.5 m - If found, edit NEW geom
    -- upstream
    SELECT INTO upstream_node
        n.id, n.geom
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, start_point, 0.50)
    ORDER BY n.id, n.geom <-> start_point
    LIMIT 1
    ;

    -- upstream - create node or just update value
    IF upstream_node IS NOT NULL THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, upstream_node NOT NULL : % -> use it',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, upstream_node.id
            ;
        END IF;

        -- Update the geometry
        NEW.geom = ST_SetPoint(NEW.geom, 0, upstream_node.geom);
        -- Update the node ID in upstream attribute
        NEW.start_node = upstream_node.id;
    ELSE
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, upstream node IS NULL -> create it',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;

        -- Create the missing node
        -- only for INSERT
        -- or for UPDATE if the start_node value has been deleted
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.start_node IS NULL) THEN
            WITH new_node AS (
                INSERT INTO road_graph.nodes
                (geom)
                VALUES
                (start_point)
                ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                RETURNING id
            )
            SELECT new_node.id
            FROM new_node
            INTO NEW.start_node
            ;
            IF NEW.start_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.start_node,
                    (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.start_node)
                ;
            END IF;
        END IF;
    END IF;

    -- get downstream node
    SELECT INTO downstream_node
        n.id, n.geom
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, end_point, 0.50)
    ORDER BY n.id, n.geom <-> end_point
    LIMIT 1
    ;
    -- downstream node - create or update value
    IF downstream_node IS NOT NULL THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, downstream node NOT NULL : % -> use it',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, downstream_node.id
            ;
        END IF;
        -- Update the geometry
        NEW.geom = ST_SetPoint(NEW.geom, ST_NPoints(NEW.geom) - 1, downstream_node.geom);
        -- Update the node ID in downstream attribute
        NEW.end_node = downstream_node.id;
    ELSE
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, downstream node IS NULL -> create it',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;

        -- Create the missing node
        -- only for INSERT
        -- or for UPDATE if the end_node value has been deleted
        IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.end_node IS NULL) THEN
            -- Create the missing node
            WITH new_node AS (
                INSERT INTO road_graph.nodes
                (geom)
                VALUES
                (end_point)
                ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                RETURNING id
            )
            SELECT new_node.id
            FROM new_node
            INTO NEW.end_node
            ;
            IF NEW.end_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.end_node,
                    (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.end_node)
                ;
            END IF;
        END IF;
    END IF;

    -- Marker - Create marker 0 if needed
    -- it does not exists for the road
    -- only for roads, not for roundabout
    IF TG_OP = 'INSERT'
        AND NOT is_roundabout
        AND edge_road.road_type NOT IN ('roundabout')
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_EndPoint(NEW.geom))
        AND NEW.previous_edge_id IS NULL
        AND NEW.road_code IS NOT NULL
        AND NOT EXISTS (
            SELECT m.id
            FROM road_graph.markers AS m
            WHERE m.road_code = NEW.road_code
            AND m.code = 0
        )
    THEN
        INSERT INTO road_graph.markers (road_code, code, abscissa, geom)
        VALUES (NEW.road_code, 0, 0, start_point)
        ON CONFLICT DO NOTHING
        ;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND OLD.start_abscissa = 0 AND NEW.start_abscissa = 0
        AND OLD.start_cumulative = 0 AND NEW.start_cumulative = 0
        AND OLD.start_marker = 0 AND NEW.start_marker = 0
        AND OLD.road_code = NEW.road_code
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_StartPoint(OLD.geom))
    THEN
        -- Do not run the function update_edge_references
        -- on the road triggered inside the after_marker_insert_or_update_or_delete
        -- trigger since it will already been triggered with the edge geometry change
        SET road.graph.edge.ref.calc.disabled = 'yes';
        UPDATE road_graph.markers AS m
        SET geom = ST_StartPoint(NEW.geom)
        WHERE m.road_code = NEW.road_code
        AND m.code = 0
        AND ST_Equals(m.geom, ST_StartPoint(OLD.geom))
        ;
        SET road.graph.edge.ref.calc.disabled = 'no';
    END IF;

    -- Calculate references
    IF NOT is_roundabout
    THEN
        BEGIN
            -- start point
            SELECT INTO start_references
                road_graph.get_reference_from_point(start_point, NEW.road_code) AS ref
            ;
            NEW.start_marker = (start_references->>'marker_code')::text::integer;
            NEW.start_abscissa = (start_references->>'abscissa')::text::real;
            NEW.start_cumulative = (start_references->>'cumulative')::text::real;

            -- end point
            SELECT INTO end_references
                road_graph.get_reference_from_point(end_point, NEW.road_code) AS ref
            ;
            NEW.end_marker = (end_references->>'marker_code')::text::integer;
            NEW.end_abscissa = (end_references->>'abscissa')::text::real;
            NEW.end_cumulative = (end_references->>'cumulative')::text::real;

        EXCEPTION WHEN OTHERS THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, NEW references NOT calculated',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
                ;
            END IF;
        END;
    END IF;

    RETURN NEW;
END;
$$;


-- FUNCTION before_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.before_edge_insert_or_update()
IS 'During the creation or modification of an edge, we verify that the upstream and downstream nodes exist
within 50 cm of the start and end of the edge. If they do, we use them
and update the edge geometry so that the start and end are exactly on these nodes.
Otherwise, we create the missing nodes.
Additionally, during the creation of an edge, if the road code is provided and no marker 0 exists for this road,
we automatically create one at the start of the edge.
During the modification of an edge, if the starting point of the edge is modified and the marker 0 of the road is positioned at this starting point,
we move marker 0 to this new starting point.
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';


-- Add triggers back
CREATE TRIGGER trg_aa_before_geometry_insert_or_update
BEFORE INSERT OR UPDATE OF geom ON road_graph.edges
FOR EACH ROW EXECUTE FUNCTION road_graph.aa_before_geometry_insert_or_update();

CREATE TRIGGER trg_aa_before_geometry_insert_or_update
BEFORE INSERT OR UPDATE OF geom ON road_graph.editing_sessions
FOR EACH ROW EXECUTE FUNCTION road_graph.aa_before_geometry_insert_or_update();

CREATE TRIGGER trg_aa_before_geometry_insert_or_update
BEFORE INSERT OR UPDATE OF geom ON road_graph.markers
FOR EACH ROW EXECUTE FUNCTION road_graph.aa_before_geometry_insert_or_update();

CREATE TRIGGER trg_aa_before_geometry_insert_or_update
BEFORE INSERT OR UPDATE OF geom ON road_graph.nodes
FOR EACH ROW EXECUTE FUNCTION road_graph.aa_before_geometry_insert_or_update();

CREATE TRIGGER trg_after_edge_delete
AFTER DELETE ON road_graph.edges
FOR EACH ROW EXECUTE FUNCTION road_graph.after_edge_delete();

CREATE TRIGGER trg_after_edge_insert_or_update
AFTER INSERT OR UPDATE ON road_graph.edges
FOR EACH ROW EXECUTE FUNCTION road_graph.after_edge_insert_or_update();

CREATE TRIGGER trg_after_marker_insert_or_update_or_delete
AFTER INSERT OR DELETE OR UPDATE ON road_graph.markers
FOR EACH ROW EXECUTE FUNCTION road_graph.after_marker_insert_or_update_or_delete();

CREATE TRIGGER trg_after_node_insert_or_update
AFTER INSERT OR UPDATE ON road_graph.nodes
FOR EACH ROW EXECUTE FUNCTION road_graph.after_node_insert_or_update();

CREATE TRIGGER trg_before_edge_insert_or_update
BEFORE INSERT OR UPDATE ON road_graph.edges
FOR EACH ROW EXECUTE FUNCTION road_graph.before_edge_insert_or_update();
