ALTER TABLE road_graph.markers  ADD COLUMN IF NOT EXISTS id_source text;
COMMENT ON COLUMN road_graph.markers.id_source
IS 'Original ID of the source data (used after an import, for example to check references between original data and the imported data in road_graph';
ALTER TABLE road_graph.edges  ADD COLUMN IF NOT EXISTS id_source text;
COMMENT ON COLUMN road_graph.edges.id_source
IS 'Original ID of the source data (used after an import, for example to check references between original data and the imported data in road_graph';


CREATE OR REPLACE FUNCTION road_graph.import_data_from_template_tables(
	_source_schema text,
	_source_edges_table text,
	_source_markers_table text)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    sql_text text;
    roads_count integer;
    edges_count integer;
    nodes_count integer;
    markers_zero_count integer;
    markers_count integer;
    _set_config text;
    _toggle_triggers boolean;
    _set_val_result integer;
    wrong_road_type record;
    wrong_road_class record;
BEGIN
    -- Default values
    roads_count = 0;
    edges_count = 0;
    nodes_count = 0;
    markers_zero_count = 0;
    markers_count = 0;

    -- Reset edges sequence
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.edges', 'id'),
        (SELECT max(id) FROM road_graph.edges)
    );

    -- Build temporary tables from source tables
    -- Temporary edges
    sql_text = format(
        $SQL$
        CREATE TEMPORARY TABLE temp_edges ON COMMIT DROP AS (
            WITH source AS (
                SELECT
                    -- Stores the original data
                    -- We must use the sequences to keep a link between previous and next edges
                    nextval(pg_get_serial_sequence('road_graph.edges', 'id')) AS id,
                    e.id_source AS id_source,
                    e."edge_order",
                    trim(e."road_code") AS road_code,
                    trim(e."road_class") AS road_class,
                    trim(e."road_type") AS road_type,
                    trim(e."road_class_code") AS road_class_code,
                    e."is_active",
                    trim(e."road_topic") AS road_topic,
                    e."is_access_road",
                    -- Move the geometries nodes to a grid
                    ST_ReducePrecision(ST_geometryN(e.geom, 1), 0.10) AS geom
                FROM %I.%I AS e
            )
            SELECT
                s.*,
                -- get the start point
                ST_StartPoint(s.geom) AS start_point,
                -- get the end point
                ST_EndPoint(s.geom) AS end_point,
                -- previous and next edges
                lag(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS previous_edge_id,
                lead(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS next_edge_id
            FROM source AS s
            WHERE TRUE
            ORDER BY road_code, edge_order
        )
        ;
        $SQL$,
        _source_schema,
        _source_edges_table
    );
    -- RAISE NOTICE 'sql_text = %', sql_text;
    EXECUTE sql_text;

    CREATE INDEX ON temp_edges (road_code);
    CREATE INDEX ON temp_edges USING GIST (start_point);
    CREATE INDEX ON temp_edges USING GIST (end_point);

    -- Check unique values of some fields
    -- road_type
    SELECT INTO wrong_road_type
        count(*) AS nb,
        string_agg(DISTINCT e.road_type, ', ' ORDER BY e.road_type) AS road_types
    FROM temp_edges AS e
    WHERE e.road_type NOT IN ('road', 'roundabout')
    ;
    IF wrong_road_type.nb > 0 THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'Some values of the field "road_type" are not road or roundabout',
            'data', json_build_object(
                'number', wrong_road_type.nb,
                'details', wrong_road_type.road_types
            ),
            'roads_count', roads_count,
            'edges_count', edges_count,
            'nodes_count', nodes_count,
            'markers_count', markers_zero_count + markers_count
        );
    END IF;

    -- road_class
    SELECT INTO wrong_road_class
        count(*) AS nb,
        string_agg(DISTINCT e.road_class, ', ' ORDER BY e.road_class) AS road_classes
    FROM temp_edges AS e
    WHERE e.road_class NOT IN (
        SELECT "code" FROM road_graph.glossary_road_class
    )
    ;
    IF wrong_road_class.nb > 0 THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'Some values of the field "road_class" are not present in the table "glossary_road_class"',
            'data', json_build_object(
                'number', wrong_road_class.nb,
                'details', wrong_road_class.road_classes
            ),
            'roads_count', roads_count,
            'edges_count', edges_count,
            'nodes_count', nodes_count,
            'markers_count', markers_zero_count + markers_count
        );
    END IF;

    -- Temporary nodes
    CREATE TEMPORARY TABLE temp_nodes ON COMMIT DROP AS (
        WITH
        union_start_end AS (
            SELECT id AS start_of, NULL AS end_of, start_point AS geom
            FROM temp_edges
            UNION ALL
            SELECT NULL AS start_of, id AS end_of, end_point AS geom
            FROM temp_edges
        ),
        distinct_nodes AS (
            SELECT
                json_agg(DISTINCT start_of) FILTER (WHERE start_of IS NOT NULL) AS start_of,
                json_agg(DISTINCT end_of) FILTER (WHERE end_of IS NOT NULL) AS end_of,
                geom
            FROM union_start_end
            GROUP BY geom
        )
        SELECT *
        FROM distinct_nodes
    )
    ;
    CREATE INDEX ON temp_nodes USING GIST (geom);

    -- Disable topology triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(FALSE)
        INTO _toggle_triggers;

    -- Insert nodes
    RAISE NOTICE 'insert nodes';
    INSERT INTO road_graph.nodes AS n
    (created_at, updated_at, geom)
    SELECT
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone,
        geom
    FROM temp_nodes
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS nodes_count = ROW_COUNT;

    -- Insert roads
    RAISE NOTICE 'insert roads';
    INSERT INTO road_graph.roads (
        road_code, road_type, road_class,
        road_class_code, is_active, road_topic,
        is_access_road,
        created_at, updated_at
    )
    SELECT DISTINCT
        road_code,
        road_type,
        road_class,
        road_class_code,
        is_active,
        road_topic,
        is_access_road,
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone
    FROM temp_edges
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS roads_count = ROW_COUNT;

    -- Insert edges
    RAISE NOTICE 'insert edges';
    INSERT INTO road_graph.edges (
        id, id_source,
        road_code, start_node, end_node,
        previous_edge_id, next_edge_id,
        geom,
        created_at, updated_at
    )
    SELECT
        e.id, e.id_source, e.road_code,
        ns.id, ne.id,
        e.previous_edge_id, e.next_edge_id,
        e.geom,
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone
    FROM temp_edges AS e
    LEFT JOIN road_graph.nodes AS ns
        -- = is faster than ST_Equals
        ON ns.geom = e.start_point
    LEFT JOIN road_graph.nodes AS ne
        ON ne.geom = e.end_point
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS edges_count = ROW_COUNT;

    DROP TABLE IF EXISTS temp_nodes;
    DROP TABLE IF EXISTS temp_edges;

    -- Insert markers from source data
    RAISE NOTICE 'insert markers from source';
    sql_text = format(
        $SQL$
        INSERT INTO road_graph.markers (
            id_source,
            road_code, code, abscissa, is_virtual, geom,
            created_at, updated_at
        )
        SELECT
            id_source,
            road_code,
            code,
            abscissa,
            is_virtual,
            ST_ReducePrecision(geom, 0.10) AS geom,
            now()::timestamp(0) without time zone,
            now()::timestamp(0) without time zone
        FROM %I.%I
        -- Only import markers if their road_code is referenced in road_graph.roads
        -- WHERE road_code IN (
        --     SELECT road_code FROM road_graph.roads
        -- )
        ON CONFLICT DO NOTHING
        ;
        $SQL$,
        _source_schema,
        _source_markers_table
    );
    EXECUTE sql_text;
    GET DIAGNOSTICS markers_count = ROW_COUNT;

    -- Insert markers automatic markers at the start of the first edge
    -- Only if there is no other marker nearby (for example with a given abscissa)
    RAISE NOTICE 'insert markers zero';
    WITH zero AS (
        SELECT
            e.road_code,
            0,
            0,
            True,
            ST_StartPoint(e.geom),
            now()::timestamp(0) without time zone,
            now()::timestamp(0) without time zone
        FROM road_graph.edges AS e
        LEFT JOIN road_graph.markers AS m
            ON e.road_code = m.road_code
            AND ST_DWithin(ST_StartPoint(e.geom), m.geom, 1)
        WHERE TRUE
        -- No previous edge : this means it is the start of the road
        AND e.previous_edge_id IS NULL
        -- No corresponding marker within the given distance
        AND m.id IS NULL
    )
    INSERT INTO road_graph.markers (
        road_code, code, abscissa, is_virtual, geom,
        created_at, updated_at
    )
    SELECT *
    FROM zero
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS markers_zero_count = ROW_COUNT;

    -- Set the serial values
    RAISE NOTICE 'set serials';
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.edges', 'id'),
        (SELECT max(id) FROM road_graph.edges)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.nodes', 'id'),
        (SELECT max(id) FROM road_graph.nodes)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.roads', 'id'),
        (SELECT max(id) FROM road_graph.roads)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.markers', 'id'),
        (SELECT max(id) FROM road_graph.markers)
    );

    -- Update edges references
    RAISE NOTICE 'update references';
    WITH s AS (
        SELECT e.road_code, array_agg(DISTINCT e.id) AS ids
        FROM road_graph.edges AS e
        WHERE start_marker IS NULL OR end_marker IS NULL
        GROUP BY road_code
    ),
    up AS (
        SELECT
            road_code,
            road_graph.update_edge_references(s.road_code, s.ids) AS travail
        FROM s
        ORDER BY road_code
    )
    SELECT count(*)
    FROM up
    INTO _set_val_result
    ;

    -- Re-enable triggers
    RAISE NOTICE 'triggers and config';
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    RETURN json_build_object(
        'status', 'success',
        'message', 'ok',
        'data', null::json,
        'roads_count', roads_count,
        'edges_count', edges_count,
        'nodes_count', nodes_count,
        'markers_count', markers_zero_count + markers_count
    );
END;
$BODY$;

COMMENT ON FUNCTION road_graph.import_data_from_template_tables(text, text, text)
    IS 'Import data from the given schema and edges & markers source tables';



-- get_road_previous_marker_from_point(text, geometry, boolean)
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
            _previous_marker.road_linestring_from_marker_to_point := road_linestring_from_start_to_point;
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
