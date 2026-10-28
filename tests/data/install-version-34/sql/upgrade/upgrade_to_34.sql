-- Map matching of GPS tracks on the road graph
--
-- * get_snapped_line_from_track: build a line snapped on the road graph edges
--   from a (possibly imprecise) GPS track, for example imported from a GPX file
-- * get_road_sections_from_snapped_line: return the list of the roads crossed by a snapped line,
--   with the start and end references (marker & abscissa) for each road
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION.

-- FUNCTION: road_graph.get_snapped_line_from_track
CREATE OR REPLACE FUNCTION road_graph.get_snapped_line_from_track(
    _track geometry,
    _densify_distance real DEFAULT 2,
    _max_distance real DEFAULT 25,
    _road_change_penalty real DEFAULT 100
) RETURNS geometry
    LANGUAGE plpgsql
    AS $$
DECLARE
    track geometry;
    points geometry[];
    point_record record;
    raise_notice text;
    -- Choice of the road for each point
    previous_roads text[];
    previous_costs double precision[];
    current_costs double precision[];
    candidate_cost double precision;
    best_cost double precision;
    best_previous_road text;
    current_road text;
    point_ids integer[] := '{}';
    point_offsets integer[] := '{}';
    flat_roads text[] := '{}';
    flat_previous_roads text[] := '{}';
    chosen_roads text[];
    block_end integer;
    -- Runs of consecutive points snapped on the same edge
    s integer := 0;
    run_edges integer[] := '{}';
    run_start_nodes integer[] := '{}';
    run_end_nodes integer[] := '{}';
    run_lengths double precision[] := '{}';
    run_first_m double precision[] := '{}';
    run_last_m double precision[] := '{}';
    run_min_m double precision[] := '{}';
    run_max_m double precision[] := '{}';
    node_in integer;
    node_out integer;
    node_m double precision;
    -- Final list of edges, with the edges added to fill the gaps
    f integer := 0;
    final_edges integer[] := '{}';
    final_start_nodes integer[] := '{}';
    final_end_nodes integer[] := '{}';
    final_first_m double precision[] := '{}';
    final_last_m double precision[] := '{}';
    bridge record;
    bridge_edge record;
    -- Geometry building
    edge_geom geometry;
    entry_m double precision;
    exit_m double precision;
    pieces geometry[] := '{}';
    parts geometry[] := '{}';
    i integer;
    j integer;
    k integer;
    p integer;
BEGIN
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    IF _track IS NULL OR ST_IsEmpty(_track) THEN
        RETURN NULL;
    END IF;

    -- Use the road graph projection
    -- A geometry without SRID is considered to be in the road graph projection
    track = ST_Force2D(_track);
    IF ST_SRID(track) = 0 THEN
        track = ST_SetSRID(track, 2154);
    ELSIF ST_SRID(track) != 2154 THEN
        track = ST_Transform(track, 2154);
    END IF;

    -- Build a single linestring from the track (a GPX track can contain multiple segments)
    track = ST_LineMerge(track);
    IF GeometryType(track) != 'LINESTRING' THEN
        track = ST_MakeLine(ARRAY(
            SELECT d.geom
            FROM ST_Dump(track) AS d
            ORDER BY d.path
        ));
    END IF;

    -- Densify the track and get its points
    points = ARRAY(
        SELECT d.geom
        FROM ST_DumpPoints(ST_Segmentize(track, _densify_distance)) AS d
        ORDER BY d.path
    );
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE 'get_snapped_line_from_track - % points', array_length(points, 1);
    END IF;

    -- 1/ Choose the road for each point
    -- We do not simply take the closest road, which would lead to zigzags
    -- between two roads when the track is not precise.
    -- We search the sequence of roads which minimizes the sum of:
    -- * the distances between the points and their roads,
    --   multiplied by the densify distance (~ the surface between the track and the roads)
    -- * a penalty each time the road changes between two consecutive points (Viterbi algorithm)
    -- Points further than _max_distance from any road are ignored
    FOR point_record IN
        SELECT
            c.idx,
            array_agg(c.road_code ORDER BY c.road_code) AS roads,
            array_agg(c.distance ORDER BY c.road_code) AS distances
        FROM (
            SELECT pt.idx, e.road_code, min(ST_Distance(e.geom, points[pt.idx])) AS distance
            FROM generate_subscripts(points, 1) AS pt(idx)
            INNER JOIN road_graph.edges AS e
                ON ST_DWithin(e.geom, points[pt.idx], _max_distance)
            GROUP BY pt.idx, e.road_code
        ) AS c
        GROUP BY c.idx
        ORDER BY c.idx
    LOOP
        current_costs = '{}';
        FOR j IN 1..array_length(point_record.roads, 1) LOOP
            best_cost = NULL;
            best_previous_road = NULL;
            FOR k IN 1..coalesce(array_length(previous_roads, 1), 0) LOOP
                candidate_cost = previous_costs[k]
                    + CASE WHEN previous_roads[k] = point_record.roads[j] THEN 0 ELSE _road_change_penalty END
                ;
                -- In case of equality, keep the same road
                IF best_cost IS NULL OR candidate_cost < best_cost
                    OR (candidate_cost = best_cost AND previous_roads[k] = point_record.roads[j])
                THEN
                    best_cost = candidate_cost;
                    best_previous_road = previous_roads[k];
                END IF;
            END LOOP;
            current_costs = current_costs || (coalesce(best_cost, 0) + point_record.distances[j] * _densify_distance);
            flat_roads = flat_roads || point_record.roads[j];
            flat_previous_roads = flat_previous_roads || best_previous_road;
        END LOOP;
        point_ids = point_ids || point_record.idx;
        point_offsets = point_offsets || (array_length(flat_roads, 1) - array_length(point_record.roads, 1) + 1);
        previous_roads = point_record.roads;
        previous_costs = current_costs;
    END LOOP;

    IF array_length(point_ids, 1) IS NULL THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE 'get_snapped_line_from_track - no road found closer than % m', _max_distance;
        END IF;
        RETURN NULL;
    END IF;

    -- Get back the best sequence of roads, from the last point to the first one
    best_cost = NULL;
    FOR k IN 1..array_length(previous_roads, 1) LOOP
        IF best_cost IS NULL OR previous_costs[k] < best_cost THEN
            best_cost = previous_costs[k];
            current_road = previous_roads[k];
        END IF;
    END LOOP;
    chosen_roads = array_fill(NULL::text, ARRAY[array_length(point_ids, 1)]);
    FOR i IN REVERSE array_length(point_ids, 1)..1 LOOP
        chosen_roads[i] = current_road;
        block_end = CASE
            WHEN i < array_length(point_ids, 1) THEN point_offsets[i + 1] - 1
            ELSE array_length(flat_roads, 1)
        END;
        FOR p IN point_offsets[i]..block_end LOOP
            IF flat_roads[p] = current_road THEN
                current_road = flat_previous_roads[p];
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    -- 2/ Snap each point on the closest edge of its road
    -- and group consecutive points snapped on the same edge.
    -- When the road changes, the points located just before or after the intersection
    -- can be snapped beyond the intersection node, which creates a spur:
    -- an edge entered and left by the same node.
    -- We remove these spurs if they are shorter than _max_distance.
    FOR point_record IN
        SELECT
            c.ord,
            e.id, e.start_node, e.end_node,
            ST_Length(e.geom) AS edge_length,
            ST_LineLocatePoint(e.geom, points[c.idx]) AS measure
        FROM unnest(point_ids, chosen_roads) WITH ORDINALITY AS c(idx, road_code, ord)
        CROSS JOIN LATERAL (
            SELECT ee.id, ee.start_node, ee.end_node, ee.geom
            FROM road_graph.edges AS ee
            WHERE ee.road_code = c.road_code
            AND ST_DWithin(ee.geom, points[c.idx], _max_distance)
            ORDER BY ST_Distance(ee.geom, points[c.idx]), ee.id
            LIMIT 1
        ) AS e
        ORDER BY c.ord
    LOOP
        LOOP
            -- Same edge as the previous run: merge
            IF s > 0 AND run_edges[s] = point_record.id THEN
                run_last_m[s] = point_record.measure;
                run_min_m[s] = least(run_min_m[s], point_record.measure);
                run_max_m[s] = greatest(run_max_m[s], point_record.measure);
                EXIT;
            END IF;

            -- Remove the previous run if it is a spur
            IF s > 1 AND run_start_nodes[s] != run_end_nodes[s] THEN
                node_in = CASE
                    WHEN run_end_nodes[s - 1] IN (run_start_nodes[s], run_end_nodes[s]) THEN run_end_nodes[s - 1]
                    WHEN run_start_nodes[s - 1] IN (run_start_nodes[s], run_end_nodes[s]) THEN run_start_nodes[s - 1]
                END;
                node_out = CASE
                    WHEN point_record.start_node IN (run_start_nodes[s], run_end_nodes[s]) THEN point_record.start_node
                    WHEN point_record.end_node IN (run_start_nodes[s], run_end_nodes[s]) THEN point_record.end_node
                END;
                IF node_in = node_out THEN
                    node_m = CASE WHEN node_in = run_start_nodes[s] THEN 0 ELSE 1 END;
                    IF greatest(abs(run_min_m[s] - node_m), abs(run_max_m[s] - node_m)) * run_lengths[s] <= _max_distance THEN
                        s = s - 1;
                        -- Test again with the new previous run
                        CONTINUE;
                    END IF;
                END IF;
            END IF;

            -- New run
            s = s + 1;
            run_edges[s] = point_record.id;
            run_start_nodes[s] = point_record.start_node;
            run_end_nodes[s] = point_record.end_node;
            run_lengths[s] = point_record.edge_length;
            run_first_m[s] = point_record.measure;
            run_last_m[s] = point_record.measure;
            run_min_m[s] = point_record.measure;
            run_max_m[s] = point_record.measure;
            EXIT;
        END LOOP;
    END LOOP;

    -- 3/ Fill the gaps between two consecutive edges which are not connected,
    -- for example when a short road (roundabout) has been skipped.
    -- We search the shortest path of maximum 5 edges between the two edges
    FOR i IN 1..s LOOP
        f = f + 1;
        final_edges[f] = run_edges[i];
        final_start_nodes[f] = run_start_nodes[i];
        final_end_nodes[f] = run_end_nodes[i];
        final_first_m[f] = run_first_m[i];
        final_last_m[f] = run_last_m[i];

        CONTINUE WHEN i = s;
        CONTINUE WHEN run_start_nodes[i] IN (run_start_nodes[i + 1], run_end_nodes[i + 1])
            OR run_end_nodes[i] IN (run_start_nodes[i + 1], run_end_nodes[i + 1])
        ;

        WITH RECURSIVE
        edge_a AS (
            SELECT e.*, ST_LineInterpolatePoint(e.geom, run_last_m[i]) AS point
            FROM road_graph.edges AS e
            WHERE e.id = run_edges[i]
        ),
        edge_b AS (
            SELECT e.*, ST_LineInterpolatePoint(e.geom, run_first_m[i + 1]) AS point
            FROM road_graph.edges AS e
            WHERE e.id = run_edges[i + 1]
        ),
        local_edges AS (
            SELECT e.id, e.start_node, e.end_node, ST_Length(e.geom) AS edge_length
            FROM road_graph.edges AS e, edge_a AS a, edge_b AS b
            WHERE ST_DWithin(e.geom, ST_MakeLine(a.point, b.point), 2 * _max_distance)
            AND e.id NOT IN (a.id, b.id)
        ),
        start_nodes AS (
            SELECT a.start_node AS node, run_last_m[i] * ST_Length(a.geom) AS cost FROM edge_a AS a
            UNION ALL
            SELECT a.end_node, (1 - run_last_m[i]) * ST_Length(a.geom) FROM edge_a AS a
        ),
        end_nodes AS (
            SELECT b.start_node AS node, run_first_m[i + 1] * ST_Length(b.geom) AS cost FROM edge_b AS b
            UNION ALL
            SELECT b.end_node, (1 - run_first_m[i + 1]) * ST_Length(b.geom) FROM edge_b AS b
        ),
        paths AS (
            SELECT
                sn.node, ARRAY[]::integer[] AS edges, ARRAY[sn.node] AS nodes,
                sn.cost, 0 AS depth
            FROM start_nodes AS sn
            UNION ALL
            SELECT
                CASE WHEN l.start_node = pa.node THEN l.end_node ELSE l.start_node END,
                pa.edges || l.id,
                pa.nodes || CASE WHEN l.start_node = pa.node THEN l.end_node ELSE l.start_node END,
                pa.cost + l.edge_length,
                pa.depth + 1
            FROM paths AS pa
            INNER JOIN local_edges AS l
                ON pa.node IN (l.start_node, l.end_node)
            WHERE pa.depth < 5
            AND NOT l.id = ANY(pa.edges)
        )
        SELECT INTO bridge
            pa.edges, pa.nodes
        FROM paths AS pa
        INNER JOIN end_nodes AS en
            ON en.node = pa.node
        WHERE pa.depth > 0
        ORDER BY pa.cost + en.cost, pa.depth
        LIMIT 1
        ;
        CONTINUE WHEN bridge.edges IS NULL;

        IF raise_notice = 'debug' THEN
            RAISE NOTICE 'get_snapped_line_from_track - gap between edges % and % filled with edges %',
                run_edges[i], run_edges[i + 1], bridge.edges
            ;
        END IF;
        FOR j IN 1..array_length(bridge.edges, 1) LOOP
            SELECT INTO bridge_edge e.id, e.start_node, e.end_node
            FROM road_graph.edges AS e
            WHERE e.id = bridge.edges[j]
            ;
            f = f + 1;
            final_edges[f] = bridge_edge.id;
            final_start_nodes[f] = bridge_edge.start_node;
            final_end_nodes[f] = bridge_edge.end_node;
            final_first_m[f] = CASE
                WHEN bridge_edge.start_node = bridge_edge.end_node THEN 0
                WHEN bridge.nodes[j] = bridge_edge.start_node THEN 0
                ELSE 1
            END;
            final_last_m[f] = 1 - final_first_m[f];
        END LOOP;
    END LOOP;

    -- 4/ Build the line from the edges geometries
    -- Each edge is used from the node connecting it to the previous edge
    -- (or from the first snapped point) to the node connecting it to the next edge
    -- (or to the last snapped point).
    -- If two consecutive edges are not connected, a new part is created.
    FOR i IN 1..f LOOP
        node_in = NULL;
        node_out = NULL;
        IF i > 1 THEN
            node_in = CASE
                WHEN final_start_nodes[i] IN (final_start_nodes[i - 1], final_end_nodes[i - 1]) THEN final_start_nodes[i]
                WHEN final_end_nodes[i] IN (final_start_nodes[i - 1], final_end_nodes[i - 1]) THEN final_end_nodes[i]
            END;
        END IF;
        IF i < f THEN
            node_out = CASE
                WHEN final_end_nodes[i] IN (final_start_nodes[i + 1], final_end_nodes[i + 1]) THEN final_end_nodes[i]
                WHEN final_start_nodes[i] IN (final_start_nodes[i + 1], final_end_nodes[i + 1]) THEN final_start_nodes[i]
            END;
        END IF;

        -- A closed edge (roundabout) is entered by its start and left by its end
        entry_m = CASE
            WHEN node_in IS NULL THEN final_first_m[i]
            WHEN final_start_nodes[i] = final_end_nodes[i] THEN 0
            WHEN node_in = final_start_nodes[i] THEN 0
            ELSE 1
        END;
        exit_m = CASE
            WHEN node_out IS NULL THEN final_last_m[i]
            WHEN final_start_nodes[i] = final_end_nodes[i] THEN 1
            WHEN node_out = final_start_nodes[i] THEN 0
            ELSE 1
        END;

        -- New part if the edge is not connected to the previous one
        IF i > 1 AND node_in IS NULL AND array_length(pieces, 1) > 0 THEN
            parts = parts || ST_MakeLine(pieces);
            pieces = '{}';
        END IF;

        CONTINUE WHEN entry_m = exit_m;
        SELECT INTO edge_geom e.geom
        FROM road_graph.edges AS e
        WHERE e.id = final_edges[i]
        ;
        pieces = pieces || CASE
            WHEN entry_m < exit_m THEN ST_LineSubstring(edge_geom, entry_m, exit_m)
            ELSE ST_Reverse(ST_LineSubstring(edge_geom, exit_m, entry_m))
        END;
    END LOOP;
    IF array_length(pieces, 1) > 0 THEN
        parts = parts || ST_MakeLine(pieces);
    END IF;

    IF array_length(parts, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN ST_Multi(ST_RemoveRepeatedPoints(ST_Collect(parts), 0.001));
END;
$$;

COMMENT ON FUNCTION road_graph.get_snapped_line_from_track(_track geometry, _densify_distance real, _max_distance real, _road_change_penalty real)
IS 'Build a multilinestring snapped on the road graph edges from a GPS track (for example from a GPX file) which can be imprecise.
The track is densified with a point every _densify_distance meters (default 2m), and each point is assigned to one of the roads closer than _max_distance (default 25m).
To avoid zigzags between two close roads, the chosen sequence of roads minimizes the sum of the distances between the points and their roads, plus a penalty _road_change_penalty (default 100, expressed as a surface in m² between the track and the roads) each time the road changes.
The returned geometry follows the edges geometries, in the direction of the track. Gaps between two unconnected edges are filled with the shortest path of maximum 5 edges. If no path is found, a new part is created in the multilinestring.
A track without SRID is considered to be in the road graph projection, and other tracks are reprojected. The result is returned in the road graph projection.
The result can be passed to the function get_road_sections_from_snapped_line to get the references of the crossed roads.';


-- FUNCTION: road_graph.get_road_sections_from_snapped_line
CREATE OR REPLACE FUNCTION road_graph.get_road_sections_from_snapped_line(
    _line geometry,
    _tolerance real DEFAULT 1
) RETURNS TABLE(
    road_order integer,
    road_code text,
    start_marker integer,
    start_abscissa real,
    start_cumulative real,
    end_marker integer,
    end_abscissa real,
    end_cumulative real,
    direction text,
    length real,
    geom geometry(LineString, 2154)
)
    LANGUAGE plpgsql
    AS $$
DECLARE
    line geometry;
BEGIN
    IF _line IS NULL OR ST_IsEmpty(_line) THEN
        RETURN;
    END IF;

    -- Use the road graph projection
    line = ST_Force2D(_line);
    IF ST_SRID(line) = 0 THEN
        line = ST_SetSRID(line, 2154);
    ELSIF ST_SRID(line) != 2154 THEN
        line = ST_Transform(line, 2154);
    END IF;

    RETURN QUERY
    WITH
    -- Points of each part of the line, densified
    -- to avoid long segments if the line has not been snapped on the edges
    line_points AS (
        SELECT
            coalesce(d.path[1], 1) AS part,
            coalesce(d.path[array_length(d.path, 1)], 1) AS point_index,
            d.geom
        FROM ST_DumpPoints(ST_Segmentize(ST_Multi(line), 5)) AS d
    ),
    segments AS (
        SELECT
            lp.part,
            row_number() OVER (ORDER BY lp.part, lp.point_index) AS segment_index,
            ST_MakeLine(lp.geom, lead(lp.geom) OVER (PARTITION BY lp.part ORDER BY lp.point_index)) AS geom
        FROM line_points AS lp
    ),
    -- Find the road of each segment, with its middle point
    -- which avoids the ambiguity of the points located on the intersection nodes
    segment_roads AS (
        SELECT
            sg.part, sg.segment_index, sg.geom, r.road_code
        FROM segments AS sg
        CROSS JOIN LATERAL (
            SELECT e.road_code
            FROM road_graph.edges AS e
            WHERE ST_DWithin(e.geom, ST_LineInterpolatePoint(sg.geom, 0.5), _tolerance)
            ORDER BY ST_Distance(e.geom, ST_LineInterpolatePoint(sg.geom, 0.5)), e.road_code
            LIMIT 1
        ) AS r
        WHERE sg.geom IS NOT NULL
        AND ST_Length(sg.geom) > 0
    ),
    -- Group the consecutive segments on the same road
    groups AS (
        SELECT
            sr.*,
            sr.segment_index - row_number() OVER (PARTITION BY sr.part, sr.road_code ORDER BY sr.segment_index) AS group_id
        FROM segment_roads AS sr
    ),
    road_lines AS (
        SELECT
            row_number() OVER (ORDER BY min(g.segment_index))::integer AS road_order,
            g.road_code,
            ST_LineMerge(ST_MakeLine(g.geom ORDER BY g.segment_index)) AS geom
        FROM groups AS g
        GROUP BY g.part, g.road_code, g.group_id
    ),
    road_references AS (
        SELECT
            rl.*,
            -- For roundabouts, the start and end points of the closed road are the same:
            -- use points 1cm inside the line to avoid any ambiguity
            -- and correct the references afterwards with this shift
            CASE
                WHEN ro.road_type = 'roundabout' AND ST_Length(rl.geom) > 0.1 THEN 0.01
                ELSE 0
            END AS shift,
            road_graph.get_reference_from_point(
                CASE
                    WHEN ro.road_type = 'roundabout' AND ST_Length(rl.geom) > 0.1
                        THEN ST_LineInterpolatePoint(rl.geom, 0.01 / ST_Length(rl.geom))
                    ELSE ST_StartPoint(rl.geom)
                END,
                rl.road_code,
                False
            ) AS start_ref,
            road_graph.get_reference_from_point(
                CASE
                    WHEN ro.road_type = 'roundabout' AND ST_Length(rl.geom) > 0.1
                        THEN ST_LineInterpolatePoint(rl.geom, 1 - 0.01 / ST_Length(rl.geom))
                    ELSE ST_EndPoint(rl.geom)
                END,
                rl.road_code,
                False
            ) AS end_ref
        FROM road_lines AS rl
        LEFT JOIN road_graph.roads AS ro
            ON ro.road_code = rl.road_code
    ),
    road_directions AS (
        SELECT
            rr.*,
            CASE
                WHEN (rr.end_ref->>'cumulative')::numeric >= (rr.start_ref->>'cumulative')::numeric THEN 1
                ELSE -1
            END AS sign
        FROM road_references AS rr
    )
    SELECT
        rd.road_order,
        rd.road_code,
        (rd.start_ref->>'marker_code')::integer,
        ((rd.start_ref->>'abscissa')::numeric - rd.sign * rd.shift)::real,
        ((rd.start_ref->>'cumulative')::numeric - rd.sign * rd.shift)::real,
        (rd.end_ref->>'marker_code')::integer,
        ((rd.end_ref->>'abscissa')::numeric + rd.sign * rd.shift)::real,
        ((rd.end_ref->>'cumulative')::numeric + rd.sign * rd.shift)::real,
        CASE WHEN rd.sign = 1 THEN 'downstream' ELSE 'upstream' END,
        round(ST_Length(rd.geom)::numeric, 2)::real,
        rd.geom::geometry(LineString, 2154)
    FROM road_directions AS rd
    ORDER BY rd.road_order
    ;
END;
$$;

COMMENT ON FUNCTION road_graph.get_road_sections_from_snapped_line(_line geometry, _tolerance real)
IS 'Return the ordered list of the roads followed by the given line, with the start and end references (marker, abscissa and cumulative) for each road, the direction (downstream or upstream) and the line portion on this road.
The line must follow the road graph edges, for example the result of the function get_snapped_line_from_track. The parameter _tolerance (default 1m) is the maximum distance between the line and the edges.
A line without SRID is considered to be in the road graph projection, and other lines are reprojected.
Example of output (the geom column is not shown):
 road_order | road_code     | start_marker | start_abscissa | start_cumulative | end_marker | end_abscissa | end_cumulative | direction  | length
------------+---------------+--------------+----------------+------------------+------------+--------------+----------------+------------+--------
          1 | D40           |           14 |         905.27 |          16361.8 |         15 |       396.15 |        16761.4 | downstream |  399.6
          2 | GIR_D40_D16-1 |            0 |              0 |                0 |          0 |        39.17 |          39.17 | downstream |  39.17
          3 | D16           |            0 |         716.82 |           716.82 |          0 |         1.62 |           1.62 | upstream   | 715.19
';
