-- Road graph checks
--
-- glossary_graph_check: list of the checks of the road graph. The rows are added by the migrations,
-- the administrator can modify the default options of each check and deactivate it.
-- check_graph: runs the checks of the road graph and returns one line per issue,
-- with a point locating it and an optional line showing it.
-- fix_graph_issue: fixes an issue with its fix action, only in the editing session.
--
-- Checks:
-- * G01: Crossing edges without node
-- * G02: Edge ending near another edge without being connected
-- * G03: Nodes close to each other
-- * G04: Useless node
-- * G05: Edges in the opposite direction of the road
-- * G06: Road with branches
-- * G07: Parallel edges of the same road
-- * G08: Short edge
-- * G09: First marker not at the start of the road
-- * G10: Markers not in the order of the road

-- glossary_graph_check
CREATE TABLE IF NOT EXISTS road_graph.glossary_graph_check (
    code text NOT NULL,
    title text NOT NULL,
    severity text NOT NULL,
    function_name text NOT NULL,
    fix_actions text[] DEFAULT '{}'::text[] NOT NULL,
    default_options jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    description text,
    CONSTRAINT glossary_graph_check_severity_values CHECK ((severity = ANY (ARRAY['error'::text, 'warning'::text])))
);

-- glossary_graph_check glossary_graph_check_pkey
ALTER TABLE ONLY road_graph.glossary_graph_check
    DROP CONSTRAINT IF EXISTS glossary_graph_check_pkey;
ALTER TABLE ONLY road_graph.glossary_graph_check
    ADD CONSTRAINT glossary_graph_check_pkey PRIMARY KEY (code);

-- glossary_graph_check
COMMENT ON TABLE road_graph.glossary_graph_check IS 'Checks of the road graph run by the function check_graph. The rows are added by the plugin migrations: only the columns default_options and is_active should be modified.';


-- glossary_graph_check.code
COMMENT ON COLUMN road_graph.glossary_graph_check.code IS 'Code of the check, e.g. G01. The checks are numbered in the recommended order of the fixes.';


-- glossary_graph_check.title
COMMENT ON COLUMN road_graph.glossary_graph_check.title IS 'Title of the check';


-- glossary_graph_check.severity
COMMENT ON COLUMN road_graph.glossary_graph_check.severity IS 'Severity of the issues found by the check: error or warning';


-- glossary_graph_check.function_name
COMMENT ON COLUMN road_graph.glossary_graph_check.function_name IS 'Name of the function running the check, in the same schema as the function check_graph';


-- glossary_graph_check.fix_actions
COMMENT ON COLUMN road_graph.glossary_graph_check.fix_actions IS 'Actions which can fix the issues found by the check, with the function fix_graph_issue';


-- glossary_graph_check.default_options
COMMENT ON COLUMN road_graph.glossary_graph_check.default_options IS 'Default options of the check, for example its tolerance. They can be modified by the administrator, and overridden by the options given to the function check_graph.';


-- glossary_graph_check.is_active
COMMENT ON COLUMN road_graph.glossary_graph_check.is_active IS 'If False, the check is not run by the function check_graph, unless it is explicitly asked';


-- glossary_graph_check.description
COMMENT ON COLUMN road_graph.glossary_graph_check.description IS 'Description of the check and of its fix actions';


-- Checks of the road graph
-- The default options and is_active, which can be modified by the administrator, are not updated
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G01', 'Crossing edges without node', 'error', 'check_graph_crossing_edges', '{cut_edges,mark_as_bridge}', '{}', 'Edges crossing or touching each other without being split by a node. The edges with no_intersection_cutting set to True are ignored. Fix: cut_edges creates a node at the point and splits the edges, mark_as_bridge sets no_intersection_cutting to True for the edges which must not be connected (bridge, tunnel). The edges touching each other are fixed with cut_edges. For the edges crossing each other, the action must be chosen for each issue, unless the option crossing_fix_action is set.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;

INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G02', 'Edge ending near another edge without being connected', 'error', 'check_graph_t_junction_gaps', '{extend_edge}', '{"tolerance": 2}', 'Free end of an edge (no other edge connected to it) less than the tolerance from another edge, for example a road stopping a few decimeters before the road it arrives on (T junction). Option: tolerance, in meters. Fix: extend_edge extends the edge to the closest point of the other edge, or to its start or end point if it is less than 50 cm from it, and the other edge is split there.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;

INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G03', 'Nodes close to each other', 'error', 'check_graph_close_nodes', '{merge_nodes}', '{"tolerance": 1}', 'Nodes less than the tolerance from each other, which should be the same node. The nodes linked by an edge are ignored. Option: tolerance, in meters. Fix: merge_nodes moves the edges of the other node to the node with the most edges, or else to the node with the lowest id, then deletes the other node.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G04', 'Useless node', 'warning', 'check_graph_useless_nodes', '{delete_node}', '{}', 'Node without edge, or node between two edges of the same road which can be merged: one edge arrives at the node and the other one leaves it, with the same value of no_intersection_cutting. Fix: delete_node deletes the node, and the triggers merge the two edges into the edge with the lowest id.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G05', 'Edges in the opposite direction of the road', 'error', 'check_graph_reversed_edges', '{reverse_edges}', '{"marker_tolerance": 10}', 'Edges in the opposite direction of their road. The edges of a road following each other are grouped in sequences, whose direction is given by the order of the markers along them (option marker_tolerance, in meters), or else by the direction of most of their length. Fix: reverse_edges reverses the geometry of the edges, and the triggers swap their start and end nodes.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G06', 'Road with branches', 'error', 'check_graph_road_branches', '{}', '{}', 'Node with more than two edges of the same road, for example the ramps or the carriageways of a road using the code of the road. The roundabouts are ignored. The branches are the edges which are not the most aligned edges arriving at the node and leaving it. Fix: manual, for example by giving another road code to the edges of the branches.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G07', 'Parallel edges of the same road', 'warning', 'check_graph_parallel_edges', '{}', '{"tolerance": 30, "min_length": 10}', 'Edges of the same road, not connected by a node, running along each other less than the tolerance from each other along at least the minimum length, for example the two carriageways of a road using the same road code. The edges which only approach each other, for example on both sides of a roundabout, and the roundabouts are ignored. Options: tolerance and min_length, in meters. Fix: manual, for example by giving another road code to one of the carriageways.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G08', 'Short edge', 'warning', 'check_graph_short_edges', '{collapse_edge}', '{"min_length": 3}', 'Edge shorter than the minimum length, for example two junctions a few decimeters from each other. The edges of the roundabouts and the edges arriving at a roundabout or leaving it are ignored, unless they are reduced to a point. Option: min_length, in meters. Fix: collapse_edge deletes the edge and merges its nodes: its geometry is added to the next or previous edge of the same road, and the other edges of the removed node are moved to the kept node. The only edge of a road has no fix action.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G09', 'First marker not at the start of the road', 'error', 'check_graph_first_marker', '{move_marker}', '{"tolerance": 1}', 'First marker of a road (lowest code and abscissa, which can be virtual) not at the start point of the first edge of the road. When the marker is after the start point, the start of the road has no reference. The roundabouts are ignored. Option: tolerance, in meters. Fix: move_marker moves the marker to the start of the road, if it is less than the tolerance from it.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G10', 'Markers not in the order of the road', 'error', 'check_graph_markers_order', '{}', '{"marker_tolerance": 50}', 'Markers whose position along their road is not in the order of their code and abscissa. The longest sequence of markers in the order of the road is kept, and the other markers are returned. Also returns the markers far from their road (option marker_tolerance, in meters), or whose closest edge has no reference. The roundabouts are ignored. Fix: manual, by moving the marker or changing its code.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;

-- check_graph(text[], text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph(_checks text[] DEFAULT NULL::text[], _road_codes text[] DEFAULT NULL::text[], _options jsonb DEFAULT NULL::jsonb) RETURNS TABLE(id integer, check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $_$
DECLARE
    unknown_checks text[];
    graph_check record;
    sql_text text;
    issues_count integer = 0;
    check_issues_count integer;
BEGIN
    -- Check the given check codes
    -- The checks are always read from the production schema, where the administrator sets their options
    SELECT array_agg(c ORDER BY c)
    INTO unknown_checks
    FROM unnest(_checks) AS c
    WHERE c NOT IN (
        SELECT g.code
        FROM "road_graph".glossary_graph_check AS g
    );
    IF unknown_checks IS NOT NULL THEN
        RAISE EXCEPTION 'Unknown road graph checks: %', array_to_string(unknown_checks, ', ');
    END IF;

    -- Run each requested check, or all the active checks if none is given
    FOR graph_check IN
        SELECT g.*
        FROM "road_graph".glossary_graph_check AS g
        WHERE (_checks IS NULL AND g.is_active) OR g.code = ANY(_checks)
        ORDER BY g.code
    LOOP
        -- The function of the check must exist
        -- The schema of the function is replaced when the function is installed in the editing schema
        IF to_regproc('road_graph.' || quote_ident(graph_check.function_name)) IS NULL THEN
            RAISE EXCEPTION 'The function % of the road graph check % does not exist',
                graph_check.function_name, graph_check.code;
        END IF;

        -- Return the issues found by the check, with a unique id following the ids of the previous checks
        -- The check function gets the road codes, and its options:
        -- the default options of the check, overridden by the given options
        sql_text = format(
            'SELECT ($3 + row_number() OVER ())::integer, i.* FROM road_graph.%I($1, $2) AS i',
            graph_check.function_name
        );
        RETURN QUERY EXECUTE sql_text
        USING
            _road_codes,
            graph_check.default_options || Coalesce(_options->graph_check.code, '{}'::jsonb),
            issues_count;
        GET DIAGNOSTICS check_issues_count = ROW_COUNT;
        issues_count = issues_count + check_issues_count;
    END LOOP;

    RETURN;
END;
$_$;


-- FUNCTION check_graph(_checks text[], _road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph(_checks text[], _road_codes text[], _options jsonb) IS 'Run the checks of the road graph and return one line per issue found.

The checks are listed in the table glossary_graph_check of the production schema.
All the active checks are run if _checks is NULL, and all the roads are checked if _road_codes is NULL.
The parameter _options overrides the default options of the checks stored in this table,
with the check code as key: {"G02": {"tolerance": 2}}.

Each line gives the check code and title, the severity (error or warning), the road code,
the ids of the edges, nodes and markers concerned, some details, the action which can fix the issue
with its parameters (NULL if the issue must be fixed manually), a point locating the issue (geom)
and, if useful, a line showing it (geom_detail).

The function can be run on the production schema or on the editing schema: road_graph_editing.check_graph()
checks the data of the editing session. The issues must be fixed in an editing session, with the function fix_graph_issue.';


-- check_graph_close_nodes(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_close_nodes(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between two nodes
    tolerance real = Coalesce((_options->>'tolerance')::real, 1.0);
BEGIN
    RETURN QUERY
    WITH
    -- Nodes with their edges
    nodes AS (
        SELECT
            n.id, n.geom,
            array_agg(e.id ORDER BY e.id) FILTER (WHERE e.id IS NOT NULL) AS node_edge_ids,
            array_agg(DISTINCT e.road_code) FILTER (WHERE e.road_code IS NOT NULL) AS node_road_codes
        FROM road_graph.nodes AS n
        LEFT JOIN road_graph.edges AS e
            ON n.id IN (e.start_node, e.end_node)
        GROUP BY n.id, n.geom
    ),
    -- Pairs of close nodes, not linked by an edge: a short edge is found by another check
    pairs AS (
        SELECT
            a.id AS a_id, b.id AS b_id,
            a.geom AS a_geom, b.geom AS b_geom,
            Coalesce(a.node_edge_ids, '{}') AS a_edge_ids, Coalesce(b.node_edge_ids, '{}') AS b_edge_ids,
            Coalesce(a.node_road_codes, '{}') || Coalesce(b.node_road_codes, '{}') AS pair_road_codes
        FROM nodes AS a
        JOIN nodes AS b
            ON a.id < b.id
            AND ST_DWithin(a.geom, b.geom, tolerance)
        WHERE NOT EXISTS (
            SELECT 1
            FROM road_graph.edges AS e
            WHERE (e.start_node = a.id AND e.end_node = b.id)
            OR (e.start_node = b.id AND e.end_node = a.id)
        )
    ),
    -- Node to keep: the node with the most edges, so that the fewest edges are moved,
    -- or else the node with the lowest id (the first node of the pair), i.e. the oldest node
    issues AS (
        SELECT
            p.*,
            CASE WHEN keep_a THEN p.a_id ELSE p.b_id END AS keep_id,
            CASE WHEN keep_a THEN p.a_geom ELSE p.b_geom END AS keep_geom,
            CASE WHEN keep_a THEN p.b_id ELSE p.a_id END AS remove_id,
            CASE WHEN keep_a THEN p.b_geom ELSE p.a_geom END AS remove_geom
        FROM pairs AS p,
        LATERAL (SELECT cardinality(p.a_edge_ids) >= cardinality(p.b_edge_ids) AS keep_a) AS k
        WHERE _road_codes IS NULL OR p.pair_road_codes && _road_codes
    )
    SELECT
        'G03'::text,
        'Nodes close to each other'::text,
        'error'::text,
        (SELECT min(c) FROM unnest(i.pair_road_codes) AS c),
        (SELECT array_agg(DISTINCT x ORDER BY x) FROM unnest(i.a_edge_ids || i.b_edge_ids) AS x),
        ARRAY[i.keep_id, i.remove_id],
        NULL::integer[],
        jsonb_build_object(
            'road_codes', (SELECT jsonb_agg(DISTINCT c ORDER BY c) FROM unnest(i.pair_road_codes) AS c),
            'distance', round(ST_Distance(i.a_geom, i.b_geom)::numeric, 2)
        ),
        'merge_nodes'::text,
        jsonb_build_object(
            'keep_node_id', i.keep_id,
            'remove_node_id', i.remove_id
        ),
        i.keep_geom,
        CASE WHEN ST_Equals(i.a_geom, i.b_geom) THEN NULL ELSE ST_MakeLine(i.remove_geom, i.keep_geom) END
    FROM issues AS i
    ORDER BY 4, i.a_id, i.b_id
    ;
END;
$$;


-- FUNCTION check_graph_close_nodes(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_close_nodes(_road_codes text[], _options jsonb) IS 'Check G03 run by the function check_graph: find the nodes close to each other, which should be the same node.

An issue is returned for each pair of nodes less than the tolerance from each other: option tolerance, 1 m by default.
The nodes linked by an edge are ignored: this edge is very short, and is found by another check.

The node to keep is the node with the most edges, so that the fewest edges are moved,
or else the node with the lowest id, i.e. the oldest node: it is the first node of node_ids,
and the point of the issue. The details give the road codes of the edges of the two nodes, and the distance.
The issue can be fixed with the action merge_nodes, which moves the edges of the other node to the node to keep.';


-- check_graph_crossing_edges(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_crossing_edges(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Distance used by the triggers to split an edge by a node, and to reuse an existing node
    tolerance real = 0.50;
    -- Action fixing the edges crossing each other, chosen by the administrator: NULL by default,
    -- since they may be on a bridge. The edges touching each other are always cut
    crossing_fix_action text = Nullif(_options->>'crossing_fix_action', '');
BEGIN
    IF crossing_fix_action NOT IN ('cut_edges', 'mark_as_bridge') THEN
        RAISE EXCEPTION 'The option crossing_fix_action of the check G01 must be cut_edges or mark_as_bridge, not %',
            crossing_fix_action;
    END IF;

    RETURN QUERY
    WITH
    -- Points where two edges cross or touch each other.
    -- The edges which must not be cut, for example on a bridge, are ignored
    crossings AS (
        SELECT
            a.id AS a_id, b.id AS b_id,
            a.road_code AS a_road_code, b.road_code AS b_road_code,
            a.geom AS a_geom, b.geom AS b_geom,
            (ST_Dump(ST_CollectionExtract(ST_Intersection(a.geom, b.geom), 1))).geom AS point
        FROM road_graph.edges AS a
        JOIN road_graph.edges AS b
            ON a.id < b.id
            AND ST_Intersects(a.geom, b.geom)
        WHERE NOT Coalesce(a.no_intersection_cutting, False)
        AND NOT Coalesce(b.no_intersection_cutting, False)
        AND (
            _road_codes IS NULL
            OR a.road_code = ANY(_road_codes)
            OR b.road_code = ANY(_road_codes)
        )
    ),
    -- Keep the points which are not at the start or end of both edges:
    -- at least one of the edges must be split there.
    -- If the point is at the start or end of one edge, the edges touch each other: they are surely connected.
    -- Else they cross each other, maybe on a bridge
    issues AS (
        SELECT
            c.*,
            (
                ST_DWithin(c.point, ST_StartPoint(c.a_geom), tolerance)
                OR ST_DWithin(c.point, ST_EndPoint(c.a_geom), tolerance)
                OR ST_DWithin(c.point, ST_StartPoint(c.b_geom), tolerance)
                OR ST_DWithin(c.point, ST_EndPoint(c.b_geom), tolerance)
            ) AS is_touching
        FROM crossings AS c
        WHERE (
            NOT ST_DWithin(c.point, ST_StartPoint(c.a_geom), tolerance)
            AND NOT ST_DWithin(c.point, ST_EndPoint(c.a_geom), tolerance)
        ) OR (
            NOT ST_DWithin(c.point, ST_StartPoint(c.b_geom), tolerance)
            AND NOT ST_DWithin(c.point, ST_EndPoint(c.b_geom), tolerance)
        )
    ),
    -- Direction of each edge at the crossing point, from its points 1 m before and after,
    -- to get the angle between the edges: it helps to find the bridges
    directions AS (
        SELECT
            i.*,
            degrees(ST_Azimuth(
                ST_LineInterpolatePoint(i.a_geom, greatest(ST_LineLocatePoint(i.a_geom, i.point) - 1 / ST_Length(i.a_geom), 0)),
                ST_LineInterpolatePoint(i.a_geom, least(ST_LineLocatePoint(i.a_geom, i.point) + 1 / ST_Length(i.a_geom), 1))
            )) AS a_azimuth,
            degrees(ST_Azimuth(
                ST_LineInterpolatePoint(i.b_geom, greatest(ST_LineLocatePoint(i.b_geom, i.point) - 1 / ST_Length(i.b_geom), 0)),
                ST_LineInterpolatePoint(i.b_geom, least(ST_LineLocatePoint(i.b_geom, i.point) + 1 / ST_Length(i.b_geom), 1))
            )) AS b_azimuth
        FROM issues AS i
    )
    SELECT
        'G01'::text,
        'Crossing edges without node'::text,
        'error'::text,
        d.a_road_code,
        ARRAY[d.a_id, d.b_id],
        -- Existing node close to the crossing point, not used by the edges
        (
            SELECT array_agg(n.id ORDER BY n.id)
            FROM road_graph.nodes AS n
            WHERE ST_DWithin(n.geom, d.point, tolerance)
        ),
        NULL::integer[],
        jsonb_build_object(
            'crossing_type', CASE WHEN d.is_touching THEN 'touching' ELSE 'crossing' END,
            'road_codes', jsonb_build_array(d.a_road_code, d.b_road_code),
            'road_classes', jsonb_build_array(ra.road_class, rb.road_class),
            -- Angle between the two edges, from 0 (parallel) to 90 degrees (perpendicular)
            'angle', round((90 - abs(90 - mod((abs(d.a_azimuth - d.b_azimuth))::numeric, 180)))::numeric, 1)
        ),
        CASE WHEN d.is_touching THEN 'cut_edges' ELSE crossing_fix_action END,
        jsonb_build_object(
            'point', ST_AsEWKT(d.point),
            'edge_ids', jsonb_build_array(d.a_id, d.b_id)
        ),
        d.point,
        ST_Collect(d.a_geom, d.b_geom)
    FROM directions AS d
    LEFT JOIN road_graph.roads AS ra ON ra.road_code = d.a_road_code
    LEFT JOIN road_graph.roads AS rb ON rb.road_code = d.b_road_code
    ORDER BY d.a_road_code, d.a_id, d.b_id
    ;
END;
$$;


-- FUNCTION check_graph_crossing_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_crossing_edges(_road_codes text[], _options jsonb) IS 'Check G01 run by the function check_graph: find the edges crossing each other without being split by a node.

An issue is returned for each crossing point which is not at the start or end point of both edges (50 cm tolerance,
like the triggers splitting the edges). The edges with the field no_intersection_cutting set to True,
for example on a bridge, are ignored.

The details give the crossing type, the road codes and classes of the two edges, and the angle between them,
which help to find the bridges:

* touching: the point is the start or end point of one of the edges, for example a road arriving on another road.
  The edges are surely connected: the fix action is cut_edges, which creates a node at the point and splits the edges,
* crossing: the edges cross each other, maybe on a bridge or in a tunnel. The fix action is given by the option
  crossing_fix_action of the check, NULL by default: it must be chosen for each issue, cut_edges if the roads
  are connected, or mark_as_bridge, which sets no_intersection_cutting to True for the edges.';


-- check_graph_first_marker(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_first_marker(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between the first marker and the start of the road to move the marker automatically
    tolerance real = Coalesce((_options->>'tolerance')::real, 1.0);
    -- Distance under which the marker is at the start of the road: the coordinates are rounded to 10 cm
    start_tolerance real = 0.05;
BEGIN
    RETURN QUERY
    WITH
    -- First marker of each road, except the roundabouts: the marker with the lowest code and abscissa,
    -- which can be virtual
    first_markers AS (
        SELECT DISTINCT ON (m.road_code)
            m.id, m.road_code, m.code, m.abscissa, m.is_virtual, m.geom
        FROM road_graph.markers AS m
        JOIN road_graph.roads AS r ON r.road_code = m.road_code
        WHERE r.road_type != 'roundabout'
        AND (_road_codes IS NULL OR m.road_code = ANY(_road_codes))
        ORDER BY m.road_code, m.code, m.abscissa, m.id
    ),
    -- First edge of each road: the edge without previous edge, with the lowest start cumulative
    first_edges AS (
        SELECT DISTINCT ON (e.road_code)
            e.id, e.road_code, e.geom, e.start_node, e.start_marker
        FROM road_graph.edges AS e
        WHERE _road_codes IS NULL OR e.road_code = ANY(_road_codes)
        ORDER BY e.road_code, (e.previous_edge_id IS NOT NULL), e.start_cumulative NULLS LAST, e.id
    ),
    issues AS (
        SELECT
            m.id AS marker_id, m.road_code, m.code, m.abscissa, m.is_virtual, m.geom AS marker_geom,
            e.id AS edge_id, e.start_node, e.start_marker,
            ST_StartPoint(e.geom) AS start_point,
            ST_Distance(m.geom, ST_StartPoint(e.geom)) AS distance,
            -- Position of the marker along the first edge, from its start
            ST_LineLocatePoint(e.geom, m.geom) * ST_Length(e.geom) AS position
        FROM first_markers AS m
        JOIN first_edges AS e ON e.road_code = m.road_code
        WHERE NOT ST_DWithin(m.geom, ST_StartPoint(e.geom), start_tolerance)
    )
    SELECT
        'G09'::text,
        'First marker not at the start of the road'::text,
        -- The start of the first edge has no reference when the marker is after it
        CASE WHEN i.start_marker IS NULL THEN 'error' ELSE 'warning' END,
        i.road_code,
        ARRAY[i.edge_id],
        ARRAY[i.start_node],
        ARRAY[i.marker_id],
        jsonb_build_object(
            'marker_code', i.code,
            'marker_abscissa', i.abscissa,
            'is_virtual', i.is_virtual,
            'distance', round(i.distance::numeric, 2),
            'position_on_edge', round(i.position::numeric, 2),
            'has_start_reference', i.start_marker IS NOT NULL
        ),
        CASE WHEN i.distance <= tolerance THEN 'move_marker' ELSE NULL END,
        CASE
            WHEN i.distance <= tolerance THEN jsonb_build_object(
                'marker_id', i.marker_id,
                'edge_id', i.edge_id,
                'point', ST_AsEWKT(i.start_point)
            )
            ELSE NULL
        END,
        i.marker_geom,
        ST_MakeLine(i.marker_geom, i.start_point)
    FROM issues AS i
    ORDER BY 4
    ;
END;
$$;


-- FUNCTION check_graph_first_marker(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_first_marker(_road_codes text[], _options jsonb) IS 'Check G09 run by the function check_graph: find the roads whose first marker is not at the start of the road.

The first marker of a road is the marker with the lowest code and abscissa: it is often the marker 0,
but it can be a virtual marker with a higher code. It must be at the start point of the first edge of the road,
the edge without previous edge. Else, when the marker is after the start point, the start of the road has no reference.
The roundabouts are ignored.

An issue is returned for each road whose first marker is more than 5 cm from the start of the road.
The severity is error if the start of the first edge has no reference, else warning.
The details give the code, abscissa and type of the marker, its distance to the start of the road,
its position along the first edge, and if the start of the first edge has a reference.

The issue can be fixed with the action move_marker, which moves the marker to the start of the road,
when it is less than the option tolerance from it (1 m by default). Else, the issue must be fixed manually.';


-- check_graph_markers_order(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_markers_order(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between a marker and the edges of its road
    marker_tolerance real = Coalesce((_options->>'marker_tolerance')::real, 50.0);
    -- Tolerance on the positions of the markers: two markers at the same point can have slightly different positions,
    -- for example at the end of an edge and at the start of the next edge
    position_tolerance real = 1.0;
    road record;
    -- Markers of the road, sorted by code and abscissa
    marker_ids_list integer[];
    marker_codes integer[];
    marker_abscissas real[];
    marker_virtuals boolean[];
    marker_geoms geometry[];
    marker_edge_ids integer[];
    marker_positions double precision[];
    marker_count integer;
    -- Longest sequence of markers in the order of their position, ending at each marker
    sequence_lengths integer[];
    previous_indexes integer[];
    best_index integer;
    in_order boolean[];
    i integer;
    j integer;
BEGIN
    -- Roads with markers, except the roundabouts
    FOR road IN
        SELECT DISTINCT m.road_code
        FROM road_graph.markers AS m
        JOIN road_graph.roads AS r ON r.road_code = m.road_code
        WHERE r.road_type != 'roundabout'
        AND (_road_codes IS NULL OR m.road_code = ANY(_road_codes))
        ORDER BY m.road_code
    LOOP
        -- Markers of the road in the order of their code and abscissa, with their position along the road:
        -- the cumulative length at their closest edge of the road
        SELECT
            array_agg(x.id ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.code ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.abscissa ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.is_virtual ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.geom ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.edge_id ORDER BY x.code, x.abscissa, x.id),
            array_agg(x.position ORDER BY x.code, x.abscissa, x.id)
        INTO
            marker_ids_list, marker_codes, marker_abscissas, marker_virtuals,
            marker_geoms, marker_edge_ids, marker_positions
        FROM (
            SELECT
                m.id, m.code, m.abscissa, m.is_virtual, m.geom,
                c.id AS edge_id, c.distance,
                c.start_cumulative + ST_LineLocatePoint(c.geom, m.geom) * ST_Length(c.geom) AS position
            FROM road_graph.markers AS m
            LEFT JOIN LATERAL (
                SELECT e.id, e.geom, e.start_cumulative, ST_Distance(e.geom, m.geom) AS distance
                FROM road_graph.edges AS e
                WHERE e.road_code = m.road_code
                AND ST_DWithin(e.geom, m.geom, marker_tolerance)
                ORDER BY ST_Distance(e.geom, m.geom), e.id
                LIMIT 1
            ) AS c ON True
            WHERE m.road_code = road.road_code
        ) AS x
        ;
        marker_count = cardinality(marker_ids_list);

        -- Longest sequence of markers whose positions increase with their code and abscissa:
        -- the markers out of this sequence are not in the order of the road
        sequence_lengths = array_fill(0, ARRAY[marker_count]);
        previous_indexes = array_fill(0, ARRAY[marker_count]);
        in_order = array_fill(False, ARRAY[marker_count]);
        best_index = 0;
        FOR i IN 1..marker_count LOOP
            CONTINUE WHEN marker_positions[i] IS NULL;
            sequence_lengths[i] = 1;
            FOR j IN 1..i - 1 LOOP
                IF marker_positions[j] IS NOT NULL
                    -- Two markers can be at the same position, for example a virtual marker
                    -- and the marker of the next code, with the same reference
                    AND marker_positions[j] <= marker_positions[i] + position_tolerance
                    AND sequence_lengths[j] + 1 > sequence_lengths[i]
                THEN
                    sequence_lengths[i] = sequence_lengths[j] + 1;
                    previous_indexes[i] = j;
                END IF;
            END LOOP;
            IF best_index = 0 OR sequence_lengths[i] > sequence_lengths[best_index] THEN
                best_index = i;
            END IF;
        END LOOP;
        i = best_index;
        WHILE i > 0 LOOP
            in_order[i] = True;
            i = previous_indexes[i];
        END LOOP;

        -- One issue for each marker far from the road, on an edge without reference,
        -- or out of the order of the road
        FOR i IN 1..marker_count LOOP
            CONTINUE WHEN in_order[i];
            check_code = 'G10';
            check_title = 'Markers not in the order of the road';
            severity = 'error';
            road_code = road.road_code;
            edge_ids = CASE WHEN marker_edge_ids[i] IS NULL THEN NULL ELSE ARRAY[marker_edge_ids[i]] END;
            node_ids = NULL;
            marker_ids = ARRAY[marker_ids_list[i]];
            details = jsonb_build_object(
                'issue_type', CASE
                    WHEN marker_edge_ids[i] IS NULL THEN 'far_from_road'
                    WHEN marker_positions[i] IS NULL THEN 'edge_without_reference'
                    ELSE 'order'
                END,
                'marker_code', marker_codes[i],
                'marker_abscissa', marker_abscissas[i],
                'is_virtual', marker_virtuals[i],
                'position', round(marker_positions[i]::numeric, 1),
                -- Previous and next markers by code which are in the order of the road
                'previous_marker_code', (
                    SELECT marker_codes[k] FROM generate_series(i - 1, 1, -1) AS k WHERE in_order[k] LIMIT 1
                ),
                'previous_marker_position', (
                    SELECT round(marker_positions[k]::numeric, 1)
                    FROM generate_series(i - 1, 1, -1) AS k WHERE in_order[k] LIMIT 1
                ),
                'next_marker_code', (
                    SELECT marker_codes[k] FROM generate_series(i + 1, marker_count) AS k WHERE in_order[k] LIMIT 1
                ),
                'next_marker_position', (
                    SELECT round(marker_positions[k]::numeric, 1)
                    FROM generate_series(i + 1, marker_count) AS k WHERE in_order[k] LIMIT 1
                )
            );
            fix_action = NULL;
            fix_params = NULL;
            geom = marker_geoms[i];
            geom_detail = NULL;
            RETURN NEXT;
        END LOOP;
    END LOOP;
    RETURN;
END;
$$;


-- FUNCTION check_graph_markers_order(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_markers_order(_road_codes text[], _options jsonb) IS 'Check G10 run by the function check_graph: find the markers which are not in the order of their road.

The position of each marker along its road is the cumulative length of the road at its closest edge of the road,
less than the option marker_tolerance from it (50 m by default). Sorted by code and abscissa, the markers must be
in the order of their position: two markers can be at the same position (1 m tolerance). The longest sequence of markers in the order of the road is kept:
an issue is returned for each other marker. The roundabouts are ignored.
An issue is also returned for a marker far from its road, or whose closest edge has no reference.

The details give the type of issue (order, far_from_road or edge_without_reference), the code, abscissa,
type and position of the marker, and the code and position of the previous and next markers in the order of the road.
The issue must be fixed manually, by moving the marker or changing its code.';


-- check_graph_parallel_edges(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_parallel_edges(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between two parallel edges
    tolerance real = Coalesce((_options->>'tolerance')::real, 30.0);
    -- Minimum length along which the two edges are parallel
    min_length real = Coalesce((_options->>'min_length')::real, 10.0);
BEGIN
    RETURN QUERY
    WITH
    -- Pairs of edges of the same road less than the tolerance from each other, not connected by a node,
    -- and the parts of each edge less than the tolerance from the other edge.
    -- The roundabouts are ignored
    pairs AS (
        SELECT
            a.road_code, a.id AS a_id, b.id AS b_id, a.geom AS a_geom, b.geom AS b_geom,
            ST_Distance(a.geom, b.geom) AS distance,
            -- The parts are segmentized, to locate their points along the other edge
            ST_Segmentize(ST_Intersection(a.geom, ST_Buffer(b.geom, tolerance)), 2) AS a_part,
            ST_Segmentize(ST_Intersection(b.geom, ST_Buffer(a.geom, tolerance)), 2) AS b_part
        FROM road_graph.edges AS a
        JOIN road_graph.edges AS b
            ON b.road_code = a.road_code
            AND b.id > a.id
            AND ST_DWithin(a.geom, b.geom, tolerance)
        JOIN road_graph.roads AS r
            ON r.road_code = a.road_code
        WHERE r.road_type != 'roundabout'
        AND (_road_codes IS NULL OR a.road_code = ANY(_road_codes))
        AND NOT (ARRAY[a.start_node, a.end_node] && ARRAY[b.start_node, b.end_node])
    ),
    -- Length of the other edge along which each part runs: the parts of two edges which only approach each other,
    -- for example the edges of a road on both sides of a roundabout, run along a very short length of the other edge
    spans AS (
        SELECT
            p.*,
            (
                SELECT (max(ST_LineLocatePoint(p.b_geom, d.geom)) - min(ST_LineLocatePoint(p.b_geom, d.geom)))
                    * ST_Length(p.b_geom)
                FROM ST_DumpPoints(p.a_part) AS d
            ) AS a_span,
            (
                SELECT (max(ST_LineLocatePoint(p.a_geom, d.geom)) - min(ST_LineLocatePoint(p.a_geom, d.geom)))
                    * ST_Length(p.a_geom)
                FROM ST_DumpPoints(p.b_part) AS d
            ) AS b_span
        FROM pairs AS p
        WHERE ST_Length(p.a_part) >= min_length
        AND ST_Length(p.b_part) >= min_length
    )
    SELECT
        'G07'::text,
        'Parallel edges of the same road'::text,
        'warning'::text,
        s.road_code,
        ARRAY[s.a_id, s.b_id],
        NULL::integer[],
        NULL::integer[],
        jsonb_build_object(
            'distance', round(s.distance::numeric, 2),
            'parallel_length', round(least(s.a_span, s.b_span)::numeric, 1)
        ),
        NULL::text,
        NULL::jsonb,
        ST_ClosestPoint(s.a_geom, s.b_geom),
        ST_Collect(s.a_part, s.b_part)
    FROM spans AS s
    WHERE s.a_span >= min_length
    AND s.b_span >= min_length
    ORDER BY 4, s.a_id, s.b_id
    ;
END;
$$;


-- FUNCTION check_graph_parallel_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_parallel_edges(_road_codes text[], _options jsonb) IS 'Check G07 run by the function check_graph: find the edges of the same road running parallel to each other,
for example the two carriageways of a road using the same road code.

An issue is returned for each pair of edges of the same road, not connected by a node, which run along each other
less than the option tolerance from each other (30 m by default), along at least the option min_length (10 m by default).
The edges which only approach each other, for example on both sides of a roundabout, are ignored. The roundabouts are ignored.
The details give the distance between the edges and the length along which they are parallel.
The line of the issue shows the parallel parts of the edges.

The issue must be fixed manually, for example by giving another road code to one of the carriageways.';


-- check_graph_reversed_edges(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_reversed_edges(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between a marker and the edges of its road
    marker_tolerance real = Coalesce((_options->>'marker_tolerance')::real, 10.0);
BEGIN
    RETURN QUERY
    WITH RECURSIVE
    -- Edges of the checked roads
    road_edges AS MATERIALIZED (
        SELECT
            e.id, e.road_code, e.geom,
            ST_Length(e.geom) AS edge_length,
            r.road_type = 'roundabout' AS is_roundabout,
            -- Nodes of the start and end points of the edge.
            -- The geometry gives the direction of the edge: the start and end nodes
            -- may have not been swapped when the geometry has been reversed
            CASE WHEN ST_DWithin(ST_StartPoint(e.geom), sn.geom, 0.01) THEN e.start_node ELSE e.end_node END
                AS first_node,
            CASE WHEN ST_DWithin(ST_StartPoint(e.geom), sn.geom, 0.01) THEN e.end_node ELSE e.start_node END
                AS last_node
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r
            ON r.road_code = e.road_code
        JOIN road_graph.nodes AS sn
            ON sn.id = e.start_node
        WHERE (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
        -- The loops cannot be in the opposite direction of their neighbours
        AND e.start_node != e.end_node
    ),
    -- Start and end points of the edges, with the number of edges of the road at their node
    edge_ends AS MATERIALIZED (
        SELECT
            x.*,
            count(*) OVER (PARTITION BY x.road_code, x.node_id) AS node_edge_count
        FROM (
            SELECT e.id, e.road_code, e.first_node AS node_id, True AS is_first
            FROM road_edges AS e
            UNION ALL
            SELECT e.id, e.road_code, e.last_node AS node_id, False AS is_first
            FROM road_edges AS e
        ) AS x
    ),
    -- Walks along the sequences of edges of a road following each other:
    -- two edges follow each other at a node where there are only these two edges of the road.
    -- A walk starts from an end of a sequence (a node with one edge or more than two edges of the road),
    -- and follows the next edges. Each edge is walked in its direction (is_forward) or not
    walks AS (
        SELECT
            x.id AS walk_id, x.is_first AS is_walk_forward,
            x.id, x.is_first AS is_forward,
            1 AS edge_order, 0::double precision AS start_position,
            -- Node by which the walk leaves the edge
            CASE WHEN x.is_first THEN e.last_node ELSE e.first_node END AS exit_node,
            ARRAY[x.id] AS walked_ids
        FROM edge_ends AS x
        JOIN road_edges AS e
            ON e.id = x.id
        WHERE x.node_edge_count != 2
        UNION ALL
        SELECT
            w.walk_id, w.is_walk_forward,
            n.id, n.is_first,
            w.edge_order + 1,
            w.start_position + p.edge_length,
            CASE WHEN n.is_first THEN e.last_node ELSE e.first_node END,
            w.walked_ids || n.id
        FROM walks AS w
        JOIN road_edges AS p
            ON p.id = w.id
        JOIN edge_ends AS n
            ON n.road_code = p.road_code
            AND n.node_id = w.exit_node
            AND n.node_edge_count = 2
            AND n.id != w.id
        JOIN road_edges AS e
            ON e.id = n.id
        WHERE NOT n.id = ANY(w.walked_ids)
    ),
    -- Each sequence is walked from its two ends: keep the walk starting from the edge with the lowest id,
    -- or for a sequence of one edge, the walk in the direction of the edge
    walk_ends AS (
        SELECT
            w.walk_id, w.is_walk_forward,
            (array_agg(w.id ORDER BY w.edge_order DESC))[1] AS last_id
        FROM walks AS w
        GROUP BY w.walk_id, w.is_walk_forward
    ),
    sequence_edges AS MATERIALIZED (
        SELECT
            w.walk_id AS sequence_id, w.id, w.is_forward, w.edge_order, w.start_position,
            e.road_code, e.geom, e.edge_length, e.is_roundabout
        FROM walks AS w
        JOIN walk_ends AS we
            ON we.walk_id = w.walk_id
            AND we.is_walk_forward = w.is_walk_forward
        JOIN road_edges AS e
            ON e.id = w.id
        WHERE we.walk_id < we.last_id
        OR (we.walk_id = we.last_id AND we.is_walk_forward)
    ),
    -- Position of the markers of the road along the sequences, in the direction of the walk,
    -- from their closest edge of the sequence. The markers of the roundabouts are not used
    marker_positions AS (
        SELECT
            se.sequence_id, m.id AS marker_id, m.code, m.abscissa,
            se.start_position + se.edge_length * CASE
                WHEN se.is_forward THEN ST_LineLocatePoint(se.geom, m.geom)
                ELSE 1 - ST_LineLocatePoint(se.geom, m.geom)
            END AS position
        FROM road_graph.markers AS m
        -- Closest edge of the road, found with the spatial index of the edges
        JOIN LATERAL (
            SELECT e.id
            FROM road_graph.edges AS e
            WHERE e.road_code = m.road_code
            AND ST_DWithin(e.geom, m.geom, marker_tolerance)
            ORDER BY ST_Distance(e.geom, m.geom), e.id
            LIMIT 1
        ) AS c ON True
        JOIN sequence_edges AS se
            ON se.id = c.id
            AND NOT se.is_roundabout
        WHERE _road_codes IS NULL OR m.road_code = ANY(_road_codes)
    ),
    -- Consecutive markers by code and abscissa: they must be in the direction of the road
    marker_pairs AS (
        SELECT
            p.sequence_id,
            p.position < lead(p.position) OVER w AS is_increasing,
            p.position > lead(p.position) OVER w AS is_decreasing
        FROM marker_positions AS p
        WINDOW w AS (PARTITION BY p.sequence_id ORDER BY p.code, p.abscissa)
    ),
    marker_votes AS (
        SELECT
            mp.sequence_id,
            count(*) FILTER (WHERE mp.is_increasing) AS increasing_count,
            count(*) FILTER (WHERE mp.is_decreasing) AS decreasing_count
        FROM marker_pairs AS mp
        GROUP BY mp.sequence_id
    ),
    -- Direction of each sequence: the direction of most of the consecutive markers,
    -- or else the direction of most of the length of the edges.
    -- The edges in the other direction are reversed
    sequence_directions AS (
        SELECT
            s.sequence_id,
            -- Number of consecutive markers in the direction of the road, and in the opposite direction
            CASE
                WHEN Coalesce(v.increasing_count, 0) >= Coalesce(v.decreasing_count, 0)
                    THEN Coalesce(v.increasing_count, 0)
                ELSE v.decreasing_count
            END AS increasing_count,
            CASE
                WHEN Coalesce(v.increasing_count, 0) >= Coalesce(v.decreasing_count, 0)
                    THEN Coalesce(v.decreasing_count, 0)
                ELSE v.increasing_count
            END AS decreasing_count,
            CASE
                WHEN Coalesce(v.increasing_count, 0) != Coalesce(v.decreasing_count, 0) THEN 'markers'
                ELSE 'neighbours'
            END AS reason,
            CASE
                WHEN Coalesce(v.increasing_count, 0) > Coalesce(v.decreasing_count, 0) THEN True
                WHEN Coalesce(v.increasing_count, 0) < Coalesce(v.decreasing_count, 0) THEN False
                -- Without markers, the walk is in the direction of the road
                -- if most of the length of the edges is in the direction of the walk
                ELSE s.forward_length >= s.backward_length
            END AS is_walk_in_road_direction
        FROM (
            SELECT
                se.sequence_id,
                Coalesce(sum(se.edge_length) FILTER (WHERE se.is_forward), 0) AS forward_length,
                Coalesce(sum(se.edge_length) FILTER (WHERE NOT se.is_forward), 0) AS backward_length
            FROM sequence_edges AS se
            GROUP BY se.sequence_id
        ) AS s
        LEFT JOIN marker_votes AS v
            ON v.sequence_id = s.sequence_id
    ),
    -- Reversed edges, and groups of consecutive reversed edges along each sequence: one issue for each group
    reversed_edges AS (
        SELECT
            se.*, d.reason, d.increasing_count, d.decreasing_count,
            -- Order of the edge in the direction of the road
            CASE WHEN d.is_walk_in_road_direction THEN se.edge_order ELSE - se.edge_order END AS road_order,
            se.edge_order - row_number() OVER (PARTITION BY se.sequence_id ORDER BY se.edge_order) AS group_number
        FROM sequence_edges AS se
        JOIN sequence_directions AS d
            ON d.sequence_id = se.sequence_id
        WHERE se.is_forward != d.is_walk_in_road_direction
    )
    SELECT
        'G05'::text,
        'Edges in the opposite direction of the road'::text,
        'error'::text,
        r.road_code,
        array_agg(r.id ORDER BY r.road_order),
        NULL::integer[],
        NULL::integer[],
        jsonb_build_object(
            'reason', min(r.reason),
            'edge_count', count(*),
            'length', round(sum(r.edge_length)::numeric, 1),
            'markers_in_road_direction', max(r.increasing_count),
            'markers_in_opposite_direction', max(r.decreasing_count)
        ),
        'reverse_edges'::text,
        jsonb_build_object('edge_ids', array_agg(r.id ORDER BY r.road_order)),
        ST_LineInterpolatePoint((array_agg(r.geom ORDER BY r.road_order))[1], 0.5),
        ST_LineMerge(ST_Collect(r.geom))
    FROM reversed_edges AS r
    GROUP BY r.road_code, r.sequence_id, r.group_number
    ORDER BY 4, min(r.id)
    ;
END;
$$;


-- FUNCTION check_graph_reversed_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_reversed_edges(_road_codes text[], _options jsonb) IS 'Check G05 run by the function check_graph: find the edges in the opposite direction of their road.

The edges of a road are grouped in sequences: two edges follow each other at a node where there are only
these two edges of the road. The direction of each sequence is given by the order of the markers of the road
located along it (less than the option marker_tolerance from it, 10 m by default), or else by the direction
of most of the length of its edges. The markers of the roundabouts are not used.
The direction of the edges is given by their geometry, not by their start and end nodes.

An issue is returned for each group of consecutive edges in the opposite direction of their sequence.
The details give the reason (markers or neighbours), the number of edges and their length,
and the number of consecutive markers in the direction of the road and in the opposite direction.
The issue can be fixed with the action reverse_edges, which reverses the geometry of the edges.';


-- check_graph_road_branches(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_road_branches(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Start and end points of the edges of the roads, except the roundabouts.
    -- The edges starting and ending at the same node (loops) are ignored:
    -- the short ones are found by the check of the short edges
    edge_ends AS (
        SELECT e.id, e.road_code, e.start_node AS node_id, e.geom
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r ON r.road_code = e.road_code
        WHERE r.road_type != 'roundabout'
        AND e.start_node != e.end_node
        AND (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
        UNION
        SELECT e.id, e.road_code, e.end_node AS node_id, e.geom
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r ON r.road_code = e.road_code
        WHERE r.road_type != 'roundabout'
        AND e.start_node != e.end_node
        AND (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
    ),
    -- Nodes with more than two edges of the same road: the road has branches
    branch_nodes AS (
        SELECT x.road_code, x.node_id, array_agg(x.id ORDER BY x.id) AS node_edge_ids
        FROM edge_ends AS x
        GROUP BY x.road_code, x.node_id
        HAVING count(*) > 2
    ),
    -- Direction of the edges at the node: the azimuth of the edges arriving at the node at their end,
    -- and of the edges leaving the node at their start, given by their geometry
    node_edges AS (
        SELECT
            b.road_code, b.node_id, n.geom AS node_geom, x.id, x.geom,
            ST_DWithin(ST_EndPoint(x.geom), n.geom, 0.01) AS is_arriving,
            ST_DWithin(ST_StartPoint(x.geom), n.geom, 0.01) AS is_leaving,
            ST_Azimuth(ST_PointN(x.geom, ST_NPoints(x.geom) - 1), ST_EndPoint(x.geom)) AS end_azimuth,
            ST_Azimuth(ST_StartPoint(x.geom), ST_PointN(x.geom, 2)) AS start_azimuth
        FROM branch_nodes AS b
        JOIN road_graph.nodes AS n ON n.id = b.node_id
        JOIN edge_ends AS x ON x.road_code = b.road_code AND x.node_id = b.node_id
    ),
    -- Main line of the road at the node: the edge arriving at the node and the edge leaving it
    -- which are the most aligned. The other edges are the branches
    main_lines AS (
        SELECT DISTINCT ON (a.road_code, a.node_id)
            a.road_code, a.node_id, a.id AS arriving_id, l.id AS leaving_id
        FROM node_edges AS a
        JOIN node_edges AS l
            ON l.road_code = a.road_code
            AND l.node_id = a.node_id
            AND l.id != a.id
        WHERE a.is_arriving AND l.is_leaving
        -- Deflection angle between the two edges, from 0 (aligned) to pi
        ORDER BY
            a.road_code, a.node_id,
            abs(mod((l.start_azimuth - a.end_azimuth + 3 * pi())::numeric, (2 * pi())::numeric) - pi()),
            a.id, l.id
    )
    SELECT
        'G06'::text,
        'Road with branches'::text,
        'error'::text,
        b.road_code,
        b.node_edge_ids,
        ARRAY[b.node_id],
        NULL::integer[],
        jsonb_build_object(
            'edge_count', cardinality(b.node_edge_ids),
            'main_edge_ids', CASE
                WHEN m.arriving_id IS NULL THEN '[]'::jsonb
                ELSE jsonb_build_array(m.arriving_id, m.leaving_id)
            END,
            'branch_edge_ids', (
                SELECT jsonb_agg(x ORDER BY x)
                FROM unnest(b.node_edge_ids) AS x
                WHERE x IS DISTINCT FROM m.arriving_id AND x IS DISTINCT FROM m.leaving_id
            )
        ),
        NULL::text,
        NULL::jsonb,
        n.geom,
        (
            SELECT ST_Collect(e.geom ORDER BY e.id)
            FROM road_graph.edges AS e
            WHERE e.id = ANY(b.node_edge_ids)
            AND e.id IS DISTINCT FROM m.arriving_id
            AND e.id IS DISTINCT FROM m.leaving_id
        )
    FROM branch_nodes AS b
    JOIN road_graph.nodes AS n ON n.id = b.node_id
    LEFT JOIN main_lines AS m ON m.road_code = b.road_code AND m.node_id = b.node_id
    ORDER BY 4, b.node_id
    ;
END;
$$;


-- FUNCTION check_graph_road_branches(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_road_branches(_road_codes text[], _options jsonb) IS 'Check G06 run by the function check_graph: find the roads with branches, for example the ramps
or the carriageways of a road using the code of the road.

An issue is returned for each node with more than two edges of the same road. The roundabouts are ignored,
and the edges starting and ending at the same node.
The main line of the road at the node is given by the edge arriving at the node and the edge leaving it
which are the most aligned: the other edges are the branches. The details give the number of edges at the node,
and the ids of the edges of the main line and of the branches. The line of the issue shows the branches.

The issue must be fixed manually, for example by giving another road code to the edges of the branches.';


-- check_graph_short_edges(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_short_edges(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum length of a short edge
    min_length real = Coalesce((_options->>'min_length')::real, 3.0);
    -- Length of an edge reduced to a point: the coordinates are rounded to 10 cm
    zero_length real = 0.10;
BEGIN
    RETURN QUERY
    WITH
    -- Short edges, with the number of other edges at their start and end nodes
    short_edges AS (
        SELECT
            e.id, e.road_code, e.start_node, e.end_node, e.geom,
            ST_Length(e.geom) AS edge_length,
            (
                SELECT count(*)
                FROM road_graph.edges AS o
                WHERE o.id != e.id
                AND e.start_node IN (o.start_node, o.end_node)
            ) AS start_edge_count,
            (
                SELECT count(*)
                FROM road_graph.edges AS o
                WHERE o.id != e.id
                AND e.end_node IN (o.start_node, o.end_node)
            ) AS end_edge_count,
            (
                SELECT count(*)
                FROM road_graph.edges AS o
                WHERE o.road_code = e.road_code
            ) AS road_edge_count,
            -- The edges of the roundabouts, and the edges arriving at a roundabout or leaving it
            r.road_type = 'roundabout' OR EXISTS (
                SELECT 1
                FROM road_graph.edges AS o
                JOIN road_graph.roads AS ro ON ro.road_code = o.road_code
                WHERE ro.road_type = 'roundabout'
                AND o.id != e.id
                AND ARRAY[o.start_node, o.end_node] && ARRAY[e.start_node, e.end_node]
            ) AS is_roundabout_edge
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r ON r.road_code = e.road_code
        WHERE ST_Length(e.geom) < min_length
        AND (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
    ),
    -- The edges of the roundabouts, or arriving at a roundabout, are often short: they are ignored,
    -- unless they are reduced to a point.
    -- The node to keep is the node with the most edges, or else the node with the lowest id
    issues AS (
        SELECT
            s.*,
            s.edge_length < zero_length AS is_zero_length,
            CASE
                WHEN s.start_edge_count > s.end_edge_count THEN True
                WHEN s.start_edge_count < s.end_edge_count THEN False
                ELSE s.start_node <= s.end_node
            END AS keep_start
        FROM short_edges AS s
        WHERE NOT s.is_roundabout_edge OR s.edge_length < zero_length
    )
    SELECT
        'G08'::text,
        'Short edge'::text,
        CASE WHEN i.is_zero_length THEN 'error' ELSE 'warning' END,
        i.road_code,
        ARRAY[i.id],
        CASE WHEN i.keep_start THEN ARRAY[i.start_node, i.end_node] ELSE ARRAY[i.end_node, i.start_node] END,
        NULL::integer[],
        jsonb_build_object(
            'length', round(i.edge_length::numeric, 2),
            'start_edge_count', i.start_edge_count,
            'end_edge_count', i.end_edge_count,
            'is_roundabout_edge', i.is_roundabout_edge
        ),
        -- The only edge of a road is not removed: the road would not have any edge anymore
        CASE WHEN i.road_edge_count > 1 THEN 'collapse_edge' ELSE NULL END,
        CASE
            WHEN i.road_edge_count = 1 THEN NULL
            ELSE jsonb_build_object(
                'edge_id', i.id,
                'keep_node_id', CASE WHEN i.keep_start THEN i.start_node ELSE i.end_node END,
                'remove_node_id', CASE WHEN i.keep_start THEN i.end_node ELSE i.start_node END
            )
        END,
        ST_LineInterpolatePoint(i.geom, 0.5),
        i.geom
    FROM issues AS i
    ORDER BY 4, i.id
    ;
END;
$$;


-- FUNCTION check_graph_short_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_short_edges(_road_codes text[], _options jsonb) IS 'Check G08 run by the function check_graph: find the short edges, often coming from the digitizing,
for example two junctions a few decimeters from each other.

An issue is returned for each edge shorter than the option min_length (3 m by default).
The edges of the roundabouts and the edges arriving at a roundabout or leaving it are often short: they are ignored,
unless they are reduced to a point (less than 10 cm). The severity is error for an edge reduced to a point, else warning.
The details give the length, the number of other edges at the start and end nodes, and if the edge is near a roundabout.

The issue can be fixed with the action collapse_edge, which deletes the edge and merges its nodes,
adding its geometry to the next or previous edge of the same road: the node to keep is the node with the most edges, or else the node with the lowest id,
and is the first node of node_ids. The only edge of a road has no fix action.';


-- check_graph_t_junction_gaps(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_t_junction_gaps(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between the free end of an edge and another edge
    tolerance real = Coalesce((_options->>'tolerance')::real, 2.0);
    -- Distance used by the triggers to split an edge by a node, and to reuse an existing node
    node_tolerance real = 0.50;
    -- Maximum distance between two connected edges
    connected_tolerance real = 0.001;
BEGIN
    RETURN QUERY
    WITH
    -- Start and end points of the edges
    ends AS (
        SELECT e.id, e.road_code, e.geom AS edge_geom, 'start'::text AS side, e.start_node AS node_id,
            ST_StartPoint(e.geom) AS point, ST_EndPoint(e.geom) AS opposite_point
        FROM road_graph.edges AS e
        WHERE _road_codes IS NULL OR e.road_code = ANY(_road_codes)
        UNION ALL
        SELECT e.id, e.road_code, e.geom AS edge_geom, 'end'::text AS side, e.end_node AS node_id,
            ST_EndPoint(e.geom) AS point, ST_StartPoint(e.geom) AS opposite_point
        FROM road_graph.edges AS e
        WHERE _road_codes IS NULL OR e.road_code = ANY(_road_codes)
    ),
    -- Free ends: no other edge is connected to them
    free_ends AS (
        SELECT f.*
        FROM ends AS f
        WHERE NOT EXISTS (
            SELECT 1
            FROM road_graph.edges AS o
            WHERE o.id != f.id
            AND ST_DWithin(o.geom, f.point, connected_tolerance)
        )
    ),
    -- Closest other edge less than the tolerance from each free end. Are ignored:
    -- * the edges connected to the opposite end of the edge, for example its next edge if it is very short,
    -- * the edges crossed by the edge near its free end: the edge goes beyond the other edge,
    --   the crossing is found by the check G01
    gaps AS (
        SELECT DISTINCT ON (f.id, f.side)
            f.*,
            o.id AS other_id, o.road_code AS other_road_code,
            ST_Distance(o.geom, f.point) AS distance,
            ST_ClosestPoint(o.geom, f.point) AS closest_point,
            ST_StartPoint(o.geom) AS other_start, ST_EndPoint(o.geom) AS other_end
        FROM free_ends AS f
        JOIN road_graph.edges AS o
            ON o.id != f.id
            AND ST_DWithin(o.geom, f.point, tolerance)
            AND NOT ST_DWithin(o.geom, f.opposite_point, connected_tolerance)
            AND NOT (
                ST_Intersects(o.geom, f.edge_geom)
                AND ST_DWithin(ST_Intersection(o.geom, f.edge_geom), f.point, tolerance)
            )
        ORDER BY f.id, f.side, ST_Distance(o.geom, f.point)
    ),
    -- Point to join: the closest point of the other edge,
    -- or its start or end point if it is closer than the distance used by the triggers to split it
    targets AS (
        SELECT
            g.*,
            CASE
                WHEN ST_DWithin(g.closest_point, g.other_start, node_tolerance)
                    AND ST_Distance(g.closest_point, g.other_start) <= ST_Distance(g.closest_point, g.other_end)
                    THEN g.other_start
                WHEN ST_DWithin(g.closest_point, g.other_end, node_tolerance)
                    THEN g.other_end
                ELSE g.closest_point
            END AS target
        FROM gaps AS g
    ),
    -- Two free ends close to each other: keep only one issue
    issues AS (
        SELECT t.*
        FROM targets AS t
        WHERE NOT EXISTS (
            SELECT 1
            FROM free_ends AS f
            WHERE f.id = t.other_id
            AND ST_Equals(f.point, t.target)
            AND f.id < t.id
            AND ST_DWithin(f.point, t.point, tolerance)
        )
    )
    SELECT
        'G02'::text,
        'Edge ending near another edge without being connected'::text,
        'error'::text,
        i.road_code,
        ARRAY[i.id, i.other_id],
        CASE WHEN i.node_id IS NULL THEN NULL ELSE ARRAY[i.node_id] END,
        NULL::integer[],
        jsonb_build_object(
            'road_codes', jsonb_build_array(i.road_code, i.other_road_code),
            'distance', round(ST_Distance(i.point, i.target)::numeric, 2),
            -- The free end joins the other edge in its middle, or at its start or end point
            'join_type', CASE WHEN ST_Equals(i.target, i.closest_point) THEN 'edge' ELSE 'edge_end' END
        ),
        'extend_edge'::text,
        jsonb_build_object(
            'edge_id', i.id,
            'side', i.side,
            'point', ST_AsEWKT(i.point),
            'target', ST_AsEWKT(i.target)
        ),
        i.point,
        ST_MakeLine(i.point, i.target)
    FROM issues AS i
    ORDER BY i.road_code, i.id, i.side
    ;
END;
$$;


-- FUNCTION check_graph_t_junction_gaps(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_t_junction_gaps(_road_codes text[], _options jsonb) IS 'Check G02 run by the function check_graph: find the edges ending near another edge without being connected to it,
for example a road arriving on another road and stopping a few decimeters before it (T junction).

An issue is returned for each free end of an edge (no other edge is connected to it)
which is less than the tolerance from another edge: option tolerance, 2 m by default.
The edges connected to the opposite end of the edge are ignored, for example its next edge if it is very short,
and the edges crossed by the edge near its free end: the edge goes beyond the other edge, which is found by the check G01.
When two free ends are close to each other, only one issue is returned.

The point to join is the closest point of the other edge, or its start or end point if it is less than 50 cm from it.
The details give the road codes of the two edges, the distance, and the join type: edge or edge_end.
The issue can be fixed with the action extend_edge, which adds the point to join at the free end of the edge.';


-- check_graph_useless_nodes(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_useless_nodes(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Nodes with their edges. An edge starting and ending at the node (loop)
    -- is not counted as an edge arriving at or leaving the node
    nodes AS (
        SELECT
            n.id, n.geom,
            count(e.id) AS edge_count,
            array_agg(e.id ORDER BY e.id) FILTER (WHERE e.id IS NOT NULL) AS node_edge_ids,
            array_agg(DISTINCT e.road_code) FILTER (WHERE e.road_code IS NOT NULL) AS node_road_codes,
            count(e.id) FILTER (WHERE e.end_node = n.id AND e.start_node != n.id) AS arriving_count,
            count(e.id) FILTER (WHERE e.start_node = n.id AND e.end_node != n.id) AS leaving_count,
            (array_agg(e.geom) FILTER (WHERE e.end_node = n.id AND e.start_node != n.id))[1] AS arriving_geom,
            (array_agg(e.geom) FILTER (WHERE e.start_node = n.id AND e.end_node != n.id))[1] AS leaving_geom,
            count(DISTINCT Coalesce(e.no_intersection_cutting, False)) AS cutting_count,
            count(DISTINCT CASE WHEN e.start_node = n.id THEN e.end_node ELSE e.start_node END) AS other_node_count
        FROM road_graph.nodes AS n
        LEFT JOIN road_graph.edges AS e
            ON n.id IN (e.start_node, e.end_node)
        GROUP BY n.id, n.geom
    ),
    -- Nodes without edge, only when all the roads are checked,
    -- and nodes between two edges of the same road which can be merged:
    -- one edge arrives at the node and the other one leaves it,
    -- with the same value of no_intersection_cutting, they do not form a loop,
    -- and they only touch each other at the node: for example a loop ramp crossing itself cannot be merged
    issues AS (
        SELECT
            nd.*,
            CASE WHEN nd.edge_count = 0 THEN 'no_edge' ELSE 'two_edges' END AS node_type
        FROM nodes AS nd
        WHERE (nd.edge_count = 0 AND _road_codes IS NULL)
        OR (
            nd.edge_count = 2
            AND cardinality(nd.node_road_codes) = 1
            AND nd.arriving_count = 1
            AND nd.leaving_count = 1
            AND nd.cutting_count = 1
            AND nd.other_node_count = 2
            AND ST_Touches(nd.arriving_geom, nd.leaving_geom)
            AND (_road_codes IS NULL OR nd.node_road_codes && _road_codes)
        )
    )
    SELECT
        'G04'::text,
        'Useless node'::text,
        'warning'::text,
        i.node_road_codes[1],
        i.node_edge_ids,
        ARRAY[i.id],
        NULL::integer[],
        jsonb_build_object(
            'node_type', i.node_type,
            'road_codes', to_jsonb(Coalesce(i.node_road_codes, '{}'::text[]))
        ),
        'delete_node'::text,
        jsonb_build_object('node_id', i.id),
        i.geom,
        NULL::geometry
    FROM issues AS i
    ORDER BY 4, i.id
    ;
END;
$$;


-- FUNCTION check_graph_useless_nodes(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_useless_nodes(_road_codes text[], _options jsonb) IS 'Check G04 run by the function check_graph: find the useless nodes, which can be deleted.

An issue is returned for each node without edge (only when all the roads are checked),
and for each node between two edges of the same road which can be merged: one edge arrives at the node
and the other one leaves it, with the same value of no_intersection_cutting, they do not form a loop,
and they only touch each other at the node.
The details give the node type (no_edge or two_edges) and the road code of the edges.

The issue can be fixed with the action delete_node, which deletes the node: the triggers merge the two edges.';


-- fix_graph_collapse_edge(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_collapse_edge(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    keep_node record;
    remove_node_id integer;
    extended_edge_id integer;
BEGIN
    -- The edge may have been removed or modified by a previous fix
    SELECT e.*
    INTO edge
    FROM road_graph.edges AS e
    WHERE e.id = (_params->>'edge_id')::integer
    ;
    IF edge.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s does not exist anymore', _params->>'edge_id')
        );
    END IF;
    IF NOT ARRAY[edge.start_node, edge.end_node] @> ARRAY[
        (_params->>'keep_node_id')::integer, (_params->>'remove_node_id')::integer
    ] THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The nodes of the edge %s have changed', edge.id)
        );
    END IF;
    IF (SELECT count(*) FROM road_graph.edges AS e WHERE e.road_code = edge.road_code) = 1 THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format('The edge %s is the only edge of the road %s', edge.id, edge.road_code)
        );
    END IF;
    -- An edge starting and ending at the same node, for example an edge reduced to a point
    -- after being cut, is only deleted
    IF edge.start_node = edge.end_node THEN
        DELETE FROM road_graph.edges AS e
        WHERE e.id = edge.id
        ;
        RETURN jsonb_build_object(
            'status', 'success',
            'message', format('The edge %s starting and ending at the node %s has been deleted', edge.id, edge.start_node)
        );
    END IF;

    SELECT n.*
    INTO keep_node
    FROM road_graph.nodes AS n
    WHERE n.id = (_params->>'keep_node_id')::integer
    ;
    remove_node_id = (_params->>'remove_node_id')::integer;

    -- The edges extended or moved below run along the short edge and pass near the node to remove:
    -- the variables prevent the triggers from splitting them at the crossings with the short edge
    -- and at the node to remove (see after_edge_insert_or_update)
    PERFORM set_config('road.graph.edge.crossing.node.creation.pending', 'yes', true);
    PERFORM set_config('road.graph.merge.edges.useless.node', remove_node_id::text, true);

    -- The edge of the same road following the short edge at the node to remove, if there is only one,
    -- is extended with the geometry of the short edge up to the node to keep: the geometry of the road is kept
    SELECT e.id
    INTO extended_edge_id
    FROM road_graph.edges AS e
    WHERE e.road_code = edge.road_code
    AND e.id != edge.id
    AND (
        (remove_node_id = edge.end_node AND e.start_node = remove_node_id)
        OR (remove_node_id = edge.start_node AND e.end_node = remove_node_id)
    )
    AND (
        SELECT count(*)
        FROM road_graph.edges AS x
        WHERE x.road_code = edge.road_code
        AND x.id != edge.id
        AND remove_node_id IN (x.start_node, x.end_node)
    ) = 1
    ;
    IF extended_edge_id IS NOT NULL THEN
        UPDATE road_graph.edges AS e
        SET geom = ST_RemoveRepeatedPoints(
            CASE
                WHEN remove_node_id = edge.end_node THEN ST_MakeLine(edge.geom, e.geom)
                ELSE ST_MakeLine(e.geom, edge.geom)
            END
        )
        WHERE e.id = extended_edge_id
        ;
    END IF;

    -- Move the start and end points of the other edges of the node to remove to the node to keep,
    -- for example the edges of the other roads: the triggers use the closest node, i.e. the node to keep
    UPDATE road_graph.edges AS e
    SET geom = ST_SetPoint(e.geom, 0, keep_node.geom)
    WHERE e.start_node = remove_node_id
    AND e.id != edge.id
    ;
    UPDATE road_graph.edges AS e
    SET geom = ST_SetPoint(e.geom, ST_NPoints(e.geom) - 1, keep_node.geom)
    WHERE e.end_node = remove_node_id
    AND e.id != edge.id
    ;
    PERFORM set_config('road.graph.edge.crossing.node.creation.pending', 'no', true);
    PERFORM set_config('road.graph.merge.edges.useless.node', '-1', true);

    -- Delete the short edge, then the node to remove if it is not used anymore
    DELETE FROM road_graph.edges AS e
    WHERE e.id = edge.id
    ;
    DELETE FROM road_graph.nodes AS n
    WHERE n.id = remove_node_id
    AND NOT EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE remove_node_id IN (e.start_node, e.end_node)
    )
    ;

    -- Check no edge uses the node to remove anymore.
    -- If not, the exception cancels the changes of this fix (see fix_graph_issue)
    IF EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE remove_node_id IN (e.start_node, e.end_node)
    ) THEN
        RAISE EXCEPTION 'The edges of the node % could not be moved to the node %', remove_node_id, keep_node.id;
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The edge %s has been deleted%s, and the edges of the node %s have been moved to the node %s',
            edge.id,
            CASE
                WHEN extended_edge_id IS NULL THEN ''
                ELSE format(' and its geometry added to the edge %s', extended_edge_id)
            END,
            remove_node_id, keep_node.id
        )
    );
END;
$$;


-- FUNCTION fix_graph_collapse_edge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_collapse_edge(_params jsonb) IS 'Action collapse_edge run by the function fix_graph_issue, to fix the issues of the check G08:
remove a short edge, by merging its two nodes.

The parameter _params must contain the ids of the edge, of the node to keep and of the node to remove:
{"edge_id": 12, "keep_node_id": 34, "remove_node_id": 56}.
The edge of the same road following the short edge at the node to remove is extended with the geometry
of the short edge up to the node to keep: the geometry of the road is kept. The start and end points
of the other edges of the node to remove, for example the edges of the other roads, are moved to the node to keep.
Then the edge and the node to remove are deleted. The only edge of a road is not removed.
An edge starting and ending at the same node, for example an edge reduced to a point after being cut, is only deleted.

It returns a JSON object with the keys status (success, skipped if the edge does not exist anymore
or its nodes have changed, or error) and message.';


-- fix_graph_cut_edges(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_cut_edges(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Distance used by the triggers to split an edge by a node, and to reuse an existing node
    tolerance real = 0.50;
    cut_point geometry;
    node_id integer;
BEGIN
    cut_point = ST_GeomFromEWKT(_params->>'point');
    IF cut_point IS NULL THEN
        RETURN jsonb_build_object('status', 'error', 'message', 'The parameter point is missing');
    END IF;

    -- The issue has already been fixed if no edge must be split at this point:
    -- no edge which can be cut passes there without starting or ending there
    IF NOT EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE ST_DWithin(e.geom, cut_point, tolerance)
        AND NOT ST_DWithin(ST_StartPoint(e.geom), cut_point, tolerance)
        AND NOT ST_DWithin(ST_EndPoint(e.geom), cut_point, tolerance)
        AND NOT Coalesce(e.no_intersection_cutting, False)
    ) THEN
        RETURN jsonb_build_object('status', 'skipped', 'message', 'No edge to split at this point');
    END IF;

    -- Use the existing node close to the cut_point, if any: touching it splits the edges under it
    SELECT n.id
    INTO node_id
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, cut_point, tolerance)
    ORDER BY n.geom <-> cut_point
    LIMIT 1
    ;
    IF node_id IS NOT NULL THEN
        SET LOCAL road.graph.edge.update.touching.node = 'yes';
        UPDATE road_graph.nodes AS n
        SET geom = n.geom
        WHERE n.id = node_id
        ;
        SET LOCAL road.graph.edge.update.touching.node = 'no';
        RETURN jsonb_build_object(
            'status', 'success',
            'message', format('The edges have been split by the existing node %s', node_id)
        );
    END IF;

    -- Create a node at the point: the triggers split the edges under it
    INSERT INTO road_graph.nodes (geom)
    VALUES (cut_point)
    RETURNING id INTO node_id
    ;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format('The edges have been split by the new node %s', node_id)
    );
END;
$$;


-- FUNCTION fix_graph_cut_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_cut_edges(_params jsonb) IS 'Action cut_edges run by the function fix_graph_issue, to fix the issues of the check G01:
split the edges crossing each other at the given point.

The parameter _params must contain the crossing point as EWKT: {"point": "SRID=2154;POINT(x y)"}.
A node is created at this point, or the existing node less than 50 cm from it is used,
and the triggers split the edges passing there.

It returns a JSON object with the keys status (success, or skipped if no edge must be split at this point) and message.';


-- fix_graph_delete_node(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_delete_node(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    node record;
    edges_data record;
    kept_edge record;
BEGIN
    -- The node may have been deleted by a previous fix
    SELECT n.*
    INTO node
    FROM road_graph.nodes AS n
    WHERE n.id = (_params->>'node_id')::integer
    ;
    IF node.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The node %s does not exist anymore', _params->>'node_id')
        );
    END IF;

    -- Edges of the node: the edge arriving at the node and the edge leaving it
    SELECT
        count(e.id) AS edge_count,
        count(DISTINCT e.road_code) AS road_code_count,
        count(DISTINCT Coalesce(e.no_intersection_cutting, False)) AS cutting_count,
        (array_agg(e.id) FILTER (WHERE e.end_node = node.id AND e.start_node != node.id))[1] AS arriving_edge_id,
        (array_agg(e.start_node) FILTER (WHERE e.end_node = node.id AND e.start_node != node.id))[1] AS start_node_id,
        (array_agg(e.geom) FILTER (WHERE e.end_node = node.id AND e.start_node != node.id))[1] AS arriving_geom,
        (array_agg(e.id) FILTER (WHERE e.start_node = node.id AND e.end_node != node.id))[1] AS leaving_edge_id,
        (array_agg(e.end_node) FILTER (WHERE e.start_node = node.id AND e.end_node != node.id))[1] AS end_node_id,
        (array_agg(e.geom) FILTER (WHERE e.start_node = node.id AND e.end_node != node.id))[1] AS leaving_geom
    INTO edges_data
    FROM road_graph.edges AS e
    WHERE node.id IN (e.start_node, e.end_node)
    ;

    -- Node without edge: delete it
    IF edges_data.edge_count = 0 THEN
        DELETE FROM road_graph.nodes AS n
        WHERE n.id = node.id
        ;
        RETURN jsonb_build_object(
            'status', 'success',
            'message', format('The node %s without edge has been deleted', node.id)
        );
    END IF;

    -- The node may have been connected to other edges by a previous fix
    IF NOT (
        edges_data.edge_count = 2
        AND edges_data.road_code_count = 1
        AND edges_data.cutting_count = 1
        AND edges_data.arriving_edge_id IS NOT NULL
        AND edges_data.leaving_edge_id IS NOT NULL
        AND edges_data.start_node_id != edges_data.end_node_id
    ) THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The node %s is not useless anymore', node.id)
        );
    END IF;

    -- The two edges must only touch each other at the node:
    -- an edge crossing the other one cannot be merged
    IF NOT ST_Touches(edges_data.arriving_geom, edges_data.leaving_geom) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format(
                'The edges %s and %s cross each other and cannot be merged: fix the issues of the check G01 first',
                edges_data.arriving_edge_id, edges_data.leaving_edge_id
            )
        );
    END IF;

    -- Delete the node: the trigger before_node_delete merges the two edges into the edge with the lowest id
    -- An error of the triggers, for example a node still used by an edge, cancels the changes of this fix
    BEGIN
        DELETE FROM road_graph.nodes AS n
        WHERE n.id = node.id
        ;
    EXCEPTION WHEN foreign_key_violation THEN
        RAISE EXCEPTION
            'The edges % and % cannot be merged: fix the issues of the checks G01 and G03 first',
            edges_data.arriving_edge_id, edges_data.leaving_edge_id
        ;
    END;

    -- Check the merged edge goes from the start node of the arriving edge to the end node of the leaving edge.
    -- If not, for example if the merged edge has been split by the triggers at a crossing or near another node,
    -- the exception cancels the changes of this fix (see fix_graph_issue)
    SELECT e.*
    INTO kept_edge
    FROM road_graph.edges AS e
    WHERE e.id IN (edges_data.arriving_edge_id, edges_data.leaving_edge_id)
    ;
    IF kept_edge.id IS NULL
        OR (
            SELECT count(*)
            FROM road_graph.edges AS e
            WHERE e.id IN (edges_data.arriving_edge_id, edges_data.leaving_edge_id)
        ) != 1
        OR kept_edge.start_node IS DISTINCT FROM edges_data.start_node_id
        OR kept_edge.end_node IS DISTINCT FROM edges_data.end_node_id
    THEN
        RAISE EXCEPTION
            'The edges % and % cannot be merged without other changes of the graph, for example because they cross other edges or pass near other nodes: fix the issues of the checks G01 and G03 first',
            edges_data.arriving_edge_id, edges_data.leaving_edge_id
        ;
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The node %s has been deleted, and the edges %s and %s have been merged into the edge %s',
            node.id, edges_data.arriving_edge_id, edges_data.leaving_edge_id, kept_edge.id
        )
    );
END;
$$;


-- FUNCTION fix_graph_delete_node(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_delete_node(_params jsonb) IS 'Action delete_node run by the function fix_graph_issue, to fix the issues of the check G04:
delete a useless node.

The parameter _params must contain the id of the node: {"node_id": 12}.
A node without edge is deleted. A node between two edges of the same road is deleted
if one edge arrives at the node and the other one leaves it: the trigger before_node_delete merges the two edges
into the edge with the lowest id. If the merged edge is not between the start node of the first edge
and the end node of the second edge, for example because it has been split at a crossing with another edge,
an exception is raised: the function fix_graph_issue cancels the changes and returns an error.

It returns a JSON object with the keys status (success, skipped if the node does not exist anymore
or is not useless anymore, or error) and message.';


-- fix_graph_extend_edge(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_extend_edge(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between two connected edges
    connected_tolerance real = 0.001;
    -- Distance used by the triggers to reuse an existing node for the start or end point of an edge
    node_tolerance real = 0.50;
    -- Precision of the geometries: the coordinates of the nodes and edges are rounded to 10 cm
    precision_tolerance real = 0.10;
    free_end geometry;
    target geometry;
    edge record;
    free_node_id integer;
    target_node record;
BEGIN
    free_end = ST_GeomFromEWKT(_params->>'point');
    target = ST_GeomFromEWKT(_params->>'target');
    IF free_end IS NULL OR target IS NULL OR _params->>'side' NOT IN ('start', 'end') THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', 'The parameters edge_id, side, point and target are needed'
        );
    END IF;

    -- The edge must still end at the free end
    SELECT e.*
    INTO edge
    FROM road_graph.edges AS e
    WHERE e.id = (_params->>'edge_id')::integer
    AND ST_DWithin(
        CASE WHEN _params->>'side' = 'start' THEN ST_StartPoint(e.geom) ELSE ST_EndPoint(e.geom) END,
        free_end,
        connected_tolerance
    )
    ;
    IF edge.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s does not end anymore at the given point', _params->>'edge_id')
        );
    END IF;

    -- The free end may have been connected by a previous fix
    IF EXISTS (
        SELECT 1
        FROM road_graph.edges AS o
        WHERE o.id != edge.id
        AND ST_DWithin(o.geom, free_end, connected_tolerance)
    ) THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s is already connected at this point', edge.id)
        );
    END IF;
    free_node_id = CASE WHEN _params->>'side' = 'start' THEN edge.start_node ELSE edge.end_node END;

    -- Existing node at the point to join, for example the start or end point of the other edge
    SELECT n.id, n.geom
    INTO target_node
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, target, precision_tolerance)
    AND n.id IS DISTINCT FROM free_node_id
    ORDER BY n.geom <-> target, n.id
    LIMIT 1
    ;

    IF target_node.id IS NOT NULL THEN
        -- Connect the free end to this node. If the free end is less than 50 cm from it,
        -- the free end is replaced by the node: a point added would be too close to the node of the free end,
        -- which would be used again by the triggers
        UPDATE road_graph.edges AS e
        SET geom = CASE
            WHEN ST_DWithin(free_end, target_node.geom, node_tolerance) AND _params->>'side' = 'start'
                THEN ST_SetPoint(e.geom, 0, target_node.geom)
            WHEN ST_DWithin(free_end, target_node.geom, node_tolerance)
                THEN ST_SetPoint(e.geom, ST_NPoints(e.geom) - 1, target_node.geom)
            WHEN _params->>'side' = 'start' THEN ST_AddPoint(e.geom, target_node.geom, 0)
            ELSE ST_AddPoint(e.geom, target_node.geom)
        END
        WHERE e.id = edge.id
        ;
        -- Delete the node of the free end, which is not used anymore
        DELETE FROM road_graph.nodes AS n
        WHERE n.id = free_node_id
        AND NOT EXISTS (
            SELECT 1
            FROM road_graph.edges AS e
            WHERE n.id IN (e.start_node, e.end_node)
        )
        ;
    ELSIF ST_DWithin(free_end, target, node_tolerance) THEN
        -- The free end is less than 50 cm from the other edge: move the node of the free end to the point to join.
        -- The triggers move the free end with the node, and split the other edge at the node.
        -- The variables force the split even if the node does not move (the point to join is rounded to 10 cm),
        -- and even if the other edge must not be cut (see after_node_insert_or_update)
        SET LOCAL road.graph.edge.update.touching.node = 'yes';
        SET LOCAL road.graph.node.edge.extremity = 'yes';
        UPDATE road_graph.nodes AS n
        SET geom = target
        WHERE n.id = free_node_id
        ;
        SET LOCAL road.graph.edge.update.touching.node = 'no';
        SET LOCAL road.graph.node.edge.extremity = 'no';
    ELSE
        -- Add the point to join at the free end of the edge:
        -- the triggers move the node of the free end to this point, and split the other edge there
        UPDATE road_graph.edges AS e
        SET geom = CASE
            WHEN _params->>'side' = 'start' THEN ST_AddPoint(e.geom, target, 0)
            ELSE ST_AddPoint(e.geom, target)
        END
        WHERE e.id = edge.id
        ;
    END IF;

    -- Check the free end is now connected to another edge.
    -- If not, the exception cancels the changes of this fix (see fix_graph_issue)
    IF NOT EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        JOIN road_graph.edges AS o
            ON o.id != e.id
            AND CASE WHEN _params->>'side' = 'start' THEN e.start_node ELSE e.end_node END IN (o.start_node, o.end_node)
        WHERE e.id = edge.id
    ) THEN
        RAISE EXCEPTION 'The edge % could not be connected to the other edge', edge.id;
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The edge %s has been extended by %s m to the other edge',
            edge.id, round(ST_Distance(free_end, target)::numeric, 2)
        )
    );
END;
$$;


-- FUNCTION fix_graph_extend_edge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_extend_edge(_params jsonb) IS 'Action extend_edge run by the function fix_graph_issue, to fix the issues of the check G02:
extend an edge from its free end to the point to join on another edge.

The parameter _params must contain the edge id, the side of the free end (start or end),
the free end and the point to join as EWKT:
{"edge_id": 12, "side": "end", "point": "SRID=2154;POINT(x y)", "target": "SRID=2154;POINT(x y)"}.
When the point to join is the start or end point of the other edge, the free end is connected to its node,
and the node of the free end is deleted. Else, when the free end is less than 50 cm from the other edge,
the node of the free end is moved to the point to join. Else, the point to join is added at the free end of the edge,
and the triggers move the node of the free end to this point. In the two last cases, the triggers split the other edge there.
If the free end is not connected to another edge afterwards, an exception is raised:
the function fix_graph_issue cancels the changes and returns an error.

It returns a JSON object with the keys status (success, or skipped if the edge does not end at the given point anymore,
or is already connected there) and message.';


-- fix_graph_issue(text, jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_issue(_fix_action text, _params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
DECLARE
    schema_name text;
    sql_text text;
    result jsonb;
BEGIN
    -- The issues must be fixed in an editing session:
    -- the name of the schema of the edges table is replaced when the function is installed in the editing schema
    SELECT n.nspname
    INTO schema_name
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = 'road_graph.edges'::regclass
    ;
    IF schema_name = 'road_graph' THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', 'The road graph issues must be fixed in an editing session, with the function road_graph_editing.fix_graph_issue'
        );
    END IF;

    -- Check the action is known
    IF NOT EXISTS (
        SELECT 1
        FROM "road_graph".glossary_graph_check AS g
        WHERE _fix_action = ANY(g.fix_actions)
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format('Unknown action to fix the road graph: %L', _fix_action)
        );
    END IF;

    -- Run the function of the action, named fix_graph_ followed by the action name
    -- It returns a JSON object with the keys status (success, skipped or error) and message
    -- An error does not stop the fixes of the other issues: the changes of this action are cancelled
    sql_text = format('SELECT road_graph.%I($1)', 'fix_graph_' || _fix_action);
    BEGIN
        EXECUTE sql_text
        INTO result
        USING Coalesce(_params, '{}'::jsonb);
    EXCEPTION WHEN OTHERS THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', SQLERRM
        );
    END;

    RETURN result;
END;
$_$;


-- FUNCTION fix_graph_issue(_fix_action text, _params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_issue(_fix_action text, _params jsonb) IS 'Fix an issue of the road graph found by the function check_graph, with the given action and parameters.

The issues must be fixed in an editing session: the function only works in the editing schema,
with road_graph_editing.fix_graph_issue. It runs the function named fix_graph_ followed by the action name.

The issue may have been fixed by a previous action, for example when several issues concern the same edges:
the action then does nothing and returns the status skipped. If the action fails, its changes are cancelled
and the status error is returned with the error message.

It returns a JSON object with the keys status (success, skipped or error) and message.';


-- fix_graph_mark_as_bridge(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_mark_as_bridge(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between the edges and the crossing point
    tolerance real = 0.50;
    cut_point geometry;
    marked_ids integer[];
BEGIN
    cut_point = ST_GeomFromEWKT(_params->>'point');
    IF cut_point IS NULL THEN
        RETURN jsonb_build_object('status', 'error', 'message', 'The parameter point is missing');
    END IF;

    -- Set no_intersection_cutting to True for the given edge, or for all the given edges,
    -- if they still pass by the crossing point
    WITH marked AS (
        UPDATE road_graph.edges AS e
        SET no_intersection_cutting = True
        WHERE ST_DWithin(e.geom, cut_point, tolerance)
        AND NOT Coalesce(e.no_intersection_cutting, False)
        AND (
            CASE
                WHEN _params ? 'edge_id' THEN e.id = (_params->>'edge_id')::integer
                ELSE e.id IN (SELECT jsonb_array_elements_text(_params->'edge_ids')::integer)
            END
        )
        RETURNING e.id
    )
    SELECT array_agg(m.id ORDER BY m.id)
    INTO marked_ids
    FROM marked AS m
    ;

    IF marked_ids IS NULL THEN
        RETURN jsonb_build_object('status', 'skipped', 'message', 'No edge to mark at this point');
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format('The field no_intersection_cutting has been set to True for the edges %s', array_to_string(marked_ids, ', '))
    );
END;
$$;


-- FUNCTION fix_graph_mark_as_bridge(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_mark_as_bridge(_params jsonb) IS 'Action mark_as_bridge run by the function fix_graph_issue, to fix the issues of the check G01
when the edges must not be connected, for example on a bridge or in a tunnel.

It sets the field no_intersection_cutting to True for the edges passing by the crossing point:
the given edge if the parameter _params contains edge_id, or else all the edges of edge_ids:
{"point": "SRID=2154;POINT(x y)", "edge_ids": [12, 34], "edge_id": 12}.

It returns a JSON object with the keys status (success, or skipped if the edges are already marked) and message.';


-- fix_graph_merge_nodes(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_merge_nodes(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    keep_node record;
    remove_node record;
BEGIN
    -- Nodes to keep and to remove
    SELECT n.*
    INTO keep_node
    FROM road_graph.nodes AS n
    WHERE n.id = (_params->>'keep_node_id')::integer
    ;
    SELECT n.*
    INTO remove_node
    FROM road_graph.nodes AS n
    WHERE n.id = (_params->>'remove_node_id')::integer
    ;

    -- The node may have been merged or deleted by a previous fix
    IF remove_node.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The node %s does not exist anymore', _params->>'remove_node_id')
        );
    END IF;
    IF keep_node.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format('The node to keep %s does not exist anymore', _params->>'keep_node_id')
        );
    END IF;

    -- The nodes must not be linked by an edge, which would be reduced to a point
    IF EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE (e.start_node = keep_node.id AND e.end_node = remove_node.id)
        OR (e.start_node = remove_node.id AND e.end_node = keep_node.id)
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format('The nodes %s and %s are linked by an edge', keep_node.id, remove_node.id)
        );
    END IF;

    -- Move the start and end points of the edges of the node to remove to the node to keep:
    -- the triggers use the node to keep for these edges
    UPDATE road_graph.edges AS e
    SET geom = ST_SetPoint(e.geom, 0, keep_node.geom)
    WHERE e.start_node = remove_node.id
    ;
    UPDATE road_graph.edges AS e
    SET geom = ST_SetPoint(e.geom, ST_NPoints(e.geom) - 1, keep_node.geom)
    WHERE e.end_node = remove_node.id
    ;

    -- Delete the node to remove, which has no edge anymore
    DELETE FROM road_graph.nodes AS n
    WHERE n.id = remove_node.id
    AND NOT EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        WHERE remove_node.id IN (e.start_node, e.end_node)
    )
    ;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format('The node %s has been merged into the node %s', remove_node.id, keep_node.id)
    );
END;
$$;


-- FUNCTION fix_graph_merge_nodes(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_merge_nodes(_params jsonb) IS 'Action merge_nodes run by the function fix_graph_issue, to fix the issues of the check G03:
merge two nodes close to each other.

The parameter _params must contain the ids of the node to keep and of the node to remove:
{"keep_node_id": 12, "remove_node_id": 34}.
The start and end points of the edges of the node to remove are moved to the node to keep,
then the node to remove is deleted.

It returns a JSON object with the keys status (success, skipped if the node to remove does not exist anymore,
or error if the nodes are linked by an edge) and message.';


-- fix_graph_move_marker(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_move_marker(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    marker record;
    edge record;
BEGIN
    -- The marker and the edge may have been modified by a previous fix
    SELECT m.*
    INTO marker
    FROM road_graph.markers AS m
    WHERE m.id = (_params->>'marker_id')::integer
    ;
    IF marker.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The marker %s does not exist anymore', _params->>'marker_id')
        );
    END IF;
    SELECT e.*
    INTO edge
    FROM road_graph.edges AS e
    WHERE e.id = (_params->>'edge_id')::integer
    AND e.road_code = marker.road_code
    AND ST_DWithin(ST_StartPoint(e.geom), ST_GeomFromEWKT(_params->>'point'), 0.01)
    ;
    IF edge.id IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The edge %s does not start anymore at the given point', _params->>'edge_id')
        );
    END IF;
    IF ST_DWithin(marker.geom, ST_StartPoint(edge.geom), 0.01) THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format('The marker %s is already at the start of the road', marker.id)
        );
    END IF;

    -- Move the marker to the start of the edge: the triggers calculate again the references of the road
    UPDATE road_graph.markers AS m
    SET geom = ST_StartPoint(edge.geom)
    WHERE m.id = marker.id
    ;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The marker %s of the road %s has been moved by %s m to the start of the road',
            marker.code, marker.road_code, round(ST_Distance(marker.geom, ST_StartPoint(edge.geom))::numeric, 2)
        )
    );
END;
$$;


-- FUNCTION fix_graph_move_marker(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_move_marker(_params jsonb) IS 'Action move_marker run by the function fix_graph_issue, to fix the issues of the check G09:
move the first marker of a road to the start of the road.

The parameter _params must contain the ids of the marker and of the first edge of the road,
and the start point of the edge as EWKT: {"marker_id": 12, "edge_id": 34, "point": "SRID=2154;POINT(x y)"}.
The marker is moved to the start point of the edge: the triggers calculate again the references of the road.

It returns a JSON object with the keys status (success, skipped if the marker does not exist anymore,
is already at the start of the road, or if the edge does not start at the given point anymore) and message.';


-- fix_graph_reverse_edges(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_reverse_edges(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    issue_edge_ids integer[];
    road_codes text[];
    reversed_ids integer[];
    reversed_edge_id integer;
BEGIN
    -- Edges given by the issue
    SELECT array_agg(x::integer)
    INTO issue_edge_ids
    FROM jsonb_array_elements_text(_params->'edge_ids') AS x
    ;
    IF issue_edge_ids IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', 'The parameter edge_ids is needed'
        );
    END IF;

    -- The edges still in the opposite direction of their road:
    -- the edges may have been reversed, split or merged by a previous fix
    SELECT array_agg(DISTINCT e.road_code)
    INTO road_codes
    FROM road_graph.edges AS e
    WHERE e.id = ANY(issue_edge_ids)
    ;
    SELECT array_agg(DISTINCT x ORDER BY x)
    INTO reversed_ids
    FROM road_graph.check_graph_reversed_edges(road_codes, NULL) AS i,
    unnest(i.edge_ids) AS x
    WHERE x = ANY(issue_edge_ids)
    ;
    IF reversed_ids IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format(
                'The edges %s are not in the opposite direction of their road anymore',
                array_to_string(issue_edge_ids, ', ')
            )
        );
    END IF;

    -- Reverse the edges one by one: the triggers swap the start and end nodes of each edge,
    -- and calculate again its previous and next edges and the references of its road
    FOREACH reversed_edge_id IN ARRAY reversed_ids LOOP
        UPDATE road_graph.edges AS e
        SET geom = ST_Reverse(e.geom)
        WHERE e.id = reversed_edge_id
        ;
    END LOOP;

    -- Check the edges are now in the direction of their road.
    -- If not, the exception cancels the changes of this fix (see fix_graph_issue)
    IF EXISTS (
        SELECT 1
        FROM road_graph.check_graph_reversed_edges(road_codes, NULL) AS i
        WHERE i.edge_ids && reversed_ids
    ) THEN
        RAISE EXCEPTION 'The edges % are still in the opposite direction of their road after being reversed',
            array_to_string(reversed_ids, ', ');
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format('The edges %s have been reversed', array_to_string(reversed_ids, ', '))
    );
END;
$$;


-- FUNCTION fix_graph_reverse_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_reverse_edges(_params jsonb) IS 'Action reverse_edges run by the function fix_graph_issue, to fix the issues of the check G05:
reverse the edges in the opposite direction of their road.

The parameter _params must contain the ids of the edges: {"edge_ids": [12, 34]}.
Only the edges still found by the check G05 are reversed, one by one: the triggers swap their start and end nodes,
and calculate again their previous and next edges and the references of their road.

It returns a JSON object with the keys status (success, skipped if the edges are not in the opposite direction
of their road anymore, or error) and message.';


-- get_or_create_edge_node(geometry, boolean)
CREATE OR REPLACE FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) RETURNS TABLE(node_id integer, node_geom geometry, is_created boolean)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Get the closest node less than 0.5 m from the given start or end point of an edge,
    -- or the node with the lowest id if several nodes are at the same distance.
    -- The closest node must be used: when an edge is modified, its start or end point
    -- must stay on its node, even if another node is less than 0.5 m from it
    SELECT n.id, n.geom
    INTO node_id, node_geom
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, _point, 0.50)
    ORDER BY n.geom <-> _point, n.id
    LIMIT 1
    ;
    is_created = False;
    IF node_id IS NOT NULL THEN
        RETURN NEXT;
        RETURN;
    END IF;

    -- Create the missing node if asked
    IF _create THEN
        -- Declare that the node is the extremity of an edge:
        -- the edges under it will be split, even if their field no_intersection_cutting is True
        -- (for example a new access road arriving in the middle of a highway edge)
        -- The variable is used by the trigger function after_node_insert_or_update
        SET LOCAL road.graph.node.edge.extremity = 'yes';
        WITH new_node AS (
            INSERT INTO road_graph.nodes
            (geom)
            VALUES
            (_point)
            ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
            RETURNING id
        )
        SELECT new_node.id
        FROM new_node
        INTO node_id
        ;
        SET LOCAL road.graph.node.edge.extremity = 'no';
        node_geom = _point;
        is_created = True;
    END IF;

    RETURN NEXT;
    RETURN;
END;
$$;


-- FUNCTION get_or_create_edge_node(_point geometry, _create boolean)
COMMENT ON FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) IS 'Get the node of the given start or end point of an edge: the closest node less than 0.5 m from it,
or the node with the lowest id if several nodes are at the same distance.

If there is no node and _create is True, the node is created (its id can be NULL if a node already exists at the same place).
Used by the trigger function before_edge_insert_or_update.';


-- Edges triggers: a geometry which has only been reversed is now taken into account.
-- ST_Equals does not take the direction into account: the start and end nodes were not swapped,
-- and the previous and next edges and the references were not calculated again.

-- after_edge_insert_or_update()
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
    node_already_deleted integer;
    update_edge_references_result boolean;
    raise_notice text;
    cascade_edge_id integer;
    edge_road record;
    is_roundabout boolean;
    previous_ref_calc_disabled text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

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
            -- Declare that the node is the extremity of the edge:
            -- the edges under it will be split, even if their field no_intersection_cutting is True
            SET LOCAL road.graph.node.edge.extremity = 'yes';
            UPDATE road_graph.nodes AS n
            SET geom = ST_StartPoint(NEW.geom)
            WHERE n.id = NEW.start_node
            AND NOT ST_Equals(n.geom, ST_StartPoint(NEW.geom))
            ;
            SET LOCAL road.graph.node.edge.extremity = 'no';
        END IF;
        IF NOT ST_Equals(ST_EndPoint(OLD.geom), ST_EndPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update downstream node if needed
            -- Declare that the node is the extremity of the edge:
            -- the edges under it will be split, even if their field no_intersection_cutting is True
            SET LOCAL road.graph.node.edge.extremity = 'yes';
            UPDATE road_graph.nodes AS n
            SET geom = ST_EndPoint(NEW.geom)
            WHERE n.id = NEW.end_node
            AND NOT ST_Equals(n.geom, ST_EndPoint(NEW.geom))
            ;
            SET LOCAL road.graph.node.edge.extremity = 'no';
        END IF;

        -- Check if old referenced nodes are still referenced by edges
        -- If not, delete them
        -- Get node ID which has been manually deleted by user
        -- and must not be deleted twice
        node_already_deleted = road_graph.get_current_setting('road.graph.node.already.deleted', '-1');

        -- Upstream node
        IF OLD.start_node != NEW.start_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.start_node
                AND id != node_already_deleted
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
        IF OLD.end_node != NEW.end_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.end_node
                AND id != node_already_deleted
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
    -- The cascading changes are only made when the edge is inserted or its previous or next edge has changed:
    -- the references of the road are then calculated at the end of this function.
    -- Do not calculate them for each updated neighbour edge
    previous_ref_calc_disabled = road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no');
    PERFORM set_config('road.graph.edge.ref.calc.disabled', 'yes', true);
    -- When the geometry has only been reversed, the edges of the road which referenced this edge
    -- as their next or previous edge do not follow it anymore: their reference to this edge is removed,
    -- and their trigger calculates again their next or previous edge
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND ST_Equals(NEW.geom, OLD.geom) IS TRUE
        AND NOT ST_OrderingEquals(NEW.geom, OLD.geom)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NULL
        WHERE e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.next_edge_id = NEW.id
        AND e.id IS DISTINCT FROM NEW.previous_edge_id
        ;
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NULL
        WHERE e.road_code = NEW.road_code
        AND e.id != NEW.id
        AND e.previous_edge_id = NEW.id
        AND e.id IS DISTINCT FROM NEW.next_edge_id
        ;
    END IF;
    -- previous edge id
    IF (TG_OP = 'INSERT' AND NEW.previous_edge_id IS NOT NULL AND NOT is_roundabout)
        OR (TG_OP = 'UPDATE' AND NEW.previous_edge_id != Coalesce(OLD.previous_edge_id, -1) AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NEW.id
        WHERE TRUE
        AND e.road_code = NEW.road_code
        AND e.id != NEW.id
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
        OR (TG_OP = 'UPDATE' AND NEW.next_edge_id != Coalesce(OLD.next_edge_id, -1) AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NEW.id
        WHERE TRUE
        AND e.road_code = NEW.road_code
        AND e.id != NEW.id
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
    -- Set back the value
    PERFORM set_config('road.graph.edge.ref.calc.disabled', previous_ref_calc_disabled, true);

    -- For UPDATE, if the edge road_code has changed,
    -- we must also edit the previous and next edge ids of the old road edges
    IF TG_OP = 'UPDATE'
        AND OLD.road_code IS NOT NULL
        AND Coalesce(NEW.road_code, '') != OLD.road_code
    THEN
        -- Update OLD previous edge field "next_edge_id" if concerned
        UPDATE road_graph.edges AS e
        SET next_edge_id = OLD.next_edge_id
        WHERE TRUE
        -- same road
        AND e.road_code = OLD.road_code
        -- reference as the next_edge_id by the OLD edge
        AND e.next_edge_id = OLD.id
        AND e.id != NEW.id
        ;
        -- Update OLD next edge field "previous_edge_id" if concerned
        UPDATE road_graph.edges AS e
        SET previous_edge_id = OLD.previous_edge_id
        WHERE TRUE
        -- same road
        AND e.road_code = OLD.road_code
        -- reference as the previous_edge_id by the OLD edge
        AND e.previous_edge_id = OLD.id
        AND e.id != NEW.id
        ;
    END IF;

    -- For INSERT OR UPDATE
    -- Create nodes at intersection with other edges if needed
    -- This will then run the trigger after_node_insert_or_update
    -- which will eventually split the edges intersecting this NEW edge
    -- DO IT ONLY FOR GEOMETRIES INSIDE THE EDITION AREA
    -- DO IT ONLY FOR EDGES WITH THE SAME LEVEL
    created_nodes_at_intersection = ARRAY[]::integer[];
    -- Beware of the parentheses: AND has a higher precedence than OR,
    -- and the test on the variable must also be applied for INSERT
    IF (TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom)))
        -- avoid infinite loop and self-crossing
        AND road_graph.get_current_setting('road.graph.edge.crossing.node.creation.pending', 'no') != 'yes'

    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER edge % n° %, no_intersection_cutting %, crossing node test',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.no_intersection_cutting
            ;
        END IF;
        FOR crossing_node IN
            WITH new_nodes AS (
                SELECT
                    ST_Intersection(e.geom, NEW.geom) AS geom
                FROM road_graph.edges AS e
                WHERE e.id != NEW.id
                AND ST_Intersects(e.geom, NEW.geom)
                -- Do not get the intersection if one the edges has the value True in no_intersection_cutting
                AND NOT Coalesce(e.no_intersection_cutting, False)
                AND NOT Coalesce(NEW.no_intersection_cutting, False)
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
            -- DO NOT USE NODE IF OUTSIDE THE EDITION AREA
            AND ST_Intersects(
                c.geom,
                (
                    SELECT es.geom
                    FROM "road_graph".editing_sessions AS es
                    LIMIT 1
                )
            )
        LOOP
            IF crossing_node.geom IS NOT NULL
                AND ST_GeometryType(crossing_node.geom) = 'ST_Point'
                -- The crossing points are calculated once, before the loop.
                -- Since then, the node could have been created by the cascading changes:
                -- the creation of the previous node has split this edge, and the new parts
                -- have created the nodes at their own intersections.
                -- These nodes are calculated from the rounded coordinates of the parts,
                -- and can be a few centimeters away from this point: check with the same tolerance
                AND NOT EXISTS (
                    SELECT 1
                    FROM road_graph.nodes AS n
                    WHERE ST_DWithin(n.geom, crossing_node.geom, 0.50)
                )
            THEN
                -- Create the missing node, which will trigger the split of the edge
                -- (see the trigger after_node_insert_or_update)
                -- Set variable to avoid infinite loop & self-crossing detection
                SET LOCAL road.graph.edge.crossing.node.creation.pending = 'yes';
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
                SET LOCAL road.graph.edge.crossing.node.creation.pending = 'no';

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
        node_to_be_deleted = road_graph.get_current_setting('road.graph.merge.edges.useless.node', '-1');

        -- RAISE NOTICE '% AFTER edge % n° %, list touching nodes, useless node = %',
        ---    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, node_to_be_deleted
        -- ;
        FOR touching_node IN
            SELECT DISTINCT
                n.*
            FROM road_graph.nodes AS n
            WHERE True
            -- Do not split the NEW edge if it has the value True in no_intersection_cutting
            AND NOT Coalesce(NEW.no_intersection_cutting, False)
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
            AND n.id != node_to_be_deleted::integer
            -- DO NOT USE NODE IF OUTSIDE THE EDITION AREA
            AND ST_Intersects(
                n.geom,
                (
                    SELECT es.geom
                    FROM "road_graph".editing_sessions AS es
                    LIMIT 1
                )
            )
        LOOP
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % n° % start % end %, update existing node under new edge to launch the split : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.start_node, NEW.end_node,
                    touching_node.id, ST_AsText(touching_node.geom)
                ;
            END IF;

            IF road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no') != 'yes'
            THEN
                SET LOCAL road.graph.edge.update.touching.node = 'yes';
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
                SET LOCAL road.graph.edge.update.touching.node = 'no';
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, NEW node created in same node place: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id, new_node_id
                    ;
                END IF;
            END IF;
        END LOOP;
    END IF;

    -- Create a new marker feature with code 0 if needed
    IF TG_OP IN ('INSERT', 'UPDATE')
        AND NEW.geom IS NOT NULL
        AND NEW.previous_edge_id IS NULL
        AND NEW.road_code IS NOT NULL
        AND NOT is_roundabout
        AND Coalesce(edge_road.road_type, '') NOT IN ('roundabout')
        -- the road does not have any marker yet (new road)
        -- Do not test only the marker 0: some roads do not start with the marker 0
        AND NOT EXISTS (
            SELECT m.id
            FROM road_graph.markers AS m
            WHERE m.road_code = NEW.road_code
        )
        -- There is no edge with the same road_code and without previous_edge_id (which means the first edge of the road)
        AND NOT EXISTS (
            SELECT e.id
            FROM road_graph.edges AS e
            WHERE e.road_code = NEW.road_code
            AND e.id != NEW.id
            AND (
                e.previous_edge_id IS NULL
                OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(NEW.geom), 0.50)
            )
        )
    THEN
        INSERT INTO road_graph.markers (road_code, code, abscissa, geom)
        VALUES (NEW.road_code, 0, 0, ST_StartPoint(NEW.geom))
        ON CONFLICT DO NOTHING
        ;
    END IF;

    -- Calculate references of the edge whole road
    -- Beware to choose correct conditions
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and marker)
    IF NOT is_roundabout THEN
        IF TG_OP = 'INSERT'
        OR (
            TG_OP = 'UPDATE' AND (
                -- The references are also calculated when the geometry has only been reversed
                NOT ST_OrderingEquals(OLD.geom, NEW.geom)
                OR Coalesce(OLD.previous_edge_id, -1) != NEW.previous_edge_id
                OR Coalesce(OLD.next_edge_id, -1) != NEW.next_edge_id
            )
        )
        THEN
            IF road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
            THEN
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, update all NEW road edges references: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                        update_edge_references_result::text
                    ;
                END IF;
                SELECT INTO update_edge_references_result
                    road_graph.update_edge_references(NEW.road_code, NULL)
                ;
            END IF;
        END IF;
    END IF;


    -- Also update old road references if road code has changed
    -- and it remains at least one edge for this old road code
    -- No need, it is done as a consequence of updating the edges
    -- of the OLD road code which referenced this edge in next_edge_id or previous_edge_id

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - End',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    RETURN NEW;
END;
$$;



-- before_edge_insert_or_update()
CREATE OR REPLACE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_point geometry(point);
    end_point geometry(point);
    extremity_node record;
    references_calculated_after boolean;
    references_unchanged boolean;
    distance real;
    start_references jsonb;
    end_references jsonb;
    marker_zero record;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
    has_changed_road_code boolean;
    is_reversed boolean;
	v_sqlstate text;
	v_message text;
	v_context text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

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
    is_roundabout = (edge_road.road_type = 'roundabout');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% BEFORE edge % n° %, edge is_roundabout %,
        %',
            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, is_roundabout,
            to_json(edge_road)
        ;
    END IF;

    -- If it is a new roundabout, reverse the geometry if needed
    -- QGIS digitizing circle tool create counter-clockwise polygons
    IF is_roundabout AND ST_IsRing(NEW.geom) AND ST_IsPolygonCW(ST_MakePolygon(NEW.geom))
    THEN
        NEW.geom = ST_Reverse(NEW.geom);
    END IF;

    -- For UPDATE, check if the road_code has changed
    -- If so, we need to empty the next_edge_id and previous_edge_id to avoid wrong calculations
    has_changed_road_code = false;
    IF NOT is_roundabout
        AND TG_OP = 'UPDATE'
        AND Coalesce(OLD.road_code, '') != Coalesce(NEW.road_code, '')
    THEN
        has_changed_road_code = true;
        IF Coalesce(OLD.previous_edge_id, -1) = Coalesce(NEW.previous_edge_id, -1) THEN
            NEW.previous_edge_id = NULL;
        END IF;
        IF Coalesce(OLD.next_edge_id, -1) = Coalesce(NEW.next_edge_id, -1) THEN
            NEW.next_edge_id = NULL;
        END IF;
    END IF;

    -- For UPDATE, check if the geometry has only been reversed.
    -- ST_Equals does not take the direction into account: ST_OrderingEquals is used below
    -- to detect any change of the geometry, including its direction
    is_reversed = (
        TG_OP = 'UPDATE'
        AND ST_Equals(NEW.geom, OLD.geom) IS TRUE
        AND NOT ST_OrderingEquals(NEW.geom, OLD.geom)
    );
    -- The previous and next edges of a reversed edge must be calculated again,
    -- unless they have been given
    IF is_reversed AND NOT is_roundabout AND NOT has_changed_road_code THEN
        IF Coalesce(OLD.previous_edge_id, -1) = Coalesce(NEW.previous_edge_id, -1) THEN
            NEW.previous_edge_id = NULL;
        END IF;
        IF Coalesce(OLD.next_edge_id, -1) = Coalesce(NEW.next_edge_id, -1) THEN
            NEW.next_edge_id = NULL;
        END IF;
    END IF;

    -- Create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

    -- The nodes are also searched when the geometry has only been reversed:
    -- the start and end nodes are then swapped
    IF TG_OP = 'INSERT'
    OR (TG_OP = 'UPDATE' AND NOT ST_OrderingEquals(NEW.geom, OLD.geom) AND NEW.geom IS NOT NULL)
    THEN

        -- Get the nodes closer than 0.5 m from the start and end points, and use them.
        -- If not found, create them: only for INSERT,
        -- or for UPDATE if the start_node or end_node value has been deleted

        -- upstream node
        SELECT INTO extremity_node *
        FROM road_graph.get_or_create_edge_node(
            start_point,
            TG_OP = 'INSERT' OR NEW.start_node IS NULL
        );
        IF extremity_node.node_id IS NOT NULL AND NOT extremity_node.is_created THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream_node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, extremity_node.node_id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, 0, extremity_node.node_geom);
            -- Update the node ID in upstream attribute
            NEW.start_node = extremity_node.node_id;
        ELSIF extremity_node.is_created THEN
            NEW.start_node = extremity_node.node_id;
            IF NEW.start_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.start_node,
                    ST_AsText(extremity_node.node_geom)
                ;
            END IF;
        END IF;

        -- downstream node
        SELECT INTO extremity_node *
        FROM road_graph.get_or_create_edge_node(
            end_point,
            TG_OP = 'INSERT' OR NEW.end_node IS NULL
        );
        IF extremity_node.node_id IS NOT NULL AND NOT extremity_node.is_created THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, extremity_node.node_id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, ST_NPoints(NEW.geom) - 1, extremity_node.node_geom);
            -- Update the node ID in downstream attribute
            NEW.end_node = extremity_node.node_id;
        ELSIF extremity_node.is_created THEN
            NEW.end_node = extremity_node.node_id;
            IF NEW.end_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node created : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.end_node,
                    ST_AsText(extremity_node.node_geom)
                ;
            END IF;
        END IF;
    END IF;

    -- Calculate previous_edge_id and next_edge_id values
    -- for touching edges if the road_code has not changed
    IF NOT is_roundabout AND NOT has_changed_road_code
    THEN
        IF NEW.previous_edge_id IS NULL THEN
            -- Get the previous edge
            NEW.previous_edge_id = (
                SELECT e.id
                FROM road_graph.edges AS e
                WHERE e.road_code = NEW.road_code
                AND e.end_node = NEW.start_node
                -- not the edge itself, whose old row is still in the table, for example when it is reversed
                AND e.id != NEW.id
                ORDER BY e.id
                LIMIT 1
            );
        END IF;
        IF NEW.next_edge_id IS NULL THEN
            -- Get the next edge
            NEW.next_edge_id = (
                SELECT e.id
                FROM road_graph.edges AS e
                WHERE e.road_code = NEW.road_code
                AND e.start_node = NEW.end_node
                -- not the edge itself, whose old row is still in the table, for example when it is reversed
                AND e.id != NEW.id
                ORDER BY e.id
                LIMIT 1
            );
        END IF;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    -- The marker 0 is not moved when the geometry has only been reversed: it stays at the start of the road
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND NOT has_changed_road_code
        AND NOT is_reversed
        AND OLD.start_abscissa = 0 AND NEW.start_abscissa = 0
        AND OLD.start_cumulative = 0 AND NEW.start_cumulative = 0
        AND OLD.start_marker = 0 AND NEW.start_marker = 0
        AND OLD.road_code = NEW.road_code
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_StartPoint(OLD.geom))
    THEN
        -- Do not run the function update_edge_references
        -- on the road triggered inside the after_marker_insert_or_update_or_delete
        -- trigger since it will already been triggered with the edge geometry change
        SET LOCAL road.graph.edge.ref.calc.disabled = 'yes';
        UPDATE road_graph.markers AS m
        SET geom = ST_StartPoint(NEW.geom)
        WHERE m.road_code = NEW.road_code
        AND m.code = 0
        AND ST_Equals(m.geom, ST_StartPoint(OLD.geom))
        ;
        SET LOCAL road.graph.edge.ref.calc.disabled = 'no';
    END IF;

    -- Calculate references
    -- They are not calculated for an UPDATE:
    -- * if the trigger after_edge_insert_or_update will calculate again the references
    --   of all the edges of the road (same condition as in this trigger function):
    --   the references calculated here would be overwritten,
    -- * or if the geometry, the road and the references have not been changed, for example
    --   when only another field has been modified: the references are still right.
    -- They are always calculated for an INSERT: the start cumulative is used to order
    -- the edges of the road (see get_ordered_edges)
    references_calculated_after = False;
    references_unchanged = False;
    IF TG_OP = 'UPDATE' THEN
        references_calculated_after = (
            (
                NOT ST_OrderingEquals(OLD.geom, NEW.geom)
                OR Coalesce(OLD.previous_edge_id, -1) != NEW.previous_edge_id
                OR Coalesce(OLD.next_edge_id, -1) != NEW.next_edge_id
            ) IS TRUE
            AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
        );
        references_unchanged = (
            ST_OrderingEquals(OLD.geom, NEW.geom) IS TRUE
            AND OLD.road_code = NEW.road_code
            AND (
                NEW.start_marker, NEW.start_abscissa, NEW.start_cumulative,
                NEW.end_marker, NEW.end_abscissa, NEW.end_cumulative
            ) IS NOT DISTINCT FROM (
                OLD.start_marker, OLD.start_abscissa, OLD.start_cumulative,
                OLD.end_marker, OLD.end_abscissa, OLD.end_cumulative
            )
        );
    END IF;
    IF NOT is_roundabout
        AND NOT references_calculated_after
        AND NOT references_unchanged
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
                GET STACKED DIAGNOSTICS
                    v_sqlstate = returned_sqlstate,
                    v_message = message_text,
                    v_context = pg_exception_context
                ;
                RAISE NOTICE '% BEFORE edge % n° %, NEW references NOT calculated, error = %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP,
                    NEW.id, v_sqlstate || ' - ' || v_message
                ;
            END IF;
        END;
    END IF;

    RETURN NEW;
END;
$$;


-- FUNCTION before_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.before_edge_insert_or_update() IS 'During the creation or modification of an edge, we verify that the upstream and downstream nodes exist
within 50 cm of the start and end of the edge. If they do, we use them
and update the edge geometry so that the start and end are exactly on these nodes.
Otherwise, we create the missing nodes.
Additionally, during the creation of an edge, if the road code is provided and no marker 0 exists for this road,
we automatically create one at the start of the edge.
During the modification of an edge, if the starting point of the edge is modified and the marker 0 of the road is positioned at this starting point,
we move marker 0 to this new starting point.
When the geometry of an edge is only reversed, its start and end nodes are swapped,
its previous and next edges are calculated again, and the marker 0 is not moved.
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';



-- Repair the edges whose geometry has been reversed before this fix:
-- their start and end nodes are swapped, then their previous and next edges and the references
-- of their road are calculated again by the triggers.
-- The value -1 of previous_edge_id and next_edge_id marks the repaired edges between the two queries
SELECT set_config('road.graph.disable.trigger', '1', true);
WITH reversed_edges AS (
    SELECT e.id
    FROM road_graph.edges AS e
    JOIN road_graph.nodes AS sn
        ON sn.id = e.start_node
    JOIN road_graph.nodes AS en
        ON en.id = e.end_node
    WHERE e.start_node != e.end_node
    AND ST_DWithin(ST_StartPoint(e.geom), en.geom, 0.01)
    AND ST_DWithin(ST_EndPoint(e.geom), sn.geom, 0.01)
    AND NOT ST_DWithin(ST_StartPoint(e.geom), sn.geom, 0.01)
)
UPDATE road_graph.edges AS e
SET start_node = e.end_node, end_node = e.start_node, previous_edge_id = -1, next_edge_id = -1
FROM reversed_edges AS r
WHERE r.id = e.id
;
SELECT set_config('road.graph.disable.trigger', '0', true);
UPDATE road_graph.edges AS e
SET previous_edge_id = NULL, next_edge_id = NULL
WHERE e.previous_edge_id = -1 AND e.next_edge_id = -1
;


-- Edges triggers: the end node of an edge which is not used anymore is deleted,
-- like its start node. The function split_edge_by_node keeps the end node of the split edge,
-- used again by the new edge

-- split_edge_by_node(record, record, real, real)
CREATE OR REPLACE FUNCTION road_graph.split_edge_by_node(edge_record record, node_record record, minimum_distance_from_linestring real, maximum_distance_from_start_end_points real) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    source_fields text;
    start_geom geometry;
    end_geom geometry;
    id_new_object integer;
    new_edge_record record;
    sql_text text;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
    previous_crossing_node_creation_pending text;
    previous_ref_calc_disabled text;
    previous_node_already_deleted text;
    split_length_difference float8;
BEGIN
    -- log level
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = edge_record.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this edge does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    -- Check point is close enough to the given edge
    IF ST_Distance(node_record.geom, edge_record.geom) > minimum_distance_from_linestring THEN
        RAISE NOTICE 'No edge found close enough to the node : Distance % m > % m ',
            ST_Distance(node_record.geom, edge_record.geom), minimum_distance_from_linestring
        ;
        RETURN NULL;
    END IF;

    -- Check the edge is not already cut
    IF node_record.id IN (edge_record.start_node, edge_record.end_node)
        OR ST_DWithin(node_record.geom, ST_StartPoint(edge_record.geom), 0.50)
        OR ST_DWithin(node_record.geom, ST_EndPoint(edge_record.geom), 0.50)
    THEN
        RAISE NOTICE 'Given edge is already cut for this node which corresponds to its start or end point'
        ;
        RETURN NULL;
    END IF;

    -- Create new geometries
    start_geom := ST_LineSubstring(
        edge_record.geom,
        0,
        ST_LineLocatePoint(edge_record.geom, node_record.geom)
    );
    end_geom := ST_LineSubstring(
        edge_record.geom,
        ST_LineLocatePoint(edge_record.geom, node_record.geom),
        1
    );

    -- Check the cut point is not to close to the start or end point of the given linestring
    IF ST_length(start_geom) < maximum_distance_from_start_end_points
        OR ST_length(end_geom) < maximum_distance_from_start_end_points
    THEN-- Set variable to avoid infinite loops
        RAISE NOTICE 'Abort splitting the edge: given point too close from the edge end points ( < %m)', maximum_distance_from_start_end_points;
        RETURN NULL;
    END IF;

    -- Modify the initial edge :
    -- OA----------(O)----------OB becomes  OA----------(O)
    -- we must get the new edge id beforehand to use it as next_edge_id
    -- For roundabout, do not calculate previous and next edge id
    -- To let the user decide
    -- The quoted schema name "road_graph" is not replaced by the plugin:
    -- the edges of the road_graph_editing schema also use the road_graph sequence
    id_new_object = nextval(pg_get_serial_sequence('"road_graph".edges', 'id'));

    -- The two parts of the split edge must not create nodes at their intersections with the other edges,
    -- nor be split by the existing nodes located under them: these intersections existed before the split.
    -- Without this, each split was propagated to the edges crossing the split edge, and so on,
    -- for example to all the bridges of a highway interchange.
    -- The intersections of the edge being drawn are managed by its own trigger (after_edge_insert_or_update).
    previous_crossing_node_creation_pending = road_graph.get_current_setting(
        'road.graph.edge.crossing.node.creation.pending', 'no'
    );
    PERFORM set_config('road.graph.edge.crossing.node.creation.pending', 'yes', true);

    -- The references of the road are calculated once at the end of this function,
    -- and not after the update of the edge, then after the insertion of the new edge,
    -- and after the update of the next edge by the cascading changes
    previous_ref_calc_disabled = road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no');
    PERFORM set_config('road.graph.edge.ref.calc.disabled', 'yes', true);

    -- The end node of the initial edge is not used by any edge after its update,
    -- until the new edge is inserted: it must not be deleted by the trigger after_edge_insert_or_update.
    -- The variable road.graph.node.already.deleted gives the node which must not be deleted
    previous_node_already_deleted = road_graph.get_current_setting('road.graph.node.already.deleted', '-1');
    PERFORM set_config('road.graph.node.already.deleted', edge_record.end_node::text, true);

    UPDATE road_graph.edges
    SET
        -- Set the geometry
        geom = start_geom,
        -- We must also set the downstream node id column
        end_node = node_record.id,
        -- next edge_id
        next_edge_id = CASE WHEN is_roundabout THEN NULL ELSE id_new_object END,
        -- end_cumulative
        end_cumulative = (end_cumulative - ST_Length(end_geom))
    WHERE id = edge_record.id
    ;
    PERFORM set_config('road.graph.node.already.deleted', previous_node_already_deleted, true);

    -- Get the object fields minus the PK, upstream node & geometry column
    WITH fields AS (
        SELECT json_object_keys(to_json(edge_record.*)) AS field
    )
    SELECT concat ('"', string_agg(field, '", "'), '"')
    INTO source_fields
    FROM fields
    WHERE field NOT IN (
        'id', 'start_node', 'geom',
        'previous_edge_id', 'next_edge_id',
        'start_cumulative', 'uid'
    )
    ;

    -- Check if the new edge geometry does not exists
    -- This avoid to create 2 superimposed edges
    -- especially for roundabouts.
    SELECT INTO new_edge_record
        e.id
    FROM road_graph.edges AS e
    WHERE
        -- use ST_DWithin and not ST_Intersects to use spatial indexing
        -- but keep a tolerance to avoid precision issues
        ST_DWithin(e.geom, end_geom, 1)
        AND (
            ST_Equals(e.geom, end_geom)
            OR ST_Equals(e.geom, ST_ReducePrecision(end_geom, 0.10))
        )
    LIMIT 1
    ;

    -- Create the new edge:
    -- (O)----------OB
    -- Get the values from the original linestring
    -- Only if the geometry does not exists
    IF new_edge_record.id IS NULL
    THEN
        sql_text = format(
            $$
                INSERT INTO road_graph.edges (
                    id,
                    %1$s,
                    start_node,
                    geom,
                    previous_edge_id,
                    next_edge_id,
                    start_cumulative,
                    uid
                )
                WITH source AS (
                    SELECT e.*, r.road_type
                    FROM road_graph.edges AS e
                    JOIN road_graph.roads AS r
                        ON r.road_code = e.road_code
                    WHERE e.id = %2$s::integer
                    LIMIT 1
                )
                SELECT
                    %5$s::integer AS id,
                    %1$s,
                    %3$s::integer AS start_node,
                    '%4$s'::geometry(LINESTRING) AS geom,
                    CASE WHEN road_type = 'roundabout' THEN NULL ELSE %2$s::integer END AS previous_edge_id,
                    CASE WHEN road_type = 'roundabout' THEN NULL ELSE(Nullif(%6$s, -1))::integer END AS next_edge_id,
                    %7$s::real AS start_cumulative,
                    uuid_generate_v4()::text
                FROM source
                LIMIT 1
                RETURNING id
            $$,
            source_fields,
            edge_record.id,
            node_record.id,
            end_geom,
            id_new_object,
            Coalesce(edge_record.next_edge_id, -1),
            Coalesce(edge_record.end_cumulative, 0)
        );

        -- Set again the variables, in case they have been reset by the cascading changes
        PERFORM set_config('road.graph.edge.crossing.node.creation.pending', 'yes', true);
        PERFORM set_config('road.graph.edge.ref.calc.disabled', 'yes', true);
        EXECUTE sql_text
        INTO id_new_object
        ;

        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% split_edge_by_node - node n° % %- splitting edge n° % : created edge n° %',
               repeat('    ', pg_trigger_depth()::integer), node_record.id, ST_AsText(node_record.geom), edge_record.id, id_new_object
            ;
        END IF;
    ELSE
        id_new_object = -1;
    END IF;

    -- Restore the variables
    PERFORM set_config(
        'road.graph.edge.crossing.node.creation.pending', previous_crossing_node_creation_pending, true
    );
    PERFORM set_config('road.graph.edge.ref.calc.disabled', previous_ref_calc_disabled, true);

    -- Calculate the references of the two parts of the split edge only.
    -- The geometry of the road has not changed: the references of its other edges are still right.
    -- The stored geometries are rounded to 10 cm by the trigger function aa_before_geometry_insert_or_update:
    -- the length of the road only changes by less than a millimeter, which is negligible.
    -- If the two parts are not as long as the split edge, the references of the whole road are calculated.
    -- Not for the roundabouts, like in the trigger function after_edge_insert_or_update
    IF previous_ref_calc_disabled = 'no' AND NOT is_roundabout THEN
        SELECT sum(ST_Length(e.geom)) - ST_Length(edge_record.geom)
        INTO split_length_difference
        FROM road_graph.edges AS e
        WHERE e.id IN (edge_record.id, id_new_object)
        ;
        IF id_new_object > 0 AND abs(split_length_difference) < 0.005 THEN
            PERFORM road_graph.update_edge_references(
                edge_record.road_code,
                ARRAY[edge_record.id, id_new_object]
            );
        ELSE
            PERFORM road_graph.update_edge_references(edge_record.road_code);
        END IF;
    END IF;

    -- Return True
    RETURN id_new_object;

END;
$_$;


-- Nodes trigger: when a node is created or moved less than 50 cm from another node, the two nodes are merged.
-- The edges which then start and end at the node, for example the edge between the two nodes, are deleted

-- after_node_insert_or_update()
CREATE OR REPLACE FUNCTION road_graph.after_node_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_under_node record;
    id_new_edge integer;
    raise_notice text;
    is_edge_extremity boolean;
    attached_edge_ids integer[];
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Check if the node is the start or end node of an edge being created or moved
    -- (see before_edge_insert_or_update and after_edge_insert_or_update)
    -- The variable is reset at once, so that it is not used by the cascading changes
    is_edge_extremity = (road_graph.get_current_setting('road.graph.node.edge.extremity', 'no') = 'yes');
    SET LOCAL road.graph.node.edge.extremity = 'no';

    -- Do nothing if geometry has not changed
    -- except road.graph.edge.update.touching.node equals yes
    IF
        TG_OP = 'UPDATE'
        AND ST_Equals(NEW.geom, OLD.geom)
        AND road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER node % n° %, OLD & NEW geometries are equal, return',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;
        RETURN NEW;
    END IF;

    -- Update all upstream edges referenced ids.
    -- The edges of another node less than 50 cm from this node are attached to this node:
    -- the two nodes are merged. The ids of the attached edges are kept
    WITH attached AS (
        UPDATE road_graph.edges AS t
        SET start_node = NEW.id
        WHERE ST_DWithin(NEW.geom, ST_StartPoint(t.geom), 0.50)
        AND start_node != NEW.id
        RETURNING t.id
    )
    SELECT array_agg(a.id)
    INTO attached_edge_ids
    FROM attached AS a
    ;

    -- Update all downstream edges referenced ids
    WITH attached AS (
        UPDATE road_graph.edges AS t
        SET end_node = NEW.id
        WHERE ST_DWithin(NEW.geom, ST_EndPoint(t.geom), 0.50)
        AND end_node != NEW.id
        RETURNING t.id
    )
    SELECT Coalesce(attached_edge_ids, '{}') || Coalesce(array_agg(a.id), '{}')
    INTO attached_edge_ids
    FROM attached AS a
    ;

    -- Update geometry of all upstream edges linked to the node
    -- We replace the first edge node by the node NEW geometry
    -- We deactivate the creation of crossing nodes for these updated edges
    SET LOCAL road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, 0, NEW.geom)
    WHERE TRUE
    AND start_node = NEW.id
    AND NOT ST_Intersects(ST_StartPoint(geom), NEW.geom)
    ;
    SET LOCAL road.graph.edge.crossing.node.creation.pending = 'no';

    -- Update geometry of all downstream edges linked to the node
    -- We replace the last edge node but the node NEW geometry
    -- We deactivate the creation of crossing nodes for these updated edges
    SET LOCAL road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, ST_NPoints(geom) - 1, NEW.geom)
    WHERE True
    AND end_node = NEW.id
    AND NOT ST_Intersects(ST_EndPoint(geom), NEW.geom)
    ;
    SET LOCAL road.graph.edge.crossing.node.creation.pending = 'no';

    -- Delete the edges which now start and end at this node, and did not before:
    -- for example the edge between this node and the merged node
    IF cardinality(attached_edge_ids) > 0 THEN
        DELETE FROM road_graph.edges AS t
        WHERE t.id = ANY(attached_edge_ids)
        AND t.start_node = NEW.id
        AND t.end_node = NEW.id
        ;
    END IF;

    -- Split edges under the node if needed
    FOR edge_under_node IN
        SELECT
            -- we must pass all the fields
            -- to be able to copy/paste attributes data from the original edge
            -- when spliting it
            t.*
        FROM road_graph.edges AS t
        WHERE True
        -- Do not split the edges having the value True in no_intersection_cutting,
        -- except if the node is the extremity of an edge being created or moved
        -- (for example a new access road arriving in the middle of a highway edge)
        AND (
            NOT Coalesce(t.no_intersection_cutting, False)
            OR is_edge_extremity
        )
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

3. Les extrémités des tronçons d''un autre Node situé à moins de 50 cm sont rattachées à ce Node : les deux Nodes sont fusionnés.
Les tronçons qui commencent et finissent alors à ce Node, par exemple le tronçon qui reliait les deux Nodes, sont supprimés.
';
