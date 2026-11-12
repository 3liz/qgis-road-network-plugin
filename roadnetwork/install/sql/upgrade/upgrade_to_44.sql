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
VALUES ('G03', 'Nodes close to each other', 'error', 'check_graph_close_nodes', '{merge_nodes}', '{"tolerance": 1}', 'Nodes less than the tolerance from each other, which should be the same node. The nodes linked by an edge are ignored. Option: tolerance, in meters. Fix: merge_nodes moves the edges of the node with the highest id to the other node, then deletes it.')
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


-- fix_graph_extend_edge(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_extend_edge(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    -- Maximum distance between two connected edges
    connected_tolerance real = 0.001;
    free_end geometry;
    target geometry;
    edge record;
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

    -- Add the point to join at the free end of the edge:
    -- the triggers move the node of the free end to this point, and split the other edge there
    UPDATE road_graph.edges AS e
    SET geom = CASE
        WHEN _params->>'side' = 'start' THEN ST_AddPoint(e.geom, target, 0)
        ELSE ST_AddPoint(e.geom, target)
    END
    WHERE e.id = edge.id
    ;

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
The point to join is added at the free end of the edge: the triggers move the node of the free end
to this point, and split the other edge there.

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
