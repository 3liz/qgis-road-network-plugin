ROLLBACK;
BEGIN;

-- On vide les tables
-- schema road_graph
TRUNCATE road_graph.glossary_road_class RESTART IDENTITY CASCADE;
TRUNCATE road_graph.roads RESTART IDENTITY CASCADE;
TRUNCATE road_graph.nodes RESTART IDENTITY CASCADE;
TRUNCATE road_graph.edges RESTART IDENTITY CASCADE;
TRUNCATE road_graph.markers RESTART IDENTITY CASCADE;
TRUNCATE road_graph.editing_sessions RESTART IDENTITY CASCADE;
TRUNCATE road_graph.managed_objects RESTART IDENTITY CASCADE;
-- schema editing_session
TRUNCATE editing_session.glossary_road_class RESTART IDENTITY CASCADE;
TRUNCATE editing_session.roads RESTART IDENTITY CASCADE;
TRUNCATE editing_session.nodes RESTART IDENTITY CASCADE;
TRUNCATE editing_session.edges RESTART IDENTITY CASCADE;
TRUNCATE editing_session.markers RESTART IDENTITY CASCADE;
TRUNCATE editing_session.editing_sessions RESTART IDENTITY CASCADE;
TRUNCATE editing_session.managed_objects RESTART IDENTITY CASCADE;

-- Disable complex triggers
ALTER TABLE road_graph.edges DISABLE TRIGGER trg_aa_before_geometry_insert_or_update ;
ALTER TABLE road_graph.edges DISABLE TRIGGER trg_after_edge_delete ;
ALTER TABLE road_graph.edges DISABLE TRIGGER trg_after_edge_insert_or_update;
ALTER TABLE road_graph.edges DISABLE TRIGGER trg_before_edge_insert_or_update;
ALTER TABLE road_graph.nodes DISABLE TRIGGER trg_aa_before_geometry_insert_or_update;
ALTER TABLE road_graph.nodes DISABLE TRIGGER trg_before_node_delete;
ALTER TABLE road_graph.nodes DISABLE TRIGGER trg_after_node_insert_or_update;
ALTER TABLE road_graph.markers DISABLE TRIGGER trg_aa_before_geometry_insert_or_update;
ALTER TABLE road_graph.markers DISABLE TRIGGER trg_after_marker_insert_or_update_or_delete;

-- Build temporary tables from source tables
-- Temporary edges
DROP TABLE IF EXISTS import.temp_edges;
CREATE TABLE import.temp_edges AS
WITH source AS (
    SELECT
        -- Stores the original data
        e.id AS id,
        e.axe AS road_code,
        e.order AS edge_order,
        e.nature_route,
        -- Move the geometries nodes to a grid
        ST_ReducePrecision(ST_geometryN(e.geom, 1), 0.10) AS geom
    -- bdtopo.troncon_de_route is the source road table
    FROM import.source_edges AS e
    -- Filter data if needed
    WHERE TRUE
)
SELECT
    s.id,
    s.road_code,
    s.edge_order,
    s.nature_route,
    s.geom,
    -- get the start point
    ST_StartPoint(s.geom) AS start_point,
    -- get the end point
    ST_EndPoint(s.geom) AS end_point,
    -- previous and next edges
    lag(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS previous_edge_id,
    lead(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS next_edge_id
FROM source AS s
ORDER BY road_code, edge_order
;
CREATE INDEX ON import.temp_edges (road_code);
CREATE INDEX ON import.temp_edges USING GIST (start_point);
CREATE INDEX ON import.temp_edges USING GIST (end_point);

-- Temporary nodes
DROP TABLE IF EXISTS import.temp_nodes CASCADE;
CREATE TABLE import.temp_nodes AS
WITH
union_start_end AS (
    SELECT id AS start_of, NULL AS end_of, start_point AS geom
    FROM import.temp_edges
    UNION ALL
    SELECT NULL AS start_of, id AS end_of, end_point AS geom
    FROM import.temp_edges
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
;
CREATE INDEX ON import.temp_nodes USING GIST (geom);

-- Insert nodes
INSERT INTO road_graph.nodes (geom)
SELECT geom
FROM import.temp_nodes
ON CONFLICT DO NOTHING
;

-- Insert road scale glossary
INSERT INTO road_graph.glossary_road_class
    (code, label)
WITH classes AS (
    SELECT
        CASE
            WHEN nature_route = 'AUT' THEN 'Autoroute'
            WHEN nature_route = 'RN' THEN 'Nationale'
            WHEN nature_route LIKE 'RD%' OR nature_route LIKE 'SEMOP' THEN 'Départementale'
            WHEN nature_route LIKE 'VCdC%' THEN 'Intercommunale'
            WHEN nature_route LIKE 'VC%' AND nature_route NOT LIKE 'VCdC%' THEN 'Communale'
            WHEN nature_route = 'Vprivée' THEN 'Voie privée'
            ELSE 'Autre'
        END
    AS road_class
    FROM import.temp_edges
)
SELECT DISTINCT
    road_class,
    road_class
FROM classes
ON CONFLICT DO NOTHING
;

-- Insert roads
INSERT INTO road_graph.roads (
    road_code, road_type, road_class,
    road_class_code, is_active, road_topic,
    is_access_road
)
SELECT DISTINCT
    road_code,

    CASE
        WHEN road_code ILIKE 'GIR%' THEN 'roundabout'
        ELSE 'road'
    END AS road_type,

    CASE
        WHEN nature_route = 'AUT' THEN 'Autoroute'
        WHEN nature_route = 'RN' THEN 'Nationale'
        WHEN nature_route LIKE 'RD%' OR nature_route LIKE 'SEMOP' THEN 'Départementale'
        WHEN nature_route LIKE 'VCdC%' THEN 'Intercommunale'
        WHEN nature_route LIKE 'VC%' AND nature_route NOT LIKE 'VCdC%' THEN 'Communale'
        WHEN nature_route = 'Vprivée' THEN 'Voie privée'
        ELSE 'Autre'
    END AS road_class,

    CASE
        WHEN nature_route NOT LIKE 'RD%' THEN null
        ELSE replace(replace(replace(nature_route, 'RD', ''), '_VDess', ''), '_Port', '')
    END AS road_class_code,

    CASE
        WHEN nature_route NOT IN ('Détruite', 'PROG') THEN True
        ELSE False
    END AS is_active,

    CASE
        WHEN nature_route LIKE '%Port%' THEN 'Port'
        ELSE NULL
    END AS road_topic,
    CASE
        WHEN nature_route NOT LIKE '%VDess%' THEN False
        ELSE TRUE
    END AS is_access_road
FROM import.temp_edges
ON CONFLICT DO NOTHING
;

-- Insert edges
INSERT INTO road_graph.edges (
    id, road_code,
    start_node, end_node,
    previous_edge_id, next_edge_id,
    geom
)
SELECT
    e.id, e.road_code,
    ns.id, ne.id,
    e.previous_edge_id, e.next_edge_id,
    e.geom
FROM import.temp_edges AS e
LEFT JOIN road_graph.nodes AS ns
    -- = is faster than ST_Equals
    ON ns.geom = e.start_point
LEFT JOIN road_graph.nodes AS ne
    ON ne.geom = e.end_point
ON CONFLICT DO NOTHING
;


-- Insert markers automatic markers at the start of the first edge
INSERT INTO road_graph.markers
(road_code, code, is_virtual, geom)
SELECT
    road_code,
    0,
    True,
    ST_StartPoint(geom)
FROM road_graph.edges
WHERE TRUE
AND previous_edge_id IS NULL
ON CONFLICT DO NOTHING;

-- Insert markers from source data
INSERT INTO road_graph.markers
(road_code, code, abscissa, is_virtual, geom)
SELECT
    axe AS road_code,
    libelle::int AS code,
    absd::real AS abscissa,
    CASE
        WHEN upper(trim("type")) = 'PRV' THEN TRUE
        ELSE FALSE
    END AS is_virtual,
    geom
FROM import.source_markers
;


-- Drop the temporary tables
DROP TABLE IF EXISTS import.temp_nodes;
DROP TABLE IF EXISTS import.temp_edges;

-- Set the serial values
SELECT setval(
    pg_get_serial_sequence('road_graph.edges', 'id'),
    (SELECT max(id) FROM road_graph.edges)
);
SELECT setval(
    pg_get_serial_sequence('road_graph.nodes', 'id'),
    (SELECT max(id) FROM road_graph.nodes)
);
SELECT setval(
    pg_get_serial_sequence('road_graph.roads', 'id'),
    (SELECT max(id) FROM road_graph.roads)
);
SELECT setval(
    pg_get_serial_sequence('road_graph.markers', 'id'),
    (SELECT max(id) FROM road_graph.markers)
);


-- Contrôles
--
-- Roads with wrong number of edges containing previous or next edge id
-- For example a road with 5 edges must have 1 edge with a NULL value in previous_edge_id
-- This query should return no data
/*
WITH count_edges AS (
    SELECT e.road_code,
    count(e.id) AS nb,
    count(previous_edge_id) FILTER (WHERE previous_edge_id IS NOT NULL) AS nb_previous_edge_ids,
    count(next_edge_id) FILTER (WHERE next_edge_id IS NOT NULL) AS nb_next_edge_ids
    FROM road_graph.edges AS e
    GROUP BY e.road_code
    ORDER BY e.road_code
)
SELECT *
FROM count_edges AS e
WHERE TRUE
AND (nb_previous_edge_ids + 1 != nb OR nb_next_edge_ids + 1 != nb)
;
*/

-- Update edges references
WITH s AS (
    SELECT e.road_code, array_agg(DISTINCT e.id) AS ids
    FROM road_graph.edges AS e
    WHERE start_marker IS NULL OR end_marker IS NULL
    GROUP BY road_code
)
SELECT
    road_code,
    road_graph.update_edge_references(s.road_code, NULL) AS travail
FROM s
ORDER BY road_code
;


-- Enable triggers
ALTER TABLE road_graph.edges ENABLE TRIGGER trg_aa_before_geometry_insert_or_update ;
ALTER TABLE road_graph.edges ENABLE TRIGGER trg_after_edge_delete ;
ALTER TABLE road_graph.edges ENABLE TRIGGER trg_after_edge_insert_or_update;
ALTER TABLE road_graph.edges ENABLE TRIGGER trg_before_edge_insert_or_update;
ALTER TABLE road_graph.nodes ENABLE TRIGGER trg_aa_before_geometry_insert_or_update;
ALTER TABLE road_graph.nodes ENABLE TRIGGER trg_before_node_delete;
ALTER TABLE road_graph.nodes ENABLE TRIGGER trg_after_node_insert_or_update;
ALTER TABLE road_graph.markers ENABLE TRIGGER trg_aa_before_geometry_insert_or_update;
ALTER TABLE road_graph.markers ENABLE TRIGGER trg_after_marker_insert_or_update_or_delete;

COMMIT;
