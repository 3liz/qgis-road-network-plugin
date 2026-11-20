-- Roundabouts: clear message when the digitized circle is not connected to any road
--
-- clean_digitized_roundabout failed with the error "Argument to ST_Y() must have type POINT"
-- when the circle was not cut by any edge, for example when the edges crossing it are marked
-- as not connected (no_intersection_cutting). It now raises an exception explaining the problem,
-- with the ids of the edges marked as not connected which cross the circle.

-- clean_digitized_roundabout(text)
CREATE OR REPLACE FUNCTION road_graph.clean_digitized_roundabout(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    node_to_delete integer;
    delete_result bool;
    merge_edges_result bool;
    left_node_geom geometry(Point, 2154);
    not_cut_edge_ids text;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- The circle must be cut by the edges of the roads crossing it:
    -- stop with a clear message if none of its nodes is shared with another road,
    -- for example if the crossing edges are marked as not connected (no_intersection_cutting)
    IF NOT EXISTS (
        SELECT 1
        FROM road_graph.edges AS e
        JOIN road_graph.edges AS f
            ON f.road_code != _road_code
            AND (
                f.start_node IN (e.start_node, e.end_node)
                OR f.end_node IN (e.start_node, e.end_node)
            )
        WHERE e.road_code = _road_code
    ) THEN
        -- Edges crossing the circle but marked as not connected
        SELECT string_agg(f.id::text, ', ' ORDER BY f.id)
        INTO not_cut_edge_ids
        FROM road_graph.edges AS e
        JOIN road_graph.edges AS f
            ON f.road_code != _road_code
            AND ST_Intersects(e.geom, f.geom)
        WHERE e.road_code = _road_code
        AND Coalesce(f.no_intersection_cutting, False)
        ;
        RAISE EXCEPTION 'The roundabout % is not connected to any other road: '
            'its circle must cross the edges of the roads, which are then cut by a node. %',
            _road_code,
            CASE
                WHEN not_cut_edge_ids IS NOT NULL
                THEN format(
                    'The edges %s cross it but are marked as not connected (no_intersection_cutting): '
                    'uncheck this field, then draw the circle again.',
                    not_cut_edge_ids
                )
                ELSE 'Draw the circle again so that it crosses the roads.'
            END
        ;
    END IF;

    -- Delete the edges inside the roundabout
    -- and update the roads remainging around the roundabout
    -- (previous & next edge ids)
    WITH del AS (
        DELETE FROM road_graph.edges AS d
        WHERE d.road_code != _road_code
        AND ST_ContainsProperly(
            (
            SELECT
            ST_Buffer(ST_Polygonize(geom), 0.1)
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code
            ),
            d.geom
        ) RETURNING d.id, d.previous_edge_id, d.next_edge_id
    ),
    update_previous AS (
        UPDATE road_graph.edges AS e
        SET next_edge_id = d.next_edge_id
        FROM del AS d
        WHERE e.next_edge_id = d.id
        AND e.id != d.id
    ),
    update_next AS (
        UPDATE road_graph.edges AS e
        SET previous_edge_id = d.previous_edge_id
        FROM del AS d
        WHERE e.previous_edge_id = d.id
        AND e.id != d.id
    )
    SELECT True
    INTO delete_result;

    -- Remove the circle node added add 12 o'clock
    -- by QGIS digitizing tool
    -- We use the merge_edges function which does the job
    SELECT
        INTO node_to_delete
        road_graph.get_upper_roundabout_node_to_delete(
            _road_code
        )
    ;
    IF raise_notice in ('info', 'debug') THEN
        RAISE NOTICE 'Node to delete % ', coalesce(node_to_delete, -1);
    END IF;

    IF node_to_delete IS NOT NULL THEN
        SELECT
            INTO merge_edges_result
            road_graph.merge_edges(
                (SELECT id FROM road_graph.edges WHERE end_node = node_to_delete LIMIT 1),
                (SELECT id FROM road_graph.edges WHERE start_node = node_to_delete LIMIT 1)
            )
        ;
    END IF;

    -- Add marker 0 in the left node
    SELECT
        INTO left_node_geom
        n.geom
    FROM road_graph.nodes AS n
    JOIN road_graph.edges AS e
        ON e.start_node = n.id
    WHERE True
    AND e.road_code = _road_code
    ORDER BY ST_X(n.geom)
    LIMIT 1
    ;
    IF left_node_geom IS NOT NULL AND NOT EXISTS (
        SELECT id
        FROM road_graph.markers
        WHERE road_code = _road_code
        AND "code" = 0
    )
    THEN
        INSERT INTO road_graph.markers (
            road_code, "code", geom, abscissa
        ) VALUES (
            _road_code, 0, left_node_geom, 0
        );
    END IF;

    RETURN TRUE;

END;
$$;


-- FUNCTION clean_digitized_roundabout(_road_code text)
COMMENT ON FUNCTION road_graph.clean_digitized_roundabout(_road_code text) IS 'Clean a roundabout digitized by QGIS circle tool:
* delete the edges inside the roundabout
* remove the circle node added add 12 o''clock
* add a marker for this roundabout if not already present

An exception is raised if the roundabout is not connected to any other road, for example when the edges
crossing the circle are marked as not connected (no_intersection_cutting), and are therefore not cut by it.
';
