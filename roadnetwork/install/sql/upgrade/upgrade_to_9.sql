
-- after_edge_delete()
CREATE OR REPLACE FUNCTION road_graph.after_edge_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    has_changed boolean;
    test_edges record;
    merge_result boolean;
    node_already_deleted integer;
    update_edge_references_result boolean;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN OLD;
    END IF;

    -- Raise notice ?
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Get node ID which has been manually deleted by user
    -- and must not be deleted twice
    node_already_deleted = road_graph.get_current_setting('road.graph.node.already.deleted', '-1');

    -- Check if old referenced nodes are still referenced by edges
    -- If not, delete them
    -- Upstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.start_node
    AND id != node_already_deleted
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
    AND id != node_already_deleted
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.end_node
        OR end_node = OLD.end_node
    );

    -- Merge edges which were linked to the deleted node
    -- ONLY if
    -- there are only 2 edges linked to the node
    -- AND if these edges have the same road code
    -- AND the road_code is not the same as the deleted edge
    -- to avoid merging the remaining edge with the next one which has the same road code
    -- a/ Node A - OLD.end_node
    -- RAISE NOTICE 'Node A - %', OLD.end_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes,
        max(t.road_code) AS road_code
    FROM road_graph.edges AS t
    WHERE TRUE
    AND (OLD.end_node = t.end_node OR OLD.end_node = t.start_node)
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    -- And if road_code is not the same as the deleted edge
    IF
        test_edges.nb = 2
        AND test_edges.nb_road_codes = 1
        -- prevent to merge the remaining edge with the next one which has the same road code as the deleted edge
        AND test_edges.road_code != OLD.road_code
    THEN
        IF raise_notice IN ('yes', 'info', 'debug') THEN
            RAISE NOTICE 'after_edge_delete % OLD.end_node - We must merge - % ',
                OLD.id, array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- b/ Node B - OLD.start_node
    -- RAISE NOTICE 'Node B - %', OLD.start_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes,
        max(t.road_code) AS road_code
    FROM road_graph.edges AS t
    WHERE TRUE
    AND (OLD.start_node = t.end_node OR OLD.start_node = t.start_node)
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF
        test_edges.nb = 2
        AND test_edges.nb_road_codes = 1
        AND test_edges.road_code != OLD.road_code
    THEN
        IF raise_notice IN ('yes', 'info', 'debug') THEN
            RAISE NOTICE 'after_edge_delete % OLD.start_node - We must merge - % ',
                OLD.id, array_to_string(test_edges.ids, ' et ')
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
