-- Road graph checks - G06: the error of the fix gives the issues preventing to order the edges of the road
--
-- When the previous and next edges are still inconsistent with the geometry after the fix relink_edges,
-- the error message now gives the reversed edges (G05) and the branches (G07) of the road, with their nodes and edges,
-- instead of only asking to fix the issues of these checks.

-- fix_graph_relink_edges(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_relink_edges(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    issue record;
    link record;
    relinked_ids integer[];
    blocking_issues text;
BEGIN
    IF _params->>'road_code' IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', 'The parameter road_code is needed'
        );
    END IF;

    -- Wrong links of the road, found again: they may have been fixed by a previous fix
    SELECT i.*
    INTO issue
    FROM road_graph.check_graph_edge_links(ARRAY[_params->>'road_code'], NULL) AS i
    ;
    IF issue.road_code IS NULL THEN
        RETURN jsonb_build_object(
            'status', 'skipped',
            'message', format(
                'The previous and next edges of the road %s are consistent with the geometry', _params->>'road_code'
            )
        );
    END IF;

    -- Set the expected previous and next edges. The references are not calculated by the triggers
    -- after each update, but once for the whole road at the end
    PERFORM set_config('road.graph.edge.ref.calc.disabled', 'yes', true);
    FOR link IN
        SELECT
            (l->>'edge_id')::integer AS edge_id,
            (l->>'expected_previous_edge_id')::integer AS previous_edge_id,
            (l->>'expected_next_edge_id')::integer AS next_edge_id
        FROM jsonb_array_elements(issue.details->'links') AS l
    LOOP
        UPDATE road_graph.edges AS e
        SET previous_edge_id = link.previous_edge_id, next_edge_id = link.next_edge_id
        WHERE e.id = link.edge_id
        AND (e.previous_edge_id, e.next_edge_id) IS DISTINCT FROM (link.previous_edge_id, link.next_edge_id)
        ;
        relinked_ids = relinked_ids || link.edge_id;
    END LOOP;
    PERFORM set_config('road.graph.edge.ref.calc.disabled', 'no', true);
    PERFORM road_graph.update_edge_references(issue.road_code, NULL);

    -- Check the links are now consistent with the geometry.
    -- If not, the exception cancels the changes of this fix (see fix_graph_issue)
    IF EXISTS (SELECT 1 FROM road_graph.check_graph_edge_links(ARRAY[issue.road_code], NULL)) THEN
        -- Issues of the road preventing to order its edges: reversed edges (G05) and branches (G07)
        SELECT string_agg(
            format(
                '%s%s (edges %s)',
                b.check_code,
                CASE WHEN b.node_ids IS NOT NULL THEN format(' at the node %s', array_to_string(b.node_ids, ', ')) END,
                array_to_string(b.edge_ids, ', ')
            ),
            ', ' ORDER BY b.check_code, b.edge_ids
        )
        INTO blocking_issues
        FROM (
            SELECT r.check_code, r.node_ids, r.edge_ids
            FROM road_graph.check_graph_reversed_edges(ARRAY[issue.road_code], NULL) AS r
            UNION ALL
            SELECT r.check_code, r.node_ids, r.edge_ids
            FROM road_graph.check_graph_road_branches(ARRAY[issue.road_code], NULL) AS r
        ) AS b
        ;
        RAISE EXCEPTION
            'The previous and next edges of the road % are still inconsistent with the geometry: %',
            issue.road_code,
            CASE
                WHEN blocking_issues IS NOT NULL THEN format('fix first the issues %s', blocking_issues)
                ELSE 'fix the issues of the checks G05 and G07 first'
            END;
    END IF;

    RETURN jsonb_build_object(
        'status', 'success',
        'message', format(
            'The previous and next edges of the edges %s of the road %s have been set from the geometry',
            array_to_string(relinked_ids, ', '), issue.road_code
        )
    );
END;
$$;


-- FUNCTION fix_graph_relink_edges(_params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_relink_edges(_params jsonb) IS 'Action relink_edges run by the function fix_graph_issue, to fix the issues of the check G06:
set the previous and next edges of the edges of a road from the geometry.

The parameter _params must contain the road code: {"road_code": "D12"}.
The wrong links of the road are found again with the check G06, then the expected previous and next edges are set,
and the references of the road are calculated again.

If the links are still inconsistent, for example because of reversed edges (G05) or of a branch of the road (G07),
the error message gives these issues, with their nodes and edges.

It returns a JSON object with the keys status (success, skipped if the links are consistent with the geometry,
or error) and message.';
