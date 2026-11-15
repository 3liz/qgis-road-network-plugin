-- References of the edges: only the edges downstream of a modification are updated
--
-- The references of a point only depend on the geometry of the road upstream of it.
-- When an edge is created, modified or deleted, or when a marker is created, moved or deleted,
-- the triggers now only calculate again the references of the edges downstream of the modification,
-- instead of all the edges of the road. For example, for a vertex moved near the end of a road of 135 edges,
-- 0.3 s instead of 1.7 s.
--
-- update_edge_references: new parameter _from_edge_ids, giving the edges from which the references are updated

DROP FUNCTION IF EXISTS road_graph.update_edge_references(text, integer[]);

-- update_edge_references(text, integer[], integer[])
CREATE OR REPLACE FUNCTION road_graph.update_edge_references(_road_code text DEFAULT NULL::text, _edge_ids integer[] DEFAULT NULL::integer[], _from_edge_ids integer[] DEFAULT NULL::integer[]) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    _run_build_cache boolean;
    _set_config text;
    _use_cache boolean;
    raise_notice text;
    from_edge_order integer;
BEGIN
    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Deactivate triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- If there is less than 2 edges, we do not use cache
    -- since it will not be useful and will raise an exception ( by function get_ordered_edges)
    SELECT INTO _use_cache
        CASE
            WHEN (SELECT COUNT(*) FROM road_graph.edges AS e WHERE e.road_code = _road_code) < 2 THEN False
            ELSE True
        END
    ;
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% update_edge_references road n° % - Use cache %',
            repeat('    ', pg_trigger_depth()::integer), _road_code, _use_cache
        ;
    END IF;

    -- Build road & marker objects cache for speeding up linear referencing
    -- It is always built again, since the road graph has been modified:
    -- it also removes the outdated cache of a road which has now less than 2 edges
    IF _road_code IS NOT NULL THEN
        SELECT road_graph.build_road_cached_objects(_road_code)
        INTO _run_build_cache;
    END IF;

    -- Only the edges downstream of the given edges are updated: the references of a point
    -- only depend on the geometry of the road upstream of it. The first edge is the given edge
    -- with the lowest order in the road, stored in the cache. The edges without order,
    -- for example after a gap without next edge, are also updated.
    -- All the edges of the road are updated if none of the given edges has an order
    IF _from_edge_ids IS NOT NULL AND _road_code IS NOT NULL AND _use_cache THEN
        SELECT min((r.edge_orders->>(f.id::text))::integer)
        INTO from_edge_order
        FROM road_cache AS r, unnest(_from_edge_ids) AS f(id)
        WHERE r.road_code = _road_code
        ;
        IF from_edge_order IS NOT NULL THEN
            SELECT array_agg(e.id)
            INTO _edge_ids
            FROM road_graph.edges AS e, road_cache AS r
            WHERE e.road_code = _road_code
            AND r.road_code = _road_code
            AND (_edge_ids IS NULL OR e.id = ANY(_edge_ids))
            AND Coalesce((r.edge_orders->>(e.id::text))::integer >= from_edge_order, True)
            ;
            -- No edge to update
            IF _edge_ids IS NULL THEN
                PERFORM set_config('road.graph.disable.trigger', '0'::text, true);
                RETURN True;
            END IF;
        END IF;
    END IF;

    -- Get edges references
    WITH s AS (
        SELECT
            e.road_code, e.id,
            x.*
        FROM
            road_graph.edges AS e,
            json_to_record(
                -- 2nd parameter is TRUE so that we use precomputed object cache
                road_graph.get_edge_references(e.id, _use_cache)
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
$$;


-- FUNCTION update_edge_references(_road_code text, _edge_ids integer[], _from_edge_ids integer[])
COMMENT ON FUNCTION road_graph.update_edge_references(_road_code text, _edge_ids integer[], _from_edge_ids integer[]) IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges.

If _from_edge_ids is given with _road_code, only the edges downstream of the most upstream of these edges are updated,
including this edge: the references of a point only depend on the geometry of the road upstream of it.
The edges without order in the road, for example after a gap without next edge, are also updated.
All the edges are updated if none of the given edges is found in the order of the road.
';

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
        SET LOCAL road.graph.edge.ref.calc.disabled = 'yes';

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
        SET LOCAL road.graph.edge.ref.calc.disabled = 'no';

        -- Calculate references
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER EDGE % N° %, update all road edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                update_edge_references_result::text
            ;
        END IF;
        -- Only from the previous edge, or else the next edge, of the deleted edge
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL, ARRAY[OLD.previous_edge_id, OLD.next_edge_id])
        ;

    END IF;

    RETURN OLD;
END;
$$;

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
                -- Only from the edge, or from its previous edge, whose end point may have moved with the node
                SELECT INTO update_edge_references_result
                    road_graph.update_edge_references(
                        NEW.road_code,
                        NULL,
                        ARRAY[
                            NEW.id,
                            NEW.previous_edge_id,
                            CASE WHEN TG_OP = 'UPDATE' THEN OLD.previous_edge_id END
                        ]
                    )
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

-- after_marker_insert_or_update_or_delete()
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
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    -- Get log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = Coalesce(NEW.road_code, OLD.road_code)
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this marker does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    -- Check if only a marker 0 is used
    IF TG_OP != 'DELETE' AND is_roundabout AND NEW.code != 0 THEN
        RAISE EXCEPTION 'The value of the roundabout marker code must be 0 !';
    END IF;

    -- Update : Do nothing if geometry has not changed
    IF TG_OP = 'UPDATE' AND ST_Equals(NEW.geom, OLD.geom) THEN
        RETURN NEW;
    END IF;

    -- For roundabout, calculate edges previous and next ids
    -- anytime geometry is modified
    IF TG_OP != 'DELETE' AND is_roundabout
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
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                NEW.road_code,
                update_edge_references_result::text
            ;
        END IF;
        -- Only from the edge closest to the marker, or to its old position on the same road,
        -- or from the previous edge of this edge, whose end point can be at the marker.
        -- All the edges of a roundabout are updated: their order starts at the marker
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(
                NEW.road_code,
                NULL,
                CASE WHEN is_roundabout THEN NULL ELSE ARRAY(
                    SELECT unnest(ARRAY[c.id, c.previous_edge_id])
                    FROM unnest(ARRAY[
                        NEW.geom,
                        CASE WHEN TG_OP = 'UPDATE' AND OLD.road_code = NEW.road_code THEN OLD.geom END
                    ]) AS p(geom)
                    CROSS JOIN LATERAL (
                        SELECT e.id, e.previous_edge_id
                        FROM road_graph.edges AS e
                        WHERE e.road_code = NEW.road_code
                        AND ST_DWithin(e.geom, p.geom, 50)
                        ORDER BY ST_Distance(e.geom, p.geom), e.id
                        LIMIT 1
                    ) AS c
                    WHERE p.geom IS NOT NULL
                ) END
            )
        ;
    END IF;

    -- Also update old road if marker road has changed
    IF (
        (TG_OP = 'UPDATE' AND NEW.road_code != OLD.road_code)
            OR (TG_OP = 'DELETE')
        )
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                OLD.road_code,
                update_edge_references_result::text
            ;
        END IF;
        -- Only from the edge closest to the old position of the marker, or from its previous edge.
        -- The OLD road can be a roundabout: in this case, all its edges are updated
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(
                OLD.road_code,
                NULL,
                CASE WHEN (SELECT r.road_type FROM road_graph.roads AS r WHERE r.road_code = OLD.road_code) = 'roundabout'
                THEN NULL ELSE ARRAY(
                    SELECT unnest(ARRAY[c.id, c.previous_edge_id])
                    FROM (
                        SELECT e.id, e.previous_edge_id
                        FROM road_graph.edges AS e
                        WHERE e.road_code = OLD.road_code
                        AND ST_DWithin(e.geom, OLD.geom, 50)
                        ORDER BY ST_Distance(e.geom, OLD.geom), e.id
                        LIMIT 1
                    ) AS c
                ) END
            )
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
COMMENT ON FUNCTION road_graph.after_marker_insert_or_update_or_delete() IS 'Update road edges references after a marker has been created, updated or deleted.

Only the edges downstream of the edge closest to the marker, or to its old position, are updated.';
