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


-- Parts of the lines calculated from references: they are ordered along the road with the order of the edges,
-- instead of the references of their start and end points. For example, 0.4 s instead of 10 s
-- for 480 lines with several parts. The results are the same

-- reorder_multilinestring_parts(geometry, text)
CREATE OR REPLACE FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text) RETURNS geometry
    LANGUAGE plpgsql
    SET jit TO 'off'
    AS $$
DECLARE
    _result_multilinestring geometry(MULTILINESTRING, 2154);
BEGIN

    -- Return untouched geometry if road code is not given
    IF _road_code IS NULL OR trim(_road_code) = '' THEN
        RETURN _multilinestring;
    END IF;

    -- Return untouched geometry if NULL or not a MultiLineString
    IF Coalesce(ST_GeometryType(_multilinestring), '') != 'ST_MultiLineString' THEN
        RETURN _multilinestring;
    END IF;

    -- Return untouched geometry if there is only one part
    IF ST_NumGeometries(_multilinestring) <= 1 THEN
        RETURN _multilinestring;
    END IF;

    BEGIN
        -- Reorder the geometry parts
        WITH
        -- Order of the edges of the road, in the direction of the road
        ordered_edges AS MATERIALIZED (
            SELECT o.id, min(o.edge_order) AS edge_order
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
            GROUP BY o.id
        ),
        -- Split the multilinestring into parts
        parts AS (
            SELECT (ST_Dump(_multilinestring)).geom
        ),
        -- Position of the start and end points of each part along the road:
        -- the order of the closest edge of the road, plus the location of the point along this edge, from 0 to 1.
        -- It increases in the direction of the road, without calculating the references of the points
        positions AS (
            SELECT
                p.geom,
                (
                    SELECT o.edge_order + ST_LineLocatePoint(e.geom, ST_StartPoint(p.geom))
                    FROM road_graph.edges AS e
                    JOIN ordered_edges AS o ON o.id = e.id
                    WHERE e.road_code = _road_code
                    AND ST_DWithin(e.geom, ST_StartPoint(p.geom), 50)
                    ORDER BY ST_Distance(e.geom, ST_StartPoint(p.geom)), o.edge_order
                    LIMIT 1
                ) AS start_position,
                (
                    SELECT o.edge_order + ST_LineLocatePoint(e.geom, ST_EndPoint(p.geom))
                    FROM road_graph.edges AS e
                    JOIN ordered_edges AS o ON o.id = e.id
                    WHERE e.road_code = _road_code
                    AND ST_DWithin(e.geom, ST_EndPoint(p.geom), 50)
                    ORDER BY ST_Distance(e.geom, ST_EndPoint(p.geom)), o.edge_order
                    LIMIT 1
                ) AS end_position
            FROM parts AS p
        )
        -- Reassemble the parts in the order of their position along the road,
        -- and reverse the parts in the opposite direction of the road
        SELECT INTO _result_multilinestring
            ST_Multi(
                ST_Collect(
                    CASE WHEN p.start_position > p.end_position THEN ST_Reverse(p.geom) ELSE p.geom END
                    ORDER BY least(p.start_position, p.end_position)
                )
            )
        FROM positions AS p
        -- No result if a part is far from the road: the geometry is returned untouched
        HAVING bool_and(p.start_position IS NOT NULL AND p.end_position IS NOT NULL)
        ;
    EXCEPTION WHEN OTHERS THEN
        _result_multilinestring = _multilinestring;
    END;

    RETURN Coalesce(_result_multilinestring, _multilinestring);

END;
$$;


-- FUNCTION reorder_multilinestring_parts(_multilinestring geometry, _road_code text)
COMMENT ON FUNCTION road_graph.reorder_multilinestring_parts(_multilinestring geometry, _road_code text) IS 'Reorder the parts of the given road MULTILINESTRING based on the graph.

For each part, the position of its start and end points along the road is given by the order of their closest edge
of the road, and by their location along this edge: the parts are ordered by these positions, and reversed
if needed (since the ST_OffsetCurve sometimes does not respect the node order).
The references of the points are not calculated, which is much faster.
The function is not compiled by the JIT: the high costs of the PostGIS functions trigger it, which is slower here.

The geometry is returned untouched if a part is more than 50 m from the road.';


-- Check G06: previous and next edges inconsistent with the geometry.
-- They give the order of the edges of the road, used to calculate the references.
-- The checks are numbered in the order of their fixes: this check must be fixed after G05,
-- and before the checks using the order of the edges. The checks G06 to G10 become G07 to G11.
-- Their codes are changed first, to keep the options set by the administrator

UPDATE road_graph.glossary_graph_check
SET code = 'renumbered_' || code
WHERE code IN ('G06', 'G07', 'G08', 'G09', 'G10')
;
UPDATE road_graph.glossary_graph_check
SET code = CASE code
    WHEN 'renumbered_G06' THEN 'G07'
    WHEN 'renumbered_G07' THEN 'G08'
    WHEN 'renumbered_G08' THEN 'G09'
    WHEN 'renumbered_G09' THEN 'G10'
    WHEN 'renumbered_G10' THEN 'G11'
END
WHERE code LIKE 'renumbered_%'
;

INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G06', 'Previous and next edges inconsistent with the geometry', 'error', 'check_graph_edge_links', '{relink_edges}', '{}', 'Edges whose previous or next edge contradicts the geometry: the linked edge is not connected to the edge whereas another edge of the road is, or it is connected to another edge. The links across a gap of the road are kept. The roundabouts are ignored. Fix: relink_edges sets the previous and next edges from the geometry, then calculates again the references of the road.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G07', 'Road with branches', 'error', 'check_graph_road_branches', '{}', '{}', 'Node with more than two edges of the same road, for example the ramps or the carriageways of a road using the code of the road. The roundabouts are ignored. The branches are the edges which are not the most aligned edges arriving at the node and leaving it. Fix: manual, for example by giving another road code to the edges of the branches.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G08', 'Parallel edges of the same road', 'warning', 'check_graph_parallel_edges', '{}', '{"tolerance": 30, "min_length": 10}', 'Edges of the same road, not connected by a node, running along each other less than the tolerance from each other along at least the minimum length, for example the two carriageways of a road using the same road code. The edges which only approach each other, for example on both sides of a roundabout, and the roundabouts are ignored. Options: tolerance and min_length, in meters. Fix: manual, for example by giving another road code to one of the carriageways.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G09', 'Short edge', 'warning', 'check_graph_short_edges', '{collapse_edge}', '{"min_length": 3}', 'Edge shorter than the minimum length, for example two junctions a few decimeters from each other. The edges of the roundabouts and the edges arriving at a roundabout or leaving it are ignored, unless they are reduced to a point. Option: min_length, in meters. Fix: collapse_edge deletes the edge and merges its nodes: its geometry is added to the next or previous edge of the same road, and the other edges of the removed node are moved to the kept node. The only edge of a road has no fix action.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G10', 'First marker not at the start of the road', 'error', 'check_graph_first_marker', '{move_marker}', '{"tolerance": 1}', 'First marker of a road (lowest code and abscissa, which can be virtual) not at the start point of the first edge of the road. When the marker is after the start point, the start of the road has no reference. The roundabouts are ignored. Option: tolerance, in meters. Fix: move_marker moves the marker to the start of the road, if it is less than the tolerance from it.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;
INSERT INTO road_graph.glossary_graph_check (code, title, severity, function_name, fix_actions, default_options, description)
VALUES ('G11', 'Markers not in the order of the road', 'error', 'check_graph_markers_order', '{}', '{"marker_tolerance": 50}', 'Markers whose position along their road is not in the order of their code and abscissa. The longest sequence of markers in the order of the road is kept, and the other markers are returned. Also returns the markers far from their road (option marker_tolerance, in meters), or whose closest edge has no reference. The roundabouts are ignored. Fix: manual, by moving the marker or changing its code.')
ON CONFLICT (code) DO UPDATE SET
    title = EXCLUDED.title,
    severity = EXCLUDED.severity,
    function_name = EXCLUDED.function_name,
    fix_actions = EXCLUDED.fix_actions,
    description = EXCLUDED.description
;


-- check_graph_edge_links(text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph_edge_links(_road_codes text[], _options jsonb) RETURNS TABLE(check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Edges of the checked roads, except the roundabouts, whose order starts at their marker,
    -- and the edges starting and ending at the same node
    road_edges AS MATERIALIZED (
        SELECT e.id, e.road_code, e.start_node, e.end_node, e.previous_edge_id, e.next_edge_id, e.geom
        FROM road_graph.edges AS e
        JOIN road_graph.roads AS r ON r.road_code = e.road_code
        WHERE r.road_type != 'roundabout'
        AND e.start_node != e.end_node
        AND (_road_codes IS NULL OR e.road_code = ANY(_road_codes))
    ),
    -- Edges connected to each edge by the nodes: the only edge of the road starting at its end node,
    -- and the only edge of the road ending at its start node. NULL if there is none, or several (branches)
    connected AS MATERIALIZED (
        SELECT
            e.*,
            (
                SELECT CASE WHEN count(*) = 1 THEN min(n.id) END
                FROM road_edges AS n
                WHERE n.road_code = e.road_code
                AND n.start_node = e.end_node
                AND n.id != e.id
            ) AS connected_next_id,
            (
                SELECT CASE WHEN count(*) = 1 THEN min(p.id) END
                FROM road_edges AS p
                WHERE p.road_code = e.road_code
                AND p.end_node = e.start_node
                AND p.id != e.id
            ) AS connected_previous_id
        FROM road_edges AS e
    ),
    -- Next and previous edges contradicting the geometry:
    -- * another edge of the road is connected to the edge,
    -- * or the linked edge is connected to another edge, or connected to the edge in the opposite direction.
    -- The other links, across a gap of the road, for example on both sides of a roundabout, are kept
    wrong_links AS (
        SELECT
            c.*,
            c.next_edge_id IS DISTINCT FROM c.connected_next_id AND (
                c.connected_next_id IS NOT NULL
                OR n.connected_previous_id IS NOT NULL
                OR n.connected_next_id = c.id
            ) AS is_next_wrong,
            c.previous_edge_id IS DISTINCT FROM c.connected_previous_id AND (
                c.connected_previous_id IS NOT NULL
                OR p.connected_next_id IS NOT NULL
                OR p.connected_previous_id = c.id
            ) AS is_previous_wrong
        FROM connected AS c
        LEFT JOIN connected AS n ON n.id = c.next_edge_id
        LEFT JOIN connected AS p ON p.id = c.previous_edge_id
    ),
    -- Edges with a wrong link, and their expected links: the connected edges, or no link
    issues AS (
        SELECT
            w.*,
            CASE WHEN w.is_next_wrong THEN w.connected_next_id ELSE w.next_edge_id END AS expected_next_id,
            CASE WHEN w.is_previous_wrong THEN w.connected_previous_id ELSE w.previous_edge_id END
                AS expected_previous_id
        FROM wrong_links AS w
        WHERE w.is_next_wrong OR w.is_previous_wrong
    )
    -- One issue for each road
    SELECT
        'G06'::text,
        'Previous and next edges inconsistent with the geometry'::text,
        'error'::text,
        i.road_code,
        array_agg(i.id ORDER BY i.id),
        NULL::integer[],
        NULL::integer[],
        jsonb_build_object(
            'edge_count', count(*),
            'links', jsonb_agg(
                jsonb_build_object(
                    'edge_id', i.id,
                    'previous_edge_id', i.previous_edge_id,
                    'next_edge_id', i.next_edge_id,
                    'expected_previous_edge_id', i.expected_previous_id,
                    'expected_next_edge_id', i.expected_next_id
                )
                ORDER BY i.id
            )
        ),
        'relink_edges'::text,
        jsonb_build_object('road_code', i.road_code),
        ST_LineInterpolatePoint((array_agg(i.geom ORDER BY i.id))[1], 0.5),
        ST_Multi(ST_Collect(i.geom ORDER BY i.id))
    FROM issues AS i
    GROUP BY i.road_code
    ORDER BY i.road_code
    ;
END;
$$;


-- FUNCTION check_graph_edge_links(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_edge_links(_road_codes text[], _options jsonb) IS 'Check G06 run by the function check_graph: find the edges whose previous or next edge contradicts the geometry.

The previous and next edges (fields previous_edge_id and next_edge_id) give the order of the edges of the road,
used to calculate the references. The next edge of an edge is wrong when it does not start at the end node
of the edge, whereas another edge of the road starts there, or when it is connected to another edge,
or to the edge in the opposite direction. The previous edge is checked the same way. The other links,
across a gap of the road, for example on both sides of a roundabout, are kept. The roundabouts are ignored.
The expected link is the connected edge, or no link: a gap between two close nodes is fixed by the check G03.

An issue is returned for each road. The details give the number of edges and, for each edge,
its previous and next edges and the expected ones.
The issue can be fixed with the action relink_edges, which sets the expected previous and next edges
of the road, then calculates again the references of the road.';


-- fix_graph_relink_edges(jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_relink_edges(_params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    issue record;
    link record;
    relinked_ids integer[];
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
        RAISE EXCEPTION
            'The previous and next edges of the road % are still inconsistent with the geometry: '
            'fix the issues of the checks G05 and G07 first',
            issue.road_code;
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

It returns a JSON object with the keys status (success, skipped if the links are consistent with the geometry,
or error) and message.';


-- Functions of the renumbered checks: check code in their result, and in their comment

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
    -- First markers which are not at the start of the first edge of their road
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
        'G10'::text,
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
    ORDER BY i.road_code
    ;
END;
$$;


-- FUNCTION check_graph_first_marker(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_first_marker(_road_codes text[], _options jsonb) IS 'Check G10 run by the function check_graph: find the roads whose first marker is not at the start of the road.

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
    -- Tolerance on the positions of the markers: two markers at the same point can have
    -- slightly different positions, for example at the end of an edge and at the start of the next edge
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
    -- Previous and next markers in the order of the road, around a marker out of this order
    previous_index integer;
    next_index integer;
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
            previous_index = (SELECT k FROM generate_series(i - 1, 1, -1) AS k WHERE in_order[k] LIMIT 1);
            next_index = (SELECT k FROM generate_series(i + 1, marker_count) AS k WHERE in_order[k] LIMIT 1);
            check_code = 'G11';
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
                'previous_marker_code', marker_codes[previous_index],
                'previous_marker_position', round(marker_positions[previous_index]::numeric, 1),
                'next_marker_code', marker_codes[next_index],
                'next_marker_position', round(marker_positions[next_index]::numeric, 1)
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
COMMENT ON FUNCTION road_graph.check_graph_markers_order(_road_codes text[], _options jsonb) IS 'Check G11 run by the function check_graph: find the markers which are not in the order of their road.

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
    -- Length of the other edge along which each part runs: the parts of two edges which only approach
    -- each other, for example the edges of a road on both sides of a roundabout,
    -- run along a very short length of the other edge
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
        'G08'::text,
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
    ORDER BY s.road_code, s.a_id, s.b_id
    ;
END;
$$;


-- FUNCTION check_graph_parallel_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_parallel_edges(_road_codes text[], _options jsonb) IS 'Check G08 run by the function check_graph: find the edges of the same road running parallel to each other,
for example the two carriageways of a road using the same road code.

An issue is returned for each pair of edges of the same road, not connected by a node, which run along each other
less than the option tolerance from each other (30 m by default), along at least the option min_length (10 m by default).
The edges which only approach each other, for example on both sides of a roundabout, are ignored. The roundabouts are ignored.
The details give the distance between the edges and the length along which they are parallel.
The line of the issue shows the parallel parts of the edges.

The issue must be fixed manually, for example by giving another road code to one of the carriageways.';


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
        UNION ALL
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
        'G07'::text,
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
    ORDER BY b.road_code, b.node_id
    ;
END;
$$;


-- FUNCTION check_graph_road_branches(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_road_branches(_road_codes text[], _options jsonb) IS 'Check G07 run by the function check_graph: find the roads with branches, for example the ramps
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
        'G09'::text,
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
    ORDER BY i.road_code, i.id
    ;
END;
$$;


-- FUNCTION check_graph_short_edges(_road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph_short_edges(_road_codes text[], _options jsonb) IS 'Check G09 run by the function check_graph: find the short edges, often coming from the digitizing,
for example two junctions a few decimeters from each other.

An issue is returned for each edge shorter than the option min_length (3 m by default).
The edges of the roundabouts and the edges arriving at a roundabout or leaving it are often short: they are ignored,
unless they are reduced to a point (less than 10 cm). The severity is error for an edge reduced to a point, else warning.
The details give the length, the number of other edges at the start and end nodes, and if the edge is near a roundabout.

The issue can be fixed with the action collapse_edge, which deletes the edge and merges its nodes,
adding its geometry to the next or previous edge of the same road: the node to keep is the node with the most edges, or else the node with the lowest id,
and is the first node of node_ids. The only edge of a road has no fix action.';


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
            'message', format(
                'The edge %s starting and ending at the node %s has been deleted', edge.id, edge.start_node
            )
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
COMMENT ON FUNCTION road_graph.fix_graph_collapse_edge(_params jsonb) IS 'Action collapse_edge run by the function fix_graph_issue, to fix the issues of the check G09:
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
COMMENT ON FUNCTION road_graph.fix_graph_move_marker(_params jsonb) IS 'Action move_marker run by the function fix_graph_issue, to fix the issues of the check G10:
move the first marker of a road to the start of the road.

The parameter _params must contain the ids of the marker and of the first edge of the road,
and the start point of the edge as EWKT: {"marker_id": 12, "edge_id": 34, "point": "SRID=2154;POINT(x y)"}.
The marker is moved to the start point of the edge: the triggers calculate again the references of the road.

It returns a JSON object with the keys status (success, skipped if the marker does not exist anymore,
is already at the start of the road, or if the edge does not start at the given point anymore) and message.';
