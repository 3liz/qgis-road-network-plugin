-- Session variables and search_path
--
-- * after_edge_insert_or_update: only create the marker 0 of a road if the road does not have any marker
-- * use SET LOCAL instead of SET for the road.graph.* session variables used by the topology triggers,
--   so that their value cannot leak outside of the current transaction
--   (for example with a connection pooler or a QGIS connection kept opened)
-- * remove the SET / RESET search_path inside the functions bodies:
--   RESET does not restore the caller search_path, and a nested function RESET
--   would cancel the search_path of the calling function.
--   The functions now use the quoted schema name "road_graph", which is not replaced
--   by the plugin when creating the road_graph_editing functions,
--   or the search_path is set in the function definition.
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION.

-- FUNCTION: road_graph.after_edge_delete()
-- Use SET LOCAL for the road.graph.* variables: their value is limited to the current transaction
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
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL)
        ;

    END IF;

    RETURN OLD;
END;
$$;

-- FUNCTION: road_graph.after_edge_insert_or_update()
-- Use SET LOCAL for the road.graph.* variables: their value is limited to the current transaction
-- Only create the marker 0 for a new road, i.e. a road without any marker:
-- the roads which do not start with the marker 0 (for example D147A starting at the marker 4 + 918)
-- must not get a marker 0 when their first edge is updated, for example when it is split
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
            UPDATE road_graph.nodes AS n
            SET geom = ST_StartPoint(NEW.geom)
            WHERE n.id = NEW.start_node
            AND NOT ST_Equals(n.geom, ST_StartPoint(NEW.geom))
            ;
        END IF;
        IF NOT ST_Equals(ST_EndPoint(OLD.geom), ST_EndPoint(NEW.geom))
            -- not just an inversion
            AND NOT (NEW.geom = ST_Reverse(OLD.geom))
        THEN
            -- Update downstream node if needed
            UPDATE road_graph.nodes AS n
            SET geom = ST_EndPoint(NEW.geom)
            WHERE n.id = NEW.end_node
            AND NOT ST_Equals(n.geom, ST_EndPoint(NEW.geom))
            ;
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
        IF OLD.start_node != NEW.start_node THEN
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
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom))
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
                NOT ST_Equals(OLD.geom, NEW.geom)
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

-- FUNCTION: road_graph.after_node_insert_or_update()
-- Use SET LOCAL for the road.graph.* variables: their value is limited to the current transaction
CREATE OR REPLACE FUNCTION road_graph.after_node_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_under_node record;
    id_new_edge integer;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');

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

    -- Update all upstream edges referenced ids
    UPDATE road_graph.edges AS t
    SET start_node = NEW.id
    WHERE ST_DWithin(NEW.geom, ST_StartPoint(t.geom), 0.50)
    AND start_node != NEW.id
    ;

    -- Update all downstream edges referenced ids
    UPDATE road_graph.edges AS t
    SET end_node = NEW.id
    WHERE ST_DWithin(NEW.geom, ST_EndPoint(t.geom), 0.50)
    AND end_node != NEW.id
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

    -- Split edges under the node if needed
    FOR edge_under_node IN
        SELECT
            -- we must pass all the fields
            -- to be able to copy/paste attributes data from the original edge
            -- when spliting it
            t.*
        FROM road_graph.edges AS t
        WHERE True
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

-- FUNCTION: road_graph.before_edge_insert_or_update()
-- Use SET LOCAL for the road.graph.* variables: their value is limited to the current transaction
CREATE OR REPLACE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_point geometry(point);
    end_point geometry(point);
    upstream_node record;
    downstream_node record;
    distance real;
    start_references jsonb;
    end_references jsonb;
    marker_zero record;
    raise_notice text;
    edge_road record;
    is_roundabout boolean;
    has_changed_road_code boolean;
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

    -- Create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

    IF TG_OP = 'INSERT'
    OR (TG_OP = 'UPDATE' AND NOT ST_Equals(NEW.geom, OLD.geom) AND NEW.geom IS NOT NULL)
    THEN

        -- Get first nodes < 0.5 m - If found, edit NEW geom
        -- upstream
        SELECT INTO upstream_node
            n.id, n.geom
        FROM road_graph.nodes AS n
        WHERE ST_DWithin(n.geom, start_point, 0.50)
        ORDER BY n.id, n.geom <-> start_point
        LIMIT 1
        ;

        -- upstream - create node or just update value
        IF upstream_node IS NOT NULL THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream_node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, upstream_node.id
                ;
            END IF;

            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, 0, upstream_node.geom);
            -- Update the node ID in upstream attribute
            NEW.start_node = upstream_node.id;
        ELSE
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, upstream node IS NULL -> create it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
                ;
            END IF;

            -- Create the missing node
            -- only for INSERT
            -- or for UPDATE if the start_node value has been deleted
            IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.start_node IS NULL) THEN
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (start_point)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT new_node.id
                FROM new_node
                INTO NEW.start_node
                ;
                IF NEW.start_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% BEFORE edge % n° %, upstream node created : % %',
                        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.start_node,
                        (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.start_node)
                    ;
                END IF;
            END IF;
        END IF;

        -- get downstream node
        SELECT INTO downstream_node
            n.id, n.geom
        FROM road_graph.nodes AS n
        WHERE ST_DWithin(n.geom, end_point, 0.50)
        ORDER BY n.id, n.geom <-> end_point
        LIMIT 1
        ;
        -- downstream node - create or update value
        IF downstream_node IS NOT NULL THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node NOT NULL : % -> use it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, downstream_node.id
                ;
            END IF;
            -- Update the geometry
            NEW.geom = ST_SetPoint(NEW.geom, ST_NPoints(NEW.geom) - 1, downstream_node.geom);
            -- Update the node ID in downstream attribute
            NEW.end_node = downstream_node.id;
        ELSE
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% BEFORE edge % n° %, downstream node IS NULL -> create it',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
                ;
            END IF;

            -- Create the missing node
            -- only for INSERT
            -- or for UPDATE if the end_node value has been deleted
            IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NEW.end_node IS NULL) THEN
                -- Create the missing node
                WITH new_node AS (
                    INSERT INTO road_graph.nodes
                    (geom)
                    VALUES
                    (end_point)
                    ON CONFLICT ON CONSTRAINT nodes_geom_key DO NOTHING
                    RETURNING id
                )
                SELECT new_node.id
                FROM new_node
                INTO NEW.end_node
                ;
                IF NEW.end_node IS NOT NULL AND raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% BEFORE edge % n° %, downstream node created : % %',
                        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, NEW.end_node,
                        (SELECT ST_AsText(geom) FROM road_graph.nodes WHERE id = NEW.end_node)
                    ;
                END IF;
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
            );
        END IF;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND NOT has_changed_road_code
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
    IF NOT is_roundabout
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

-- FUNCTION: road_graph.drop_editing_session(integer)
-- Use "road_graph".xxx instead of changing the search_path with SET / RESET search_path:
-- the quoted schema name is not replaced by the plugin when creating the road_graph_editing functions
CREATE OR REPLACE FUNCTION road_graph.drop_editing_session(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    _set_config text;
    _set_val bigint;
BEGIN
    -- Get editing session record
    -- The quoted schema name "road_graph" is not replaced by the plugin
    -- since we need to get information from the road_graph schema,
    -- not the copy
    SELECT INTO editing_session_record
        *
    FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Raise exception if no record found
    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;

    -- Set the sequences back to the maximum id in the main tables
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('"road_graph".roads', 'id'), (SELECT max(id) FROM "road_graph".roads))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('"road_graph".markers', 'id'), (SELECT max(id) FROM "road_graph".markers))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('"road_graph".edges', 'id'), (SELECT max(id) FROM "road_graph".edges))
    ;
    SELECT INTO _set_val
        setval(pg_get_serial_sequence('"road_graph".nodes', 'id'), (SELECT max(id) FROM "road_graph".nodes))
    ;

    -- Delete merged editing session
    DELETE FROM "road_graph".editing_sessions
    WHERE id = _editing_session_id
    ;

    RETURN True;
END;
$$;

-- FUNCTION: road_graph.editing_survey()
-- Use "road_graph".xxx instead of changing the search_path with SET / RESET search_path:
-- the quoted schema name is not replaced by the plugin when creating the road_graph_editing functions
CREATE OR REPLACE FUNCTION road_graph.editing_survey() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    object_geom geometry;
    object_id integer;
    editing_session_geom geometry;
    original_edge_object record;
    new_logged_ids jsonb;
BEGIN

    -- Do nothing if the table is inside the production road_graph schema
    IF TG_TABLE_SCHEMA = 'road_graph' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSE
            RETURN NEW;
        END IF;
    END IF;

    -- Do nothing if the object has not changed for an update
    IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN
        RETURN NEW;
    END IF;

    -- Do nothing if data are the same as the road_graph data
    -- Since UPDATEs can occur on newly created object in the road_graph_editing schema
    -- we must check if a corresponding object exists in the road_graph schema
    -- before checking if they are equal
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'edges' THEN
        -- We use the quoted schema name "road_graph" to avoid using syntax
        -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
        -- when creating the road_graph_editing schema

        -- Get original edge
        SELECT INTO original_edge_object
            *
        FROM "road_graph".edges
        WHERE id = NEW.id
        ;
        IF
            original_edge_object IS NOT NULL
            AND md5(to_json(NEW.*)::text) = md5(to_json(original_edge_object.*)::text)
        THEN
            RETURN NEW;
        END IF;
    END IF;

    -- 1/ Check if geometry is inside the editing session geometry
    -- Get editing session geometry
    IF to_jsonb(NEW) ? 'geom' THEN
        SELECT INTO editing_session_geom
            geom
        FROM road_graph_editing.editing_sessions
        LIMIT 1
        ;

        -- Get edited object geometry
        IF TG_OP = 'DELETE' THEN
            object_geom = OLD.geom;
        ELSE
            object_geom = NEW.geom;
        END IF;

        -- Check if the inserted, updated or deleted geometry is inside the editing session polygon
        -- The actual check depends on the operation
        IF TG_OP IN ('INSERT', 'DELETE') THEN
            -- INSERT AND DELETE must concern geometries which are fully inside the polygon
            -- NB : We must be less rigid since split actions can insert or update
            -- an edge which is not fully in the polygon
            -- IF NOT ST_Within(object_geom, editing_session_geom) THEN
            IF NOT ST_Intersects(object_geom, editing_session_geom) THEN
                RAISE EXCEPTION
                    -- 'The object geometry must be strictly inside the editing session polygon
                    -- object = %, polygon = %',
                    'The object geometry must intersect the editing session polygon
                    object = %, polygon = %',
                    ST_AsText(object_geom),
                    ST_AsText(editing_session_geom)
                ;
            END IF;
        END IF;

        -- UPDATE must modify only the part of the geometry which is inside the polygon
        IF TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom) THEN
            IF (
                -- Easy for points
                TG_TABLE_NAME != 'edges'
                AND NOT ST_Within(object_geom, editing_session_geom)
            )
            OR (
                -- For edges (linestrings) we must check the user did not change
                -- the geometry outside the polygon
                -- TODO : issue when deleting node which launches the merge of 2 edges
                -- the edge which was outside the polygon has been merged which modifies its geometry
                -- We do not enforce this constraint
                TG_TABLE_NAME = 'edges'
                /*
                AND NOT ST_Equals(
                    ST_SymDifference(OLD.geom, editing_session_geom),
                    ST_SymDifference(NEW.geom, editing_session_geom)
                )
                */
                AND NOT ST_Intersects(object_geom, editing_session_geom)
            )
            THEN
                -- RAISE EXCEPTION 'The updated geometry must be strictly inside the editing session polygon';
                RAISE EXCEPTION 'The updated geometry must intersect the editing session polygon';
            END IF;
        END IF;
    END IF;

    -- 2/ Log editing inside the road_graph_editing table*
    -- edited object id
    object_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.id
        ELSE NEW.id
    END
    ;

    -- Update log in the editing session
    -- We use the quoted schema name "road_graph" to avoid using syntax
    -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
    -- when creating the road_graph_editing schema
    SELECT INTO new_logged_ids
        logged_ids
    FROM "road_graph".editing_sessions
    WHERE unique_code = (
        SELECT unique_code
        FROM road_graph_editing.editing_sessions
        LIMIT 1
    )
    ;

    IF new_logged_ids[TG_TABLE_NAME] IS NULL THEN
        new_logged_ids[TG_TABLE_NAME] = '{}'::jsonb;
        new_logged_ids[TG_TABLE_NAME][object_id::text] = format('"%s"', substr(TG_OP, 1, 1))::jsonb;
    ELSE
        IF new_logged_ids[TG_TABLE_NAME][object_id::text] IS NULL THEN
            new_logged_ids[TG_TABLE_NAME][object_id::text] = format('"%s"', substr(TG_OP, 1, 1))::jsonb;
        ELSE
            -- DELETE
            IF TG_OP = 'DELETE' THEN
                -- If the DELETE comes after an INSERT, remove it completely
                -- The user has just deleted an object which has been inserted in the editing session
                IF new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"'::jsonb THEN
                    new_logged_ids = new_logged_ids #- (format('{%s,%s}', TG_TABLE_NAME, object_id::text))::text[];
                ELSE
                    new_logged_ids[TG_TABLE_NAME][object_id::text] = '"D"';
                END IF;
            -- UPDATE
            ELSIF TG_OP = 'UPDATE' THEN
                -- If the UPDATE comes after an INSERT, keep the I
                -- The object could be updated several times afterwards
                -- we must keep the track of its insert
                IF new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"'::jsonb THEN
                     new_logged_ids = new_logged_ids;
                ELSE
                    new_logged_ids[TG_TABLE_NAME][object_id::text] = '"U"';
                END IF;
            -- INSERT
            ELSE
                new_logged_ids[TG_TABLE_NAME][object_id::text] = '"I"';
            END IF;
        END IF;
    END IF;

    UPDATE "road_graph".editing_sessions
    SET
        -- Add the id of the object
        logged_ids = new_logged_ids,
        "status" = 'edited'
    WHERE unique_code = (
        SELECT unique_code
        FROM road_graph_editing.editing_sessions
        LIMIT 1
    )
    ;

    -- Return object
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;
$$;

-- FUNCTION: road_graph.split_edge_by_node(record, record, real, real)
-- Use "road_graph".xxx instead of changing the search_path with SET / RESET search_path:
-- the quoted schema name is not replaced by the plugin when creating the road_graph_editing functions
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

    -- Return True
    RETURN id_new_object;

END;
$_$;

-- FUNCTION: road_graph.merge_editing_session_data(integer)
-- Set the search_path in the function definition (recommended for SECURITY DEFINER functions):
-- it is restored automatically at the end of the function, instead of SET / RESET search_path
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO road_graph, public
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    road_codes text[];
    managed_object record;
    updated_objects_number integer;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    IF editing_session_record.status != 'edited' THEN
        RAISE EXCEPTION 'The given id % does not correspond to editing session with status ''edited'' !', _editing_session_id;
    END IF;

    -- Disable topology triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(FALSE)
        INTO _toggle_triggers;

    -- Get the road codes concerned by the editing session
    -- no need to prefix the function by the schema name because the search_path is set in the function definition
    SELECT INTO road_codes
        get_updated_roads_from_editing_session(_editing_session_id)
    ;

    -- Create SQL to run for each edited data
    -- no need to prefix the function by the schema name because the search_path is set in the function definition
    FOR sql_record IN
        SELECT
            sql_text
        FROM create_queries_from_editing_session(_editing_session_id)
    LOOP
       -- RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    -- Update the managed objects concerned by the editing session changes
    IF road_codes IS NOT NULL AND array_length(road_codes, 1) > 0 THEN
        -- Disable the trigger (to avoid calculate_managed_object_references to be executed)
        SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
        -- Loop for each managed_object tables
        FOR managed_object IN
            SELECT o.*
            FROM "road_graph".managed_objects AS o
        LOOP
            SELECT INTO updated_objects_number
                road_graph.update_managed_objects_on_graph_change(
                    managed_object.schema_name,
                    managed_object.table_name,
                    road_codes
                )
            ;
            RAISE NOTICE 'Number of updated managed objects for "%"."%": %',
                managed_object.schema_name,
                managed_object.table_name,
                Coalesce(updated_objects_number, 0)
            ;
        END LOOP;
        -- Re-enable triggers
        SELECT set_config('road.graph.disable.trigger', '0'::text, true)
        INTO _set_config;

    END IF;

    -- Truncate road_graph_editing tables
    TRUNCATE road_graph_editing.roads CASCADE;
    TRUNCATE road_graph_editing.markers CASCADE;
    TRUNCATE road_graph_editing.edges CASCADE;
    TRUNCATE road_graph_editing.nodes CASCADE;
    TRUNCATE road_graph_editing.editing_sessions CASCADE;

    -- Delete merged editing session
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Refresh the materialized view of the last updated managed objects
    REFRESH MATERIALIZED VIEW CONCURRENTLY road_graph.vm_managed_objects;

    RETURN True;
END;
$$;
