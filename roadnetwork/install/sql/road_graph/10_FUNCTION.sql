--
-- PostgreSQL database dump
--






SET statement_timeout = 0;
SET lock_timeout = 0;


SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- aa_before_geometry_insert_or_update()
CREATE FUNCTION road_graph.aa_before_geometry_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    is_roundabout boolean;
    _road_type text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Do not modify the geometry if geom field has not been changed
    IF TG_OP = 'UPDATE' AND (
            ST_Equals(OLD.geom, NEW.geom)
            OR
            ST_Equals(NEW.geom, ST_ReducePrecision(NEW.geom, 0.10))
        )
    THEN
        RETURN NEW;
    END IF;

    -- Do not modifiy for roundabout, else we have weird aspect
    IF TG_TABLE_NAME = 'edges'
    THEN
        -- Get edge road
        SELECT INTO _road_type
            r.road_type
        FROM road_graph.roads AS r
        WHERE r.road_code = NEW.road_code
        ;
        IF _road_type = 'roundabout' THEN
            RETURN NEW;
        END IF;
    END IF;

    -- Reduce geometry precision
    NEW.geom = ST_ReducePrecision(NEW.geom, 0.10);

    RETURN NEW;
END;
$$;


-- FUNCTION aa_before_geometry_insert_or_update()
COMMENT ON FUNCTION road_graph.aa_before_geometry_insert_or_update() IS 'Fonction qui arrondit la précision des coordonnées à 0.1 soit 10cm si ce n''est pas déjà fait
lors d''une création ou d''une modification de géométrie.
Elle est préfixée par aa_ pour être lancée avant les autres trigger,
car l''ordre alphabétique compte.
';


-- after_edge_delete()
CREATE FUNCTION road_graph.after_edge_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    has_changed boolean;
    test_edges record;
    merge_result boolean;
    update_edge_references_result boolean;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN OLD;
    END IF;

    -- Raise notice ?
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Check if old referenced nodes are still referenced by edges
    -- If not, delete them
    -- Upstream node
    DELETE FROM road_graph.nodes
    WHERE id = OLD.start_node
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
    AND NOT EXISTS (
        SELECT id
        FROM road_graph.edges
        WHERE start_node = OLD.end_node
        OR end_node = OLD.end_node
    );

    -- Merge edges which were linked to the deleted node
    -- Node A - OLD.end_node
    -- RAISE NOTICE 'Node A - %', OLD.end_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes
    FROM road_graph.edges AS t
    WHERE OLD.end_node = t.end_node OR OLD.end_node = t.start_node
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF test_edges.nb = 2 AND test_edges.nb_road_codes = 1 THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE 'We must merge - % ',
                array_to_string(test_edges.ids, ' et ')
            ;
        END IF;
        SELECT road_graph.merge_edges(test_edges.ids[1], test_edges.ids[2]) AS merge_edges
        INTO merge_result;
    END IF;

    -- Node B - OLD.start_node
    -- RAISE NOTICE 'Node B - %', OLD.start_node;
    SELECT INTO test_edges
        count(DISTINCT t.id) AS nb,
        array_agg(DISTINCT t.id) AS ids,
        count(DISTINCT t.road_code) AS nb_road_codes
    FROM road_graph.edges AS t
    WHERE OLD.start_node = t.end_node OR OLD.start_node = t.start_node
    AND t.id != OLD.id
    ;
    -- Only merge edges if they are only 2 and if road_code is the same
    IF test_edges.nb = 2 AND test_edges.nb_road_codes = 1 THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE 'We must merge - % ',
                array_to_string(test_edges.ids, ' et ')
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


-- after_edge_insert_or_update()
CREATE FUNCTION road_graph.after_edge_insert_or_update() RETURNS trigger
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
    update_edge_references_result boolean;
    raise_notice text;
    cascade_edge_id integer;
    edge_road record;
    is_roundabout boolean;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

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
        -- Upstream node
        IF OLD.start_node != NEW.start_node THEN
            WITH del AS (
                DELETE FROM road_graph.nodes
                WHERE id = OLD.start_node
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
        OR (TG_OP = 'UPDATE' AND NEW.previous_edge_id != OLD.previous_edge_id AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET next_edge_id = NEW.id
        WHERE e.id != NEW.id
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
        OR (TG_OP = 'UPDATE' AND NEW.next_edge_id != OLD.next_edge_id AND NOT is_roundabout)
    THEN
        UPDATE road_graph.edges AS e
        SET previous_edge_id = NEW.id
        WHERE e.id != NEW.id
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

    -- For INSERT OR UPDATE
    -- Create nodes at intersection with other edges if needed
    -- This will then run the trigger after_node_insert_or_update
    -- which will eventually split the edges intersecting this NEW edge
    created_nodes_at_intersection = ARRAY[]::integer[];
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND NOT ST_Equals(OLD.geom, NEW.geom))
        -- avoid infinite loop and self-crossing
        AND road_graph.get_current_setting('road.graph.edge.crossing.node.creation.pending', 'no', 'text') != 'yes'

    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER edge % n° %, crossing node test',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;
        FOR crossing_node IN
            WITH new_nodes AS (
                SELECT
                    ST_Intersection(e.geom, NEW.geom) AS geom
                FROM road_graph.edges AS e
                WHERE e.id != NEW.id
                AND ST_Intersects(e.geom, NEW.geom)
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
        LOOP
            IF crossing_node.geom IS NOT NULL
                AND ST_GeometryType(crossing_node.geom) = 'ST_Point'
            THEN
                -- Create the missing node, which will trigger the split of the edge
                -- (see the trigger after_node_insert_or_update)
                -- Set variable to avoid infinite loop & self-crossing detection
                SET road.graph.edge.crossing.node.creation.pending = 'yes';
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
                SET road.graph.edge.crossing.node.creation.pending = 'no';

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
        node_to_be_deleted = road_graph.get_current_setting('road.graph.merge.edges.useless.node', '-1', 'integer');

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
            AND n.id != node_to_be_deleted
        LOOP
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % n° % start % end %, update existing node under new edge to launch the split : % %',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id,
                NEW.start_node, NEW.end_node,
                    touching_node.id, ST_AsText(touching_node.geom)
                ;
            END IF;

            IF road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no', 'text') != 'yes'
            THEN
                SET road.graph.edge.update.touching.node = 'yes';
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
                SET road.graph.edge.update.touching.node = 'no';
                IF raise_notice IN ('info', 'debug') THEN
                    RAISE NOTICE '% AFTER EDGE % N° %, NEW node created in same node place: %',
                        REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id, new_node_id
                    ;
                END IF;
            END IF;
        END LOOP;
    END IF;

    -- Calculate references of the edge whole road
    -- Beware to choose correct conditions
    -- For roundabout, do not do it
    -- (this is a choice, to let the user choose the first road and marker)
    IF (TG_OP = 'INSERT' AND NOT is_roundabout) OR (
        TG_OP = 'UPDATE' AND (
            NOT ST_Equals(OLD.geom, NEW.geom)
            OR OLD.previous_edge_id != NEW.previous_edge_id
            OR OLD.next_edge_id != NEW.next_edge_id
        ) AND NOT is_roundabout
    )
    THEN
        IF road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
        THEN
            IF raise_notice IN ('info', 'debug') THEN
                RAISE NOTICE '% AFTER EDGE % N° %, update all road edges references: %',
                    REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                    update_edge_references_result::text
                ;
            END IF;
            SELECT INTO update_edge_references_result
                road_graph.update_edge_references(NEW.road_code, NULL)
            ;
        END IF;
    END IF;

    -- Notice used for big imports
    RAISE NOTICE '% AFTER edge % n° % - End',
        repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
    ;

    RETURN NEW;
END;
$$;


-- FUNCTION after_edge_insert_or_update()
COMMENT ON FUNCTION road_graph.after_edge_insert_or_update() IS 'Multiples opérations lancées suite à la modification d''un troncon.
Déplacement du noeud initial et terminal liés si besoin.
Suppression des noeuds orphelins si besoin.
Création des noeuds non existants à l''intersection avec les autres edges';


-- after_marker_insert_or_update_or_delete()
CREATE FUNCTION road_graph.after_marker_insert_or_update_or_delete() RETURNS trigger
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
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    -- Get log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Get edge road
    SELECT INTO edge_road
        r.*
    FROM road_graph.roads AS r
    WHERE r.road_code = NEW.road_code
    ;
    IF edge_road.id IS NULL THEN
        RAISE EXCEPTION 'The road code given for this marker does not exist !';
    END IF;
    is_roundabout = (edge_road.road_type = 'roundabout');

    -- Check if only a marker 0 is used
    IF is_roundabout AND NEW.code != 0 AND TG_OP != 'DELETE' THEN
        RAISE EXCEPTION 'The value of the roundabout marker code must be 0 !';
    END IF;

    -- Update : Do nothing if geometry has not changed
    IF TG_OP = 'UPDATE' AND ST_Equals(NEW.geom, OLD.geom) THEN
        RETURN NEW;
    END IF;

    -- For roundabout, calculate edges previous and next ids
    -- anytime geometry is modified
    IF is_roundabout
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
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, NEW.id,
                NEW.road_code,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(NEW.road_code, NULL)
        ;
    END IF;

    -- Also update old road if marker road has changed
    IF (
        (TG_OP = 'UPDATE' AND NEW.road_code != OLD.road_code)
            OR (TG_OP = 'DELETE')
        )
        AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no', 'text') = 'no'
    THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% AFTER MARKER % N° %, update all road % edges references: %',
                REPEAT('    ', pg_trigger_depth()::INTEGER), TG_OP, OLD.id,
                OLD.road_code,
                update_edge_references_result::text
            ;
        END IF;
        SELECT INTO update_edge_references_result
            road_graph.update_edge_references(OLD.road_code, NULL)
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
COMMENT ON FUNCTION road_graph.after_marker_insert_or_update_or_delete() IS 'Update road edges references after a marker has been created, updated or deleted.';


-- after_node_insert_or_update()
CREATE FUNCTION road_graph.after_node_insert_or_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_under_node record;
    id_new_edge integer;
    raise_notice text;
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- Check if we must log
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

    -- Do nothing if geometry has not changed
    -- except road.graph.edge.update.touching.node equals yes
    IF
        TG_OP = 'UPDATE'
        AND ST_Equals(NEW.geom, OLD.geom)
        AND road_graph.get_current_setting('road.graph.edge.update.touching.node', 'no', 'text') = 'no'
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
    SET road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, 0, NEW.geom)
    WHERE TRUE
    AND start_node = NEW.id
    AND NOT ST_Intersects(ST_StartPoint(geom), NEW.geom)
    ;
    SET road.graph.edge.crossing.node.creation.pending = 'no';

    -- Update geometry of all downstream edges linked to the node
    -- We replace the last edge node but the node NEW geometry
    -- We deactivate the creation of crossing nodes for these updated edges
    SET road.graph.edge.crossing.node.creation.pending = 'yes';
    UPDATE road_graph.edges AS t
    SET geom = ST_SetPoint(geom, ST_NPoints(geom) - 1, NEW.geom)
    WHERE True
    AND end_node = NEW.id
    AND NOT ST_Intersects(ST_EndPoint(geom), NEW.geom)
    ;
    SET road.graph.edge.crossing.node.creation.pending = 'no';

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
';


-- before_edge_insert_or_update()
CREATE FUNCTION road_graph.before_edge_insert_or_update() RETURNS trigger
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
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0', 'integer') = 1
    THEN
        RETURN NEW;
    END IF;

    -- log level
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');

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
    is_roundabout = (edge_road.road_type = 'roundabout' AND ST_IsRing(NEW.geom));
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% BEFORE edge % n° %, edge is_roundabout %,
        %',
            repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id, is_roundabout,
            to_json(edge_road)
        ;
    END IF;

    -- If it is a new roundabout, reverse the geometry if needed
    -- QGIS digitizing circle tool create counter-clockwise polygons
    IF is_roundabout AND ST_IsPolygonCW(ST_MakePolygon(NEW.geom))
    THEN
        NEW.geom = ST_Reverse(NEW.geom);
    END IF;

    -- Do nothing if geometry has not changed
    IF TG_OP = 'UPDATE' AND ST_Equals(NEW.geom, OLD.geom) THEN
        IF raise_notice IN ('info', 'debug') THEN
            RAISE NOTICE '% BEFORE edge % n° %, NEW and OLD geom are equal',
                repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
            ;
        END IF;

        RETURN NEW;
    END IF;

    -- INSERT - create missing nodes if necessary
    -- start & end point
    start_point = ST_StartPoint(NEW.geom);
    end_point = ST_EndPoint(NEW.geom);

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

    -- Marker - Create marker 0 if needed
    -- it does not exists for the road
    -- only for roads, not for roundabout
    IF TG_OP = 'INSERT'
        AND NOT is_roundabout
        AND edge_road.road_type NOT IN ('roundabout')
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_EndPoint(NEW.geom))
        AND NEW.previous_edge_id IS NULL
        AND NEW.road_code IS NOT NULL
        AND NOT EXISTS (
            SELECT m.id
            FROM road_graph.markers AS m
            WHERE m.road_code = NEW.road_code
            AND m.code = 0
        )
    THEN
        INSERT INTO road_graph.markers (road_code, code, abscissa, geom)
        VALUES (NEW.road_code, 0, 0, start_point)
        ON CONFLICT DO NOTHING
        ;
    END IF;

    -- Move marker 0 if needed
    -- If the first edge of the road has changed
    -- move it to the start_point of the changed geometry
    IF TG_OP = 'UPDATE'
        AND NOT is_roundabout
        AND OLD.start_abscissa = 0 AND NEW.start_abscissa = 0
        AND OLD.start_cumulative = 0 AND NEW.start_cumulative = 0
        AND OLD.start_marker = 0 AND NEW.start_marker = 0
        AND OLD.road_code = NEW.road_code
        AND NOT ST_Equals(ST_StartPoint(NEW.geom), ST_StartPoint(OLD.geom))
    THEN
        -- Do not run the function update_edge_references
        -- on the road triggered inside the after_marker_insert_or_update_or_delete
        -- trigger since it will already been triggered with the edge geometry change
        SET road.graph.edge.ref.calc.disabled = 'yes';
        UPDATE road_graph.markers AS m
        SET geom = ST_StartPoint(NEW.geom)
        WHERE m.road_code = NEW.road_code
        AND m.code = 0
        AND ST_Equals(m.geom, ST_StartPoint(OLD.geom))
        ;
        SET road.graph.edge.ref.calc.disabled = 'no';
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
                RAISE NOTICE '% BEFORE edge % n° %, NEW references NOT calculated',
                    repeat('    ', pg_trigger_depth()::integer), TG_OP, NEW.id
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
Finally, we calculate the references (marker, abscissa, cumulative) for the start and end of the edge
based on its geometry and position on the road.
';


-- before_editing_sessions_update()
CREATE FUNCTION road_graph.before_editing_sessions_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN

    -- Geometry must not be updated if the status is not created
    IF NOT ST_Equals(OLD.geom, NEW.geom) AND NEW."status" != 'created' THEN
        RAISE EXCEPTION 'The geometry of the editing session cannot be updated after data has been cloned or edited !';
    END IF;

    RETURN NEW;
END;
$$;


-- FUNCTION before_editing_sessions_update()
COMMENT ON FUNCTION road_graph.before_editing_sessions_update() IS 'Prevent from updating an editing session geometry if there is data inside the editing_session schema';


-- build_road_cached_objects(text)
CREATE FUNCTION road_graph.build_road_cached_objects(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN

    DROP TABLE IF EXISTS road_cache;
    CREATE TEMPORARY TABLE IF NOT EXISTS road_cache
    ON COMMIT DROP AS (
        WITH
        ordered_edges AS (
            -- get road ordered edges (use previous and next edge ids)
            SELECT o.id, o.road_code, o.edge_order, o.geom
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
        ),
        touching_edges AS (
            -- For each edge, add a line at the end of it
            -- from the previous edge (lag) end point
            -- to the edge start_point
            SELECT
                o.id, o.road_code, o.edge_order,
                -- closing lines between last end point and edge start point
                ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                ) AS closing_line,
                -- Merge closing lines and edge geometry
                ST_LineMerge(ST_Union(
                    ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                    ),
                    o.geom
                )) AS geom
            FROM ordered_edges AS o
            ORDER BY o.edge_order
        ),
        road_line AS (
            -- Create a single linestring by merging all edge augmented linestrings
            SELECT
                max(t.road_code) AS road_code,
                ST_MakeLine(t.geom ORDER BY t.edge_order) AS geom,
                ST_CollectionExtract(
                    ST_MakeValid(
                        ST_Multi(ST_Collect(t.closing_line ORDER BY t.edge_order))
                    ),
                    2
                ) AS closing_multilinestring
            FROM touching_edges AS t
        )
        SELECT
            _road_code AS road_code,
            l.geom AS simple_linestring,
            l.closing_multilinestring,
            jsonb_agg(
                jsonb_build_object(
                    m.id,
                    ST_LineLocatePoint(l.geom, m.geom)
                )
            ) AS marker_locations
        FROM
            road_line as l,
            road_graph.markers AS m
        WHERE TRUE
        AND m.road_code = _road_code
        AND l.road_code = _road_code
        GROUP BY l.geom, l.closing_multilinestring
    );

    RETURN TRUE;

END;
$$;


-- FUNCTION build_road_cached_objects(_road_code text)
COMMENT ON FUNCTION road_graph.build_road_cached_objects(_road_code text) IS 'Calculate the given road geometries used in linear referencing tools & the road marker locations:
* the simple linestring (no gaps) made by merging all edges linestrings after connecting edges
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)

Cached data is store in a temporary table living only until COMMIT
';


-- clean_digitized_roundabout(text)
CREATE FUNCTION road_graph.clean_digitized_roundabout(_road_code text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    node_to_delete integer;
    delete_result bool;
    merge_edges_result bool;
    left_node_geom geometry(Point, 2154);
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    IF raise_notice in ('info', 'debug') THEN
        RAISE NOTICE 'get_ordered_edges - _initial_id = %',
            _initial_id
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
';


-- copy_data_to_editing_session(integer)
CREATE FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    _set_config text;
BEGIN
    -- Get editing session record
    SELECT INTO editing_session_record
    *
    FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;
    IF editing_session_record.id IS NULL THEN
        RAISE EXCEPTION 'There is no editing session in the database with given id % !', _editing_session_id;
    END IF;

    IF editing_session_record.status != 'created' THEN
        RAISE EXCEPTION 'The given id % does not correspond to editing session with status ''created'' !', _editing_session_id;
    END IF;

    -- Disable triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- Truncate tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;

    -- Get roads to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    roads AS (
        SELECT DISTINCT r.*
        FROM road_graph.roads AS r
        JOIN edges_roads AS e
            ON r.road_code = e.road_code
    )
    INSERT INTO editing_session.roads
    SELECT *
    FROM roads
    ;

    -- Get nodes to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.start_node, e.end_node
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    ),
    nodes AS (
        SELECT DISTINCT n.*
        FROM road_graph.nodes AS n
        JOIN edges AS e
            ON n.id IN (e.start_node, e.end_node)
    )
    INSERT INTO editing_session.nodes
    SELECT *
    FROM nodes
    ;

    -- Get edges to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    edges AS (
        SELECT DISTINCT e.*
        FROM road_graph.edges AS e
        JOIN edges_roads AS er
            ON e.road_code = er.road_code
    )
    INSERT INTO editing_session.edges
    SELECT *
    FROM edges
    ;

    -- Get markers to edit
    WITH
    edges_roads AS (
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        JOIN road_graph.editing_sessions AS s
            ON ST_Intersects(s.geom, e.geom)
        WHERE s.id = _editing_session_id
    ),
    markers AS (
        SELECT DISTINCT m.*
        FROM road_graph.markers AS m
        JOIN edges_roads AS er
            ON m.road_code = er.road_code
    )
    INSERT INTO editing_session.markers
    SELECT *
    FROM markers
    ;

    -- Replace identities by road_schema sequences to get correct ids
    BEGIN
        ALTER TABLE editing_session.roads ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.nodes ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.markers ALTER COLUMN id DROP IDENTITY;
        ALTER TABLE editing_session.edges ALTER COLUMN id DROP IDENTITY;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Cannot drop identity in editing_session tables';
    END;
    -- roads
    ALTER TABLE editing_session.roads
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.roads', 'id'));
    -- nodes
    ALTER TABLE editing_session.nodes
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.nodes', 'id'));
    -- markers
    ALTER TABLE editing_session.markers
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.markers', 'id'));
    -- edges
    ALTER TABLE editing_session.edges
        ALTER COLUMN id SET DEFAULT nextval(pg_get_serial_sequence('road_graph.edges', 'id'));

    -- Set ids in the editing_sessions
    UPDATE road_graph.editing_sessions
    SET cloned_ids = jsonb_build_object(
        'roads', (SELECT jsonb_agg(id::text) FROM editing_session.roads),
        'nodes', (SELECT jsonb_agg(id::text) FROM editing_session.nodes),
        'markers', (SELECT jsonb_agg(id::text) FROM editing_session.markers),
        'edges', (SELECT jsonb_agg(id::text) FROM editing_session.edges)
    ),
    "status" = 'cloned'
    WHERE id = _editing_session_id
    ;

    -- In the table editing_session.editing_sessions
    -- keep only the current editing session
    -- This table will be used by the trigger editing_survey
    -- to collect changes in the editing_sessions tables
    TRUNCATE editing_session.editing_sessions RESTART IDENTITY;
    INSERT INTO editing_session.editing_sessions
    SELECT * FROM road_graph.editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Audit trigger on editing_session tables
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.roads;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.roads
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.nodes;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.nodes
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.markers;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.markers
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();
    DROP TRIGGER IF EXISTS trg_editing_survey ON editing_session.edges;
    CREATE TRIGGER trg_editing_survey
    AFTER INSERT OR UPDATE OR DELETE ON editing_session.edges
    FOR EACH ROW EXECUTE PROCEDURE road_graph.editing_survey();

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;

    RETURN True;
END;
$$;


-- FUNCTION copy_data_to_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.copy_data_to_editing_session(_editing_session_id integer) IS 'Copy production data from the road_graph shema to the editing_session schema corresponding to the given editing session ID.';


-- create_queries_from_editing_session(integer)
CREATE FUNCTION road_graph.create_queries_from_editing_session(_editing_session_id integer) RETURNS TABLE(sql_text text)
    LANGUAGE plpgsql
    AS $$
DECLARE
    editing_session_record record;
BEGIN
    RETURN QUERY
    WITH
    items AS (
        SELECT unnest(array['nodes', 'roads', 'edges', 'markers']) AS item
    ),
    ids AS (
        SELECT item, logged_ids->item AS item_ids
        FROM items, "road_graph".editing_sessions
        WHERE status = 'edited'
        AND id = _editing_session_id
    ),
    item_cols AS (
        SELECT item, string_agg(c.column_name, ', ' ORDER BY ordinal_position) AS cols
        FROM items, information_schema.columns c
        WHERE table_schema = 'road_graph'
        AND table_name = item
        AND c.column_name != 'id'
        GROUP BY item
    ),
    queries AS (
        SELECT item, "key"::integer AS id, "value" AS op
        FROM ids, jsonb_each_text(item_ids)
    )
    SELECT concat(
        CASE
            WHEN op = 'I' THEN 'INSERT INTO'
            WHEN op = 'U' THEN 'UPDATE'
            WHEN op = 'D' THEN 'DELETE FROM'
        END, ' ',
        '"road_graph".', q.item, ' AS t ',
        CASE
            WHEN op = 'I' THEN 'SELECT * FROM editing_session.' || q.item || ' AS s '
            WHEN op = 'U' THEN concat(
                ' SET (', c.cols, ') = (',
                's.', replace(c.cols, ', ', ', s.'),
                ') FROM editing_session.',  q.item, ' AS s '
            )
            WHEN op = 'D' THEN ''
        END, ' ',
        CASE
            WHEN op = 'I' THEN ' WHERE s.id = ' || id::text
            WHEN op = 'U' THEN ' WHERE s.id = t.id AND t.id = ' || id::text
            WHEN op = 'D' THEN ' WHERE t.id = ' || id::text
        END,
        CASE
            WHEN op = 'I' THEN ' ON CONFLICT DO NOTHING '
            ELSE ''
        END,
        ';'
    ) AS sql
    FROM queries AS q, item_cols AS c
    WHERE q.item = c.item
    ;

END;
$$;


-- FUNCTION create_queries_from_editing_session(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.create_queries_from_editing_session(_editing_session_id integer) IS 'Build SQL queries to run from the editing_sessions.logged_ids column content for the given editing session';


-- editing_survey()
CREATE FUNCTION road_graph.editing_survey() RETURNS trigger
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
    -- Since UPDATEs can occur on newly created object in the editing_session schema
    -- we must check if a corresponding object exists in the road_graph schema
    -- before checking if they are equal
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'edges' THEN
        -- We need to defined the search_path to avoid using syntax
        -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
        -- when creating an editing_session_xxx schema
        SET search_path TO road_graph, public;

        -- Get original edge
        SELECT INTO original_edge_object
            *
        FROM edges
        WHERE id = NEW.id
        ;
        IF
            original_edge_object IS NOT NULL
            AND md5(to_json(NEW.*)::text) = md5(to_json(original_edge_object.*)::text)
        THEN
            -- Reset the search path
            RESET search_path;
            RETURN NEW;
        END IF;
        -- Reset the search path
        RESET search_path;
    END IF;

    -- 1/ Check if geometry is inside the editing session geometry
    -- Get editing session geometry
    IF to_jsonb(NEW) ? 'geom' THEN
        SELECT INTO editing_session_geom
            geom
        FROM editing_session.editing_sessions
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

    -- 2/ Log editing inside the editing_session table*
    -- edited object id
    object_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.id
        ELSE NEW.id
    END
    ;

    -- Update log in the editing session
    -- We need to defined the search_path to avoid using syntax
    -- such as road_graph.xxxxx as it can be replaced by QGIS plugin
    -- when creating an editing_session_xxx schema
    SET search_path TO road_graph, public;
    SELECT INTO new_logged_ids
        logged_ids
    FROM editing_sessions
    WHERE unique_code = (
        SELECT unique_code
        FROM editing_session.editing_sessions
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

    UPDATE editing_sessions
    SET
        -- Add the id of the object
        logged_ids = new_logged_ids,
        "status" = 'edited'
    WHERE unique_code = (
        SELECT unique_code
        FROM editing_session.editing_sessions
        LIMIT 1
    )
    ;
    -- Reset the search path
    RESET search_path;

    -- Return object
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;

END;
$$;


-- FUNCTION editing_survey()
COMMENT ON FUNCTION road_graph.editing_survey() IS 'Logs the modifications done inside the editing session schema tables.
It also check that the edited geometries are inside the editing session polygon.';


-- get_current_setting(text, text, text)
CREATE FUNCTION road_graph.get_current_setting(setting_name text, default_value text, value_type text DEFAULT 'text'::text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    setting_value text;
BEGIN
    -- Get current setting value, if not set return default value
    setting_value = coalesce(current_setting(setting_name, true), default_value);
    -- Try to cast the setting value to the expected type, if it fails return the default value
    BEGIN
        IF value_type = 'integer' THEN
            RETURN setting_value::integer;
        ELSIF value_type = 'boolean' THEN
            RETURN  setting_value::boolean;
        ELSIF value_type = 'real' THEN
            RETURN  setting_value::real;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        IF value_type = 'integer' THEN
            RETURN default_value::integer;
        ELSIF value_type = 'boolean' THEN
            RETURN  default_value::boolean;
        ELSIF value_type = 'real' THEN
            RETURN  default_value::real;
        END IF;
    END;

    RETURN setting_value;
END;
$$;


-- FUNCTION get_current_setting(setting_name text, default_value text, value_type text)
COMMENT ON FUNCTION road_graph.get_current_setting(setting_name text, default_value text, value_type text) IS 'Get a PostgreSQL current setting, with a default value if the setting is not set or is invalid.
The function is used to avoid repeating the coalesce(current_setting(...))::TYPE, 0) = 1
and to have a single point of maintenance for getting settings.';


-- get_downstream_multilinestring_from_reference(text, integer, real, real, text)
CREATE FUNCTION road_graph.get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    marker record;
    closest_edge record;
    merged_downstream_edges geometry(MULTILINESTRING, 2154);
    downstream_road geometry(MULTILINESTRING, 2154);
    raise_notice text;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Get marker
    -- We must control the marker abscissa
    -- and compare it with the desired abscissa
    -- to be able to manage the virtual markers
    SELECT INTO marker
        m.id, m.abscissa, m.geom
    FROM
        road_graph.markers AS m
    WHERE True
    -- same road
    AND m.road_code = _road_code
    -- marker code
    AND m.code = _marker_code
    -- marker abscissa must be below given abscissa
    -- so that it is before the given reference
    AND m.abscissa <= _abscissa
    -- get the marker with bigger abscissa
    -- to keep the closest marker before the given reference
    ORDER BY m.abscissa DESC
    LIMIT 1
    ;

    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Marker found with code % for road % = id %', _marker_code, _road_code, marker.id;
    END IF;

    IF marker IS NULL THEN
        IF raise_notice = 'yes' THEN
            RAISE NOTICE 'Marker cannot be found with code % for road %', _marker_code, _road_code;
        END IF;
        RETURN NULL;
    END IF;

    -- find closest edge from marker and keep only the downstream part
    -- from the projected point
    SELECT INTO closest_edge
        e.*,
        e.geom <-> marker.geom AS distance,
        ST_LineSubstring(
            e.geom,
            ST_LineLocatePoint(e.geom, marker.geom),
            1
        )::geometry(LINESTRING, 2154) AS sub_geom
    FROM
        road_graph.edges AS e
    WHERE True
    -- same road
    AND e.road_code = _road_code
    ORDER BY distance
    LIMIT 1
    ;
    IF closest_edge IS NULL THEN
        IF raise_notice = 'yes' THEN
            RAISE NOTICE 'Closest edge from given marker % cannot be found for road %', _marker_code, _road_code;
        END IF;
        RETURN NULL;
    END IF;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Closest edge found: id = %', closest_edge.id;
    END IF;

    -- Merge all linestrings for the given road
    -- after the closest edge
    -- ordered by edge order (we use the next_edge_id to calculate the edges order)
    WITH ordered_ids AS (
        SELECT *
        FROM road_graph.get_ordered_edges(_road_code, closest_edge.id, 'downstream')
    )
    SELECT INTO merged_downstream_edges
        -- ids
        -- array_agg(e.id ORDER BY o.edge_order) AS ids,
        -- geometries of downstream edges
        ST_Collect(e.geom ORDER BY o.edge_order) AS geom
    FROM
        road_graph.edges AS e
    INNER JOIN ordered_ids AS o ON e.id = o.id
    WHERE True
    -- same road code
    AND e.road_code = _road_code
    -- not the closest edge
    AND e.id != closest_edge.id
    -- downstream compared to closest edge
    AND e.start_marker >= closest_edge.end_marker
    AND e.start_cumulative >= closest_edge.end_cumulative
    GROUP BY e.road_code
    ;

    -- Build the linestring
    IF merged_downstream_edges IS NULL THEN
        -- There is no edges after the closest edge
        -- We just need to use the splitted closest edge geometry
        -- RAISE NOTICE 'Downstream edges cannot be found for given marker %', _marker_code;
        downstream_road = ST_Multi(closest_edge.sub_geom);
    ELSE
        -- There are some edges after the closest edge
        -- We must merge the splitted closest edge and the downstream edges
        SELECT INTO downstream_road
            --ST_LineMerge(
            -- ST_LineMerge cannot be used as it creates a valid multilinestring
            -- but will not preserve order of the parts.
            -- So the final geometry could be reversed when adding measure in the next step
            -- even with the new "directed" parameter which concerns segments and not parts
            ST_CollectionExtract(
                -- even if ST_Collect collects 2 multiilnestrings
                -- we must extract only 2 to be able to get a multilinestring as result
                ST_Collect(geom ORDER BY merge_order),
                --extract lines
                2
            )
            AS geom
        FROM
        (
            SELECT
                1::int AS merge_order,
                -- We need to transform sub geom into multilinestring
                -- to have only multilinestrings in the collected result
                ST_Multi(closest_edge.sub_geom) AS geom
            UNION ALL
            SELECT
                2::int AS merge_order,
                merged_downstream_edges AS geom
        ) AS foo
        ;
    END IF;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Merge downstream edges  %', ST_AsText(downstream_road);
        RAISE NOTICE 'downstream_road length = %, _abscissa = %', ST_Length(downstream_road), _abscissa;
    END IF;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'marker_code', _marker_code,
        'abscissa', _abscissa,
        'offset', _offset,
        'side', _side,
        'closest_marker_abscissa', marker.abscissa,
        'downstream_road', downstream_road
    );

END;
$$;


-- FUNCTION get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_downstream_multilinestring_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the MULTILINESTRING downstream road from given references to the road end.';


-- get_edge_references(integer, boolean)
CREATE FUNCTION road_graph.get_edge_references(_edge_id integer, _use_cache boolean DEFAULT false) RETURNS json
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    start_references jsonb;
    end_references jsonb;
    -- timing variables
   _timing1  timestamptz;
   _start_ts timestamptz;
   _end_ts   timestamptz;
   _overhead numeric;     -- in ms
BEGIN
    -- Notice used for big imports
    -- RAISE NOTICE 'get_edge_references - %', _edge_id;

    -- timing
    _timing1  := clock_timestamp();
    _start_ts := clock_timestamp();
    _end_ts   := clock_timestamp();
    -- take minimum duration as conservative estimate
    _overhead := 1000 * extract(epoch FROM LEAST(_start_ts - _timing1, _end_ts   - _start_ts));
    _start_ts := clock_timestamp();

    -- Get edge
    SELECT INTO edge
        e.*, r.road_type
    FROM road_graph.edges AS e
    JOIN road_graph.roads AS r
        ON e.road_code = r.road_code
    WHERE e.id = _edge_id
    ;
    IF edge IS NULL THEN
        RETURN NULL::json;
    END IF;

   -- RAISE NOTICE '% = %' , 'get edge', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Check road code
    IF edge.road_code IS NULL THEN
        RETURN NULL::json;
    END IF;

    -- start point
    SELECT INTO start_references
        road_graph.get_reference_from_point(
            ST_StartPoint(edge.geom),
            edge.road_code,
            _use_cache
        ) AS ref
    ;
   -- RAISE NOTICE '% = %' , 'get start point', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- end point
    -- we do not use ST_EndPoint for roundabout because this corresponds
    -- also to the ST_STartPoint
    IF edge.road_type = 'roundabout' THEN
        -- Use ST_LineInterpolatePoint(a_linestring, a_fraction);
        SELECT INTO end_references
            road_graph.get_reference_from_point(
                ST_LineInterpolatePoint(edge.geom, 0.99999),
                edge.road_code,
                _use_cache
            ) AS ref
        ;
    ELSE
        -- Use St_EndPoint(geom)
        SELECT INTO end_references
            road_graph.get_reference_from_point(
                ST_EndPoint(edge.geom),
                edge.road_code,
                _use_cache
            ) AS ref
        ;
    END IF;
   -- RAISE NOTICE '% = %' , 'get end references', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Return calculated data
    RETURN json_build_object(
        'start_marker', (start_references->>'marker_code')::text::integer,
        'start_abscissa', (start_references->>'abscissa')::text::real,
        'start_cumulative', (start_references->>'cumulative')::text::real,
        'end_marker', (end_references->>'marker_code')::text::integer,
        'end_abscissa', (end_references->>'abscissa')::text::real,
        'end_cumulative', (end_references->>'cumulative')::text::real
    )
    ;

END;
$$;


-- FUNCTION get_edge_references(_edge_id integer, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_edge_references(_edge_id integer, _use_cache boolean) IS 'Return the references of the given edge start and end point.
Could be used to UPDATE the edges of a road.
By default does not use road object cache store in a temporary table.
This behaviour can be overriden by setting _use_cache TO True;
';


-- get_ordered_edges(text, integer, text)
CREATE FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer DEFAULT '-1'::integer, _direction text DEFAULT 'downstream'::text) RETURNS TABLE(id integer, edge_order integer, road_code text, geom geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    initial_edge record;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');
    -- Get initial edge. If not given, get the first road edge
    IF _initial_id = -1 THEN
        IF _direction = 'downstream' THEN
            SELECT into initial_edge
                e.id, e.start_node, e.end_node
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code 
            AND previous_edge_id IS NULL 
            ORDER BY start_cumulative, e.id 
            LIMIT 1
            ;
            _initial_id = initial_edge.id;
        ELSE
            SELECT INTO initial_edge
                e.id, e.start_node, e.end_node
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code 
            AND next_edge_id IS NULL 
            ORDER BY start_cumulative DESC, e.id DESC
            LIMIT 1
            ;
            _initial_id = initial_edge.id;
        END IF;
        IF _initial_id IS NULL THEN
            RAISE EXCEPTION 'No edge found for the road %s initial edge', 
                _road_code
            ;
        END IF;
    ELSE
        SELECT INTO initial_edge
            e.id, e.start_node, e.end_node
        FROM road_graph.edges AS e
        WHERE e.id = _initial_id
        ;
    END IF;
    IF raise_notice in ('info', 'debug') THEN
        RAISE NOTICE 'get_ordered_edges - _initial_id = %',
            _initial_id
        ;
    END IF;
    
    IF _direction = 'downstream' THEN
        -- Get downstream edges
        RETURN QUERY
        WITH RECURSIVE ordered_edges
        AS(
            -- anchor member
            SELECT 
                e.id, next_edge_id, 
                -- add id to the list of processed ids
                ARRAY[e.id] AS ids,
                1 AS edge_order, 
                e.start_node, e.end_node,
                e.geom --, 0.0::numeric AS distance
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code AND e.id = _initial_id
            UNION ALL
            -- recursive term
            SELECT 
                e.id, e.next_edge_id, 
                -- add id to the list of processed ids
                array_append(o.ids, e.id) AS ids,
                o.edge_order + 1 AS edge_order, 
                e.start_node, e.end_node,
                e.geom --, ST_Distance(ST_StartPoint(e.geom), ST_EndPoint(o.geom))::numeric AS distance
            FROM road_graph.edges AS e
            JOIN ordered_edges AS o
                ON e.id = o.next_edge_id 
                -- We try to get the next edge which can be far
                -- limit search radius to 200m
                -- NOT EFFICIENT FOR BIG ROADS
                -- OR ST_DWithin(ST_StartPoint(e.geom), ST_EndPoint(o.geom), 200)
                -- for roundabout, when previous and next edge ids are not set
                OR ST_DWithin(ST_StartPoint(e.geom), ST_EndPoint(o.geom), 0.20)
            WHERE e.road_code = _road_code
        	-- avoir infinite loop
            AND NOT (ARRAY[e.id] && o.ids)
        )
        SELECT --DISTINCT ON (o.edge_order)
            o.id, o.edge_order, _road_code AS road_code, o.geom
        FROM ordered_edges AS o
        ORDER BY o.edge_order --, distance
        ;
    ELSE
        RETURN QUERY
        WITH RECURSIVE ordered_edges
        AS(
            -- anchor member
            SELECT 
                e.id, previous_edge_id, 
                -- add id to the list of processed ids
                ARRAY[e.id] AS ids,
                1 AS edge_order, 
                e.start_node, e.end_node,
                e.geom --, 0.0::numeric AS distance
            FROM road_graph.edges AS e
            WHERE e.road_code = _road_code AND e.id = _initial_id
            UNION ALL
            -- recursive term
            SELECT
                e.id, e.previous_edge_id, 
                -- add id to the list of processed ids
                array_append(o.ids, e.id) AS ids,
                o.edge_order + 1 AS edge_order,
                e.start_node, e.end_node,
                e.geom --, ST_Distance(ST_EndPoint(e.geom), ST_StartPoint(o.geom))::numeric AS distance
            FROM road_graph.edges AS e
            JOIN ordered_edges AS o
                ON e.id = o.previous_edge_id 
                -- We try to get the next edge which can be far
                -- limit search radius to 200m
                -- OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(o.geom), 200)
                -- for roundabout, when previous and next edge ids are not set
                OR ST_DWithin(ST_EndPoint(e.geom), ST_StartPoint(o.geom), 0.20)
            WHERE e.road_code = _road_code
        	-- avoir infinite loop
            AND NOT (ARRAY[e.id] && o.ids)
        )
        SELECT --DISTINCT ON (o.edge_order)
            o.id, o.edge_order, _road_code AS road_code, o.geom
        FROM ordered_edges AS o
        ORDER BY o.edge_order--, distance
        ;
    END IF;
END;
$$;


-- FUNCTION get_ordered_edges(_road_code text, _initial_id integer, _direction text)
COMMENT ON FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer, _direction text) IS 'Get the list of edge id with order for a given road, edge and the direction (downtream or upstream). It always includes the given edge';


-- get_reference_from_point(geometry, text, boolean)
CREATE FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text DEFAULT NULL::text, _use_cache boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    closest_edge record;
    closest_edge_marker record;
    previous_marker record;
    merged_upstream_edges record;
    upstream_road_from_marker record;
    upstream_road_from_start record;
    found_road_code text;
    found_marker_code integer;
    found_abscissa real;
    found_offset real;
    found_side text;
    found_cumulative real;
    raise_notice text;
    -- timing variables
   _timing1  timestamptz;
   _start_ts timestamptz;
   _end_ts   timestamptz;
   _overhead numeric;     -- in ms
BEGIN
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no', 'text');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% get_reference_from_point - _point = % & _road_code = %',
            REPEAT('    ', pg_trigger_depth()::INTEGER),
            ST_AsText(_point),
            _road_code
        ;
    END IF;
    -- timing
    _timing1  := clock_timestamp();
    _start_ts := clock_timestamp();
    _end_ts   := clock_timestamp();
    -- take minimum duration as conservative estimate
    _overhead := 1000 * extract(epoch FROM LEAST(_start_ts - _timing1, _end_ts   - _start_ts));
    _start_ts := clock_timestamp();
    RAISE NOTICE 'START get_reference_from_point';

    -- Get the splitted closest road depending on given road_code
    -- we keep only the edge part between start point and given _point
    IF _road_code IS NOT NULL THEN
        WITH ordered_ids AS (
            SELECT *
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream')
        )
        SELECT INTO closest_edge
            e.*,
            e.geom <-> _point AS distance,
            -- create the line portion from edge start point to the given point
            -- BEWARE: it can be a point e.g if the _point is edge start point
            -- it why we do not cast with ::geometry(LINESTRING, 2154)
            ST_LineSubstring(
                e.geom,
                0,
                ST_LineLocatePoint(e.geom, _point)
            ) AS sub_geom,
            -- calculate the measure for the point on this edge
            ST_LineLocatePoint(e.geom, _point) AS point_measure
        FROM
            road_graph.edges AS e
        INNER JOIN ordered_ids AS o ON e.id = o.id
        -- Limit search for the given road code
        WHERE e.road_code = _road_code
        -- Limit search to 50m
        AND ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance, o.edge_order, e.start_cumulative --, ST_X(ST_Centroid(e.geom)), ST_Y(ST_Centroid(e.geom))
        -- Get only the closest edge, with the least edge_order -- start cumulative
        LIMIT 1
        ;
    ELSE
        SELECT INTO closest_edge
            e.*,
            e.geom <-> _point AS distance,
            -- create the line portion from edge start point to the given point
            -- BEWARE: it can be a point e.g if the _point is edge start point
            -- it why we do not cast with ::geometry(LINESTRING, 2154)
            ST_LineSubstring(
                e.geom,
                0,
                ST_LineLocatePoint(e.geom, _point)
            ) AS sub_geom,
            -- calculate the measure for the point on this edge
            ST_LineLocatePoint(e.geom, _point) AS point_measure
        FROM
            road_graph.edges AS e
            -- LIMIT search without road_code TO 50m
        WHERE ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance --, ST_X(ST_Centroid(e.geom)), ST_Y(ST_Centroid(e.geom))
        -- Get only the closest edge, with the least edge_order, start cumulative
        LIMIT 1
        ;
    END IF;
    IF closest_edge IS NULL THEN
        IF raise_notice = 'debug' THEN
            RAISE NOTICE 'CLOSEST_EDGE is NULL';
        END IF;
        RETURN NULL;
    END IF;
    IF raise_notice = 'debug' THEN
        RAISE NOTICE 'CLOSEST_EDGE %', to_json(closest_edge);
        RAISE NOTICE 'CLOSEST_EDGE subgeom %', ST_AsText(closest_edge.sub_geom);
    END IF;

    RAISE NOTICE '% = %' , 'CLOSEST_EDGE', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Get road_code
    found_road_code = closest_edge.road_code;

    WITH
    get_previous_marker AS (
        SELECT *
        FROM road_graph.get_road_previous_marker_from_point(
            found_road_code,
            _point,
            _use_cache
        )
    ),
    marker_data AS (
        SELECT
            -- marker data
            m.*,
            -- Multilinestring from the marker to the point
            ST_Difference(
                -- linestring between the marker and _point
                m.road_linestring_from_marker_to_point,
                -- multilinestring made with connectors between edges end points and start points
                m.closing_multilinestring,
                -- grid size to avoid rounding issues
                0.01
            ) AS upstream_road_from_marker,
            -- Multilinestring from the road start to the point
            ST_Difference(
                -- linestring between the road start and the point
                m.road_linestring_from_start_to_point,
                -- multilinestring made with connectors between edges end points and start points
                m.closing_multilinestring,
                -- grid size to avoid rounding issues
                0.01
            ) AS upstream_road_from_start
        FROM
            get_previous_marker AS m
    )
    SELECT INTO closest_edge_marker
        m.*
    FROM marker_data AS m
    ;

    IF raise_notice = 'debug' THEN
        RAISE NOTICE 'CLOSEST_EDGE_MARKER %', to_json(closest_edge_marker);
        RAISE NOTICE '-';
    END IF;
    RAISE NOTICE '% = %' , 'CLOSEST_EDGE_MARKER', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;

    -- Calculate values to return based on the generated geometries
    found_marker_code = closest_edge_marker.code;
    found_abscissa = ST_Length(closest_edge_marker.upstream_road_from_marker) + closest_edge_marker.abscissa;
    found_cumulative = Coalesce(ST_Length(closest_edge_marker.upstream_road_from_start), 0);
    found_offset = closest_edge.distance;
    found_side = (
        CASE
            WHEN ST_Contains(
                ST_Buffer(
                    closest_edge.geom,
                    closest_edge.distance +1,
                    'side=left'
                ), _point
            ) THEN 'left' ELSE 'right'
        END
    );
    RAISE NOTICE '% = %' , 'VALUES', 1000 * (extract(epoch FROM clock_timestamp() - _start_ts)) - _overhead;
    RAISE NOTICE 'END';

    -- Build the JSON to return
    RETURN json_build_object(
        'road_code', found_road_code,
        'marker_code', found_marker_code,
        'abscissa', round(found_abscissa::numeric, 2),
        'cumulative', round(found_cumulative::numeric, 2),
        'offset', round(found_offset::numeric, 2),
        'side', found_side
    );

END;
$$;


-- get_road_point_from_reference(text, integer, real, real, text)
CREATE FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    get_downstream_multilinestring_from_reference record;
    _closest_marker_abscissa real;
    _downstream_road geometry(MULTILINESTRING, 2154);
    result_point geometry(POINT, 2154);
    raise_notice text;
BEGIN

    -- Notice
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Get downstream MULTILINESTRING
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code, _marker_code, _abscissa, _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO get_downstream_multilinestring_from_reference
    ;

    _closest_marker_abscissa = get_downstream_multilinestring_from_reference.closest_marker_abscissa;
    _downstream_road = get_downstream_multilinestring_from_reference.downstream_road;

    -- Return point
    SELECT INTO result_point
        -- extract first point of resulting multipoint from ST_LocateAlong
        ST_GeometryN(
            -- Remove M measure from the result multipoint
            ST_Force2D(
                -- Locate the point on the measured downstream road part
                ST_LocateAlong(
                    -- Add measure to the downstream road part from 0 to length
                    ST_AddMeasure(_downstream_road, 0, ST_Length(_downstream_road)),
                    -- Abscissa cannot be above length
                    -- We must also add the marker abscissa
                    CASE
                        WHEN "_abscissa" - _closest_marker_abscissa >= ST_Length(_downstream_road)
                            THEN ST_Length(_downstream_road)
                        ELSE "_abscissa" - _closest_marker_abscissa
                    END,
                    -- JSON helper to choose side and offset
                    ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
                )
            )
            , 1
        )::geometry(POINT, 2154)
    ;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'marker_code', _marker_code,
        'abscissa', _abscissa,
        'offset', _offset,
        'side', _side,
        'closest_marker_abscissa', _closest_marker_abscissa,
        'geom', result_point
    );

END;
$$;


-- FUNCTION get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_road_point_from_reference(_road_code text, _marker_code integer, _abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the geometry of the corresponding point';


-- get_road_previous_marker_from_point(text, geometry, boolean)
CREATE FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean DEFAULT false) RETURNS TABLE(id integer, road_code text, code integer, abscissa real, geom geometry, road_linestring_from_marker_to_point geometry, road_linestring_from_start_to_point geometry, closing_multilinestring geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    _road_info record;
    _run_build_cache boolean;
BEGIN
    -- Retrieve cached objects such as road simple linestring and closing multilinestring
    -- and also the road markers locations agains the simple road linestring
    -- from the temporary table generated beforehand (see update_edge_references)
    IF _use_cache THEN

        -- Get given _point location against the road simple linestring
        -- And retrieve data from the temporary table road_cache
        SELECT INTO _road_info
            r.simple_linestring,
            r.closing_multilinestring,
            r.marker_locations,
            ST_LineLocatePoint(r.simple_linestring, _point) AS point_location
        FROM road_cache AS r
        WHERE r.road_code = _road_code
        ;

        -- Get the previous marker
        -- which is the one with the location just before the _point location
        -- We also compute the linestring from the marker to the end of the road
        -- since we can use it to simplify the calcution of the reference from the point
        RETURN QUERY
        SELECT
            m.id, m.road_code, m.code, m.abscissa, m.geom,
            -- Linestring (with no gaps) between the marker and the point
            ST_LineSubstring(
                _road_info.simple_linestring,
                (_road_info.marker_locations->>(m.id))::float8,
                _road_info.point_location
            ) AS road_linestring_from_marker_to_point,
            -- Linestring (with no gaps) between the start of the road and the point
            ST_LineSubstring(
                _road_info.simple_linestring,
                0,
                _road_info.point_location
            ) AS road_linestring_from_start_to_point,
            -- MultiLinestring of the linestrings generated to close the gaps between edges
            -- used in other functions to remove them from the road linestring parts
            _road_info.closing_multilinestring
            --,
            -- road simple linestring
            --_road_info.simple_linestring AS road_simple_linestring
        FROM
            road_graph.markers AS m
        WHERE True
        AND m.marker_location <= _road_info.point_location
        ORDER BY m.marker_location DESC
        LIMIT 1
        ;
    ELSE
        -- Use plain query to avoid creating temporary tables
        RETURN QUERY
        WITH
        -- get road ordered edges (use previous and next edge ids)
        ordered_edges AS (
            SELECT o.id, o.road_code, o.edge_order, o.geom
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
        ),
        touching_edges AS (
            -- For each edge, add a line at the end of it
            -- from the previous edge (lag) end point
            -- to the edge start_point
            SELECT
                o.id, o.road_code, o.edge_order,
                -- closing lines between last end point and edge start point
                ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                ) AS closing_line,
                -- Merge closing lines and edge geometry
                ST_LineMerge(ST_Union(
                    ST_MakeLine(
                        Coalesce(
                            ST_EndPoint(LAG(o.geom) OVER(ORDER BY edge_order)),
                            ST_StartPoint(o.geom)
                        ),
                        ST_StartPoint(o.geom)
                    ),
                    o.geom
                )) AS geom
            FROM ordered_edges AS o
            ORDER BY o.edge_order
        ),
        road_line AS (
            SELECT
                a.*,
                -- Calculate the location of the given point against this generated linestring
                ST_LineLocatePoint(a.geom, _point) AS point_location
            FROM (
                -- Create a single linestring by merging all edge augmented linestrings
                SELECT
                    max(t.road_code) AS road_code,
                    ST_MakeLine(t.geom ORDER BY t.edge_order) AS geom,
                    ST_CollectionExtract(
                        ST_MakeValid(
                            ST_Multi(ST_Collect(t.closing_line ORDER BY t.edge_order))
                        ),
                        2
                    ) AS closing_multilinestring
                FROM touching_edges AS t
            ) AS a
        ),
        marker_position AS (
            -- For each marker of the road, get its fractionnal location
            -- against the single merged linestring
            -- and all other needed columns values
            SELECT
                m.id, m.road_code, m.code, m.abscissa, m.geom,
                ST_LineLocatePoint(l.geom, m.geom) AS marker_location,
                l.geom AS road_simple_linestring
            FROM
                road_line as l,
                road_graph.markers AS m
            WHERE True
            AND m.road_code = _road_code
            -- No need to add filter 'AND m.road_code = l.road_code' since there is only one linestring
            -- No need to order data either
            -- ORDER BY m.code, m.abscissa
        )
        -- Get the previous marker
        -- which is the one with the location just before the _point location
        -- We also compute the linestring from the marker to the end of the road
        -- since we can use it to simplify the calcution of the reference from the point
        SELECT
            m.id, m.road_code, m.code, m.abscissa, m.geom,
            -- Linestring (with no gaps) between the marker and the point
            ST_LineSubstring(
                l.geom,
                m.marker_location,
                point_location
            ) AS road_linestring_from_marker_to_point,
            -- Linestring (with no gaps) between the start of the road and the point
            ST_LineSubstring(
                l.geom,
                0,
                point_location
            ) AS road_linestring_from_start_to_point,
            -- MultiLinestring of the linestrings generated to close the gaps between edges
            -- used in other functions to remove them from the road linestring parts
            l.closing_multilinestring
            --,
            -- road simple linestring
            --l.geom AS road_simple_linestring
        FROM
            marker_position AS m,
            road_line AS l
        WHERE True
        AND m.marker_location <= l.point_location
        ORDER BY m.marker_location DESC
        LIMIT 1
        ;

    END IF;
END;
$$;


-- FUNCTION get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_road_previous_marker_from_point(_road_code text, _point geometry, _use_cache boolean) IS 'Get the closest upstream marker for the given road from a given point.
This function can use roads cached objects generated beforehand via function build_road_cached_objects.
Or use a full SQL query with no use of temporary tables, depending of the paramter _use_cache

Illustration
|m0----m1----|  |-m2-m2b----m3----|   |--------m4---|
                 p0    p1               p2
p0 -> marker is m1
p1 -> marker is m2b (virtual marker with a non-null abscissa)
p2 -> marker is m3
The function also returns
* the simple linestring (no gaps) made by merging all edges linestrings from the marker to the point
* the simple linestring (no gaps) made by merging all edges linestrings from the start to the point
* the multilinestring made by collecting all connectors between end and start points, which will help to remove them from linestrings to create the definitive geometry (with gaps)
';


-- get_road_substring_from_references(text, integer, real, integer, real, real, text)
CREATE FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
DECLARE
    _start_multilinestring record;
    _end_multilinestring record;
    _start_closest_marker_abscissa real;
    _end_closest_marker_abscissa real;
    _start_downstream_road geometry(MULTILINESTRING, 2154);
    _start_downstream_road_m geometry(MULTILINESTRINGM, 2154);
    _end_downstream_road geometry(MULTILINESTRING, 2154);
    _end_downstream_road_m geometry(MULTILINESTRINGM, 2154);
    _start_substring geometry(MULTILINESTRING, 2154);
    _end_substring geometry(MULTILINESTRING, 2154);
    result_multilinestring geometry(MULTILINESTRING, 2154);
    raise_notice text;
BEGIN

    -- Notice
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- Tests
    IF _start_marker_code > _end_marker_code THEN
        RAISE EXCEPTION 'The start marker code cannot be greater than the end marker code';
    END IF;
    IF _start_marker_code = _end_marker_code
        AND _start_marker_abscissa >= _end_marker_abscissa
    THEN
        RAISE EXCEPTION 'The start abscissa cannot be equal or greater than the end abscissa when the start and end marker have the same code';
    END IF;

    -- Get downstream start MULTILINESTRING from start marker to end of the road
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code,
                _start_marker_code, _start_marker_abscissa,
                _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO _start_multilinestring
    ;
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_start_multilinestring  closest_marker_abscissa %', _start_multilinestring.closest_marker_abscissa;
        RAISE NOTICE '_start_multilinestring  downstream_road %', ST_AsText(_start_multilinestring.downstream_road);
    END IF;

    _start_closest_marker_abscissa = _start_multilinestring.closest_marker_abscissa;
    _start_downstream_road = _start_multilinestring.downstream_road;
    -- Add measure to the downstream road part from 0 to length
    _start_downstream_road_m = ST_AddMeasure(_start_downstream_road, 0, ST_Length(_start_downstream_road));

    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_start_downstream_road_m %', ST_AsText(_start_downstream_road_m);
    END IF;
    -- Get downstream end MULTILINESTRING from end marker to end of the road
    SELECT
        closest_marker_abscissa,
        downstream_road
    FROM
        jsonb_to_record(
            road_graph.get_downstream_multilinestring_from_reference(
                _road_code,
                _end_marker_code, _end_marker_abscissa,
                _offset, _side
            )
        ) AS (
            road_code text, marker_code integer, abscissa real,
            "offset" real, side text,
            closest_marker_abscissa real,
            downstream_road geometry(MULTILINESTRING, 2154)
        )
    INTO _end_multilinestring
    ;
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_end_multilinestring closest_marker_abscissa %', _end_multilinestring.closest_marker_abscissa;
        RAISE NOTICE '_end_multilinestring downstream_road %', ST_AsText(_end_multilinestring.downstream_road);
    END IF;

    _end_closest_marker_abscissa = _end_multilinestring.closest_marker_abscissa;
    _end_downstream_road = _end_multilinestring.downstream_road;
    -- Add measure to the downstream road part from 0 to length
    _end_downstream_road_m = ST_AddMeasure(_end_downstream_road, 0, ST_Length(_end_downstream_road));

    IF raise_notice = 'yes'  THEN
        RAISE NOTICE '_end_downstream_road_m %', ST_AsText(_end_downstream_road_m);
    END IF;
    -- Create substring lines
    -- start
    _start_substring = ST_ReducePrecision(
    -- NB : In some unpredictable cases, the use of offset gives error like
    -- ERREUR:  MultiLineString cannot contain MultiLineString element
    -- We can use ST_OffsetCurve instead
        ST_OffsetCurve(
            ST_LocateBetween(
                _start_downstream_road_m,
                CASE
                    WHEN _start_marker_abscissa - _start_closest_marker_abscissa >= ST_Length(_start_downstream_road)
                        THEN ST_Length(_start_downstream_road)
                    ELSE _start_marker_abscissa - _start_closest_marker_abscissa
                END,
                ST_Length(_start_downstream_road)
                --,
                -- JSON helper to choose side and offset
                -- ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
            ),
            ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
        ),
        0.10
    );
    IF raise_notice = 'yes' THEN
        RAISE NOTICE '_start_substring  %', ST_AsText(_start_substring);
    END IF;
    -- end
    -- NB : In some unpredictable cases, the use of offset gives error like
    -- ERREUR:  MultiLineString cannot contain MultiLineString element
    -- We can use ST_OffsetCurve instead
    _end_substring = ST_ReducePrecision(
        ST_OffsetCurve(
            ST_LocateBetween(
                _end_downstream_road_m,
                CASE
                    WHEN _end_marker_abscissa - _end_closest_marker_abscissa >= ST_Length(_end_downstream_road)
                        THEN ST_Length(_end_downstream_road)
                    ELSE _end_marker_abscissa - _end_closest_marker_abscissa
                END,
                ST_Length(_end_downstream_road)
                --,
                -- JSON helper to choose side and offset
                -- ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
            ),
            ((json_build_object('left', +1, 'right', -1))->>("_side"))::integer * "_offset"
        ),
        0.10
    );
    IF raise_notice = 'yes' THEN
        RAISE NOTICE '_end_substring  %', ST_AsText(_end_substring);
    END IF;

    -- Return multilinestring between given references
    SELECT INTO result_multilinestring
        -- MultiLinestring
        -- ST_Multi
        ST_Multi(
            ST_Difference(
                -- start
                _start_substring,
                --end
                _end_substring,
                -- We MUST use a grid size, else we have wrong results
                0.10
            )
        )::geometry(MULTILINESTRING, 2154)
    ;
    IF raise_notice = 'yes'  THEN
        RAISE NOTICE 'result_multilinestring  %', ST_AsText(result_multilinestring);
    END IF;

    -- Return full JSONb with parameters and computed geometry
    RETURN jsonb_build_object(
        'road_code', _road_code,
        'start_marker_code', _start_marker_code,
        'start_marker_abscissa', _start_marker_abscissa,
        'end_marker_code', _end_marker_code,
        'end_marker_abscissa', _end_marker_abscissa,
        'offset', _offset,
        'side', _side,
        'start_geom', _start_substring,
        'end_geom', _end_substring,
        'geom', result_multilinestring
    );

END;
$$;


-- FUNCTION get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_road_substring_from_references(_road_code text, _start_marker_code integer, _start_marker_abscissa real, _end_marker_code integer, _end_marker_abscissa real, _offset real, _side text) IS 'Returns a JSON object with the given references and the geometry of the built linestring';


-- get_spatial_road(text)
CREATE FUNCTION road_graph.get_spatial_road(_road_code text) RETURNS geometry
    LANGUAGE plpgsql
    AS $$
DECLARE
    road_geom geometry(multilinestringM, 2154);
BEGIN
    -- Get ordered edges for the road, then build the road geometry
    WITH ordered_ids AS (
        SELECT *
        FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') 
    ),
    build_geom AS (
        SELECT
            ST_Collect(e.geom ORDER BY o.edge_order, e.start_cumulative) AS geom
        FROM road_graph.edges AS e
        INNER JOIN ordered_ids AS o 
            ON e.id = o.id
        WHERE e.road_code = o.road_code
    )
    SELECT INTO road_geom
        ST_AddMeasure(ST_Multi(r.geom), 0, ST_Length(r.geom)) AS geom
    FROM build_geom AS r
    ;

    RETURN road_geom;

END;
$$;


-- FUNCTION get_spatial_road(_road_code text)
COMMENT ON FUNCTION road_graph.get_spatial_road(_road_code text) IS 'Build the road geometry for the given road id. It can then be used with ST_LocateAlong.';


-- get_upper_roundabout_node_to_delete(text)
CREATE FUNCTION road_graph.get_upper_roundabout_node_to_delete(_road_code text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    check_record record;
BEGIN
    WITH node AS (
        -- Get the node at the higher position in the roundabout
    	SELECT n.id, n.geom, e.id AS edge_id
    	FROM road_graph.nodes AS n 
    	JOIN road_graph.edges AS e 
    		ON e.road_code = _road_code
    		AND n.id IN (e.start_node, e.end_node) 
    	ORDER BY ST_Y(n.geom) DESC LIMIT 1
    ),
    check_node AS (
        -- Intersects a circle of radius 1m around the node
        -- with the related edges so that we can then compare 
        -- the generated points Y to the node Y
    	SELECT 
    		n.id, e.id AS edge_id,
    		n.geom,
    		ST_Intersection(ST_ExteriorRing(ST_Buffer(n.geom, 1)), e.geom) AS inter
    	FROM node AS n
    	JOIN road_graph.edges AS e
    		ON n.id IN (e.start_node, e.end_node)
    		AND e.road_code = _road_code
    )
    -- Get id
    SELECT INTO check_record
    	n.id, 
        bool_and(ST_Y(inter) <= ST_Y(n.geom)) AS ok
    FROM check_node AS n
    GROUP BY n.id
    ;
    
    IF check_record.ok IS TRUE
    THEN
        RETURN check_record.id;
    ELSE
        RETURN NULL::integer;
    END IF;

END;
$$;


-- FUNCTION get_upper_roundabout_node_to_delete(_road_code text)
COMMENT ON FUNCTION road_graph.get_upper_roundabout_node_to_delete(_road_code text) IS 'Get the roundabout node at the top of the circle. It only exists because QGIS creates a circle with a point at the top.
We need to delete this node and merge edges around it.
This function does nothing if no node is found at the exact top position of the circle.
';


-- merge_edges(integer, integer)
CREATE FUNCTION road_graph.merge_edges(id_edge_a integer, id_edge_b integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge_a record;
    edge_b record;
    edge_to_delete record;
    node_to_be_deleted integer;
    _set_config text;
BEGIN

    -- Get both edges
    -- edge A
    SELECT INTO edge_a
    *
    FROM road_graph.edges
    WHERE id = id_edge_a;
    IF edge_a.id IS NULL THEN
        RAISE EXCEPTION 'No edge found with id = % ', id_edge_a;
    END IF;

    -- edge B
    SELECT INTO edge_b
    *
    FROM road_graph.edges
    WHERE id = id_edge_b;
    IF edge_b.id IS NULL THEN
        RAISE EXCEPTION 'No edge found with id = % ', id_edge_b;
    END IF;

    -- edge A and B touches
    IF NOT ST_Touches(edge_a.geom, edge_b.geom) THEN
        RAISE NOTICE 'Edges does not touch';
        RETURN FALSE;
    END IF;

    -- RETURN FALSE;
    -- Merge edges: keep the oldest one,
    -- and add geometry from the other one
    edge_to_delete = edge_a;
    IF edge_a.id < edge_b.id OR edge_a.id < edge_b.id THEN
        edge_to_delete = edge_b;
    END IF;
    RAISE NOTICE 'merge_edges - % and %. Delete %', id_edge_a, id_edge_b, edge_to_delete.id
    ;

    -- Get node to be deleted
    node_to_be_deleted = -1;
    IF edge_a.start_node = edge_b.end_node THEN
        node_to_be_deleted = edge_a.start_node;
    END IF;
    IF edge_b.start_node = edge_a.end_node THEN
        node_to_be_deleted = edge_b.start_node;
    END IF;
    SELECT set_config('road.graph.merge.edges.useless.node', node_to_be_deleted::text, true)
    INTO _set_config
    ;
    RAISE NOTICE 'merge_edges -  % and %. node to be deleted = %', id_edge_a, id_edge_b, node_to_be_deleted
    ;

    -- Delete and UPDATE in the same request
    WITH del AS (
        DELETE FROM road_graph.edges
        WHERE id = edge_to_delete.id
        RETURNING *
    )
    UPDATE road_graph.edges
    SET geom = ST_LineMerge(ST_Union(geom, edge_to_delete.geom))
    WHERE True
    AND id IN (id_edge_a, id_edge_b)
    AND id != edge_to_delete.id
    ;
    SELECT set_config('road.graph.merge.edges.useless.node', '-1', true)
    INTO _set_config
    ;

    -- Return True
    RETURN TRUE;

END;
$$;


-- FUNCTION merge_edges(id_edge_a integer, id_edge_b integer)
COMMENT ON FUNCTION road_graph.merge_edges(id_edge_a integer, id_edge_b integer) IS 'Merge two edges by their geometry.
The merged edge is the one with the lowest id. The other edge is deleted.
The node between the two edges is not deleted but its id is stored
in the session variable ''road.graph.merge.edges.useless.node'' for further use if needed.
';


-- merge_editing_session_data(integer)
CREATE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    _set_config text;
    _toggle_triggers boolean;
BEGIN
    -- Get editing session record
    SET search_path TO road_graph, public;
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

    -- Create SQL to run for each edited data
    FOR sql_record IN
        SELECT
            sql_text
        FROM create_queries_from_editing_session(_editing_session_id)
    LOOP
       RAISE NOTICE 'SQL = %', sql_record.sql_text;
       EXECUTE sql_record.sql_text;
    END LOOP;

    -- Re-enable triggers
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
    INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    -- Truncate editing_session tables
    TRUNCATE editing_session.roads CASCADE;
    TRUNCATE editing_session.markers CASCADE;
    TRUNCATE editing_session.edges CASCADE;
    TRUNCATE editing_session.nodes CASCADE;
    TRUNCATE editing_session.editing_sessions CASCADE;

    -- Delete merged editing session
    DELETE FROM editing_sessions
    WHERE id = _editing_session_id
    ;

    -- Reset the search path
    RESET search_path;

    RETURN True;
END;
$$;


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';


-- split_edge_by_node(record, record, real, real)
CREATE FUNCTION road_graph.split_edge_by_node(edge_record record, node_record record, minimum_distance_from_linestring real, maximum_distance_from_start_end_points real) RETURNS integer
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
    SET search_path TO road_graph, public;
    id_new_object = nextval(pg_get_serial_sequence('edges', 'id'));
    RESET search_path;
    
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
    SELECT INTO new_edge_record
        e.id
    FROM road_graph.edges AS e
    WHERE 
        -- use ST_Intersects to use spatial indexing
        ST_Intersects(e.geom, end_geom)
        AND ST_Equals(e.geom, end_geom)
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


-- toggle_foreign_key_constraints(boolean)
CREATE FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
BEGIN

    IF _toggle THEN
        -- re-activate all triggers and foreign key constraint
        SET session_replication_role = 'origin';
    ELSE
        -- deactivate all triggers and foreign key constraint
        SET session_replication_role = 'replica';
    END IF;

    RETURN _toggle;
END;
$$;


-- FUNCTION toggle_foreign_key_constraints(_toggle boolean)
COMMENT ON FUNCTION road_graph.toggle_foreign_key_constraints(_toggle boolean) IS 'Deactivate foreign key constraints to ease the merging editing_session data into road_graph schema';


-- update_edge_references(text, integer[])
CREATE FUNCTION road_graph.update_edge_references(_road_code text DEFAULT NULL::text, _edge_ids integer[] DEFAULT NULL::integer[]) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
    _run_build_cache boolean;
    _set_config text;
BEGIN
    -- Deactivate triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
    INTO _set_config;

    -- Build road & marker objects cache for speeding up linear referencing
    SELECT road_graph.build_road_cached_objects(_road_code)
    INTO _run_build_cache;

    -- Get edges references
    WITH s AS (
        SELECT
            e.road_code, e.id,
            x.*
        FROM
            road_graph.edges AS e,
            json_to_record(
                -- 2nd parameter is TRUE so that we use precomputed object cache
                road_graph.get_edge_references(e.id, true)
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


-- FUNCTION update_edge_references(_road_code text, _edge_ids integer[])
COMMENT ON FUNCTION road_graph.update_edge_references(_road_code text, _edge_ids integer[]) IS 'Find the edges corresponding to the optionaly given _road_code and _edges_ids
and calculate their start and end points references.
This method calculates the road cached objects to speed of the process
for big roads with many edges
';


-- update_managed_objects_on_graph_change(text, text, integer[])
CREATE FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _ids integer[]) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    managed_object record;
    update_count integer;
BEGIN

    -- For the given managed object table
    -- and only for the objects with given ids
    SELECT *
    INTO managed_object
    FROM road_graph.managed_objects
    WHERE TRUE
    AND schema_name = _schema_name
    AND table_name = _table_name
    LIMIT 1
    ;
    IF managed_object IS NULL THEN
        RAISE WARNING 'Managed object %.% not found', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    IF managed_object.update_policy_on_graph_change = 'geometry'
    THEN
        -- Update geometries based on references
        IF managed_object.geometry_type = 'POINT'
        THEN
            EXECUTE FORMAT(
                $SQL$
                WITH updated_objects AS (
                    SELECT
                        mo.id,
                        mo.road_code,
                        mo.marker_code,
                        mo.abscissa,
                        mo."offset",
                        mo.side,
                        (road_graph.get_road_point_from_reference(
                            mo.road_code,
                            mo.marker_code,
                            mo.abscissa,
                            mo."offset",
                            mo.side
                        )->>''geom'')::geometry(POINT, 2154) AS geom
                    FROM
                        %1$I.%2$I AS mo
                )
                UPDATE %1$I.%2$I AS mo
                SET
                    geom = u.geom
                FROM updated_objects AS u
                WHERE mo.id = u.id
                AND NOT ST_Equals(mo.geom, u.geom)
                AND mo.id = ANY(string_to_array('%3$s', ',')::integer[])
                $SQL$,
                managed_object.schema_name,
                managed_object.table_name,
                array_to_string(_ids, ',')
            );

            GET DIAGNOSTICS update_count = ROW_COUNT;
            RAISE NOTICE 'Updated % geometries in "%"."%"', update_count, managed_object.schema_name, managed_object.table_name;
        ELSIF managed_object.geometry_type IN ('LINESTRING', 'MULTILINESTRING')
        THEN
            -- For MULTILINESTRING geometry type
            EXECUTE FORMAT(
                $SQL$
                WITH updated_objects AS (
                    SELECT
                        mo.id,
                        mo.road_code,
                        mo.start_marker_code,
                        mo.start_abscissa,
                        mo.end_marker_code,
                        mo.end_abscissa,
                        mo."offset",
                        mo.side,
                        (road_graph.get_road_substring_from_references(
                            mo.road_code,
                            mo.start_marker_code,
                            mo.start_abscissa,
                            mo.end_marker_code,
                            mo.end_abscissa,
                            mo."offset",
                            mo.side
                        )->>''geom'')::geometry(MULTILINESTRING, 2154) AS geom
                    FROM
                        %1$I.%2$I AS mo
                )
                UPDATE %1$I.%2$I AS mo
                SET
                    geom = u.geom
                FROM updated_objects AS u
                WHERE mo.id = u.id
                AND NOT ST_Equals(mo.geom, u.geom)
                AND mo.id = ANY(string_to_array('%3$s', ',')::integer[])
                $SQL$,
                managed_object.schema_name,
                managed_object.table_name,
                array_to_string(_ids, ',')
            );
            GET DIAGNOSTICS update_count = ROW_COUNT;
            RAISE NOTICE 'Updated % geometries in "%"."%"', update_count, managed_object.schema_name, managed_object.table_name;
        ELSE
            RAISE WARNING 'Geometry type % not supported for managed object %', managed_object.geometry_type, managed_object.table_name;
        END IF;
    ELSIF managed_object.update_policy_on_graph_change = 'references' THEN
        IF managed_object.geometry_type = 'POINT' THEN
            RAISE NOTICE 'Updating references for POINT managed object %.% ', managed_object.schema_name, managed_object.table_name;
            EXECUTE format(
                $SQL$
                WITH objects AS (
                    SELECT *
                    FROM %1$I.%2$I
                    WHERE id = ANY(string_to_array('%3$s', ',')::integer[])
                ), references AS (
                    SELECT o.id,
                    road_graph.get_reference_from_point(
                        o.geom, o.road_code
                    ) AS reference
                    FROM objects AS o
                )
                UPDATE %1$I.%2$I AS mo
                SET
                    road_code = r.reference->>'road_code',
                    marker_code = (r.reference->>'marker_code')::integer,
                    abscissa = (r.reference->>'abscissa')::real,
                    "offset" = (r.reference->>'offset')::real,
                    side = r.reference->>'side'
                FROM references AS r
                WHERE mo.id = r.id
                $SQL$,
                managed_object.schema_name,
                managed_object.table_name,
                array_to_string(_ids, ',')
            );
        ELSIF managed_object.geometry_type IN ('LINESTRING', 'MULTILINESTRING') THEN
            RAISE NOTICE 'Updating references for LINESTRING/MULTILINESTRING managed object %.% ', managed_object.schema_name, managed_object.table_name;
            EXECUTE format(
                $SQL$
                WITH objects AS (
                    SELECT *
                    FROM %1$I.%2$I
                    WHERE id = ANY(string_to_array('%3$s', ',')::integer[])
                ), start_references AS (
                    SELECT o.id,
                    road_graph.get_reference_from_point(
                        ST_StartPoint(o.geom), o.road_code
                    ) AS reference
                    FROM objects AS o
                ), end_references AS (
                    SELECT o.id,
                    road_graph.get_reference_from_point(
                        ST_EndPoint(o.geom, o.road_code)
                    ) AS reference
                    FROM objects AS o
                )
                UPDATE %1$I.%2$I AS mo
                SET
                    road_code = sr.reference->>'road_code',
                    start_marker_code = (sr.reference->>'marker_code')::integer,
                    start_abscissa = (sr.reference->>'abscissa')::real,
                    end_marker_code = (er.reference->>'marker_code')::integer,
                    end_abscissa = (er.reference->>'abscissa')::real,
                    "offset" = sr.reference->>'offset',
                    side = sr.reference->>'side'
                WHERE mo.id = r.id
                $SQL$,
                managed_object.schema_name,
                managed_object.table_name,
                array_to_string(_ids, ',')
            );
        END IF;

        GET DIAGNOSTICS update_count = ROW_COUNT;
        RAISE NOTICE 'Updated % references in "%"."%"', update_count, managed_object.schema_name, managed_object.table_name;
    ELSE
        RAISE NOTICE 'No update performed for managed object %.% as update policy is set to %', managed_object.schema_name, managed_object.table_name, managed_object.update_policy_on_graph_change;
    END IF;

    RETURN NULL;
END;

$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _ids integer[])
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _ids integer[]) IS 'Updates managed objects geometries based on their references when the road graph changes';


-- update_road_edges_neighbours(text, integer)
CREATE FUNCTION road_graph.update_road_edges_neighbours(_road_code text, _start_node integer) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    edge record;
BEGIN
    WITH ordered_edges AS (
        SELECT *
        FROM road_graph.get_ordered_edges(
            _road_code, 
            -- get edge ID corresponding to the given start node
            (
                SELECT e.id 
                FROM road_graph.edges AS e
                WHERE e.start_node = _start_node
                AND e.road_code = _road_code
            ),
            'downstream'
        )
    ),
    neighbours AS (
        SELECT
            *, 
            lag(o.id) OVER(ORDER BY o.edge_order) AS previous_edge_id,
            lead(o.id) OVER(ORDER BY o.edge_order) AS next_edge_id
        FROM ordered_edges AS o
        ORDER BY o.edge_order
    )
    UPDATE road_graph.edges AS e
    SET 
        previous_edge_id = n.previous_edge_id,
        next_edge_id = n.next_edge_id
    FROM neighbours AS n
    WHERE e.id = n.id
    ;
    
    -- Return boolean
    RETURN True
    ;

END;
$$;


-- FUNCTION update_road_edges_neighbours(_road_code text, _start_node integer)
COMMENT ON FUNCTION road_graph.update_road_edges_neighbours(_road_code text, _start_node integer) IS 'Try to calculate the previous_edge_id and next_edge_id of the given road edges based on the edge proximity
and an initial node given
';


--
-- PostgreSQL database dump complete
--
