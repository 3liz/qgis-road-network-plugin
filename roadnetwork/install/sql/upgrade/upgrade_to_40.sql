-- Trigger function before_edge_insert_or_update
--
-- * new function get_or_create_edge_node, which gets or creates the node of the start or end point of an edge:
--   the code was duplicated for the start and end nodes in before_edge_insert_or_update
-- * before_edge_insert_or_update: the references of an updated edge are not calculated anymore
--   if the trigger after_edge_insert_or_update calculates again the references of all the edges of the road,
--   or if the geometry, the road and the references have not been changed (update of another field)
-- * before_edge_insert_or_update: LIMIT 1 when searching the next edge,
--   which failed if two edges of the same road started at the same node
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION and COMMENT.


-- FUNCTION: road_graph.get_or_create_edge_node
CREATE OR REPLACE FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) RETURNS TABLE(node_id integer, node_geom geometry, is_created boolean)
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Get the first node closer than 0.5 m from the given start or end point of an edge
    SELECT n.id, n.geom
    INTO node_id, node_geom
    FROM road_graph.nodes AS n
    WHERE ST_DWithin(n.geom, _point, 0.50)
    ORDER BY n.id, n.geom <-> _point
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

COMMENT ON FUNCTION road_graph.get_or_create_edge_node(_point geometry, _create boolean) IS 'Get the node of the given start or end point of an edge: the first node closer than 0.5 m.
If there is no node and _create is True, the node is created (its id can be NULL if a node already exists at the same place).
Used by the trigger function before_edge_insert_or_update.';


-- FUNCTION: road_graph.before_edge_insert_or_update
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
                LIMIT 1
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
                NOT ST_Equals(OLD.geom, NEW.geom)
                OR Coalesce(OLD.previous_edge_id, -1) != NEW.previous_edge_id
                OR Coalesce(OLD.next_edge_id, -1) != NEW.next_edge_id
            ) IS TRUE
            AND road_graph.get_current_setting('road.graph.edge.ref.calc.disabled', 'no') = 'no'
        );
        references_unchanged = (
            ST_Equals(OLD.geom, NEW.geom) IS TRUE
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
