-- FUNCTION: road_graph.get_downstream_multilinestring_from_reference(text, integer, real, real, text)

-- DROP FUNCTION IF EXISTS road_graph.get_downstream_multilinestring_from_reference(text, integer, real, real, text);

CREATE OR REPLACE FUNCTION road_graph.get_downstream_multilinestring_from_reference(
	_road_code text,
	_marker_code integer,
	_abscissa real,
	_offset real,
	_side text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
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
    WITH get_closest_edge AS (
        SELECT
            e.*,
            e.geom <-> marker.geom AS distance,
            -- BEWARE: it can be a point e.g if the marker is at the end of the edge
            -- it why we do not cast with
            -- It could also be an empty geometry
            ST_LineSubstring(
                e.geom,
                ST_LineLocatePoint(e.geom, marker.geom),
                1
            )
            AS sub_geom
        FROM
            road_graph.edges AS e
        WHERE True
        -- same road
        AND e.road_code = _road_code
        ORDER BY distance, start_cumulative
        LIMIT 1
    )
    SELECT INTO closest_edge
        e.id, e.road_code, e.start_node, e.end_node,
        e.start_marker, e.start_abscissa, e.start_cumulative,
        e.end_marker, e.end_abscissa, e.end_cumulative,
        e.geom, e.previous_edge_id, e.next_edge_id,
        distance,
        (CASE
            WHEN GeometryType(sub_geom) = 'POINT'
                THEN ST_MakeLine(sub_geom, sub_geom)::geometry(LINESTRING, 2154)
            ELSE
                CASE
                    -- If the geometry is empty, we take the closest edge end point
                    WHEN ST_IsEmpty(sub_geom)
                        THEN ST_MakeLine(ST_EndPoint(e.geom), ST_EndPoint(e.geom))::geometry(LINESTRING, 2154)
                    ELSE sub_geom
                END
        END)::geometry(LINESTRING, 2154) AS sub_geom
    FROM get_closest_edge AS e
    ;
    IF closest_edge IS NULL THEN
        IF raise_notice = 'yes' THEN
            RAISE NOTICE 'Closest edge from given marker % cannot be found for road %', _marker_code, _road_code;
        END IF;
        RETURN NULL;
    END IF;
    IF raise_notice = 'yes' THEN
        RAISE NOTICE 'Closest edge found: id = %', closest_edge.id;
        RAISE NOTICE 'Closest edge found: sub_geom = %', ST_AsText(closest_edge.sub_geom);
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
$BODY$;

COMMENT ON FUNCTION road_graph.get_downstream_multilinestring_from_reference(text, integer, real, real, text)
    IS 'Returns a JSON object with the given references and the MULTILINESTRING downstream road from given references to the road end.';
