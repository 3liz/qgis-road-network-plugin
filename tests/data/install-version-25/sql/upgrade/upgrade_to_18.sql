
-- get_ordered_edges(text, integer, text)
CREATE OR REPLACE FUNCTION road_graph.get_ordered_edges(_road_code text, _initial_id integer DEFAULT '-1'::integer, _direction text DEFAULT 'downstream'::text)
RETURNS TABLE(id integer, edge_order integer, road_code text, geom geometry)
    LANGUAGE plpgsql
    AS $$
DECLARE
    raise_notice text;
    initial_edge record;
BEGIN
    raise_notice = coalesce(current_setting('road.graph.raise.notice', true), 'no');

    -- If there is only on edge for this road, return it directly
    IF (
        SELECT count(*)
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
    ) = 1
    THEN
        RETURN QUERY
        SELECT
            e.id, 1 AS edge_order, e.road_code, e.geom
        FROM road_graph.edges AS e
        WHERE e.road_code = _road_code
        ;
    END IF;

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
            RAISE NOTICE 'No edge found for the road %s initial edge',
                _road_code
            ;
            -- Do not raise any exception
            -- To allow wrong road_code in managed data
            -- to be silently used
            RETURN QUERY
            SELECT
                e.id, 1 AS edge_order, e.road_code, e.geom
            FROM road_graph.edges AS e
            LIMIT 0
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



-- update_table_geometries_from_references(text, text, text[])
CREATE OR REPLACE FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[]) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
DECLARE
    road_code text;
    update_count integer;
    sql_text text;
    table_exists boolean;
    primary_key_field text;
    needed_fields text[];
    managed_object record;
    table_cols text[];
    updated_stats record;
BEGIN

    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Get the table primary key field
    sql_text = format(
        $SQL$
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '%1$I.%2$I'::regclass AND i.indisprimary
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO primary_key_field
    ;
    IF primary_key_field IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a primary key !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check the table contains the needed fields based on the geometry type of the managed object
    sql_text = format(
        $SQL$
        SELECT array_agg(column_name) AS cols
        FROM information_schema.columns
        WHERE table_schema = '%1$s' AND table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_cols
    ;
    -- For point geometry type, we need road_code, marker_code and abscissa fields
    needed_fields = ARRAY[
        'road_code', 'marker_code', 'abscissa'
    ]::text[];
    -- For line geometry type, we also need start_marker_code, start_abscissa, end_marker_code and end_abscissa fields
    IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring')
    THEN
        needed_fields = ARRAY[
            'road_code',
            'start_marker_code', 'start_abscissa',
            'end_marker_code', 'end_abscissa'
        ]::text[];
    END IF;
    IF NOT (table_cols @> needed_fields)
        THEN
            RAISE NOTICE 'The table "%"."%" does not contain the necessary fields to update geometries from references !', _schema_name, _table_name;
            RETURN NULL;
    END IF;

    -- Update geometries based on references
    -- and geometry type of the managed object
    -- We filter the objects to update based on the given road codes
    -- We must check if the road_graph function returns a valid geometry
    sql_text = format(
        $SQL$
            WITH get_geom AS (
                SELECT
                    mo.%1$I AS id,
                    road_graph.%8$s(
                        mo.road_code::text,
                        %9$s,
                        %6$s,
                        %7$s
                    )->'geom' AS geom
                FROM
                    %2$I.%3$I AS mo
                WHERE (
                    mo.road_code = ANY(string_to_array('%4$s', ',')::text[])
                    OR '%4$s' = ''
                )
            ),
            updated_objects AS (
                SELECT
                    g.id,
                    CASE
                        WHEN g.geom = 'null'::jsonb THEN NULL
                        ELSE ST_GeomFromGeoJSON(g.geom)
                    END::geometry(%5$s, 2154) AS geom
                FROM get_geom AS g
            ),
            run_update AS (
                UPDATE %2$I.%3$I AS mo
                SET
                    geom = ST_ReducePrecision(u.geom, 0.10)
                FROM updated_objects AS u
                WHERE mo.%1$I = u.id
                AND u.geom IS NOT NULL
                AND (
                    mo.geom IS NULL
                    OR NOT ST_Equals(
                        ST_ReducePrecision(mo.geom, 0.10),
                        ST_ReducePrecision(u.geom, 0.10)
                    )
                )
                RETURNING mo.*
            )
            SELECT
                count(r.*) AS nb,
                array_agg(r.%1$I) AS last_updated_objects_ids
            FROM run_update AS r
        $SQL$,
        -- 1
        primary_key_field,
        -- 2
        _schema_name,
        -- 3
        _table_name,
        -- 4 : list of road codes
        Coalesce(array_to_string(_road_codes::text[], ','), ''),
        -- 5 : geometry type
        managed_object.geometry_type,
        -- Use default values for side and offset if columns does not exists
        -- else we need to cast to the expected function parameter formats
        -- 6
        CASE WHEN 'offset' = ANY(table_cols) THEN 'mo."offset"::real' ELSE '0.0::real' END,
        -- 7
        CASE WHEN 'side' = ANY(table_cols) THEN 'mo.side::text' ELSE 'right::text' END,
        -- used function
        -- 8
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'get_road_point_from_reference'
            ELSE 'get_road_substring_from_references'
        END,
        -- used fields: we need to cast the values taken from the managed table
        -- 9
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'mo.marker_code::integer, mo.abscissa::real'
            ELSE 'mo.start_marker_code::integer, mo.start_abscissa::real, mo.end_marker_code::integer, mo.end_abscissa::real'
        END
    );
    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$_$;


DROP FUNCTION IF EXISTS road_graph.update_table_references_from_geometries(text, text, text[]);
CREATE OR REPLACE FUNCTION road_graph.update_table_references_from_geometries(
    _schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean default True
) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
DECLARE
    road_code text;
    update_count integer;
    sql_text text;
    table_exists boolean;
    primary_key_field text;
    geometry_column text;
    needed_fields text[];
    managed_object record;
    table_cols text[];
    updated_stats record;
BEGIN

    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Get the table primary key field
    sql_text = format(
        $SQL$
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '%1$I.%2$I'::regclass AND i.indisprimary
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO primary_key_field
    ;
    IF primary_key_field IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a primary key !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Get geometry column name
    sql_text = format(
        $SQL$
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_schema = '%1$s' AND f_table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO geometry_column
    ;
    IF geometry_column IS NULL THEN
        RAISE NOTICE 'The table "%"."%" does not have a geometry column !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check the table contains the needed fields based on the geometry type of the managed object
    sql_text = format(
        $SQL$
        SELECT array_agg(column_name) AS cols
        FROM information_schema.columns
        WHERE table_schema = '%1$s' AND table_name = '%2$s';
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_cols
    ;
    -- For point geometry type, we need road_code, marker_code and abscissa fields
    needed_fields = ARRAY[
        'road_code', 'marker_code', 'abscissa'
    ]::text[];
    -- For line geometry type, we also need start_marker_code, start_abscissa, end_marker_code and end_abscissa fields
    IF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring')
    THEN
        needed_fields = ARRAY[
            'road_code',
            'start_marker_code', 'start_abscissa',
            'end_marker_code', 'end_abscissa'
        ]::text[];
    END IF;
    IF NOT (table_cols @> needed_fields)
        THEN
            RAISE NOTICE 'The table "%"."%" does not contain the necessary fields to update references from geometries !', _schema_name, _table_name;
            RETURN NULL;
    END IF;

    -- Update references based on geometries
    -- and geometry type of the managed object
    -- We filter the objects to update based on the given road codes
    IF lower(managed_object.geometry_type) = 'point' THEN
        sql_text = format(
            $SQL$
                WITH
                objects AS (
                    SELECT
                        mo.%1$I AS id,
                        trim(mo.road_code)::text AS road_code,
                        mo.%9$I AS geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code::text = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            o.geom,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code::text,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.ref->>'road_code',
                        -- cumulative if present
                        %5$s
                        -- offset if present and if _update_offset_and_side is True
                        %6$s
                        -- side if present and if _update_offset_and_side is True
                        %7$s
                        -- marker code and abscissa put here to avoid errors with commas
                        marker_code = (r.ref->>'marker_code')::integer,
                        abscissa = (r.ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.%1$I = r.id
                    --AND r.ref->'road_code' != 'null'::jsonb
                    AND (%8$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I ORDER BY r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'cumulative' = ANY(table_cols) THEN $STR$cumulative = (r.ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE
                WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"offset" = (r.ref->>'offset')::real, $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$side = (r.ref->>'side')::text, $STR$
                ELSE ''
            END,
            -- 8 / Detect if we need to update or not
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.ref->>'road_code'), '') != '' )
                )
                OR (r.ref->>'marker_code')::integer != Coalesce(mo.marker_code, -1)::integer
                OR (r.ref->>'abscissa')::real != Coalesce(mo.abscissa, -1)::real
                $STR$,
                CASE WHEN 'cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.ref->>'cumulative')::real != Coalesce(mo.cumulative, -1)::real $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR (r.ref->>'offset')::real != Coalesce(mo.offset, -1)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR (r.ref->>'side')::text != Coalesce(mo.side, '')::text $STR$
                    ELSE ''
                END
            ),
            -- 9 / geometry_column
            geometry_column
        );

    ELSIF lower(managed_object.geometry_type) IN ('linestring', 'multilinestring') THEN
        sql_text = format(
            $SQL$
                WITH
                objects AS (
                    SELECT
                        mo.%1$I AS id,
                        trim(mo.road_code)::text AS road_code,
                        mo.%10$I AS geom
                    FROM
                        %2$I.%3$I AS mo
                    WHERE (
                        mo.road_code::text = ANY(string_to_array('%4$s', ',')::text[])
                        OR '%4$s' = ''
                    )
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            -- ST_StartPoint could return NULL for a MULTILINESTRING
                            CASE
                                WHEN lower(GeometryType(o.geom)) = 'linestring'
                                    THEN ST_StartPoint(o.geom)
                                -- hopefully the last part is really the end part of the multilinestring
                                ELSE ST_StartPoint(ST_GeometryN(o.geom, 1))
                            END,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS start_ref,
                        road_graph.get_reference_from_point(
                            -- ST_EndPoint could return NULL for a MULTILINESTRING
                            CASE
                                WHEN lower(GeometryType(o.geom)) = 'linestring'
                                    THEN ST_EndPoint(o.geom)
                                -- hopefully the last part is really the end part of the multilinestring
                                ELSE ST_EndPoint(ST_GeometryN(geom, ST_NumGeometries(geom)))
                            END,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code,
                            -- NULL::text,
                            -- Do not use cache. only usable if there is only one road
                            -- see road_graph.build_road_cached_objects(_road_code)
                            -- we could check before if the given table of road codes contains only one road
                            -- or loop for each road_code...
                            FALSE
                        ) AS end_ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        road_code = r.start_ref->>'road_code',
                        -- start_cumulative if present
                        %5$s
                        -- offset if present
                        %6$s
                        -- side if present
                        %7$s
                        -- end_cumulative if present
                        %8$s
                        -- marker code and abscissa put here to avoid errors with commas
                        start_marker_code = (r.start_ref->>'marker_code')::integer,
                        start_abscissa = (r.start_ref->>'abscissa')::real,
                        end_marker_code = (r.end_ref->>'marker_code')::integer,
                        end_abscissa = (r.end_ref->>'abscissa')::real
                    FROM refs AS r
                    WHERE TRUE
                    AND mo.%1$I = r.id
                    -- Do not UPDATE if no changes must be made (values already are the same)
                    AND (%9$s)
                    RETURNING mo.*
                )
                SELECT
                    count(r.*) AS nb,
                    array_agg(r.%1$I ORDER BY r.%1$I) AS last_updated_objects_ids
                FROM run_update AS r
            $SQL$,
            primary_key_field,
            _schema_name,
            _table_name,
            Coalesce(array_to_string(_road_codes::text[], ','), ''),
            -- 5 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'start_cumulative' = ANY(table_cols) THEN $STR$"start_cumulative" = (r.start_ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 6 / add update for offset if the columns exists in the target table
            CASE
                WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"offset" = (r.start_ref->>'offset')::real, $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"side" = (r.start_ref->>'side')::text, $STR$
                ELSE ''
            END,
            -- 8 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'end_cumulative' = ANY(table_cols) THEN $STR$"end_cumulative" = (r.end_ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 9 / Detect if we need to update or not
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.start_ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.start_ref->>'road_code'), '') != '' )
                )
                OR (r.start_ref->>'marker_code')::integer != Coalesce(mo.start_marker_code, -1)::integer
                OR (r.start_ref->>'abscissa')::real != Coalesce(mo.start_abscissa, -1)::real
                OR (r.end_ref->>'marker_code')::integer != Coalesce(mo.end_marker_code, -1)::integer
                OR (r.end_ref->>'abscissa')::real != Coalesce(mo.end_abscissa, -1)::real
                $STR$,
                CASE WHEN 'start_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.start_ref->>'cumulative')::real != Coalesce(mo.start_cumulative, -1)::real $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR (r.start_ref->>'offset')::real != Coalesce(mo.offset, -1)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR (r.start_ref->>'side')::text != Coalesce(mo.side, '')::text $STR$
                    ELSE ''
                END,
                CASE WHEN 'end_cumulative' = ANY(table_cols)
                    THEN $STR$ OR (r.end_ref->>'cumulative')::real != Coalesce(mo.end_cumulative, -1)::real $STR$ ELSE ''
                END
            ),
            -- 10 / geometry_column
            geometry_column
        );
    END IF;

    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats;

    RETURN to_jsonb(updated_stats);

END;
$_$;


COMMENT ON FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean)
IS 'Update the given table references based on the geometries. This function needs the table to be listed in the table road_graph.managed_objects.
The given columns must exists:
* for points: road_code, marker_code, abscissa. Optional columns: offset & side,
* road_code, start_marker_code, start_abscissa, end_marker_code, end_abscissa. Optional columns: start_cumulative, end_cumulative, offset & side

The parameter _update_offset_and_side allows to not update the offset and side columns of the target table.
It is useful when used before updating the table geometries from the references
(to keep the object in the same start and end places but adapt the geometry)
';



-- update_managed_objects_on_graph_change(text, text, text[])
CREATE OR REPLACE FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[]) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    sql_text text;
    table_exists boolean;
    managed_object record;
    updated_stats_geometry jsonb;
    updated_stats_references jsonb;
    merged_last_updated_objects_ids integer[];
    updated_stats jsonb;
    update_count integer;
BEGIN
    -- Get info on the managed object table
    sql_text = format(
        $SQL$
        SELECT *
        FROM road_graph.managed_objects
        WHERE
            schema_name = '%1$s'
            AND table_name = '%2$s'
        LIMIT 1;
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO managed_object
    ;
    IF managed_object IS NULL THEN
        RAISE NOTICE 'The table "%"."%" is not registered as managed object in the road graph system !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- Check if the table exists in the database
    sql_text = format(
        $SQL$
        SELECT to_regclass('%1$I.%2$I') IS NOT NULL AS exists
        $SQL$,
        _schema_name,
        _table_name
    );
    EXECUTE sql_text
    INTO table_exists
    ;
    IF NOT table_exists THEN
        RAISE NOTICE 'The table "%"."%" does not exist in the database !', _schema_name, _table_name;
        RETURN NULL;
    END IF;

    -- CHOIX IMPORTANT
    -- Lorsqu'on demande la modification de la géométrie,
    -- il faut toujours au préalable recalculer d'abord les références
    -- de début et de fin à partir du nouveau graphe,
    -- pour ensuite adapter la géométrie (si jamais les edges support ont été modifiés)
    -- Cela assure que le début et la fin des lignes des données métiers restent
    -- les "mêmes" sur le terrain (ex: début à cette maison, et fin à ce carrefour)

    -- Update objects references in both cases
    -- But when we do it before updating geometries, we should not modify the offset and side values

    SELECT road_graph.update_table_references_from_geometries(
        _schema_name,
        _table_name,
        _road_codes,
        CASE
            WHEN managed_object.update_policy_on_graph_change = 'geometry'
                THEN False
            ELSE True
        END
    )
    INTO updated_stats_references
    ;
    IF updated_stats_references IS NULL THEN
        updated_stats_references = jsonb_build_object(
            'nb', 0,
            'last_updated_objects_ids', ARRAY[]::integer[]
        );
    END IF;
    updated_stats = updated_stats_references;

    IF managed_object.update_policy_on_graph_change = 'geometry' THEN
        -- Update geometries based on references
        SELECT road_graph.update_table_geometries_from_references(
            _schema_name,
            _table_name,
            _road_codes
        )
        INTO updated_stats_geometry
        ;
        IF updated_stats_geometry IS NULL THEN
            updated_stats_geometry = jsonb_build_object(
                'nb', 0,
                'last_updated_objects_ids', ARRAY[]::integer[]
            );
        END IF;
        -- Get combined stats
        -- RAISE NOTICE '--------------';
        -- RAISE NOTICE 'refs, %', updated_stats_references::json;
        -- RAISE NOTICE 'geoms, %', updated_stats_geometry::json;
        merged_last_updated_objects_ids = (
            WITH a AS (
                SELECT t FROM jsonb_array_elements(
                        CASE
                        WHEN jsonb_typeof(updated_stats_references->'last_updated_objects_ids') = 'array'
                        THEN updated_stats_references->'last_updated_objects_ids'
                        ELSE '[]'
                        END
                ) AS t
                UNION
                SELECT t FROM jsonb_array_elements(
                        CASE
                        WHEN jsonb_typeof(updated_stats_geometry->'last_updated_objects_ids') = 'array'
                        THEN updated_stats_geometry->'last_updated_objects_ids'
                        ELSE '[]'
                        END
                ) AS t
            )
            SELECT array_agg(t ORDER BY t)
            FROM a
        );
        -- RAISE NOTICE 'merged_last_updated_objects_ids, %', merged_last_updated_objects_ids;
        -- RAISE NOTICE '--------------';


        updated_stats = jsonb_build_object(
            'nb',
            array_length(merged_last_updated_objects_ids, 1),
            'last_updated_objects_ids',
            merged_last_updated_objects_ids
        )
        ;

    END IF;

    -- Check results
    IF updated_stats IS NULL THEN
        RAISE NOTICE 'No object updated for %.%', _schema_name, _table_name;
        RETURN NULL;
    END IF;
    IF (updated_stats->>'nb')::integer = 0 THEN
        RAISE NOTICE 'No object updated for %.%', _schema_name, _table_name;
        RETURN NULL;
    END IF;
    -- Update the managed_objects table
    EXECUTE format(
        $SQL$
            UPDATE road_graph.managed_objects AS o
            SET (
                last_updated_objects_ids,
                last_update
            ) = (
                ARRAY[%3$s]::integer[],
                now()::timestamp(0)
            )
            WHERE o.schema_name = '%1$I'
            AND o.table_name = '%2$I'
        $SQL$,
        _schema_name,
        _table_name,
        -- convert json array to string like 1,3,10
        (
            SELECT array_to_string(array_agg(j)::text[], ',')
            FROM jsonb_array_elements(updated_stats->'last_updated_objects_ids') AS j
        )
    );

    -- Return the number of updated features
    RETURN (updated_stats->>'nb')::integer;

END;
$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[])
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[]) IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table';



-- merge_editing_session_data(integer)
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
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

    -- Get the road codes concerned by the editing session
    -- no need to prefix the function by the schema name because we set it in the search path
    SELECT INTO road_codes
        get_updated_roads_from_editing_session(_editing_session_id)
    ;

    -- Create SQL to run for each edited data
    -- no need to prefix the function by the schema name because we set it in the search path
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

    END IF;

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

DROP VIEW IF EXISTS road_graph.v_managed_objects;
CREATE VIEW road_graph.v_managed_objects AS
SELECT m.id,
    m.schema_name,
    m.table_name,
    m.geometry_type,
    m.update_policy_on_graph_change,
    m.last_update,
    m.last_updated_objects_ids,
    road_graph.get_merged_geom_from_table_and_ids(m.schema_name, m.table_name, m.last_updated_objects_ids) AS geom
FROM road_graph.managed_objects m
-- Add a line so that QGIS does not consider the empty view as invalid
UNION
SELECT -1::integer AS id,
    'aucune'::text AS schema_name,
    'donnee'::text AS table_name,
    NULL::text AS geometry_type,
    NULL::text AS update_policy_on_graph_change,
    NULL::timestamp without time zone AS last_update,
    NULL::integer[] AS last_updated_objects_ids,
    (ST_Buffer(ST_SetSrid(ST_MakePoint(0, 0), 2154), 0))::geometry(MultiPolygon, 2154) AS geom
;


-- VIEW v_managed_objects
COMMENT ON VIEW road_graph.v_managed_objects IS 'View allowing to see the merged geometries of each managed table for the last editing session merge. Useful to have a quick look at the edited objects.';
