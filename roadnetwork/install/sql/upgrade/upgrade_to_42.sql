-- Managed objects: update only the objects impacted by the modifications of an editing session
--
-- When an editing session is merged, all the managed objects of the modified roads were updated,
-- for example about 13 seconds for a road of 118 km with 1,250 objects, even if only a node had been added.
-- Now, only the objects which may be impacted are updated (functions get_editing_session_impacts
-- and get_managed_objects_impacts_filter):
-- * the objects near the modified edges (50 m),
-- * if the geometry of the road has changed, the objects located downstream of the first modified point:
--   up to the end of the road if the table has a cumulative field, else up to the last marker of the modified edges,
-- * the objects between the previous marker and a modified marker.
-- The edges only split, or whose attributes only have changed, do not change the geometry of the road.
-- The algorithm "Update managed objects" still updates all the objects.
--
-- Rounding of the references
-- * get_reference_from_point: the abscissa and cumulative distance were converted from real to numeric,
--   which only keeps 6 significant digits: the cumulative distances greater than 10 km were rounded to 10 cm
-- * update_table_references_from_geometries: a difference of 1 cm is ignored, since calculating the geometry
--   from the references, and then the references from the geometry rounded to 1 cm, can give a difference of 1 cm:
--   the lines with the update policy "geometry" were shortened by 1 cm at each merge
--
-- The geometry of a managed object which cannot be calculated from its references no longer cancels the merge:
-- the object is not updated and a warning is raised (function get_managed_object_geometry_from_references).
--
-- The list of the last updated objects (managed_objects.last_updated_objects_ids) is emptied
-- when a merge does not update any object: it contained the objects of the previous merge.
--
-- The references of all the edges are calculated again at the end of this migration,
-- to correct the cumulative distances rounded to 10 cm.


-- New parameter _impacts
DROP FUNCTION IF EXISTS road_graph.update_managed_objects_on_graph_change(text, text, text[]);
DROP FUNCTION IF EXISTS road_graph.update_table_geometries_from_references(text, text, text[]);
DROP FUNCTION IF EXISTS road_graph.update_table_references_from_geometries(text, text, text[], boolean);


-- FUNCTION: road_graph.get_editing_session_impacts
CREATE OR REPLACE FUNCTION road_graph.get_editing_session_impacts(_editing_session_id integer) RETURNS jsonb
    LANGUAGE sql
    AS $$
    WITH
    logged AS (
        SELECT i.item, l.key::integer AS id, l.value AS op
        FROM
            "road_graph".editing_sessions AS es,
            unnest(ARRAY['edges', 'markers']) AS i(item),
            jsonb_each_text(Coalesce(es.logged_ids->i.item, '{}'::jsonb)) AS l
        WHERE es.id = _editing_session_id
    ),
    -- Only the edges whose geometry or road has changed.
    -- The edges only updated by the calculation of their references are not taken into account
    modified_edges AS (
        SELECT l.id
        FROM logged AS l
        LEFT JOIN "road_graph".edges AS o
            ON o.id = l.id
        LEFT JOIN road_graph_editing.edges AS n
            ON n.id = l.id AND l.op != 'D'
        WHERE l.item = 'edges'
        AND (
            o.id IS NULL OR n.id IS NULL
            OR o.road_code != n.road_code
            OR NOT ST_Equals(o.geom, n.geom)
        )
    ),
    -- The modified edges before (production schema) and after (editing schema) the modifications
    all_edges AS (
        SELECT 'old' AS version, e.road_code, e.geom,
            e.start_marker, e.start_abscissa, e.end_marker
        FROM "road_graph".edges AS e
        JOIN modified_edges AS m ON m.id = e.id
        UNION ALL
        SELECT 'new' AS version, e.road_code, e.geom,
            e.start_marker, e.start_abscissa, e.end_marker
        FROM road_graph_editing.edges AS e
        JOIN modified_edges AS m ON m.id = e.id
    ),
    -- The modified markers before and after the modifications
    all_markers AS (
        SELECT m.road_code, m.code
        FROM "road_graph".markers AS m
        JOIN logged AS l ON l.item = 'markers' AND l.id = m.id
        UNION
        SELECT m.road_code, m.code
        FROM road_graph_editing.markers AS m
        JOIN logged AS l ON l.item = 'markers' AND l.id = m.id AND l.op != 'D'
    ),
    roads AS (
        SELECT DISTINCT a.road_code FROM all_edges AS a
        UNION
        SELECT DISTINCT m.road_code FROM all_markers AS m
    ),
    edges_impacts AS (
        SELECT
            r.road_code,
            (
                SELECT ST_Union(a.geom) FROM all_edges AS a
                WHERE a.road_code = r.road_code AND a.version = 'old'
            ) AS old_geom,
            (
                SELECT ST_Union(a.geom) FROM all_edges AS a
                WHERE a.road_code = r.road_code AND a.version = 'new'
            ) AS new_geom,
            (
                SELECT Coalesce(sum(ST_Length(a.geom)), 0) FROM all_edges AS a
                WHERE a.road_code = r.road_code AND a.version = 'old'
            ) AS old_length,
            (
                SELECT Coalesce(sum(ST_Length(a.geom)), 0) FROM all_edges AS a
                WHERE a.road_code = r.road_code AND a.version = 'new'
            ) AS new_length,
            -- First modified point: the least start references of the modified edges
            (
                SELECT jsonb_build_array(a.start_marker, a.start_abscissa) FROM all_edges AS a
                WHERE a.road_code = r.road_code
                ORDER BY a.start_marker, a.start_abscissa
                LIMIT 1
            ) AS start_reference,
            (
                SELECT max(a.end_marker) FROM all_edges AS a
                WHERE a.road_code = r.road_code
            ) AS end_marker_code,
            -- The references are not calculated for some edges, for example the roundabouts
            (
                SELECT bool_or(a.start_marker IS NULL OR a.start_abscissa IS NULL OR a.end_marker IS NULL)
                FROM all_edges AS a
                WHERE a.road_code = r.road_code
            ) AS null_references
        FROM roads AS r
    ),
    markers_impacts AS (
        -- For each modified marker: its code and the code of the previous marker
        SELECT
            m.road_code,
            jsonb_agg(
                jsonb_build_array(
                    Coalesce(
                        (
                            SELECT max(p.code) FROM (
                                SELECT x.code FROM "road_graph".markers AS x
                                WHERE x.road_code = m.road_code AND x.code < m.code
                                UNION ALL
                                SELECT x.code FROM road_graph_editing.markers AS x
                                WHERE x.road_code = m.road_code AND x.code < m.code
                            ) AS p
                        ),
                        -1
                    ),
                    m.code
                )
            ) AS marker_ranges
        FROM all_markers AS m
        GROUP BY m.road_code
    )
    SELECT
        jsonb_agg(
            jsonb_build_object(
                'road_code', e.road_code,
                'whole_road', Coalesce(e.null_references, false),
                -- The geometry of the road has not changed when the edges have only been split
                'geometry_changed', (
                    (e.old_geom IS NOT NULL OR e.new_geom IS NOT NULL)
                    AND NOT Coalesce(
                        abs(e.old_length - e.new_length) < 0.01
                        AND ST_HausdorffDistance(e.old_geom, e.new_geom) < 0.1,
                        false
                    )
                ),
                'start_marker_code', e.start_reference->0,
                'start_abscissa', e.start_reference->1,
                'end_marker_code', e.end_marker_code,
                'near_geom', encode(ST_AsEWKB(ST_Collect(ARRAY[e.old_geom, e.new_geom])), 'hex'),
                'marker_ranges', Coalesce(m.marker_ranges, '[]'::jsonb)
            )
        )
    FROM edges_impacts AS e
    LEFT JOIN markers_impacts AS m
        ON m.road_code = e.road_code
    ;
$$;


-- FUNCTION get_editing_session_impacts(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.get_editing_session_impacts(_editing_session_id integer) IS 'Get the impacts of the given editing session on the roads, to update only the managed objects which may be impacted.
It must be run before the merge of the editing session, since it compares the production and editing data.
It returns a JSON array with one object per impacted road:
* road_code,
* whole_road: true if all the objects of the road must be updated (references not calculated for the modified edges, for example for the roundabouts),
* geometry_changed: false if the edges have only been split, or if only their attributes have changed,
* start_marker_code & start_abscissa: the references of the first modified point,
* end_marker_code: the last marker of the modified edges,
* near_geom: the modified edges geometries, before and after the modifications, as hexadecimal EWKB,
* marker_ranges: for each modified marker, the code of the previous marker and its code.
The function get_managed_objects_impacts_filter uses this JSON to filter the managed objects to update.';


-- FUNCTION: road_graph.get_managed_objects_impacts_filter
CREATE OR REPLACE FUNCTION road_graph.get_managed_objects_impacts_filter(_geometry_type text, _table_cols text[], _geometry_column text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $_$
DECLARE
    start_marker_field text;
    start_abscissa_field text;
    end_marker_field text;
    end_abscissa_field text;
    has_cumulative boolean;
BEGIN
    IF lower(_geometry_type) = 'point' THEN
        start_marker_field = 'mo.marker_code';
        start_abscissa_field = 'mo.abscissa';
        end_marker_field = 'mo.marker_code';
        end_abscissa_field = 'mo.abscissa';
        has_cumulative = 'cumulative' = ANY(_table_cols);
    ELSE
        start_marker_field = 'mo.start_marker_code';
        start_abscissa_field = 'mo.start_abscissa';
        end_marker_field = 'mo.end_marker_code';
        end_abscissa_field = 'mo.end_abscissa';
        has_cumulative = _table_cols && ARRAY['start_cumulative', 'end_cumulative']::text[];
    END IF;

    -- The objects which may be impacted are:
    -- * the objects without references,
    -- * the objects located near the modified edges,
    -- * if the geometry of the road has changed, the objects located downstream of the first modified point:
    --   up to the end of the road if the table has a cumulative field,
    --   or else up to the last marker of the modified edges, since the abscissas are measured from the markers
    -- * the objects between the previous marker and a modified marker
    RETURN format(
        $SQL$
        EXISTS (
            SELECT 1
            FROM jsonb_to_recordset($1) AS i (
                road_code text, whole_road boolean, geometry_changed boolean,
                start_marker_code integer, start_abscissa real, end_marker_code integer,
                near_geom text, marker_ranges jsonb
            )
            WHERE i.road_code = trim(mo.road_code)::text
            AND (
                i.whole_road
                OR %1$s IS NULL OR %2$s IS NULL OR %3$s IS NULL OR %4$s IS NULL
                OR ST_DWithin(mo.%5$I, ST_GeomFromEWKB(decode(i.near_geom, 'hex')), 50)
                OR (
                    i.geometry_changed
                    AND (%3$s, %4$s) >= (i.start_marker_code, i.start_abscissa)
                    AND (%6$s OR %1$s <= i.end_marker_code)
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(i.marker_ranges) AS r
                    WHERE %1$s <= (r->>1)::integer AND %3$s >= (r->>0)::integer
                )
            )
        )
        $SQL$,
        start_marker_field,
        start_abscissa_field,
        end_marker_field,
        end_abscissa_field,
        _geometry_column,
        CASE WHEN has_cumulative THEN 'TRUE' ELSE 'FALSE' END
    );
END;
$_$;


-- FUNCTION get_managed_objects_impacts_filter(_geometry_type text, _table_cols text[], _geometry_column text)
COMMENT ON FUNCTION road_graph.get_managed_objects_impacts_filter(_geometry_type text, _table_cols text[], _geometry_column text) IS 'Build the SQL filter of the managed objects which may be impacted by the modifications of the road graph.
The managed table must have the alias mo, and the impacts returned by the function get_editing_session_impacts must be given as the first parameter $1 of the query.';


-- FUNCTION: road_graph.get_managed_object_geometry_from_references
CREATE OR REPLACE FUNCTION road_graph.get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text) RETURNS jsonb
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Point
    IF _end_marker_code IS NULL THEN
        RETURN road_graph.get_road_point_from_reference(
            _road_code, _start_marker_code, _start_abscissa, _offset, _side
        );
    END IF;

    -- Line
    RETURN road_graph.get_road_substring_from_references(
        _road_code, _start_marker_code, _start_abscissa, _end_marker_code, _end_abscissa, _offset, _side
    );

EXCEPTION WHEN OTHERS THEN
    -- Do not cancel the update of all the objects because of one object
    RAISE WARNING 'The geometry of the object of the road % with the references % + % -> % + % cannot be calculated: %',
        _road_code, _start_marker_code, _start_abscissa, _end_marker_code, _end_abscissa, SQLERRM
    ;
    RETURN NULL;
END;
$$;


-- FUNCTION get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text)
COMMENT ON FUNCTION road_graph.get_managed_object_geometry_from_references(_road_code text, _start_marker_code integer, _start_abscissa real, _end_marker_code integer, _end_abscissa real, _offset real, _side text) IS 'Get the geometry of a managed object from its references, with the functions get_road_point_from_reference for a point (_end_marker_code is NULL)
or get_road_substring_from_references for a line.
It returns NULL with a warning instead of raising an exception if the geometry cannot be calculated,
for example if the end references are before the start references.';


-- FUNCTION: road_graph.get_reference_from_point
CREATE OR REPLACE FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text DEFAULT NULL::text, _use_cache boolean DEFAULT false) RETURNS jsonb
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
    -- double precision and not real: a real converted to numeric only keeps 6 significant digits
    -- for example a cumulative distance of 50955.53 m was rounded to 50955.5 m
    found_abscissa double precision;
    found_offset double precision;
    found_side text;
    found_cumulative double precision;
    cached_edge_orders jsonb;
    raise_notice text;
BEGIN
    raise_notice = road_graph.get_current_setting('road.graph.raise.notice', 'no');
    IF raise_notice IN ('info', 'debug') THEN
        RAISE NOTICE '% get_reference_from_point - _point = % & _road_code = %',
            REPEAT('    ', pg_trigger_depth()::INTEGER),
            ST_AsText(_point),
            _road_code
        ;
    END IF;

    -- Get the edge order of the road from the cache, if it has been built beforehand
    -- (see build_road_cached_objects)
    IF _use_cache AND _road_code IS NOT NULL AND to_regclass('road_cache') IS NOT NULL THEN
        SELECT INTO cached_edge_orders r.edge_orders
        FROM road_cache AS r
        WHERE r.road_code = _road_code
        ;
    END IF;

    -- Get the closest edge depending on given road_code
    IF _road_code IS NOT NULL THEN
        WITH ordered_ids AS (
            -- Edge order calculated only if not found in the cache
            SELECT o.id, o.edge_order
            FROM road_graph.get_ordered_edges(_road_code, -1, 'downstream') AS o
            WHERE cached_edge_orders IS NULL
            UNION ALL
            SELECT j.key::integer AS id, j.value::integer AS edge_order
            FROM jsonb_each_text(cached_edge_orders) AS j
        )
        SELECT INTO closest_edge
            e.*,
            -- Calculate the distance between the edge and the point
            -- Do not use e.geom <-> _point AS distance
            -- since it can lead to some PostGIS errors if used in the ORDER BY
            -- such as "ERROR:  index returned tuples in wrong order"
            ST_Distance(e.geom, _point) AS distance
        FROM
            road_graph.edges AS e
        INNER JOIN ordered_ids AS o ON e.id = o.id
        -- Limit search for the given road code
        WHERE e.road_code = _road_code
        -- Limit search to 50m
        AND ST_DWithin(e.geom, _point, 50)
        -- we must also order by edge_order, start_cumulative
        -- in case the point catches multiple edges (start and end points)
        ORDER BY distance, o.edge_order, e.start_cumulative
        -- Get only the closest edge, with the least edge_order -- start cumulative
        LIMIT 1
        ;
    ELSE
        SELECT INTO closest_edge
            e.*,
            -- Calculate the distance between the edge and the point
            -- Do not use e.geom <-> _point AS distance
            -- since it can lead to some PostGIS errors if used in the ORDER BY
            -- such as "ERROR:  index returned tuples in wrong order"
            ST_Distance(e.geom, _point) AS distance
        FROM
            road_graph.edges AS e
            -- LIMIT search without road_code TO 50m
        WHERE ST_DWithin(e.geom, _point, 50)
        ORDER BY distance
        -- Get only the closest edge
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
    END IF;

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

    -- Calculate values to return based on the generated geometries
    -- If the marker is the next edge start marker, because the point
    -- was not on an edge but between two unconnected edges,
    -- we should take information from this edge.
    found_marker_code = closest_edge_marker.code;
    found_abscissa =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN ST_Length(closest_edge_marker.upstream_road_from_marker) + closest_edge_marker.abscissa
            ELSE closest_edge_marker.abscissa
        END
    ;
    found_cumulative =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN Coalesce(ST_Length(closest_edge_marker.upstream_road_from_start), 0)
            ELSE closest_edge_marker.cumulative
        END
    ;
    found_offset =
        CASE
            WHEN closest_edge_marker.is_next_edge_start_marker IS NOT TRUE
                THEN closest_edge.distance
            ELSE 0.0
        END
    ;
    found_side = (
        CASE
            WHEN ST_Contains(
                ST_Buffer(
                    closest_edge.geom,
                    closest_edge.distance +1,
                    'side=left'
                ), _point
            ) THEN 'left'
            ELSE 'right'
        END
    );

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


-- FUNCTION get_reference_from_point(_point geometry, _road_code text, _use_cache boolean)
COMMENT ON FUNCTION road_graph.get_reference_from_point(_point geometry, _road_code text, _use_cache boolean) IS 'Calculate the references for the given point. The second parameter _road_code allows to narrow the search to the specified road.
Since this method is heavily used and the calculation is costly, we can pass a third parameter _use_cache which allows to use a pre-generated cache (to be build before using this function).';


-- FUNCTION: road_graph.update_table_references_from_geometries
CREATE OR REPLACE FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean DEFAULT true, _impacts jsonb DEFAULT NULL::jsonb) RETURNS jsonb
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
    cached_road_codes text[];
    cached_road_code text;
    impacts_filter text;
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

    -- Filter the objects which may be impacted by the modifications of the road graph, if given
    impacts_filter = CASE
        WHEN _impacts IS NULL THEN 'TRUE'
        ELSE road_graph.get_managed_objects_impacts_filter(managed_object.geometry_type, table_cols, geometry_column)
    END;

    -- Build the road cache for the roads of the objects to update
    -- It is always built again here, even if it already exists for a road,
    -- since the road graph could have been modified since its last build in the same transaction
    sql_text = format(
        $SQL$
        SELECT array_agg(DISTINCT trim(mo.road_code)::text)
        FROM %1$I.%2$I AS mo
        WHERE Coalesce(trim(mo.road_code), '') != ''
        AND (
            mo.road_code::text = ANY(string_to_array('%3$s', ',')::text[])
            OR '%3$s' = ''
        )
        $SQL$,
        _schema_name,
        _table_name,
        Coalesce(array_to_string(_road_codes::text[], ','), '')
    );
    EXECUTE sql_text
    INTO cached_road_codes
    ;
    FOREACH cached_road_code IN ARRAY Coalesce(cached_road_codes, ARRAY[]::text[]) LOOP
        PERFORM road_graph.build_road_cached_objects(cached_road_code);
    END LOOP;

    -- Get road information


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
                    AND (%10$s)
                ),
                refs AS (
                    SELECT
                        o.id,
                        road_graph.get_reference_from_point(
                            o.geom,
                            -- We need to pass the road code to force the calculation to keep references for this road
                            o.road_code::text,
                            -- NULL::text,
                            -- Use the road cache built before
                            TRUE
                        ) AS ref
                    FROM objects AS o
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        -- Do no change the road code if no references have been found
                        -- for the given road (meaning the object is too far)
                        road_code =
                        CASE
                            -- keep object road_code intact if it is not empty
                            WHEN Coalesce(mo.road_code, '') != '' THEN mo.road_code
                            ELSE r.ref->>'road_code'
                        END,
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
                    THEN $STR$"offset" = Coalesce((r.ref->>'offset')::real, 0.0::real), $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$side = Coalesce((r.ref->>'side')::text, 'right'), $STR$
                ELSE ''
            END,
            -- 8 / Detect if we need to update or not
            -- A difference of 1 cm of the abscissa or cumulative is ignored: calculating the geometry from the references,
            -- then the references from this geometry rounded to 1 cm, can give a difference of 1 cm.
            -- Without this tolerance, the lines of the update policy geometry were shortened by 1 cm at each update
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.ref->>'road_code'), '') != '' )
                )
                OR (r.ref->>'marker_code')::integer != Coalesce(mo.marker_code, -1)::integer
                OR abs((r.ref->>'abscissa')::real - Coalesce(mo.abscissa, -1)::real) > 0.015
                $STR$,
                CASE WHEN 'cumulative' = ANY(table_cols)
                    THEN $STR$ OR abs((r.ref->>'cumulative')::real - Coalesce(mo.cumulative, -1)::real) > 0.015 $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.ref->>'offset')::real, 0.0::real) != Coalesce(mo.offset, 0.0)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.ref->>'side')::text, 'right') != Coalesce(mo.side, 'right')::text $STR$
                    ELSE ''
                END
            ),
            -- 9 / geometry_column
            geometry_column,
            -- 10 / filter of the impacted objects
            impacts_filter
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
                    AND (%11$s)
                ),
                road_info AS (
                    SELECT r.road_code, r.road_type,
                        min(e.start_marker) AS min_code,
                        max(e.end_marker) AS max_code,
                        min(e.start_cumulative) AS min_cumulative,
                        max(e.end_cumulative) AS max_cumulative,
                        min(e.start_abscissa) AS min_abscissa,
                        max(e.end_abscissa) AS max_abscissa
                    FROM road_graph.edges AS e
                    JOIN road_graph.roads AS r
                        USING (road_code)
                    WHERE e.road_code IN (
                        SELECT DISTINCT o.road_code
                        FROM objects AS o
                    )
                    GROUP BY r.road_code, r.road_type
                ),
                refs AS (
                    SELECT
                        o.id, o.road_code,
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
                            -- Use the road cache built before
                            TRUE
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
                            -- Use the road cache built before
                            TRUE
                        ) AS end_ref
                    FROM objects AS o
                ),
                processed_refs AS (
                    SELECT
                        r.id,
                        r.road_code,
                        -- start_ref
                        CASE
                            -- For roundabout, if the start and end values are equal
                            -- use 0+0 for start & 0+max_cumulative for end
                            WHEN i.road_type = 'roundabout'
                            AND Coalesce((r.start_ref->>'abscissa')::real, 0) = Coalesce((r.end_ref->>'abscissa')::real, 0)
                                THEN jsonb_build_object(
                                    'road_code', r.start_ref->>'road_code',
                                    'marker_code', 0,
                                    'abscissa', 0.0,
                                    'cumulative', 0.0,
                                    'offset', r.start_ref->>'offset',
                                    'side', r.start_ref->'side'
                                )
                            -- for other roads, invert start and end if start < end
                            WHEN
                            Coalesce((r.start_ref->>'marker_code')::int * 10000 + (r.start_ref->>'abscissa')::real, 0)
                            >
                            Coalesce((r.end_ref->>'marker_code')::int * 10000 + (r.end_ref->>'abscissa')::real, 0)
                                THEN end_ref
                            ELSE r.start_ref
                        END AS start_ref,
                        -- end_ref
                        CASE
                            -- For roundabout, if the start and end values are equal
                            -- use 0+0 for start & 0+max_cumulative for end
                            WHEN i.road_type = 'roundabout'
                            AND Coalesce((r.start_ref->>'abscissa')::real, 0) = Coalesce((r.end_ref->>'abscissa')::real, 0)
                                THEN jsonb_build_object(
                                    'road_code', r.end_ref->>'road_code',
                                    'marker_code', 0,
                                    'abscissa', i.max_abscissa,
                                    'cumulative', i.max_cumulative,
                                    'offset', r.end_ref->>'offset',
                                    'side', r.end_ref->'side'
                                )
                            -- for other roads, invert start and end if start < end
                            WHEN
                            Coalesce((r.start_ref->>'marker_code')::int * 10000 + (r.start_ref->>'abscissa')::real, 0)
                            >
                            Coalesce((r.end_ref->>'marker_code')::int * 10000 + (r.end_ref->>'abscissa')::real, 0)
                                THEN start_ref
                            ELSE r.end_ref
                        END AS end_ref
                    FROM refs AS r
                    JOIN road_info AS i
                        USING (road_code)
                ),
                run_update AS (
                    UPDATE %2$I.%3$I AS mo
                    SET
                        -- Do no change the road code if no references have been found
                        -- for the given road (meaning the object is too far)
                        road_code =
                        CASE
                            -- keep object road_code intact if it is not empty
                            WHEN Coalesce(mo.road_code, '') != '' THEN mo.road_code
                            ELSE r.start_ref->>'road_code'
                        END,

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
                    FROM processed_refs AS r
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
                    THEN $STR$"offset" = Coalesce((r.start_ref->>'offset')::real, 0.0::real), $STR$
                ELSE ''
            END,
            -- 7 / add update for side if the columns exists in the target table
            CASE
                WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                    THEN $STR$"side" = Coalesce((r.start_ref->>'side')::text, 'right'::text), $STR$
                ELSE ''
            END,
            -- 8 / add update for cumulative if the columns exists in the target table
            CASE WHEN 'end_cumulative' = ANY(table_cols) THEN $STR$"end_cumulative" = (r.end_ref->>'cumulative')::real, $STR$ ELSE '' END,
            -- 9 / Detect if we need to update or not
            -- A difference of 1 cm of the abscissa or cumulative is ignored: calculating the geometry from the references,
            -- then the references from this geometry rounded to 1 cm, can give a difference of 1 cm.
            -- Without this tolerance, the lines of the update policy geometry were shortened by 1 cm at each update
            concat(
                $STR$
                (
                    ( Coalesce(mo.road_code, '') != '' AND coalesce((r.start_ref->>'road_code'), '') = '' )
                    OR
                    ( Coalesce(mo.road_code, '') = '' AND coalesce((r.start_ref->>'road_code'), '') != '' )
                )
                OR (r.start_ref->>'marker_code')::integer != Coalesce(mo.start_marker_code, -1)::integer
                OR abs((r.start_ref->>'abscissa')::real - Coalesce(mo.start_abscissa, -1)::real) > 0.015
                OR (r.end_ref->>'marker_code')::integer != Coalesce(mo.end_marker_code, -1)::integer
                OR abs((r.end_ref->>'abscissa')::real - Coalesce(mo.end_abscissa, -1)::real) > 0.015
                $STR$,
                CASE WHEN 'start_cumulative' = ANY(table_cols)
                    THEN $STR$ OR abs((r.start_ref->>'cumulative')::real - Coalesce(mo.start_cumulative, -1)::real) > 0.015 $STR$ ELSE ''
                END,
                CASE
                    WHEN 'offset' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.start_ref->>'offset')::real, 0.0::real) != Coalesce(mo.offset, 0.0)::real $STR$
                    ELSE ''
                END,
                CASE
                    WHEN 'side' = ANY(table_cols) AND _update_offset_and_side IS True
                        THEN $STR$ OR Coalesce((r.start_ref->>'side')::text, 'right') != Coalesce(mo.side, 'right')::text $STR$
                    ELSE ''
                END,
                CASE WHEN 'end_cumulative' = ANY(table_cols)
                    THEN $STR$ OR abs((r.end_ref->>'cumulative')::real - Coalesce(mo.end_cumulative, -1)::real) > 0.015 $STR$ ELSE ''
                END
            ),
            -- 10 / geometry_column
            geometry_column,
            -- 11 / filter of the impacted objects
            impacts_filter
        );
    END IF;

    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats
    USING _impacts;

    RETURN to_jsonb(updated_stats);

END;
$_$;


-- FUNCTION update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean, _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_table_references_from_geometries(_schema_name text, _table_name text, _road_codes text[], _update_offset_and_side boolean, _impacts jsonb) IS 'Update the given table references based on the geometries. This function needs the table to be listed in the table road_graph.managed_objects.
The given columns must exists:
* for points: road_code, marker_code, abscissa. Optional columns: offset & side,
* road_code, start_marker_code, start_abscissa, end_marker_code, end_abscissa. Optional columns: start_cumulative, end_cumulative, offset & side

The parameter _update_offset_and_side allows to not update the offset and side columns of the target table.
It is useful when used before updating the table geometries from the references
(to keep the object in the same start and end places but adapt the geometry)

The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.
';


-- FUNCTION: road_graph.update_table_geometries_from_references
CREATE OR REPLACE FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb DEFAULT NULL::jsonb) RETURNS jsonb
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
                    road_graph.get_managed_object_geometry_from_references(
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
                AND (%8$s)
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
                    geom = ST_ReducePrecision(u.geom, 0.01)
                FROM updated_objects AS u
                WHERE mo.%1$I = u.id
                AND u.geom IS NOT NULL
                AND (
                    mo.geom IS NULL
                    OR NOT ST_Equals(
                        ST_ReducePrecision(mo.geom, 0.01),
                        ST_ReducePrecision(u.geom, 0.01)
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
        -- Use default values for side and offset if columns does not exists or value is NULL
        -- else we need to cast to the expected function parameter formats
        -- 6
        CASE WHEN 'offset' = ANY(table_cols) THEN 'Coalesce(mo."offset"::real, 0.0::real)' ELSE '0.0::real' END,
        -- 7
        CASE WHEN 'side' = ANY(table_cols) THEN 'Coalesce(mo.side::text, ''right''::text) ' ELSE '''right''::text' END,
        -- 8 : filter of the impacted objects
        CASE
            WHEN _impacts IS NULL THEN 'TRUE'
            ELSE road_graph.get_managed_objects_impacts_filter(managed_object.geometry_type, table_cols, 'geom')
        END,
        -- used fields: we need to cast the values taken from the managed table
        -- The end references are NULL for the points
        -- 9
        CASE
            WHEN lower(managed_object.geometry_type) = 'point'
            THEN 'mo.marker_code::integer, mo.abscissa::real, NULL::integer, NULL::real'
            ELSE 'mo.start_marker_code::integer, mo.start_abscissa::real, mo.end_marker_code::integer, mo.end_abscissa::real'
        END
    );
    RAISE NOTICE 'sql = %', sql_text;
    EXECUTE sql_text
    INTO updated_stats
    USING _impacts;

    RETURN to_jsonb(updated_stats);

END;
$_$;


-- update_table_references_from_geometries(text, text, text[], boolean)


-- FUNCTION update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_table_geometries_from_references(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb) IS 'Update the given table geometries based on the references. This function needs the table to be listed in the table road_graph.managed_objects.
The objects whose geometry cannot be calculated are not updated, and a warning is raised.
The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.';


-- FUNCTION: road_graph.update_managed_objects_on_graph_change
CREATE OR REPLACE FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb DEFAULT NULL::jsonb) RETURNS integer
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
    -- Par contre on ne calcule pas les offset et side (pour respecter cette donnée)

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
        END,
        _impacts
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
            _road_codes,
            _impacts
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
    IF updated_stats IS NULL OR Coalesce((updated_stats->>'nb')::integer, 0) = 0 THEN
        RAISE NOTICE 'No object updated for %.%', _schema_name, _table_name;
        updated_stats = jsonb_build_object(
            'nb', 0,
            'last_updated_objects_ids', '[]'::jsonb
        );
    END IF;
    -- Update the managed_objects table
    -- The list of the updated objects is emptied if no object has been updated:
    -- it must only contain the objects updated by the last change of the road graph
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
            FROM jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(updated_stats->'last_updated_objects_ids') = 'array'
                        THEN updated_stats->'last_updated_objects_ids'
                    ELSE '[]'
                END
            ) AS j
        )
    );

    -- Return the number of updated features, or NULL if no object has been updated
    RETURN Nullif((updated_stats->>'nb')::integer, 0);

END;
$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb)
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[], _impacts jsonb) IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table.
The parameter _impacts, returned by the function get_editing_session_impacts, allows to update only the objects
which may be impacted by the modifications of an editing session. All the objects of the given roads are updated if it is NULL.';


-- FUNCTION: road_graph.merge_editing_session_data
CREATE OR REPLACE FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'road_graph', 'public'
    AS $$
DECLARE
    editing_session_record record;
    sql_record record;
    road_codes text[];
    impacts jsonb;
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

    -- Get the impacts of the editing session on the roads, to update only the managed objects
    -- which may be impacted by the modifications.
    -- It must be calculated before the merge, since it compares the production and editing data.
    -- Only the roads whose edges geometries or markers have changed are impacted:
    -- the other edges have only been updated by the calculation of their references
    SELECT INTO impacts
        get_editing_session_impacts(_editing_session_id)
    ;
    SELECT INTO road_codes
        array_agg(i->>'road_code')
    FROM jsonb_array_elements(Coalesce(impacts, '[]'::jsonb)) AS i
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
                    road_codes,
                    impacts
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

    ELSE
        -- No object has been updated: empty the list of the objects updated by the previous merge
        UPDATE "road_graph".managed_objects
        SET (
            last_updated_objects_ids,
            last_update
        ) = (
            ARRAY[]::integer[],
            now()::timestamp(0)
        );

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


-- FUNCTION merge_editing_session_data(_editing_session_id integer)
COMMENT ON FUNCTION road_graph.merge_editing_session_data(_editing_session_id integer) IS 'Copy data from the given editing session into the road_graph schema.';


-- Calculate again the references of all the edges, road by road with the road cache,
-- to correct the cumulative distances greater than 10 km rounded to 10 cm.
-- Only for the production schema: this migration is also run for the schema road_graph_editing,
-- in which the schema name followed by a dot is replaced, and whose edges are logged by the editing sessions.
DO $$
DECLARE
    road record;
BEGIN
    IF 'road_graph.' != concat('road_graph', '.') THEN
        RETURN;
    END IF;
    FOR road IN
        SELECT DISTINCT e.road_code
        FROM road_graph.edges AS e
        WHERE e.road_code IS NOT NULL
        ORDER BY e.road_code
    LOOP
        PERFORM road_graph.update_edge_references(road.road_code);
    END LOOP;
END;
$$;
