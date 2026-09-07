-- Add trigger for timestamp columns
CREATE OR REPLACE FUNCTION road_graph.aa_before_record_insert_or_update()
RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Trigger disabled by session variable
    IF road_graph.get_current_setting('road.graph.disable.trigger', '0') = '1'
    THEN
        RETURN NEW;
    END IF;

    -- Update values
    IF TG_OP = 'INSERT' THEN
        NEW.created_at = now()::timestamp(0) without time zone;
    END IF;
    NEW.updated_at = now()::timestamp(0) without time zone;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION road_graph.aa_before_record_insert_or_update()
IS 'Set the created_at and updated_at fields';

-- roads
DROP TRIGGER IF EXISTS trg_aa_before_record_insert_or_update ON road_graph.roads;
CREATE TRIGGER trg_aa_before_record_insert_or_update
BEFORE INSERT OR UPDATE ON road_graph.roads
FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_record_insert_or_update();
-- nodes
DROP TRIGGER IF EXISTS trg_aa_before_record_insert_or_update ON road_graph.nodes;
CREATE TRIGGER trg_aa_before_record_insert_or_update
BEFORE INSERT OR UPDATE ON road_graph.nodes
FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_record_insert_or_update();
-- edges
DROP TRIGGER IF EXISTS trg_aa_before_record_insert_or_update ON road_graph.edges;
CREATE TRIGGER trg_aa_before_record_insert_or_update
BEFORE INSERT OR UPDATE ON road_graph.edges
FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_record_insert_or_update();
-- markers
DROP TRIGGER IF EXISTS trg_aa_before_record_insert_or_update ON road_graph.markers;
CREATE TRIGGER trg_aa_before_record_insert_or_update
BEFORE INSERT OR UPDATE ON road_graph.markers
FOR EACH ROW EXECUTE PROCEDURE road_graph.aa_before_record_insert_or_update();


--- import function
-- FUNCTION: road_graph.import_data_from_template_tables(text, text, text)

DROP FUNCTION IF EXISTS road_graph.import_data_from_template_tables(text, text, text);
CREATE OR REPLACE FUNCTION road_graph.import_data_from_template_tables(
	_source_schema text,
	_source_edges_table text,
	_source_markers_table text)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    sql_text text;
    roads_count integer;
    edges_count integer;
    nodes_count integer;
    markers_zero_count integer;
    markers_count integer;
    _set_config text;
    _toggle_triggers boolean;
    _set_val_result integer;
    wrong_road_type record;
    wrong_road_class record;
BEGIN
    -- Default values
    roads_count = 0;
    edges_count = 0;
    nodes_count = 0;
    markers_zero_count = 0;
    markers_count = 0;

    -- Build temporary tables from source tables
    -- Temporary edges
    sql_text = format(
        $SQL$
        CREATE TEMPORARY TABLE temp_edges ON COMMIT DROP AS (
            WITH source AS (
                SELECT
                    -- Stores the original data
                    (row_number() OVER())::integer AS id,
                    e."edge_order",
                    trim(e."road_code") AS road_code,
                    trim(e."road_class") AS road_class,
                    trim(e."road_type") AS road_type,
                    trim(e."road_class_code") AS road_class_code,
                    e."is_active",
                    trim(e."road_topic") AS road_topic,
                    e."is_access_road",
                    -- Move the geometries nodes to a grid
                    ST_ReducePrecision(ST_geometryN(e.geom, 1), 0.10) AS geom
                FROM %I.%I AS e
            )
            SELECT
                s.*,
                -- get the start point
                ST_StartPoint(s.geom) AS start_point,
                -- get the end point
                ST_EndPoint(s.geom) AS end_point,
                -- previous and next edges
                lag(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS previous_edge_id,
                lead(s.id) OVER(PARTITION BY s.road_code ORDER BY s.edge_order) AS next_edge_id
            FROM source AS s
            WHERE TRUE
            ORDER BY road_code, edge_order
        )
        ;
        $SQL$,
        _source_schema,
        _source_edges_table
    );
    -- RAISE NOTICE 'sql_text = %', sql_text;
    EXECUTE sql_text;

    CREATE INDEX ON temp_edges (road_code);
    CREATE INDEX ON temp_edges USING GIST (start_point);
    CREATE INDEX ON temp_edges USING GIST (end_point);

    -- Check unique values of some fields
    -- road_type
    SELECT INTO wrong_road_type
        count(*) AS nb,
        string_agg(DISTINCT e.road_type, ', ' ORDER BY e.road_type) AS road_types
    FROM temp_edges AS e
    WHERE e.road_type NOT IN ('road', 'roundabout')
    ;
    IF wrong_road_type.nb > 0 THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'Some values of the field "road_type" are not road or roundabout',
            'data', json_build_object(
                'number', wrong_road_type.nb,
                'details', wrong_road_type.road_types
            ),
            'roads_count', roads_count,
            'edges_count', edges_count,
            'nodes_count', nodes_count,
            'markers_count', markers_zero_count + markers_count
        );
    END IF;

    -- road_class
    SELECT INTO wrong_road_class
        count(*) AS nb,
        string_agg(DISTINCT e.road_class, ', ' ORDER BY e.road_class) AS road_classes
    FROM temp_edges AS e
    WHERE e.road_class NOT IN (
        SELECT "code" FROM road_graph.glossary_road_class
    )
    ;
    IF wrong_road_class.nb > 0 THEN
        RETURN json_build_object(
            'status', 'error',
            'message', 'Some values of the field "road_class" are not present in the table "glossary_road_class"',
            'data', json_build_object(
                'number', wrong_road_class.nb,
                'details', wrong_road_class.road_classes
            ),
            'roads_count', roads_count,
            'edges_count', edges_count,
            'nodes_count', nodes_count,
            'markers_count', markers_zero_count + markers_count
        );
    END IF;

    -- Temporary nodes
    CREATE TEMPORARY TABLE temp_nodes ON COMMIT DROP AS (
        WITH
        union_start_end AS (
            SELECT id AS start_of, NULL AS end_of, start_point AS geom
            FROM temp_edges
            UNION ALL
            SELECT NULL AS start_of, id AS end_of, end_point AS geom
            FROM temp_edges
        ),
        distinct_nodes AS (
            SELECT
                json_agg(DISTINCT start_of) FILTER (WHERE start_of IS NOT NULL) AS start_of,
                json_agg(DISTINCT end_of) FILTER (WHERE end_of IS NOT NULL) AS end_of,
                geom
            FROM union_start_end
            GROUP BY geom
        )
        SELECT *
        FROM distinct_nodes
    )
    ;
    CREATE INDEX ON temp_nodes USING GIST (geom);

    -- Disable topology triggers
    SELECT set_config('road.graph.disable.trigger', '1'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(FALSE)
        INTO _toggle_triggers;

    -- Insert nodes
    RAISE NOTICE 'insert nodes';
    INSERT INTO road_graph.nodes AS n
    (created_at, updated_at, geom)
    SELECT
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone,
        geom
    FROM temp_nodes
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS nodes_count = ROW_COUNT;

    -- Insert roads
    RAISE NOTICE 'insert roads';
    INSERT INTO road_graph.roads (
        road_code, road_type, road_class,
        road_class_code, is_active, road_topic,
        is_access_road,
        created_at, updated_at
    )
    SELECT DISTINCT
        road_code,
        road_type,
        road_class,
        road_class_code,
        is_active,
        road_topic,
        is_access_road,
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone
    FROM temp_edges
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS roads_count = ROW_COUNT;

    -- Insert edges
    RAISE NOTICE 'insert edges';
    INSERT INTO road_graph.edges (
        id,
        road_code, start_node, end_node,
        previous_edge_id, next_edge_id,
        geom,
        created_at, updated_at
    )
    SELECT
        e.id, e.road_code,
        ns.id, ne.id,
        e.previous_edge_id, e.next_edge_id,
        e.geom,
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone
    FROM temp_edges AS e
    LEFT JOIN road_graph.nodes AS ns
        -- = is faster than ST_Equals
        ON ns.geom = e.start_point
    LEFT JOIN road_graph.nodes AS ne
        ON ne.geom = e.end_point
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS edges_count = ROW_COUNT;

    DROP TABLE IF EXISTS temp_nodes;
    DROP TABLE IF EXISTS temp_edges;

    -- Insert markers automatic markers at the start of the first edge
    RAISE NOTICE 'insert markrs zero';
    INSERT INTO road_graph.markers (
        road_code, code, abscissa, is_virtual, geom,
        created_at, updated_at
    )
    SELECT
        e.road_code,
        0,
        0,
        True,
        ST_StartPoint(e.geom),
        now()::timestamp(0) without time zone,
        now()::timestamp(0) without time zone
    FROM road_graph.edges AS e
    WHERE TRUE
    AND e.previous_edge_id IS NULL
    ON CONFLICT DO NOTHING
    ;
    GET DIAGNOSTICS markers_zero_count = ROW_COUNT;

    -- Insert markers from source data
    RAISE NOTICE 'insert markers from source';
    sql_text = format(
        $SQL$
        INSERT INTO road_graph.markers (
            road_code, code, abscissa, is_virtual, geom,
            created_at, updated_at
        )
        SELECT
            road_code,
            code,
            abscissa,
            is_virtual,
            geom,
            now()::timestamp(0) without time zone,
            now()::timestamp(0) without time zone
        FROM %I.%I
        ON CONFLICT DO NOTHING
        ;
        $SQL$,
        _source_schema,
        _source_markers_table
    );
    EXECUTE sql_text;
    GET DIAGNOSTICS markers_count = ROW_COUNT;

    -- Set the serial values
    RAISE NOTICE 'set serials';
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.edges', 'id'),
        (SELECT max(id) FROM road_graph.edges)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.nodes', 'id'),
        (SELECT max(id) FROM road_graph.nodes)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.roads', 'id'),
        (SELECT max(id) FROM road_graph.roads)
    );
    SELECT INTO _set_val_result
    setval(
        pg_get_serial_sequence('road_graph.markers', 'id'),
        (SELECT max(id) FROM road_graph.markers)
    );

    -- Update edges references
    RAISE NOTICE 'update references';
    WITH s AS (
        SELECT e.road_code, array_agg(DISTINCT e.id) AS ids
        FROM road_graph.edges AS e
        WHERE start_marker IS NULL OR end_marker IS NULL
        GROUP BY road_code
    ),
    up AS (
        SELECT
            road_code,
            road_graph.update_edge_references(s.road_code, s.ids) AS travail
        FROM s
        ORDER BY road_code
    )
    SELECT count(*)
    FROM up
    INTO _set_val_result
    ;

    -- Re-enable triggers
    RAISE NOTICE 'triggers and config';
    SELECT set_config('road.graph.disable.trigger', '0'::text, true)
        INTO _set_config;
    SELECT road_graph.toggle_foreign_key_constraints(TRUE)
        INTO _toggle_triggers;

    RETURN json_build_object(
        'status', 'success',
        'message', 'ok',
        'data', null::json,
        'roads_count', roads_count,
        'edges_count', edges_count,
        'nodes_count', nodes_count,
        'markers_count', markers_zero_count + markers_count
    );
END;
$BODY$;

COMMENT ON FUNCTION road_graph.import_data_from_template_tables(text, text, text)
    IS 'Import data from the given schema and edges & markers source tables';


-- Add trigger for timestamp columns
CREATE OR REPLACE FUNCTION road_graph.get_merged_geom_from_table_and_ids(
    _schema_name text,
    _table_name text,
    _ids integer[]
)
RETURNS geometry(MultiPolygon, 2154)
    LANGUAGE plpgsql
    AS $$
DECLARE
    primary_key_field text;
    geometry_column text;
    sql_text text;
    merged_geom geometry(MultiPolygon, 2154);
BEGIN
    -- Return NULL if no ids are passed
    IF _ids IS NULL THEN
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

    -- Get geometry colmun name
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

    -- Get multi geometry of all given features ids
    sql_text = format(
        $SQL$
        SELECT
            ST_Multi(ST_Buffer(ST_Union(%1$I), 10)) AS geom
            FROM %2$I.%3$I
            WHERE %4$I IN (%5$s)
        $SQL$,
        geometry_column,
        _schema_name,
        _table_name,
        primary_key_field,
        array_to_string(_ids::text[], ', ')
    );
    EXECUTE sql_text
    INTO merged_geom
    ;

    RETURN merged_geom;
END;
$$;

COMMENT ON FUNCTION road_graph.get_merged_geom_from_table_and_ids(text, text, integer[])
IS 'Compute the union geometry of all the features from a table and given feature ids';


CREATE OR REPLACE VIEW road_graph.v_managed_objects AS
SELECT
    m.*,
    road_graph.get_merged_geom_from_table_and_ids(
        schema_name,
        table_name,
        last_updated_objects_ids
    ) AS geom
FROM road_graph.managed_objects AS m
;
COMMENT ON VIEW road_graph.v_managed_objects
IS 'View allowing to see the merged geometries of each managed table for the last editing session merge. Useful to have a quick look at the edited objects.'
;
