-- DROP FUNCTION IF EXISTS public.lizmap_get_data(json);

CREATE OR REPLACE FUNCTION public.lizmap_get_data(
	parameters json)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    IMMUTABLE STRICT PARALLEL UNSAFE
AS $BODY$
DECLARE
    feature_id varchar;
    layer_name text;
    layer_table text;
    layer_schema text;
    action_name text;
    wkt text;
    sqltext text;
    datasource text;
    ajson json;
BEGIN

    action_name:= parameters->>'action_name';
    feature_id:= (parameters->>'feature_id')::varchar;
    layer_name:= parameters->>'layer_name';
    layer_schema:= parameters->>'layer_schema';
    layer_table:= parameters->>'layer_table';
    wkt:= parameters->>'wkt';

    -- Action get_references_from_point
    -- Récupère les PR+abscisses et autres infos à partir d'un point
    IF action_name = 'get_references_from_point' THEN
        datasource:= format(
            $SQL$
                WITH
                get_geom AS(
                    SELECT
                        ST_Transform(ST_SetSRID(ST_GeomFromText(%1$L), 4326), 2154) AS geom,
                        %2$L::integer AS vertex_number
                ),
                get_refs AS (
                    SELECT
                        road_graph.get_reference_from_point(
                            ST_Force2D(g.geom),
                            nullif(trim(%3$L), '')
                        ) AS refs
                    FROM get_geom AS g
                )
                SELECT
                    1::integer AS id,
                    '' AS "message",
                    refs->>'road_code'::text AS road_code,
                    (refs->>'marker_code')::smallint AS marker_code,
                    (refs->>'abscissa')::real AS abscissa,
                    (refs->>'cumulative')::real AS cumulative,
                    (refs->>'offset')::real AS "offset",
                    refs->>'side'::text AS side,
                    g.vertex_number,
                    g.geom
                FROM
                    get_refs AS r,
                    get_geom AS g
            $SQL$,
            wkt,
            (parameters->>'vertex_number')::integer,
            parameters->>'road_code'
        );
    ELSEIF action_name = 'get_road_point_from_reference' THEN
        -- Build SQL
        datasource:= format(
            $SQL$
                WITH
                get_refs AS (
                    SELECT
                        road_graph.get_road_point_from_reference(
                            %1$L, %2$L::integer, %3$L::real, %4$L::real, %5$L
                        ) AS refs
                )
                SELECT
                    1::integer AS id,
                    '' AS "message",
                    refs->>'road_code'::text AS road_code,
                    (refs->>'marker_code')::smallint AS marker_code,
                    (refs->>'abscissa')::real AS abscissa,
                    (refs->>'offset')::real AS "offset",
                    (refs->>'side')::text AS side,
                    ST_Transform((refs->>'geom')::geometry(POINT, 2154), 2154) AS geom
                FROM
                    get_refs AS r
            $SQL$,
            -- road_code
            parameters->>'road_code',
            -- marker code
            (parameters->>'marker_code')::real::integer,
            -- abscissa
            (parameters->>'abscissa')::real,
            -- offset
            (parameters->>'offset')::real,
            -- side
            CASE
                WHEN (parameters->>'side')::real::integer = 0 THEN 'left'
                ELSE 'right'
            END
        );

    ELSEIF action_name = 'get_edge_start_or_end_tip_from_references' THEN
        -- Get the point geometry of the given references
        -- then the edge under this point
        -- then the references of this edge start or end point
        IF (parameters->>'start_or_end') = 'start' THEN
            datasource:= format(
                $SQL$
                WITH get_refs AS (
                    SELECT e.start_marker, e.start_abscissa, ST_StartPoint(e.geom) AS geom
                    FROM road_graph.edges AS e
                    WHERE e.road_code = %1$L
                    AND e.start_marker::int * 10000 + e.start_abscissa <= %2$L::integer * 10000 + %3$L::real
                    ORDER BY e.start_cumulative DESC
                    LIMIT 1
                )
                SELECT
                    1::integer AS id,
                    '' AS "message",
                    g.start_marker AS marker_code,
                    g.start_abscissa AS abscissa,
                    'start' AS start_or_end,
                    g.geom::geometry(point, 2154) AS geom
                FROM get_refs AS g
                $SQL$,
                -- road_code
                parameters->>'road_code',
                -- marker code
                (parameters->>'marker_code')::real::integer,
                -- abscissa
                (parameters->>'abscissa')::real
            );
        ELSE
            datasource:= format(
                $SQL$
                WITH get_refs AS (
                    SELECT e.end_marker, e.end_abscissa, ST_EndPoint(e.geom) AS geom
                    FROM road_graph.edges AS e
                    WHERE e.road_code = %1$L
                    AND e.end_marker * 10000 + e.end_abscissa >=  %2$L::integer * 10000 + %3$L::real
                    ORDER BY e.end_cumulative ASC
                    LIMIT 1
                )
                SELECT
                    1::integer AS id,
                    '' AS "message",
                    g.end_marker AS marker_code,
                    g.end_abscissa AS abscissa,
                    'end' AS start_or_end,
                    g.geom::geometry(point, 2154) AS geom
                FROM get_refs AS g
                $SQL$,
                -- road_code
                parameters->>'road_code',
                -- marker code
                (parameters->>'marker_code')::real::integer,
                -- abscissa
                (parameters->>'abscissa')::real
            );
        END IF;

    ELSEIF action_name = 'get_road_substring_from_references' THEN
        -- Build SQL
        datasource:= format(
            $SQL$
                WITH
                get_refs AS (
                    SELECT
                        road_graph.get_road_substring_from_references(
                            %1$L,
                            %2$L::integer, %3$L::real,
                            %4$L::integer, %5$L::real,
                            %6$L::real, %7$L
                        ) AS refs
                )
                SELECT
                    1::integer AS id,
                    '' AS "message",
                    refs->>'road_code'::text AS road_code,
                    (refs->>'start_marker_code')::smallint AS start_marker_code,
                    (refs->>'start_abscissa')::real AS start_abscissa,
                    (refs->>'end_marker_code')::smallint AS end_marker_code,
                    (refs->>'end_abscissa')::real AS end_abscissa,
                    (refs->>'offset')::real AS "offset",
                    (refs->>'side')::text AS side,
                    ST_Transform((refs->>'geom')::geometry(MultiLinestring, 2154), 2154) AS geom
                FROM
                    get_refs AS r
            $SQL$,
            -- road_code : get it from roads table
            parameters->>'road_code',
            -- start marker code
            (parameters->>'start_marker_code')::real::integer,
            -- start abscissa
            (parameters->>'start_abscissa')::real,
            -- end marker code
            (parameters->>'end_marker_code')::real::integer,
            -- end abscissa
            (parameters->>'end_abscissa')::real,
            -- offset
            (parameters->>'offset')::real,
            -- side
            CASE
                WHEN (parameters->>'side')::real::integer = 0 THEN 'left'
                ELSE 'right'
            END
        );
        RAISE NOTICE 'Action get_road_substring_from_references SQL = %', datasource;
    ELSE
    -- Default : return geometry
        datasource:= format('
            SELECT
            %1$L::integer AS id,
            ''The geometry of the object have been displayed in the map'' AS message
            geom
            FROM %2$I.%3$I
            WHERE id = %1$L::integer
        ',
        feature_id,
        layer_schema,
        layer_table
        );

    END IF;

    SELECT query_to_geojson(datasource)
    INTO ajson
    ;
    RETURN ajson;
END;
$BODY$;

COMMENT ON FUNCTION public.lizmap_get_data(json)
    IS 'Generate a valid GeoJSON from an action described by a name, PostgreSQL schema and table name of the source data, a QGIS layer name, a feature id and additional options.';
