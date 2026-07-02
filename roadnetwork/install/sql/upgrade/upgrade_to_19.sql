
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
            FROM jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(updated_stats->'last_updated_objects_ids') = 'array'
                        THEN updated_stats->'last_updated_objects_ids'
                    ELSE '[]'
                END
            ) AS j
        )
    );

    -- Return the number of updated features
    RETURN (updated_stats->>'nb')::integer;

END;
$_$;


-- FUNCTION update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[])
COMMENT ON FUNCTION road_graph.update_managed_objects_on_graph_change(_schema_name text, _table_name text, _road_codes text[]) IS 'Updates managed objects geometries or references when the road graph changes.
It uses the information stored in the road_graph.managed_objects table';
