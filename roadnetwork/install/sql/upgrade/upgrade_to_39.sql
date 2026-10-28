-- Managed objects
--
-- * add_managed_object_reference_fields: new function adding the reference fields
--   needed by the managed objects functions to a table, depending on its geometry type
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION and COMMENT.


-- FUNCTION: road_graph.add_managed_object_reference_fields
CREATE OR REPLACE FUNCTION road_graph.add_managed_object_reference_fields(_schema_name text, _table_name text, _add_optional_fields boolean DEFAULT true) RETURNS TABLE(field_name text, field_type text, field_status text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    table_regclass regclass;
    _geometry_type text;
    field record;
    existing_type text;
BEGIN
    -- Check the table exists
    table_regclass = to_regclass(format('%I.%I', _schema_name, _table_name));
    IF table_regclass IS NULL THEN
        field_status = 'table not found';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Get the geometry type from the managed objects table if the table is registered,
    -- or else from the geometry column geom
    SELECT INTO _geometry_type
        lower(o.geometry_type)
    FROM road_graph.managed_objects AS o
    WHERE o.schema_name = _schema_name
    AND o.table_name = _table_name
    LIMIT 1
    ;
    IF _geometry_type IS NULL THEN
        SELECT INTO _geometry_type
            lower(g.type)
        FROM geometry_columns AS g
        WHERE g.f_table_schema = _schema_name
        AND g.f_table_name = _table_name
        AND g.f_geometry_column = 'geom'
        LIMIT 1
        ;
    END IF;
    IF _geometry_type IS NULL THEN
        field_status = 'geom column not found';
        RETURN NEXT;
        RETURN;
    END IF;
    IF _geometry_type NOT IN ('point', 'linestring', 'multilinestring') THEN
        field_status = format('geometry type not supported: %s', _geometry_type);
        RETURN NEXT;
        RETURN;
    END IF;

    -- Add the reference fields depending on the geometry type
    FOR field IN
        SELECT f.name, f.type
        FROM (
            VALUES
            -- name, type, geometry type, required, position
            ('road_code', 'text', 'all', True, 1),
            ('marker_code', 'integer', 'point', True, 2),
            ('abscissa', 'real', 'point', True, 3),
            ('cumulative', 'real', 'point', False, 4),
            ('start_marker_code', 'integer', 'line', True, 5),
            ('start_abscissa', 'real', 'line', True, 6),
            ('start_cumulative', 'real', 'line', False, 7),
            ('end_marker_code', 'integer', 'line', True, 8),
            ('end_abscissa', 'real', 'line', True, 9),
            ('end_cumulative', 'real', 'line', False, 10),
            ('offset', 'real', 'all', False, 11),
            ('side', 'text', 'all', False, 12)
        ) AS f(name, type, geometry, required, position)
        WHERE f.geometry IN ('all', CASE WHEN _geometry_type = 'point' THEN 'point' ELSE 'line' END)
        AND (f.required OR _add_optional_fields)
        ORDER BY f.position
    LOOP
        field_name = field.name;
        field_type = field.type;

        -- Check if the field already exists
        SELECT INTO existing_type
            format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute AS a
        WHERE a.attrelid = table_regclass
        AND a.attname = field.name
        AND a.attnum > 0
        AND NOT a.attisdropped
        ;

        IF existing_type IS NULL THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ADD COLUMN %I %s',
                _schema_name, _table_name, field.name, field.type
            );
            field_status = 'added';
        ELSIF existing_type = field.type THEN
            field_status = 'already exists';
        ELSE
            -- Do not modify an existing field with another type
            field_status = format('already exists with the type %s', existing_type);
        END IF;
        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$_$;

COMMENT ON FUNCTION road_graph.add_managed_object_reference_fields(_schema_name text, _table_name text, _add_optional_fields boolean) IS 'Add the reference fields needed by the managed objects functions to the given table, depending on its geometry type.
The geometry type is read from the table road_graph.managed_objects if the table is registered, or else from its geometry column geom.
* all the geometry types: road_code (text), and the optional fields offset (real) and side (text),
* point: marker_code (integer), abscissa (real), and the optional field cumulative (real),
* linestring and multilinestring: start_marker_code, end_marker_code (integer), start_abscissa, end_abscissa (real),
  and the optional fields start_cumulative, end_cumulative (real).
The optional fields are added only if the parameter _add_optional_fields is True (default).
The existing fields are not modified. The function returns one line per field with its name, type and status:
added, already exists, or already exists with the type of the existing field.
If the fields cannot be added, it returns one line with the status: table not found, geom column not found,
or geometry type not supported.';
