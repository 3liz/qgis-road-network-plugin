-- Road graph checks
--
-- glossary_graph_check: list of the checks of the road graph. The rows are added by the migrations,
-- the administrator can modify the default options of each check and deactivate it.
-- check_graph: runs the checks of the road graph and returns one line per issue,
-- with a point locating it and an optional line showing it.
-- fix_graph_issue: fixes an issue with its fix action, only in the editing session.

-- glossary_graph_check
CREATE TABLE IF NOT EXISTS road_graph.glossary_graph_check (
    code text NOT NULL,
    title text NOT NULL,
    severity text NOT NULL,
    function_name text NOT NULL,
    fix_actions text[] DEFAULT '{}'::text[] NOT NULL,
    default_options jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    description text,
    CONSTRAINT glossary_graph_check_severity_values CHECK ((severity = ANY (ARRAY['error'::text, 'warning'::text])))
);

-- glossary_graph_check glossary_graph_check_pkey
ALTER TABLE ONLY road_graph.glossary_graph_check
    DROP CONSTRAINT IF EXISTS glossary_graph_check_pkey;
ALTER TABLE ONLY road_graph.glossary_graph_check
    ADD CONSTRAINT glossary_graph_check_pkey PRIMARY KEY (code);

-- glossary_graph_check
COMMENT ON TABLE road_graph.glossary_graph_check IS 'Checks of the road graph run by the function check_graph. The rows are added by the plugin migrations: only the columns default_options and is_active should be modified.';


-- glossary_graph_check.code
COMMENT ON COLUMN road_graph.glossary_graph_check.code IS 'Code of the check, e.g. G01. The checks are numbered in the recommended order of the fixes.';


-- glossary_graph_check.title
COMMENT ON COLUMN road_graph.glossary_graph_check.title IS 'Title of the check';


-- glossary_graph_check.severity
COMMENT ON COLUMN road_graph.glossary_graph_check.severity IS 'Severity of the issues found by the check: error or warning';


-- glossary_graph_check.function_name
COMMENT ON COLUMN road_graph.glossary_graph_check.function_name IS 'Name of the function running the check, in the same schema as the function check_graph';


-- glossary_graph_check.fix_actions
COMMENT ON COLUMN road_graph.glossary_graph_check.fix_actions IS 'Actions which can fix the issues found by the check, with the function fix_graph_issue';


-- glossary_graph_check.default_options
COMMENT ON COLUMN road_graph.glossary_graph_check.default_options IS 'Default options of the check, for example its tolerance. They can be modified by the administrator, and overridden by the options given to the function check_graph.';


-- glossary_graph_check.is_active
COMMENT ON COLUMN road_graph.glossary_graph_check.is_active IS 'If False, the check is not run by the function check_graph, unless it is explicitly asked';


-- glossary_graph_check.description
COMMENT ON COLUMN road_graph.glossary_graph_check.description IS 'Description of the check and of its fix actions';


-- check_graph(text[], text[], jsonb)
CREATE OR REPLACE FUNCTION road_graph.check_graph(_checks text[] DEFAULT NULL::text[], _road_codes text[] DEFAULT NULL::text[], _options jsonb DEFAULT NULL::jsonb) RETURNS TABLE(id integer, check_code text, check_title text, severity text, road_code text, edge_ids integer[], node_ids integer[], marker_ids integer[], details jsonb, fix_action text, fix_params jsonb, geom geometry, geom_detail geometry)
    LANGUAGE plpgsql
    SET search_path TO 'public'
    AS $_$
DECLARE
    unknown_checks text[];
    graph_check record;
    sql_text text;
BEGIN
    -- Check the given check codes
    -- The checks are always read from the production schema, where the administrator sets their options
    SELECT array_agg(c ORDER BY c)
    INTO unknown_checks
    FROM unnest(_checks) AS c
    WHERE c NOT IN (
        SELECT g.code
        FROM "road_graph".glossary_graph_check AS g
    );
    IF unknown_checks IS NOT NULL THEN
        RAISE EXCEPTION 'Unknown road graph checks: %', array_to_string(unknown_checks, ', ');
    END IF;

    -- Create the temporary table receiving the issues found by each check
    DROP TABLE IF EXISTS pg_temp.graph_issues;
    CREATE TEMPORARY TABLE graph_issues (
        check_code text,
        check_title text,
        severity text,
        road_code text,
        edge_ids integer[],
        node_ids integer[],
        marker_ids integer[],
        details jsonb,
        fix_action text,
        fix_params jsonb,
        geom geometry,
        geom_detail geometry
    ) ON COMMIT DROP;

    -- Run each requested check, or all the active checks if none is given
    FOR graph_check IN
        SELECT g.*
        FROM "road_graph".glossary_graph_check AS g
        WHERE (_checks IS NULL AND g.is_active) OR g.code = ANY(_checks)
        ORDER BY g.code
    LOOP
        -- The function of the check must exist
        -- The schema of the function is replaced when the function is installed in the editing schema
        IF to_regproc('road_graph.' || quote_ident(graph_check.function_name)) IS NULL THEN
            RAISE EXCEPTION 'The function % of the road graph check % does not exist',
                graph_check.function_name, graph_check.code;
        END IF;

        -- The check function gets the road codes, and its options:
        -- the default options of the check, overridden by the given options
        sql_text = format(
            'INSERT INTO pg_temp.graph_issues SELECT * FROM road_graph.%I($1, $2)',
            graph_check.function_name
        );
        EXECUTE sql_text
        USING
            _road_codes,
            graph_check.default_options || Coalesce(_options->graph_check.code, '{}'::jsonb);
    END LOOP;

    -- Return the issues with a unique id
    RETURN QUERY
    SELECT
        (row_number() OVER (ORDER BY i.check_code, i.road_code, i.edge_ids, i.node_ids))::integer AS id,
        i.*
    FROM pg_temp.graph_issues AS i
    ORDER BY 1;

    DROP TABLE IF EXISTS pg_temp.graph_issues;
END;
$_$;


-- FUNCTION check_graph(_checks text[], _road_codes text[], _options jsonb)
COMMENT ON FUNCTION road_graph.check_graph(_checks text[], _road_codes text[], _options jsonb) IS 'Run the checks of the road graph and return one line per issue found.

The checks are listed in the table glossary_graph_check of the production schema.
All the active checks are run if _checks is NULL, and all the roads are checked if _road_codes is NULL.
The parameter _options overrides the default options of the checks stored in this table,
with the check code as key: {"G02": {"tolerance": 2}}.

Each line gives the check code and title, the severity (error or warning), the road code,
the ids of the edges, nodes and markers concerned, some details, the action which can fix the issue
with its parameters (NULL if the issue must be fixed manually), a point locating the issue (geom)
and, if useful, a line showing it (geom_detail).

The function can be run on the production schema or on the editing schema: road_graph_editing.check_graph()
checks the data of the editing session. The issues must be fixed in an editing session, with the function fix_graph_issue.';


-- fix_graph_issue(text, jsonb)
CREATE OR REPLACE FUNCTION road_graph.fix_graph_issue(_fix_action text, _params jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    AS $_$
DECLARE
    schema_name text;
    sql_text text;
    result jsonb;
BEGIN
    -- The issues must be fixed in an editing session:
    -- the name of the schema of the edges table is replaced when the function is installed in the editing schema
    SELECT n.nspname
    INTO schema_name
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = 'road_graph.edges'::regclass
    ;
    IF schema_name = 'road_graph' THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', 'The road graph issues must be fixed in an editing session, with the function road_graph_editing.fix_graph_issue'
        );
    END IF;

    -- Check the action is known
    IF NOT EXISTS (
        SELECT 1
        FROM "road_graph".glossary_graph_check AS g
        WHERE _fix_action = ANY(g.fix_actions)
    ) THEN
        RETURN jsonb_build_object(
            'status', 'error',
            'message', format('Unknown action to fix the road graph: %L', _fix_action)
        );
    END IF;

    -- Run the function of the action, named fix_graph_ followed by the action name
    -- It returns a JSON object with the keys status (success, skipped or error) and message
    sql_text = format('SELECT road_graph.%I($1)', 'fix_graph_' || _fix_action);
    EXECUTE sql_text
    INTO result
    USING Coalesce(_params, '{}'::jsonb);

    RETURN result;
END;
$_$;


-- FUNCTION fix_graph_issue(_fix_action text, _params jsonb)
COMMENT ON FUNCTION road_graph.fix_graph_issue(_fix_action text, _params jsonb) IS 'Fix an issue of the road graph found by the function check_graph, with the given action and parameters.

The issues must be fixed in an editing session: the function only works in the editing schema,
with road_graph_editing.fix_graph_issue. It runs the function named fix_graph_ followed by the action name.

The issue may have been fixed by a previous action, for example when several issues concern the same edges:
the action then does nothing and returns the status skipped.

It returns a JSON object with the keys status (success, skipped or error) and message.';
