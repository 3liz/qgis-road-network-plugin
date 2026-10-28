-- Editing sessions
--
-- * before_editing_sessions_update: the geometry of an editing session which status is not created
--   could still be modified in two cases:
--   * if the status was set back to created in the same query (the new status was tested),
--   * if the geometry was set to NULL (ST_Equals returns NULL).
--
-- This migration is idempotent: it only contains CREATE OR REPLACE FUNCTION and COMMENT.


-- FUNCTION: road_graph.before_editing_sessions_update
CREATE OR REPLACE FUNCTION road_graph.before_editing_sessions_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN

    -- Geometry must not be updated if the status is not created
    -- Test the status before the update: it could be set back to created in the same query.
    -- ST_Equals returns NULL if one of the geometries is NULL: test them before
    IF OLD."status" != 'created'
        AND (
            (OLD.geom IS NULL) != (NEW.geom IS NULL)
            OR NOT ST_Equals(OLD.geom, NEW.geom)
        )
    THEN
        RAISE EXCEPTION 'The geometry of the editing session cannot be updated after data has been cloned or edited !';
    END IF;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION road_graph.before_editing_sessions_update() IS 'Prevent from updating an editing session geometry if its status is not created, which means that data has been cloned into the road_graph_editing schema';
