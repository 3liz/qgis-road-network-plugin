--
-- PostgreSQL database dump
--






SET statement_timeout = 0;
SET lock_timeout = 0;


SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- glossary_test_category
COMMENT ON TABLE road_graph.glossary_test_category IS 'Glossary for the column category of the table test';


-- metadata
COMMENT ON TABLE road_graph.metadata IS 'Metadata of the structure : version and date. Useful for database structure and glossary data migrations between versions';


-- test
COMMENT ON TABLE road_graph.test IS 'Test table';


--
-- PostgreSQL database dump complete
--



