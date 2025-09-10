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

SET default_tablespace = '';


-- glossary_test_category glossary_test_category_pkey
ALTER TABLE ONLY road_graph.glossary_test_category
    ADD CONSTRAINT glossary_test_category_pkey PRIMARY KEY (id);


-- test test_pkey
ALTER TABLE ONLY road_graph.test
    ADD CONSTRAINT test_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--
