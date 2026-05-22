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

-- v_road_without_zero_marker
CREATE VIEW road_graph.v_road_without_zero_marker AS
 SELECT DISTINCT e.road_code
   FROM (road_graph.edges e
     LEFT JOIN road_graph.markers m ON (((m.road_code = e.road_code) AND (m.code = 0))))
  WHERE (m.id IS NULL);


--
-- PostgreSQL database dump complete
--



