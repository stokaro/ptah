-- The output of `ysql_dump --schema-only --no-owner --no-privileges`, run in
-- the yugabytedb/yugabyte:2026.1.2.0-b137 image on a database these
-- statements built:
--
--   CREATE EXTENSION citext;
--   CREATE SCHEMA reports;
--   COMMENT ON SCHEMA reports IS 'Dumped reports schema';
--   CREATE TABLE reports.daily (day date PRIMARY KEY, total integer NOT NULL DEFAULT 0,
--     label public.citext, note text DEFAULT 'it''s a \ backslash');
--
-- The \restrict and \unrestrict lines ysql_dump writes first and last are psql
-- commands, which the server refuses as SQL, so they are removed. Everything
-- else is as ysql_dump wrote it.

--
-- YSQL database dump
--


-- Dumped from database version 15.12-YB-2026.1.2.0-b0
-- Dumped by ysql_dump version 15.12-YB-2026.1.2.0-b0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: reports; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA reports;


--
-- Name: SCHEMA reports; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA reports IS 'Dumped reports schema';


--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: daily; Type: TABLE; Schema: reports; Owner: -
--

CREATE TABLE reports.daily (
    day date NOT NULL,
    total integer DEFAULT 0 NOT NULL,
    label public.citext,
    note text DEFAULT 'it''s a \ backslash'::text,
    CONSTRAINT daily_pkey PRIMARY KEY((day) HASH)
);


--
-- YSQL database dump complete
--


