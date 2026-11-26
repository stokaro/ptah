-- The output of `pg_dump --schema-only --no-owner --no-privileges` 18.6, run
-- in the postgres:18 image on a database these statements built:
--
--   CREATE EXTENSION citext;
--   CREATE SCHEMA reports;
--   COMMENT ON SCHEMA reports IS 'Dumped reports schema';
--   CREATE TABLE reports.daily (day date PRIMARY KEY, total integer NOT NULL DEFAULT 0,
--     label public.citext, note text DEFAULT 'it''s a \ backslash');
--
-- pg_dump 19.0 writes the same text but for its version lines. The \restrict
-- and \unrestrict lines pg_dump writes first and last are psql commands, which
-- the server refuses as SQL, so they are removed. Everything else is as
-- pg_dump wrote it.

--
-- PostgreSQL database dump
--


-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6 (Debian 18.6-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
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
    note text DEFAULT 'it''s a \ backslash'::text
);


--
-- Name: daily daily_pkey; Type: CONSTRAINT; Schema: reports; Owner: -
--

ALTER TABLE ONLY reports.daily
    ADD CONSTRAINT daily_pkey PRIMARY KEY (day);


--
-- PostgreSQL database dump complete
--


