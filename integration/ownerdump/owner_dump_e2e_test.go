//go:build integration

package ownerdump_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver the scratch database connects through

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// The two dumps below are what `pg_dump --schema-only` writes, taken from a
// PostgreSQL 18.6 server with pg_dump 18.6 from the same image
// (postgres:18, Debian build 18.6-1.pgdg13+2), as the superuser `postgres`.
// Only the comment lines and blank lines are left out. Each `ALTER ... OWNER TO`
// line is pg_dump's, and `--no-owner` is the only option that leaves it out.

// partitionedEventsBuilt is the issue's partitioned table.
const partitionedEventsBuilt = `
CREATE TABLE differs_events (id bigint NOT NULL, at date NOT NULL, note text DEFAULT 'x', PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
CREATE TABLE differs_events_2026 PARTITION OF differs_events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE differs_events_rest PARTITION OF differs_events DEFAULT;
CREATE INDEX differs_events_note ON differs_events (note);
`

// partitionedEventsDump is `pg_dump --schema-only` of partitionedEventsBuilt.
const partitionedEventsDump = `\restrict 48BHLT0fXMKROXxvUfEVZ8nGLaymfpNAOp9R8fuqU0Jo4GAgxCGcWdV3ipB0Hn6
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
SET default_tablespace = '';
CREATE TABLE public.differs_events (
    id bigint NOT NULL,
    at date NOT NULL,
    note text DEFAULT 'x'::text
)
PARTITION BY RANGE (at);
ALTER TABLE public.differs_events OWNER TO postgres;
SET default_table_access_method = heap;
CREATE TABLE public.differs_events_2026 (
    id bigint CONSTRAINT differs_events_id_not_null NOT NULL,
    at date CONSTRAINT differs_events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
ALTER TABLE public.differs_events_2026 OWNER TO postgres;
CREATE TABLE public.differs_events_rest (
    id bigint CONSTRAINT differs_events_id_not_null NOT NULL,
    at date CONSTRAINT differs_events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
ALTER TABLE public.differs_events_rest OWNER TO postgres;
ALTER TABLE ONLY public.differs_events ATTACH PARTITION public.differs_events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE ONLY public.differs_events ATTACH PARTITION public.differs_events_rest DEFAULT;
ALTER TABLE ONLY public.differs_events
    ADD CONSTRAINT differs_events_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.differs_events_2026
    ADD CONSTRAINT differs_events_2026_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.differs_events_rest
    ADD CONSTRAINT differs_events_rest_pkey PRIMARY KEY (id, at);
CREATE INDEX differs_events_note ON ONLY public.differs_events USING btree (note);
CREATE INDEX differs_events_2026_note_idx ON public.differs_events_2026 USING btree (note);
CREATE INDEX differs_events_rest_note_idx ON public.differs_events_rest USING btree (note);
ALTER INDEX public.differs_events_note ATTACH PARTITION public.differs_events_2026_note_idx;
ALTER INDEX public.differs_events_pkey ATTACH PARTITION public.differs_events_2026_pkey;
ALTER INDEX public.differs_events_note ATTACH PARTITION public.differs_events_rest_note_idx;
ALTER INDEX public.differs_events_pkey ATTACH PARTITION public.differs_events_rest_pkey;
\unrestrict 48BHLT0fXMKROXxvUfEVZ8nGLaymfpNAOp9R8fuqU0Jo4GAgxCGcWdV3ipB0Hn6
`

// everyKindBuilt holds a schema, an enum type, two functions, a sequence, a
// table and a view.
const everyKindBuilt = `
CREATE SCHEMA app;
CREATE TYPE app.mood AS ENUM ('sad', 'ok', 'happy');
CREATE FUNCTION app.f() RETURNS integer LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION app.g(a integer, b text) RETURNS text LANGUAGE sql AS 'SELECT b';
CREATE SEQUENCE app.seq;
CREATE TABLE app.t (id integer PRIMARY KEY, feeling app.mood NOT NULL DEFAULT 'ok');
CREATE VIEW app.v AS SELECT id, feeling FROM app.t;
`

// everyKindDump is `pg_dump --schema-only` of everyKindBuilt.
const everyKindDump = `\restrict GYdPpiOFCALVfnRPRu11HBe89MkoUYkRv3KNIYAx7KgLQf48V3gbHVWGF1HgLV2
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
CREATE SCHEMA app;
ALTER SCHEMA app OWNER TO postgres;
CREATE TYPE app.mood AS ENUM (
    'sad',
    'ok',
    'happy'
);
ALTER TYPE app.mood OWNER TO postgres;
CREATE FUNCTION app.f() RETURNS integer
    LANGUAGE sql
    AS $$SELECT 1$$;
ALTER FUNCTION app.f() OWNER TO postgres;
CREATE FUNCTION app.g(a integer, b text) RETURNS text
    LANGUAGE sql
    AS $$SELECT b$$;
ALTER FUNCTION app.g(a integer, b text) OWNER TO postgres;
CREATE SEQUENCE app.seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE app.seq OWNER TO postgres;
SET default_tablespace = '';
SET default_table_access_method = heap;
CREATE TABLE app.t (
    id integer NOT NULL,
    feeling app.mood DEFAULT 'ok'::app.mood NOT NULL
);
ALTER TABLE app.t OWNER TO postgres;
CREATE VIEW app.v AS
 SELECT id,
    feeling
   FROM app.t;
ALTER VIEW app.v OWNER TO postgres;
ALTER TABLE ONLY app.t
    ADD CONSTRAINT t_pkey PRIMARY KEY (id);
\unrestrict GYdPpiOFCALVfnRPRu11HBe89MkoUYkRv3KNIYAx7KgLQf48V3gbHVWGF1HgLV2
`

// ownerLine matches one ownership statement of a dump, which pg_dump writes on
// one line. Removing the lines is what `--no-owner` does.
var ownerLine = regexp.MustCompile(`(?m)^ALTER [^\n]* OWNER TO [^\n]*;\n`)

// withoutOwners is the dump as `--no-owner` writes it.
func withoutOwners(dump string) string {
	return ownerLine.ReplaceAllString(dump, "")
}

var dumps = []struct {
	name    string
	built   string
	dump    string
	schemas string
	owners  int
}{
	{name: "the issue's partitioned table", built: partitionedEventsBuilt, dump: partitionedEventsDump, schemas: "public", owners: 3},
	{name: "a schema, a type, functions, a sequence, a table and a view", built: everyKindBuilt, dump: everyKindDump, schemas: "public,app", owners: 7},
}

// ptah runs the native binary's command tree with empty standard input.
func ptah(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// scratch is an empty database of its own.
type scratch struct {
	url  string
	conn *dbschema.DatabaseConnection
}

func newScratch(c *qt.C) scratch {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	c.Assert(err, qt.IsNil)
	name := "ptah_ownerdump_" + hex.EncodeToString(random)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
	})
	renamed, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(renamed)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return scratch{url: parsed.String(), conn: conn}
}

func (s scratch) exec(c *qt.C, statements string) {
	c.Helper()
	_, err := s.conn.ExecContext(c.Context(), statements)
	c.Assert(err, qt.IsNil)
}

func writeSchema(c *qt.C, source string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
	return path
}

// plan is the output of `schema plan --dry-run` of source against the scratch database.
func (s scratch) plan(c *qt.C, source, schemas string) string {
	c.Helper()
	out, err := ptah("schema", "plan", "--db-url", s.url, "--schema-file", writeSchema(c, source), "--dry-run", "--schemas", schemas)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return out
}

// drift is the output of `schema drift` of source against the scratch database.
func (s scratch) drift(c *qt.C, source, schemas string) string {
	c.Helper()
	out, err := ptah("schema", "drift", "--db-url", s.url, "--schema-file", writeSchema(c, source), "--schemas", schemas)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return out
}

// TestAPlainDumpValidatesE2E validates each dump with its ownership statements,
// which needs no database.
func TestAPlainDumpValidatesE2E(t *testing.T) {
	for _, test := range dumps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, err := ptah("schema", "validate", "--dialect", "postgres", "--schema-file", writeSchema(c, test.dump))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(strings.Count(test.dump, " OWNER TO "), qt.Equals, test.owners)
		})
	}
}

// TestAPlainDumpReadsSyncedE2E reads each dump against the database it
// describes. The plain dump and its `--no-owner` twin plan and drift alike: the
// database is synced under both, and the output is the same text.
func TestAPlainDumpReadsSyncedE2E(t *testing.T) {
	for _, test := range dumps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c)
			dev.exec(c, test.built)

			plainPlan := dev.plan(c, test.dump, test.schemas)
			twinPlan := dev.plan(c, withoutOwners(test.dump), test.schemas)
			plainDrift := dev.drift(c, test.dump, test.schemas)
			twinDrift := dev.drift(c, withoutOwners(test.dump), test.schemas)

			c.Assert(plainPlan, qt.Contains, "Schema is synced")
			c.Assert(plainPlan, qt.Equals, twinPlan)
			c.Assert(plainDrift, qt.Contains, "No schema drift detected")
			c.Assert(plainDrift, qt.Equals, twinDrift)
		})
	}
}

// TestAPlainDumpPlansNoOwnershipE2E plans each dump against an empty database.
// The statements are the twin's: no CREATE ROLE for the role that owns the
// objects, no GRANT, and no OWNER TO.
func TestAPlainDumpPlansNoOwnershipE2E(t *testing.T) {
	for _, test := range dumps {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c)

			plainPlan := dev.plan(c, test.dump, test.schemas)
			twinPlan := dev.plan(c, withoutOwners(test.dump), test.schemas)

			c.Assert(plainPlan, qt.Equals, twinPlan)
			c.Assert(plainPlan, qt.Contains, "CREATE TABLE")
			c.Assert(plainPlan, qt.Not(qt.Contains), "ROLE")
			c.Assert(plainPlan, qt.Not(qt.Contains), "GRANT")
			c.Assert(plainPlan, qt.Not(qt.Contains), "OWNER")
		})
	}
}
