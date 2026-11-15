//go:build integration

package ownerdump_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// quotedKeysBuilt names a primary key in each spelling a name takes: the name
// the server gives the key of a quoted table, a quoted name on the table and
// on the column, and an unquoted name in mixed case, which the server folds. A
// foreign key references one of the keys.
const quotedKeysBuilt = `
CREATE TABLE "Order" ("Id" bigint PRIMARY KEY, "Note" text);
CREATE TABLE "Line" ("Id" bigint NOT NULL, "OrderId" bigint REFERENCES "Order" ("Id"), CONSTRAINT "Line_Key" PRIMARY KEY ("Id"));
CREATE TABLE "Col" ("Id" bigint CONSTRAINT "Col_PK" PRIMARY KEY);
CREATE TABLE plain_mixed (id integer, CONSTRAINT Plain_Mixed_PK PRIMARY KEY (id));
`

// quotedKeysDump is `pg_dump --schema-only` of quotedKeysBuilt, taken as the
// other dumps of this package are: PostgreSQL 18.6 and its pg_dump 18.6, with
// the comment lines and blank lines left out. pg_dump 16.16 and 17.11 write
// the same statements for it; 16.16 leaves out `SET transaction_timeout`.
const quotedKeysDump = `\restrict HzwvcBulLrTU5q2FyjclIg2qeRMWJBuPaM4zwt9LqY6peGBi8ZB1riZK5ePMwA4
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
SET default_table_access_method = heap;
CREATE TABLE public."Col" (
    "Id" bigint NOT NULL
);
ALTER TABLE public."Col" OWNER TO postgres;
CREATE TABLE public."Line" (
    "Id" bigint NOT NULL,
    "OrderId" bigint
);
ALTER TABLE public."Line" OWNER TO postgres;
CREATE TABLE public."Order" (
    "Id" bigint NOT NULL,
    "Note" text
);
ALTER TABLE public."Order" OWNER TO postgres;
CREATE TABLE public.plain_mixed (
    id integer NOT NULL
);
ALTER TABLE public.plain_mixed OWNER TO postgres;
ALTER TABLE ONLY public."Col"
    ADD CONSTRAINT "Col_PK" PRIMARY KEY ("Id");
ALTER TABLE ONLY public."Line"
    ADD CONSTRAINT "Line_Key" PRIMARY KEY ("Id");
ALTER TABLE ONLY public."Order"
    ADD CONSTRAINT "Order_pkey" PRIMARY KEY ("Id");
ALTER TABLE ONLY public.plain_mixed
    ADD CONSTRAINT plain_mixed_pk PRIMARY KEY (id);
ALTER TABLE ONLY public."Line"
    ADD CONSTRAINT "Line_OrderId_fkey" FOREIGN KEY ("OrderId") REFERENCES public."Order"("Id");
\unrestrict HzwvcBulLrTU5q2FyjclIg2qeRMWJBuPaM4zwt9LqY6peGBi8ZB1riZK5ePMwA4
`

// catalogPrimaryKeys is each primary key the database holds, by its table.
const catalogPrimaryKeys = `
SELECT string_agg(format('%s %I', c.conrelid::regclass, c.conname), E'\n' ORDER BY c.conname)
FROM pg_constraint c
JOIN pg_class r ON r.oid = c.conrelid
JOIN pg_namespace n ON n.oid = r.relnamespace
WHERE c.contype = 'p' AND n.nspname = 'public' AND r.relname <> 'ptah_schema_apply_log'`

// ownedSequencesBuilt gives a sequence an owner in each spelling CREATE
// SEQUENCE ... OWNED BY takes: table.column, the default schema written on
// the owner and on both names, quoted parts, and NONE. A qualified table that
// is dropped takes the sequence its column owns.
const ownedSequencesBuilt = `
CREATE TABLE t (id bigint NOT NULL, note text);
CREATE TABLE "T" ("Id" bigint NOT NULL);
CREATE SEQUENCE s_two OWNED BY t.id;
CREATE SEQUENCE s_three OWNED BY public.t.id;
CREATE SEQUENCE public.s_both OWNED BY public.t.note;
CREATE SEQUENCE "S_quoted" OWNED BY "public"."T"."Id";
CREATE SEQUENCE s_none OWNED BY NONE;
CREATE TABLE public.d (id bigint NOT NULL);
CREATE SEQUENCE public.s_dropped OWNED BY public.d.id;
DROP TABLE public.d;
`

// ownedSequenceOfAnotherSchema is a sequence of a schema other than the
// default, owned by a column of a table of its schema. The table of the same
// name in the default schema is dropped and leaves the sequence alone; a
// dropped table of the schema takes its sequence.
const ownedSequenceOfAnotherSchema = `
CREATE SCHEMA app;
CREATE TABLE u (id bigint NOT NULL);
CREATE TABLE app.u (id bigint NOT NULL);
CREATE SEQUENCE app.s_app OWNED BY app.u.id;
CREATE TABLE app.d (id bigint NOT NULL);
CREATE SEQUENCE app.s_dropped OWNED BY app.d.id;
DROP TABLE u;
DROP TABLE app.d;
`

// catalogSequenceOwners is each sequence of the default schema the database
// holds, and the column that owns it.
const catalogSequenceOwners = `
SELECT string_agg(format('%s %s', s.oid::regclass,
    CASE WHEN d.refobjid IS NULL THEN 'none' ELSE format('%s.%I', d.refobjid::regclass, a.attname) END),
    E'\n' ORDER BY s.relname)
FROM pg_class s
JOIN pg_namespace n ON n.oid = s.relnamespace
LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = s.oid
    AND d.refclassid = 'pg_class'::regclass AND d.deptype = 'a'
LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
WHERE s.relkind = 'S' AND n.nspname = 'public'`

// held is what the scratch database answers to a catalog query that returns
// one text value.
func (s scratch) held(c *qt.C, query string) string {
	c.Helper()
	var held string
	c.Assert(s.conn.QueryRowContext(c.Context(), query).Scan(&held), qt.IsNil)
	return held
}

// TestQuotedPrimaryKeyNamesReadSyncedE2E reads the dump, and the statements
// it was dumped from, against the database they describe: no key is renamed.
func TestQuotedPrimaryKeyNamesReadSyncedE2E(t *testing.T) {
	for _, test := range []struct {
		name, source string
	}{
		{name: "the dump", source: quotedKeysDump},
		{name: "the statements that built the database", source: quotedKeysBuilt},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c)
			dev.exec(c, quotedKeysBuilt)

			plan := dev.plan(c, test.source, "public")
			drift := dev.drift(c, test.source, "public")

			c.Assert(plan, qt.Contains, "Schema is synced")
			c.Assert(drift, qt.Contains, "No schema drift detected")
		})
	}
}

// TestQuotedPrimaryKeyNamesBuildTheirKeysE2E applies the statements to an
// empty database. Each key it builds holds the name the server gives it when
// it runs the same statements, the folded name of the unquoted one included.
func TestQuotedPrimaryKeyNamesBuildTheirKeysE2E(t *testing.T) {
	c := qt.New(t)
	source := newScratch(c)
	source.exec(c, quotedKeysBuilt)
	dev := newScratch(c)

	out, err := ptah("schema", "apply", "--db-url", dev.url, "--schema-file", writeSchema(c, quotedKeysBuilt),
		"--schemas", "public", "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(dev.held(c, catalogPrimaryKeys), qt.Equals, source.held(c, catalogPrimaryKeys))
	c.Assert(dev.plan(c, quotedKeysDump, "public"), qt.Contains, "Schema is synced")
}

// TestCreateSequenceOwnedByReadsSyncedE2E reads the statements against the
// database they built: no sequence is given its owner again.
func TestCreateSequenceOwnedByReadsSyncedE2E(t *testing.T) {
	for _, test := range []struct {
		name, built, schemas string
	}{
		{name: "the default schema", built: ownedSequencesBuilt, schemas: "public"},
		{name: "another schema", built: ownedSequenceOfAnotherSchema, schemas: "public,app"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c)
			dev.exec(c, test.built)

			plan := dev.plan(c, test.built, test.schemas)
			drift := dev.drift(c, test.built, test.schemas)

			c.Assert(plan, qt.Contains, "Schema is synced")
			c.Assert(drift, qt.Contains, "No schema drift detected")
		})
	}
}

// TestCreateSequenceOwnedByBuildsItsOwnersE2E applies the statements to an
// empty database. Each sequence it builds is owned by the column that owns it
// when the server runs the same statements.
func TestCreateSequenceOwnedByBuildsItsOwnersE2E(t *testing.T) {
	c := qt.New(t)
	source := newScratch(c)
	source.exec(c, ownedSequencesBuilt)
	dev := newScratch(c)

	out, err := ptah("schema", "apply", "--db-url", dev.url, "--schema-file", writeSchema(c, ownedSequencesBuilt),
		"--schemas", "public", "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(dev.held(c, catalogSequenceOwners), qt.Equals, source.held(c, catalogSequenceOwners))
	c.Assert(dev.plan(c, ownedSequencesBuilt, "public"), qt.Contains, "Schema is synced")
}
