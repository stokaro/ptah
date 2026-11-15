//go:build integration

package ownerdump_test

import (
	"fmt"
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
// the owner and on both names, quoted parts, names holding a dot, a double
// quote or a bracket, a column name with white space at either end, and NONE.
// The rest changes an owner the way the server follows it: a dropped table
// takes the sequences its columns own, two tables in one statement included,
// whichever spelling of the default schema names it; a dropped column takes
// its sequence, and a renamed one keeps it; a sequence whose owner was taken
// away or given to another table stays.
const ownedSequencesBuilt = `
CREATE TABLE t (id bigint NOT NULL, note text);
CREATE TABLE "T" ("Id" bigint NOT NULL);
CREATE SEQUENCE s_two OWNED BY t.id;
CREATE SEQUENCE s_three OWNED BY public.t.id;
CREATE SEQUENCE public.s_both OWNED BY public.t.note;
CREATE SEQUENCE "S_quoted" OWNED BY "public"."T"."Id";
CREATE SEQUENCE s_none OWNED BY NONE;
CREATE TABLE "a.b" (id bigint NOT NULL);
CREATE SEQUENCE s_dotted_table OWNED BY "a.b".id;
CREATE TABLE dotted (id bigint NOT NULL, "x.y" bigint);
CREATE SEQUENCE s_dotted_column OWNED BY dotted."x.y";
CREATE TABLE "q""t" (id bigint NOT NULL);
CREATE SEQUENCE s_quote OWNED BY "q""t".id;
CREATE TABLE "t[1]" (id bigint NOT NULL);
CREATE SEQUENCE s_bracket OWNED BY "t[1]".id;
CREATE TABLE pc (id bigint NOT NULL, " i " bigint);
CREATE SEQUENCE s_padded OWNED BY pc." i ";
CREATE TABLE public.d (id bigint NOT NULL);
CREATE SEQUENCE public.s_dropped OWNED BY public.d.id;
DROP TABLE public.d;
CREATE TABLE w (id bigint NOT NULL);
CREATE SEQUENCE s_w OWNED BY w.id;
DROP TABLE public.w;
CREATE TABLE "c.d" (id bigint NOT NULL, "x.y" bigint);
CREATE SEQUENCE s_dotted_dropped OWNED BY "c.d"."x.y";
DROP TABLE "c.d";
CREATE TABLE pd (id bigint NOT NULL, " i " bigint);
CREATE SEQUENCE s_padded_dropped OWNED BY pd." i ";
ALTER TABLE pd DROP COLUMN " i ";
CREATE TABLE d1 (id bigint NOT NULL);
CREATE TABLE d2 (id bigint NOT NULL);
GRANT SELECT ON d2 TO PUBLIC;
CREATE SEQUENCE s_d1 OWNED BY d1.id;
CREATE SEQUENCE s_d2 OWNED BY d2.id;
DROP TABLE d1, d2;
CREATE TABLE c (id bigint NOT NULL, v bigint);
CREATE SEQUENCE s_dropped_column OWNED BY c.v;
ALTER TABLE c DROP COLUMN v;
CREATE TABLE k (id bigint NOT NULL, v bigint);
CREATE SEQUENCE s_renamed_column OWNED BY k.v;
ALTER TABLE k RENAME COLUMN v TO w;
CREATE TABLE n (id bigint NOT NULL);
CREATE SEQUENCE s_owner_taken OWNED BY n.id;
ALTER SEQUENCE s_owner_taken OWNED BY NONE;
DROP TABLE n;
CREATE TABLE r1 (id bigint NOT NULL);
CREATE TABLE r2 (id bigint NOT NULL);
CREATE SEQUENCE s_reowned OWNED BY r1.id;
ALTER SEQUENCE s_reowned OWNED BY r2.id;
DROP TABLE r1;
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

// TestIdentityForASerialOfADottedNameE2E declares an identity generated ALWAYS
// for a serial column whose table or column name holds a dot. The serial's
// sequence is dropped ahead of the identity, which takes its name, so the
// identity's sequence is the one the plan sets past the rows: the next value
// is 42. Dropped after the identity, the old sequence takes that setval and
// the identity starts at 1.
func TestIdentityForASerialOfADottedNameE2E(t *testing.T) {
	for _, test := range []struct {
		name, table, column, tableName, columnName string
	}{
		{name: "the table", table: `"pa.rent"`, column: "n", tableName: "pa.rent", columnName: "n"},
		{name: "the column", table: "parent", column: `"n.m"`, tableName: "parent", columnName: "n.m"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c)
			dev.exec(c, fmt.Sprintf("CREATE TABLE %[1]s (id bigint PRIMARY KEY, %[2]s bigserial);\n"+
				"INSERT INTO %[1]s (id, %[2]s) VALUES (1, 41);\n", test.table, test.column))
			desired := fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, %s bigint GENERATED ALWAYS AS IDENTITY);\n", test.table, test.column)

			out, err := ptah("schema", "apply", "--db-url", dev.url, "--schema-file", writeSchema(c, desired),
				"--schemas", "public", "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(dev.held(c, fmt.Sprintf("SELECT coalesce(identity_generation, '') FROM information_schema.columns "+
				"WHERE table_schema = 'public' AND table_name = '%s' AND column_name = '%s'", test.tableName, test.columnName)), qt.Equals, "ALWAYS")
			c.Assert(dev.held(c, fmt.Sprintf("INSERT INTO %s (id) VALUES (1000) RETURNING %s::text", test.table, test.column)), qt.Equals, "42")
			c.Assert(dev.plan(c, desired, "public"), qt.Contains, "Schema is synced")
		})
	}
}
