package schemafile_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/schemafile"
)

// The fixtures below are the object kinds a PostgreSQL SQL schema file could
// not express before issue #932. Each one is loaded after a CREATE TABLE
// anchor and must both load without error and reach the rendered SQL: an
// exit-code assertion on its own passes for a statement that parses and is
// then dropped, which is exactly how five of these used to behave.
func loadRenderedPostgresSQL(c *qt.C, body string) string {
	dir := c.TempDir()
	path := writeSchemaFile(c, dir, "schema.sql", "CREATE TABLE t1 (id BIGINT PRIMARY KEY);\n"+body+"\n")
	db, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	return strings.Join(renderPostgres(c, db), "\n")
}

func TestLoadAll_PostgresObjectKindsSurviveSQLSchemaFiles(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{{
		name:      "materialized view",
		statement: `CREATE MATERIALIZED VIEW mv1 AS SELECT id FROM t1;`,
		want:      `CREATE MATERIALIZED VIEW "mv1" AS`,
	}, {
		name:      "sequence",
		statement: `CREATE SEQUENCE s1 START WITH 1;`,
		want:      `CREATE SEQUENCE "s1" START WITH 1;`,
	}, {
		name:      "role",
		statement: `CREATE ROLE r_x;`,
		want:      `CREATE ROLE "r_x" WITH NOLOGIN`,
	}, {
		name:      "grant",
		statement: `GRANT SELECT ON TABLE t1 TO r_x;`,
		want:      `GRANT SELECT ON TABLE "t1" TO "r_x";`,
	}, {
		name:      "policy",
		statement: `CREATE POLICY p1 ON t1 FOR SELECT USING (true);`,
		want:      `CREATE POLICY "p1" ON "t1" FOR SELECT`,
	}, {
		name:      "row level security",
		statement: `ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;`,
		want:      `ALTER TABLE "t1" ENABLE ROW LEVEL SECURITY;`,
	}, {
		name:      "schema qualified composite type",
		statement: `CREATE TYPE "public"."address" AS ("street" text);`,
		want:      `CREATE TYPE "public"."address" AS ("street" text);`,
	}, {
		name:      "schema qualified range type",
		statement: `CREATE TYPE "public"."floatrange" AS RANGE (SUBTYPE = float8);`,
		want:      `CREATE TYPE "public"."floatrange" AS RANGE (SUBTYPE = float8);`,
	}, {
		name:      "schema qualified enum type",
		statement: `CREATE TYPE "public"."e1" AS ENUM ('a');`,
		want:      `CREATE TYPE "public"."e1" AS ENUM ('a');`,
	}, {
		name:      "view",
		statement: `CREATE VIEW v1 AS SELECT id FROM t1;`,
		want:      `CREATE VIEW "v1" AS`,
	}, {
		name:      "domain",
		statement: `CREATE DOMAIN d1 AS TEXT;`,
		want:      `CREATE DOMAIN "d1" AS TEXT;`,
	}, {
		name:      "extension",
		statement: `CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
		want:      `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`,
	}, {
		name:      "function",
		statement: `CREATE FUNCTION f1() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE SQL;`,
		want:      `CREATE OR REPLACE FUNCTION "f1"() RETURNS int AS $$`,
	}, {
		name:      "trigger executing an existing function",
		statement: `CREATE TRIGGER tg1 AFTER INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION f1();`,
		want:      `CREATE TRIGGER "tg1" AFTER INSERT ON "t1" FOR EACH ROW EXECUTE FUNCTION "f1"();`,
	}, {
		name:      "index control",
		statement: `CREATE INDEX ix1 ON t1 (id);`,
		want:      `CREATE INDEX IF NOT EXISTS "ix1" ON "t1" ("id");`,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(loadRenderedPostgresSQL(c, tc.statement), qt.Contains, tc.want)
		})
	}
}

// TestLoadAll_TriggerOnAnExistingFunctionDoesNotRedefineIt pins the half of the
// trigger case that a "the object appears" assertion would miss. A trigger that
// executes a function it did not define must reference that function, never
// emit a body for it: rendering CREATE OR REPLACE FUNCTION "f1"() with an empty
// body would erase whatever f1 actually contains.
func TestLoadAll_TriggerOnAnExistingFunctionDoesNotRedefineIt(t *testing.T) {
	c := qt.New(t)

	sql := loadRenderedPostgresSQL(c, `CREATE TRIGGER tg1 AFTER INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION f1();`)

	c.Assert(sql, qt.Contains, `EXECUTE FUNCTION "f1"();`)
	c.Assert(sql, qt.Not(qt.Contains), `CREATE OR REPLACE FUNCTION "f1"()`)
	c.Assert(sql, qt.Not(qt.Contains), "ptah_trigger_t1_tg1")
}

// TestLoadAll_SQLSchemaFileStillRefusesStatementsOutsideTheGrammar is the
// negative control for the fixtures above: widening the grammar must not turn
// the loader into one that accepts anything.
func TestLoadAll_SQLSchemaFileStillRefusesStatementsOutsideTheGrammar(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantError string
	}{{
		name:      "not sql at all",
		statement: `DEFINITELY NOT SQL;`,
		wantError: `unsupported SQL statement: DEFINITELY`,
	}, {
		name:      "unmodelled create target",
		statement: `CREATE PUBLICATION p FOR ALL TABLES;`,
		wantError: `unsupported CREATE target: PUBLICATION`,
	}, {
		name:      "alter table operation that is not row level security",
		statement: `ALTER TABLE t1 ENABLE TRIGGER tg1;`,
		wantError: `unsupported ALTER operation: ENABLE`,
	}, {
		name:      "disable row level security has no representation",
		statement: `ALTER TABLE t1 DISABLE ROW LEVEL SECURITY;`,
		wantError: `unsupported ALTER operation: DISABLE ROW LEVEL SECURITY`,
	}, {
		name:      "restrictive policy changes what the policy means",
		statement: `CREATE POLICY p1 ON t1 AS RESTRICTIVE FOR SELECT USING (true);`,
		wantError: `unsupported CREATE POLICY clause: AS RESTRICTIVE`,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			path := writeSchemaFile(c, dir, "schema.sql", "CREATE TABLE t1 (id BIGINT PRIMARY KEY);\n"+tc.statement+"\n")

			_, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: platform.Postgres})

			c.Assert(err, qt.ErrorMatches, ".*"+tc.wantError+".*")
		})
	}
}
