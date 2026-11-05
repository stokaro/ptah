//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A desired schema that records crm as not described leaves crm whole. The
// database holds public.users and, in crm, the objects a comparison can remove:
// tables with keys, a foreign key, indexes, a comment, an enum, a sequence, a
// view and a materialized view, a function, a procedure, a trigger, row-level
// security with a policy, a domain, and grants to a role the desired schema
// manages. With the record, every verb finds the database synced and no
// statement names crm. Without it, the same verbs plan crm's removal.

// notDescribedCRM creates crm. %[1]s is the role that holds its grants.
const notDescribedCRM = `CREATE SCHEMA crm;
CREATE TYPE crm.status AS ENUM ('lead', 'customer');
CREATE SEQUENCE crm.ticket_seq;
CREATE TABLE crm.customers (
    id integer PRIMARY KEY,
    email text NOT NULL,
    state crm.status,
    CONSTRAINT customers_email_check CHECK (email <> '')
);
CREATE UNIQUE INDEX customers_email_key ON crm.customers (email);
CREATE INDEX customers_state_idx ON crm.customers (state);
CREATE TABLE crm.orders (
    id integer PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES crm.customers (id),
    owner_id integer REFERENCES public.users (id)
);
COMMENT ON TABLE crm.customers IS 'crm customers';
CREATE VIEW crm.active_customers AS SELECT id, email FROM crm.customers WHERE state = 'customer';
CREATE MATERIALIZED VIEW crm.customer_counts AS SELECT state, count(*) AS n FROM crm.customers GROUP BY state;
CREATE FUNCTION crm.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER customers_touch BEFORE UPDATE ON crm.customers FOR EACH ROW EXECUTE FUNCTION crm.touch();
CREATE FUNCTION crm.add(a integer, b integer) RETURNS integer LANGUAGE sql AS 'SELECT a + b';
CREATE PROCEDURE crm.noop() LANGUAGE sql AS 'SELECT 1';
CREATE DOMAIN crm.email AS text CHECK (VALUE LIKE '%%@%%');
ALTER TABLE crm.customers ENABLE ROW LEVEL SECURITY;
CREATE POLICY customers_reader ON crm.customers FOR SELECT TO %[1]s USING (true);
GRANT USAGE ON SCHEMA crm TO %[1]s;
GRANT SELECT ON crm.customers TO %[1]s;
GRANT EXECUTE ON FUNCTION crm.add(integer, integer) TO %[1]s;
GRANT USAGE ON SEQUENCE crm.ticket_seq TO %[1]s;
`

const (
	notDescribedSynced   = "No schema differences detected.\n"
	notDescribedApplied  = "Schema is synced, no changes to be made.\n"
	notDescribedNoChange = "no migration files generated"
	notDescribedAnyCRM   = `"crm"`
	notDescribedDropCRM  = `DROP TABLE "crm"."customers";`
)

// notDescribedSource is one desired schema describing public alone.
type notDescribedSource struct {
	name string
	// flag is how the verb is given the source: --schema-file or --root-dir.
	flag string
	// file is the source's file name and body, which {role} names the role in.
	file, body string
}

// notDescribedSources are the three spellings of the record: a SQL header, an
// HCL header, and the Go annotation. Only SQL declares the role, so only the
// SQL run has grants in crm it manages.
func notDescribedSources() []notDescribedSource {
	return []notDescribedSource{
		{
			name: "SQL header", flag: "--schema-file", file: "schema.sql",
			body: "-- ptah:not-described schema \"crm\"\nCREATE ROLE {role};\nCREATE TABLE public.users (id integer PRIMARY KEY);\n",
		},
		{
			name: "HCL header", flag: "--schema-file", file: "schema.hcl",
			body: `// ptah:not-described schema "crm"
schema "public" {}

table "users" {
  schema = schema.public
  column "id" {
    null = false
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}
`,
		},
		{
			name: "Go annotation", flag: "--root-dir", file: "models.go",
			body: `package models

//ptah:schema:notdescribed kind="schema" name="crm"
type _ struct{}

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`,
		},
	}
}

// TestNotDescribedSchemaIsLeftWholeE2E compares each spelling with the
// database. Nothing is planned, and no output names crm.
func TestNotDescribedSchemaIsLeftWholeE2E(t *testing.T) {
	c := qt.New(t)
	database, role := newNotDescribedDatabase(c)
	for _, source := range notDescribedSources() {
		t.Run(source.name, func(t *testing.T) {
			c := qt.New(t)
			sourceArgs := writeNotDescribedSource(c, source, role)

			compared := runPtahIn(c, append([]string{"schema", "compare", "--db-url", database}, sourceArgs...)...)

			c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
			c.Assert(compared.Stdout, qt.Contains, notDescribedSynced)
			c.Assert(compared.Stdout, qt.Not(qt.Contains), notDescribedAnyCRM)
			c.Assert(compared.Stderr, qt.Not(qt.Contains), "crm")
		})
	}
}

// TestNotDescribedSchemaPlansNothingToApplyOrGenerateE2E runs the verbs that
// act on a plan, with the SQL spelling, which manages the role crm grants to.
func TestNotDescribedSchemaPlansNothingToApplyOrGenerateE2E(t *testing.T) {
	c := qt.New(t)
	database, role := newNotDescribedDatabase(c)
	sourceArgs := writeNotDescribedSource(c, notDescribedSources()[0], role)
	migrations := c.TempDir()

	applied := runPtahIn(c, append([]string{"schema", "apply", "--db-url", database, "--dry-run"}, sourceArgs...)...)
	generated := runPtahIn(c, append([]string{"migrations", "generate", "--db-url", database, "--migrations-dir", migrations}, sourceArgs...)...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Equals, notDescribedApplied)
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	c.Assert(generated.Stdout, qt.Contains, notDescribedNoChange)
	ups, err := filepath.Glob(filepath.Join(migrations, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 0)
}

// TestDescribedSchemaPlansTheRemovalE2E is the control: the SQL source
// without its header plans crm's removal against the same database.
func TestDescribedSchemaPlansTheRemovalE2E(t *testing.T) {
	c := qt.New(t)
	database, role := newNotDescribedDatabase(c)
	source := notDescribedSources()[0]
	source.body = "CREATE ROLE {role};\nCREATE TABLE public.users (id integer PRIMARY KEY);\n"

	compared := runPtahIn(c, append([]string{"schema", "compare", "--db-url", database}, writeNotDescribedSource(c, source, role)...)...)

	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(compared.Stdout, qt.Contains, notDescribedDropCRM)
}

// newNotDescribedDatabase creates a database holding public.users and crm, and
// a role crm grants to. Both are dropped when the test ends; the database
// first, since it holds the grants. The URL pins no search_path.
func newNotDescribedDatabase(c *qt.C) (database, role string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	role = fmt.Sprintf("ptah_nd_reader_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+quoteE2EIdent(role))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	database = newEmptyReadScopeDatabase(c, "not_described")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE public.users (id integer PRIMARY KEY)")
	execPostgresFamilySQL(c, c.Context(), database, fmt.Sprintf(notDescribedCRM, quoteE2EIdent(role)))
	return database, role
}

// writeNotDescribedSource writes source into a directory of its own and
// returns the arguments that hand it to a verb.
func writeNotDescribedSource(c *qt.C, source notDescribedSource, role string) []string {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, source.file)
	c.Assert(os.WriteFile(path, []byte(strings.ReplaceAll(source.body, "{role}", role)), 0o600), qt.IsNil)
	return map[string][]string{"--schema-file": {"--schema-file", path}, "--root-dir": {"--root-dir", dir}}[source.flag]
}
