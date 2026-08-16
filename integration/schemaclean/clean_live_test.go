//go:build integration

package schemaclean_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemaclean"
)

// postgresCleanupFixture creates one object of every kind the PostgreSQL writer
// destroys, plus a control schema the schema-scoped cleanup never touches.
//
// Both revision-table layouts are present, which is what makes this fixture
// cover issue #1111 as well as #940 item 1. The PostgreSQL reader hides
// schema_migrations by name but surfaces atlas_schema_revisions, so the two
// rows exercise opposite halves of the same fix: one must be added to the plan
// by the catalog probe, and the other must NOT be added a second time on top of
// the row the reader already produced.
//
// The keepme copies carry the same two names on purpose. The cleanup is schema
// scoped, so a probe that ignored scope would plan two schema_migrations rows
// against one destroyed, and the plan/destroyed comparison would fail.
var postgresCleanupFixture = []string{
	`CREATE TYPE mood AS ENUM ('happy', 'sad')`,
	`CREATE DOMAIN d_email AS text CHECK (VALUE LIKE '%@%')`,
	`CREATE TYPE c_addr AS (street text, city text)`,
	`CREATE TYPE r_int AS RANGE (subtype = int4)`,
	`CREATE SEQUENCE s_counter`,
	`CREATE TABLE users (id integer PRIMARY KEY, email d_email, feeling mood)`,
	`CREATE TABLE posts (id integer PRIMARY KEY, author_id integer NOT NULL REFERENCES users (id))`,
	`CREATE VIEW v_users AS SELECT id, email FROM users`,
	`CREATE MATERIALIZED VIEW mv_users AS SELECT id FROM users`,
	`CREATE FUNCTION f_touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'`,
	`CREATE TRIGGER trg_users BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION f_touch()`,
	`CREATE INDEX idx_users_email ON users (email)`,
	`CREATE TABLE schema_migrations (version bigint PRIMARY KEY, dirty boolean)`,
	`CREATE TABLE atlas_schema_revisions (version varchar PRIMARY KEY, description varchar)`,

	`CREATE SCHEMA keepme`,
	`CREATE TYPE keepme.control_mood AS ENUM ('kept')`,
	`CREATE TABLE keepme.control_table (id integer PRIMARY KEY)`,
	`CREATE VIEW keepme.control_view AS SELECT id FROM keepme.control_table`,
	`CREATE FUNCTION keepme.control_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1'`,
	`CREATE TABLE keepme.schema_migrations (version bigint PRIMARY KEY)`,
	`CREATE TABLE keepme.atlas_schema_revisions (version varchar PRIMARY KEY)`,
}

// postgresCleanupCensusQuery enumerates, straight from pg_catalog, every object
// that SchemaWriter.DropAllTables issues a statement for, in the vocabulary of
// schemaclean.Object.Type. It is deliberately written against the catalog and
// not against schemaclean's own coverage table, so the comparison below is a
// measurement rather than a restatement.
//
// Dependent objects the writer never names are excluded on purpose, matching
// the documented Plan contract: indexes, triggers, non-foreign-key constraints,
// and the constructor routines a range type owns (pg_depend deptype 'i').
const postgresCleanupCensusQuery = `
	SELECT
		CASE c.relkind
			WHEN 'v' THEN 'view'
			WHEN 'm' THEN 'materialized_view'
			WHEN 'S' THEN 'sequence'
			ELSE 'table'
		END || '|' || c.relname
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname IN ('public', 'keepme')
	  AND c.relkind IN ('r', 'p', 'v', 'm', 'S')

	UNION ALL

	SELECT
		CASE t.typtype
			WHEN 'e' THEN 'enum'
			WHEN 'd' THEN 'domain'
			WHEN 'r' THEN 'range'
			ELSE 'composite'
		END || '|' || t.typname
	FROM pg_type t
	JOIN pg_namespace n ON n.oid = t.typnamespace
	LEFT JOIN pg_class c ON c.oid = t.typrelid
	WHERE n.nspname IN ('public', 'keepme')
	  AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c'))

	UNION ALL

	SELECT
		CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END || '|' || p.proname
	FROM pg_proc p
	JOIN pg_namespace n ON n.oid = p.pronamespace
	WHERE n.nspname IN ('public', 'keepme')
	  AND NOT EXISTS (
		SELECT 1 FROM pg_depend d
		WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'i'
	  )

	UNION ALL

	SELECT 'foreign_key|' || con.conname
	FROM pg_constraint con
	JOIN pg_class c ON c.oid = con.conrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname IN ('public', 'keepme') AND con.contype = 'f'

	ORDER BY 1
`

// TestInspectNamesEveryObjectApplyDestroys_PostgresLive is the anti-drift gate
// for issue #940 item 1 and issue #1111: it compares the plan
// schemaclean.Inspect produced against the set of objects that really
// disappeared when schemaclean.Apply ran, on a throwaway PostgreSQL database.
//
// Widening internal/dbschema/postgres/writer.go without widening
// schemaclean.coverageFor makes the destroyed set larger than the planned set
// and fails here, which is the property a duplicated constant list cannot give.
//
// The same comparison covers the revision tables, which no reader puts in a
// schema snapshot: dropping the catalog probe leaves schema_migrations
// destroyed but unplanned, and dropping the duplicate check leaves
// atlas_schema_revisions planned twice but destroyed once. Both are a
// planned/destroyed mismatch, so neither can regress silently.
func TestInspectNamesEveryObjectApplyDestroys_PostgresLive(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	conn := newPostgresCleanupLiveConnection(c, ctx)
	applyPostgresCleanupFixture(c, ctx, conn)

	before := postgresCleanupCensus(c, ctx, conn)
	plan, err := schemaclean.Inspect(conn)
	c.Assert(err, qt.IsNil)
	planned := plannedObjectKeys(plan)

	c.Assert(schemaclean.Apply(ctx, conn), qt.IsNil)
	after := postgresCleanupCensus(c, ctx, conn)

	c.Assert(destroyedKeys(before, after), qt.DeepEquals, planned)

	// A fixture that failed to create anything would make both sets empty and
	// the comparison above vacuous. Checked after it so that a genuine coverage
	// gap reports the missing objects rather than a bare count.
	c.Assert(len(planned) >= 10, qt.IsTrue, qt.Commentf("planned set is too small: %v", planned))

	// Control: the cleanup is schema scoped, so nothing in "keepme" is
	// destroyed, and nothing in "keepme" may appear in the plan either. The two
	// keepme revision tables are the scope control for the #1111 probe: they
	// survive, so the plan may name each revision table only once.
	c.Assert(after, qt.DeepEquals, []string{
		"enum|control_mood",
		"function|control_fn",
		"table|atlas_schema_revisions",
		"table|control_table",
		"table|schema_migrations",
		"view|control_view",
	})
	c.Assert(planned, qt.Not(qt.Any(qt.Contains)), "control")

	// Stated separately from the set comparison above so a regression says
	// which table went missing rather than printing two long lists. Both are
	// destroyed, so both must be named; neither reader nor probe alone
	// produces both.
	c.Assert(planned, qt.Contains, "table|schema_migrations")
	c.Assert(planned, qt.Contains, "table|atlas_schema_revisions")
}

func plannedObjectKeys(plan schemaclean.Plan) []string {
	keys := make([]string, 0, len(plan.Objects))
	for _, object := range plan.Objects {
		keys = append(keys, object.Type+"|"+object.Name)
	}
	slices.Sort(keys)
	return keys
}

// destroyedKeys returns the census rows present before the cleanup and absent
// after it.
func destroyedKeys(before, after []string) []string {
	remaining := make(map[string]int, len(after))
	for _, key := range after {
		remaining[key]++
	}
	destroyed := make([]string, 0, len(before))
	for _, key := range before {
		if remaining[key] > 0 {
			remaining[key]--
			continue
		}
		destroyed = append(destroyed, key)
	}
	slices.Sort(destroyed)
	return destroyed
}

func postgresCleanupCensus(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, postgresCleanupCensusQuery)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var census []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		census = append(census, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(census)
	return census
}

func applyPostgresCleanupFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) {
	c.Helper()
	for _, statement := range postgresCleanupFixture {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// newPostgresCleanupLiveConnection provisions a throwaway database so a test
// that deliberately destroys a whole schema cannot damage the shared fixture
// database other live tests in the same job depend on.
func newPostgresCleanupLiveConnection(c *qt.C, ctx context.Context) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := requirePostgresCleanupLiveURL(c)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_schemaclean_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""

	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(ctx),
			"DROP DATABASE IF EXISTS "+nameIdent+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return conn
}

func requirePostgresCleanupLiveURL(c *qt.C) string {
	c.Helper()
	// dbtarget answers with the address as configured, and this helper has
	// always handed its callers the postgres:// spelling. The fold stays here
	// rather than moving into the registry, where it would rewrite the address
	// every PostgreSQL consumer receives on the strength of what three
	// integration tests happen to want.
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	return parsed.String()
}
