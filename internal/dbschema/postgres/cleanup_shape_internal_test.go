package postgres

// White-box testing required: the cleanup query is assembled from an unexported
// template and never leaves the package, so no exported call reports which
// shape a given server gets.

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/platform/capability"
)

// viewOrderingRow is one server shape and what its cleanup query may contain.
type viewOrderingRow struct {
	name          string
	shape         viewOrderingShape
	wantRecursive bool
	wantViewWalk  bool
}

func TestApplyViewOrderingShapeMatchesTheServer(t *testing.T) {
	rows := []viewOrderingRow{{
		name:          "a server that accepts a recursive catalog CTE",
		shape:         recursiveViewOrdering,
		wantRecursive: true,
		wantViewWalk:  true,
	}, {
		// PGAdapter prepends its own `WITH pg_class AS (...)` to a catalog
		// query, which only merges when the query's WITH is not RECURSIVE.
		// The walk goes with it: it reads pg_rewrite, which Spanner does not
		// have either.
		name:          "a server that does not",
		shape:         flatViewOrdering,
		wantRecursive: false,
		wantViewWalk:  false,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			query := applyViewOrderingShape(
				"WITH {{RECURSIVE}} a AS ({{VIEW_DEPENDENCIES}}), b AS (x {{VIEW_DEPTH_RECURSION}})",
				row.shape)

			c.Assert(strings.Contains(query, "RECURSIVE"), qt.Equals, row.wantRecursive)
			c.Assert(strings.Contains(query, "pg_rewrite"), qt.Equals, row.wantViewWalk)
			c.Assert(query, qt.Not(qt.Contains), "{{")
		})
	}
}

// collectedQueryFor runs the real assembly path and returns the SQL that would
// reach the server. Asserting on applyViewOrderingShape alone proves the helper
// works, not that collectAllObjects asks it for the right shape -- a call site
// hard-coded to `true` passes that weaker test.
func collectedQueryFor(t *testing.T, caps capability.Capabilities) string {
	t.Helper()
	runner := newRecordingRunner(t)
	writer := NewPostgreSQLWriterForRunnerWithCapabilities(runner, "public", "", caps)
	_, _ = writer.collectAllObjects(
		context.Background(), runner, postgresSchemaCleanupScope([]string{"public"}, nil), postgresCleanupCapabilities{})
	return lastRecordedQuery
}

func TestCollectAllObjectsAsksForTheShapeTheServerAccepts(t *testing.T) {
	c := qt.New(t)

	spanner := collectedQueryFor(t, capability.SpannerPostgres())
	c.Assert(spanner, qt.Not(qt.Contains), "RECURSIVE")
	c.Assert(spanner, qt.Not(qt.Contains), "pg_rewrite")

	postgres := collectedQueryFor(t, capability.Postgres16())
	c.Assert(postgres, qt.Contains, "RECURSIVE")
	c.Assert(postgres, qt.Contains, "pg_rewrite")
}

// TestCollectAllObjectsDropsPoliciesWhereTheServerHasThem reads the query a
// cleanup sends. A server that models row-level security is asked for the
// policies of the managed tables, each dropped through its table, so a policy
// that reads another relation does not hold that relation back. A server
// without the key has no pg_policy to read.
func TestCollectAllObjectsDropsPoliciesWhereTheServerHasThem(t *testing.T) {
	rows := []struct {
		name string
		caps capability.Capabilities
		want bool
	}{
		{name: "postgres", caps: capability.Postgres16(), want: true},
		{name: "cockroachdb", caps: capability.CockroachDB26(), want: true},
		{name: "yugabytedb", caps: capability.YugabyteDB25(), want: true},
		{name: "a server without row-level security", caps: capability.Postgres16().With(capability.RowLevelSecurity, false), want: false},
		{name: "spanner", caps: capability.SpannerPostgres(), want: false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			query := collectedQueryFor(t, row.caps)

			c.Assert(strings.Contains(query, "FROM pg_policy pol"), qt.Equals, row.want)
			c.Assert(strings.Contains(query, "DROP POLICY IF EXISTS %I ON %I.%I"), qt.Equals, row.want && row.caps.Has(capability.PostgresCatalogFunctions))
			c.Assert(query, qt.Not(qt.Contains), "{{")
		})
	}
}

// TestCollectAllObjectsKeepsTheKeptExtensionsPolicies reads the realm query,
// which leaves out what a kept extension owns: the policies of an extension's
// table stay with the table.
func TestCollectAllObjectsKeepsTheKeptExtensionsPolicies(t *testing.T) {
	c := qt.New(t)
	runner := newRecordingRunner(t)
	writer := NewPostgreSQLWriterForRunnerWithCapabilities(runner, "public", "", capability.Postgres16())

	_, _ = writer.collectAllObjects(context.Background(), runner,
		postgresDatabaseCleanupScope([]string{"public"}, []string{"hstore"}), postgresCleanupCapabilities{})

	policies, _, found := strings.Cut(lastRecordedQuery[strings.Index(lastRecordedQuery, "FROM pg_policy pol"):], "UNION ALL")
	c.Assert(found, qt.IsTrue)
	c.Assert(policies, qt.Contains, "WHERE NOT EXISTS")
	c.Assert(policies, qt.Contains, "owned.classid = 'pg_class'::regclass")
	c.Assert(policies, qt.Contains, "owned.objid = c.oid")
}

// beganTx records whether the cleanup asked for a transaction. It is the only
// offline way to tell the two paths apart: both fail on the fake runner, and
// the difference is which call they made first.
var beganTx bool

func TestDropAllTablesTakesATransactionOnlyWhereTheServerAcceptsDDLInOne(t *testing.T) {
	rows := []struct {
		name   string
		caps   capability.Capabilities
		wantTx bool
	}{
		{name: "postgres takes one", caps: capability.Postgres16(), wantTx: true},
		// Spanner answers `DDL statements are only allowed outside explicit
		// transactions` (SQLSTATE 25000), so asking for one cannot work.
		{name: "spanner does not", caps: capability.SpannerPostgres(), wantTx: false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			beganTx = false
			writer := NewPostgreSQLWriterForRunnerWithCapabilities(newRecordingRunner(t), "public", "", row.caps)

			_ = writer.DropAllTables(context.Background())

			c.Assert(beganTx, qt.Equals, row.wantTx)
		})
	}
}

func TestCollectAllObjectsKeepsTheRecursiveShapeForPostgres(t *testing.T) {
	c := qt.New(t)

	// The default constructor is used wherever the dialect is not known, and
	// every real PostgreSQL-family server takes the recursive shape.
	writer := NewPostgreSQLWriterForRunner(nil, "public")

	c.Assert(writer.caps.Has(capability.CatalogRecursiveCTE), qt.IsTrue)
	c.Assert(writer.caps.Has(capability.DDLInsideTransaction), qt.IsTrue)
	// And every key that gates a branch, so its cleanup drops policies too.
	c.Assert(writer.caps.Has(capability.RowLevelSecurity), qt.IsTrue)
}

func TestWriterWithCapabilitiesCarriesTheServersAnswer(t *testing.T) {
	c := qt.New(t)

	writer := NewPostgreSQLWriterForRunnerWithCapabilities(
		nil, "public", "", capability.SpannerPostgres())

	// Both are what drive the two cleanup shapes, and both are measured facts
	// about the Spanner PostgreSQL interface rather than defaults.
	c.Assert(writer.caps.Has(capability.DDLInsideTransaction), qt.IsFalse)
	c.Assert(writer.caps.Has(capability.CatalogRecursiveCTE), qt.IsFalse)
}

// lastRecordedQuery is what recordingRunner saw. The cleanup query is built and
// handed to the connection in one call, so capturing it is enough.
var lastRecordedQuery string

// recordingRunner captures the SQL and delegates to a real *sql.DB pointed at a
// closed port.
//
// Returning a hand-made nil *sql.Row is not an option: Scan dereferences it.
// A real handle produces a genuine connection error, which is what every caller
// here is written to expect.
type recordingRunner struct{ db *sql.DB }

func newRecordingRunner(t *testing.T) recordingRunner {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("open recording handle: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return recordingRunner{db: db}
}

func (r recordingRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	lastRecordedQuery = query
	return r.db.QueryContext(ctx, query, args...)
}

func (r recordingRunner) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	lastRecordedQuery = query
	return r.db.ExecContext(ctx, query, args...)
}

func (r recordingRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	lastRecordedQuery = query
	return r.db.QueryRowContext(ctx, query, args...)
}

func (r recordingRunner) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	beganTx = true
	return r.db.BeginTx(ctx, opts)
}

func (r recordingRunner) Exec(query string, args ...any) (sql.Result, error) {
	lastRecordedQuery = query
	return r.db.Exec(query, args...)
}

func (r recordingRunner) Query(query string, args ...any) (*sql.Rows, error) {
	lastRecordedQuery = query
	return r.db.Query(query, args...)
}

func (r recordingRunner) QueryRow(query string, args ...any) *sql.Row {
	lastRecordedQuery = query
	return r.db.QueryRow(query, args...)
}
