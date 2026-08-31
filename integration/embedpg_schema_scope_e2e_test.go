//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedstore"
)

// TestEmbedPGSchemaScopeE2E is stokaro/ptah#2629 measured against the server
// that produces it.
//
// Every generated object name and every catalog read used a bare relation name,
// so `search_path` decided which table a statement meant. Two same-named tables
// in two schemas is not an exotic arrangement -- it is what a tenant-per-schema
// database looks like -- and it made four things go wrong at once, each of them
// silently:
//
//   - retiring a generation in one schema dropped the vector columns and the
//     index of a LIVE generation in another, exit 0, reporting the named one as
//     gone with a row count read from the wrong table;
//   - one outbox table, one capture function and one pair of trigger names
//     served both, so preparing the second rewrote the first's capture function
//     and every insert and delete on the first source table then failed;
//   - the installation check answered yes for a table carrying no trigger,
//     because a like-named one elsewhere had them;
//   - the pointer was keyed on the table alone, so a cutover in one schema
//     moved the other schema's readers.
//
// It needs a live server because every one of those is `search_path` resolving
// a name, which nothing below the database can be asked about.
func TestEmbedPGSchemaScopeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_scope_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	for _, schema := range []string{"tenant_a", "tenant_b"} {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA %q`, schema))
		c.Assert(err, qt.IsNil)
		_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q.notes (
			id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`, schema))
		c.Assert(err, qt.IsNil)
	}

	assertEachSchemaGetsItsOwnOutbox(c, ctx, db)
	assertAnotherSchemasTriggersDoNotCountAsInstalled(c, ctx, db)
	assertRetiringOneSchemaLeavesTheOtherIntact(c, ctx, db)
	assertTwoSchemasHaveTwoPointers(c, ctx, db)
}

// scopedSpec is one tenant's generation over its own `notes` table.
func scopedSpec(schema string) embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: schema, Table: "notes",
			KeyFields: []string{"id"}, InputFields: []string{"title", "body"},
			VersionStrategy: embedgen.VersionUpdatedAt, VersionField: "updated_at",
		},
		Preprocessing: embedgen.Preprocessing{Separator: "\n", NullPolicy: embedgen.NullAsEmpty},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "fake-model", Revision: "1", ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: schema, Table: "notes", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}

// assertEachSchemaGetsItsOwnOutbox is the collision at its source.
//
// The names have to differ, and both installations have to keep working after
// the other one is installed: `Install` issues CREATE OR REPLACE FUNCTION, so a
// shared name means the second prepare rewrites the first's capture function to
// read its own key and version columns. The write that proves it is an INSERT
// into the FIRST table after the second is installed -- under the shared name
// it failed with `record "new" has no field ...`.
func assertEachSchemaGetsItsOwnOutbox(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	first, err := embedpg.NewOutbox(db, scopedSpec("tenant_a"))
	c.Assert(err, qt.IsNil)
	second, err := embedpg.NewOutbox(db, scopedSpec("tenant_b"))
	c.Assert(err, qt.IsNil)

	c.Assert(first.TableName(), qt.Not(qt.Equals), second.TableName())
	c.Assert(first.FunctionName(), qt.Not(qt.Equals), second.FunctionName())
	c.Assert(first.TriggerNames()[0], qt.Not(qt.Equals), second.TriggerNames()[0])
	c.Assert(first.TriggerNames()[1], qt.Not(qt.Equals), second.TriggerNames()[1])

	c.Assert(first.Install(ctx), qt.IsNil)
	c.Assert(second.Install(ctx), qt.IsNil)

	_, err = db.ExecContext(ctx,
		`INSERT INTO tenant_a.notes (id, title, body, updated_at) VALUES (1, 'A', 'a', '1')`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`INSERT INTO tenant_b.notes (id, title, body, updated_at) VALUES (1, 'B', 'b', '1')`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `DELETE FROM tenant_a.notes WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`INSERT INTO tenant_a.notes (id, title, body, updated_at) VALUES (1, 'A', 'a', '2')`)
	c.Assert(err, qt.IsNil)

	// Three events for the first tenant and one for the second, each in its own
	// outbox. A shared stream would put all four in one.
	firstEvents, _, err := first.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(firstEvents, qt.HasLen, 3)
	secondEvents, _, err := second.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(secondEvents, qt.HasLen, 1)
}

// assertAnotherSchemasTriggersDoNotCountAsInstalled is the false positive.
//
// A run whose source carries no trigger and reports itself as capturing changes
// is the worst state available: the operator believes every write is recorded,
// and none is.
func assertAnotherSchemasTriggersDoNotCountAsInstalled(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	_, err := db.ExecContext(ctx, `CREATE SCHEMA tenant_c`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TABLE tenant_c.notes (
		id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)

	bare, err := embedpg.NewOutbox(db, scopedSpec("tenant_c"))
	c.Assert(err, qt.IsNil)

	// Triggers carrying tenant_c's EXACT names, on another schema's table. The
	// disambiguated object names make this arrangement unreachable through
	// Install, so the fixture builds it by hand -- without that, this assertion
	// passes against a relname-matching query too and measures nothing, because
	// the names it looks for would not exist anywhere.
	for _, name := range bare.TriggerNames() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			`CREATE TRIGGER %q AFTER INSERT ON tenant_a.notes
			 FOR EACH ROW EXECUTE FUNCTION %q()`, name, otherFunctionName(c, db, ctx)))
		c.Assert(err, qt.IsNil)
	}

	installed, err := bare.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsFalse,
		qt.Commentf("tenant_c.notes has no trigger, and tenant_a.notes carrying two by those names is not its business"))

	// The control: the tenant that IS installed still reports so, which is what
	// separates the fix from an installation check that answers no to everything.
	first, err := embedpg.NewOutbox(db, scopedSpec("tenant_a"))
	c.Assert(err, qt.IsNil)
	installed, err = first.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsTrue)
}

// otherFunctionName is a trigger function the fixture can attach anywhere.
func otherFunctionName(c *qt.C, db *sql.DB, ctx context.Context) string {
	c.Helper()
	_, err := db.ExecContext(ctx, `CREATE OR REPLACE FUNCTION scope_fixture_noop()
		RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`)
	c.Assert(err, qt.IsNil)
	return "scope_fixture_noop"
}

// assertRetiringOneSchemaLeavesTheOtherIntact is the destructive half.
//
// Retirement reads the target's location out of the REGISTRY, which recorded no
// schema, so the DDL it built resolved through search_path. Retiring tenant_b's
// generation destroyed tenant_a's five columns and its index, exited 0, and
// reported "is gone, with 0 vectors" -- the count taken from the wrong table
// too. It cannot be undone.
func assertRetiringOneSchemaLeavesTheOtherIntact(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	// TEXT rather than vector(4): what is under test is which TABLE the DDL
	// reaches, and a column type would only add an extension this database does
	// not need. The index is real, and like-named in both schemas, because
	// RetireIndex dropped one by bare name too.
	for _, schema := range []string{"tenant_a", "tenant_b"} {
		for _, suffix := range append([]string{""}, embedpg.MetadataSuffixes()...) {
			_, err := db.ExecContext(ctx, fmt.Sprintf(
				`ALTER TABLE %q.notes ADD COLUMN %q TEXT`, schema, "embedding"+suffix))
			c.Assert(err, qt.IsNil)
		}
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			`CREATE INDEX notes_embedding_idx ON %q.notes (embedding)`, schema))
		c.Assert(err, qt.IsNil)
	}

	retiring := embedstore.Generation{
		Identity: "gen-tenant-b", TargetSchema: "tenant_b",
		TargetTable: "notes", TargetColumn: "embedding",
	}
	c.Assert(embedpg.RetireColumns(ctx, db, retiring), qt.IsNil)

	c.Assert(columnCount(c, ctx, db, "tenant_b"), qt.Equals, 0)
	// The whole point: the other tenant's generation is untouched.
	c.Assert(columnCount(c, ctx, db, "tenant_a"), qt.Equals, 1+len(embedpg.MetadataSuffixes()))
	// Dropping the columns took tenant_b's index with them, and left the other
	// schema's like-named index where it was.
	c.Assert(indexCount(c, ctx, db, "tenant_b"), qt.Equals, 0)
	c.Assert(indexCount(c, ctx, db, "tenant_a"), qt.Equals, 1)
}

// columnCount is how many of a generation's columns a schema's table still has.
func columnCount(c *qt.C, ctx context.Context, db *sql.DB, schema string) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'notes' AND column_name LIKE 'embedding%'`,
		schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// indexCount is how many of a generation's indexes a schema's table still has.
func indexCount(c *qt.C, ctx context.Context, db *sql.DB, schema string) int {
	c.Helper()
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = $1 AND tablename = 'notes' AND indexname = 'notes_embedding_idx'`,
		schema).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// assertTwoSchemasHaveTwoPointers keeps one tenant's cutover out of another's.
//
// Keyed on the table alone, the second MovePointer was a compare-and-set
// against the FIRST tenant's active generation, so it either refused a correct
// cutover or moved readers who had not asked to move.
func assertTwoSchemasHaveTwoPointers(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)

	at := time.Now().UTC().Truncate(time.Second)
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "tenant_a", TargetTable: "notes", Active: "gen-a", CutOverAt: at,
	}, ""), qt.IsNil)
	// The same table name in another schema is a FIRST cutover, so it expects
	// no pointer -- and it gets one rather than colliding with tenant_a's.
	c.Assert(store.MovePointer(ctx, embedstore.Pointer{
		TargetSchema: "tenant_b", TargetTable: "notes", Active: "gen-b", CutOverAt: at,
	}, ""), qt.IsNil)

	a, err := store.Pointer(ctx, "tenant_a", "notes")
	c.Assert(err, qt.IsNil)
	c.Assert(a.Active, qt.Equals, "gen-a")
	b, err := store.Pointer(ctx, "tenant_b", "notes")
	c.Assert(err, qt.IsNil)
	c.Assert(b.Active, qt.Equals, "gen-b")
}
