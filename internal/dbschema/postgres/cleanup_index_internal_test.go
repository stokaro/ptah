package postgres

// White-box testing required: collectStandaloneIndexes, withIndexesDroppedFirst
// and the cleanup object they produce are all unexported, and the only exported
// caller is DropAllTables, which needs a live server that refuses a table drop
// to reach them. What is under test is which server gets asked and where the
// answer lands in the drop order, both of which are decided before any
// statement runs.

import (
	"context"
	"database/sql/driver"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// twoIndexes is what the index query returns for a table carrying a unique and
// a plain index beside its primary key. The primary key's own index is absent
// because the query filters it out, and a server that stopped filtering would
// be asked to drop an index it refuses to drop on its own.
func twoIndexes() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"nspname", "relname"},
		Rows: [][]driver.Value{
			{"public", "dfp_plain"},
			{"public", "dfp_uq"},
		},
	}
}

// TestWithIndexesDroppedFirstOnlyAsksWhereTheDropIsBlocked pins both halves of
// the gate: the target that needs the indexes gets them first, and every other
// target is not asked at all.
//
// The PostgreSQL row is the one that carries the risk. Cloud Spanner refuses
// `DROP TABLE dfp` with `Cannot drop table dfp with indices: dfp_uq`
// (SQLSTATE 0A000) while every other PostgreSQL-family server drops a table's
// indexes with the table, so a fix that queried everywhere would add statements
// to a cleanup that was already correct -- and, on a server whose unique
// constraints own their indexes, statements that server refuses
// (stokaro/ptah#1901).
//
// Asserting on the recorded queries rather than on the returned slice alone is
// deliberate: returning the objects unchanged after asking anyway would satisfy
// an order-only assertion while still issuing the query.
func TestWithIndexesDroppedFirstOnlyAsksWhereTheDropIsBlocked(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		// before is what the cleanup query already found ahead of the tables.
		// A foreign key belongs here: Cloud Spanner backs one with an index and
		// refuses to drop that index while the key still names it, so an index
		// step that ran before the constraints would trade one blocked drop for
		// another.
		before []postgresCleanupObject
		// wantAsked is whether the server is queried for indexes at all.
		wantAsked bool
		// wantOrder is the object name order the cleanup then drops in.
		wantOrder []string
	}{
		{
			name:      "a server that blocks the drop drops the indexes before its tables",
			caps:      capability.SpannerPostgres(),
			wantAsked: true,
			wantOrder: []string{"dfp_plain", "dfp_uq", "dfp"},
		},
		{
			name:      "and after the foreign keys that hold them",
			caps:      capability.SpannerPostgres(),
			before:    []postgresCleanupObject{{Kind: "constraint", Schema: "public", Name: "fk_dept_manager", Qualifier: "departments"}},
			wantAsked: true,
			wantOrder: []string{"fk_dept_manager", "dfp_plain", "dfp_uq", "dfp"},
		},
		{
			name:      "a server that drops indexes with the table is not asked",
			caps:      capability.Postgres16(),
			wantAsked: false,
			wantOrder: []string{"dfp"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var asked []string
			db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = append(asked, query)
				return twoIndexes(), nil
			})

			writer := NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public", test.caps)
			table := postgresCleanupObject{Kind: "table", Schema: "public", Name: "dfp"}
			found := append(slices.Clone(test.before), table)
			objects, err := writer.withIndexesDroppedFirst(
				context.Background(), db.SQL, found)
			c.Assert(err, qt.IsNil)

			names := make([]string, 0, len(objects))
			for _, object := range objects {
				names = append(names, object.Name)
			}
			c.Assert(names, qt.DeepEquals, test.wantOrder)
			c.Assert(len(asked) > 0, qt.Equals, test.wantAsked)
		})
	}
}

// TestStandaloneIndexQueryLeavesThePrimaryKeyAlone pins the one filter the query
// carries.
//
// A primary key's index cannot be dropped on its own -- on Cloud Spanner the
// key IS the storage -- so a query that returned it would turn a cleanup that
// fails on the table drop into one that fails a step earlier. pg_depend would
// be the PostgreSQL way to say this, and the target that needs the query is
// exactly the one whose catalog has no pg_depend.
func TestStandaloneIndexQueryLeavesThePrimaryKeyAlone(t *testing.T) {
	c := qt.New(t)

	var asked string
	db := dbtest.Open(t, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		asked = query
		return twoIndexes(), nil
	})

	writer := NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public", capability.SpannerPostgres())
	_, err := writer.collectStandaloneIndexes(context.Background(), db.SQL)
	c.Assert(err, qt.IsNil)
	c.Assert(asked, qt.Contains, "NOT i.indisprimary",
		qt.Commentf("the index query must exclude the primary key's index"))
	c.Assert(asked, qt.Not(qt.Contains), "pg_depend",
		qt.Commentf("the target this query is for has no pg_depend"))
}

// TestBuildCleanupStatementForAnIndex pins the statement the collector attaches.
//
// The spelling is not a guess: `DROP INDEX IF EXISTS "public"."dfp_uq" RESTRICT`
// was accepted by the Cloud Spanner emulator behind PGAdapter 0.55.2, which is
// the server that has to run it. It goes through the same builder as every
// other kind, so a kind added to the verb table without a statement to build
// would fail here rather than silently drop nothing.
func TestBuildCleanupStatementForAnIndex(t *testing.T) {
	c := qt.New(t)

	statement, err := buildCleanupStatement(
		postgresCleanupObject{Kind: "index", Schema: "public", Name: "dfp_uq"},
		capability.SpannerPostgres(),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statement, qt.Equals, `DROP INDEX IF EXISTS "public"."dfp_uq" RESTRICT`)
}
