package postgres_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// postgresWriter is a writer for dialect that sends nothing to a server.
func postgresWriter(dialect string) *postgres.PostgreSQLWriter {
	return postgres.NewPostgreSQLWriterForRunnerWithCapabilities(nil, "public", dialect, capability.ForDialect(platform.Postgres))
}

// TestCleanupCatalog_OffersTheInventoryWhereItWasMeasured pins where the
// PostgreSQL family's writer offers its catalog strategy: on PostgreSQL, whose
// descriptor states the cleanup was measured against its catalog, and on no
// other member it serves, nor on a writer built without a dialect.
func TestCleanupCatalog_OffersTheInventoryWhereItWasMeasured(t *testing.T) {
	tests := []struct {
		dialect string
		offered bool
	}{
		{dialect: platform.Postgres, offered: true},
		{dialect: "postgresql", offered: true},
		{dialect: platform.CockroachDB, offered: false},
		{dialect: platform.YugabyteDB, offered: false},
		{dialect: platform.Spanner, offered: false},
		{dialect: "", offered: false},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(postgresWriter(test.dialect).CleanupCatalog() != nil, qt.Equals, test.offered)
		})
	}
}

// runtimeRows answers the runtime inventory with rows and every other read,
// the default privileges among them, with nothing.
func runtimeRows(rows ...[]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "runtime_objects") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"object_kind", "object_schema", "object_name", "selector_name", "is_implicit", "owner_table", "drop_statement"},
			Rows:    rows,
		}, nil
	}
}

func TestCleanupCatalog_Objects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, runtimeRows(
		[]driver.Value{"sequence", "public", "users_id_seq", "users_id_seq", true, "users", "DROP SEQUENCE users_id_seq"},
		[]driver.Value{" FOREIGN_TABLE ", "public", "remote", "remote", false, nil, "DROP FOREIGN TABLE remote"},
		[]driver.Value{"aggregate", "public", "total(integer)", "total", false, nil, "DROP AGGREGATE total"},
		[]driver.Value{"collation", "public", "ci", "ci", false, nil, "DROP COLLATION ci"},
		[]driver.Value{"function", "public", "win()", "win", false, nil, "DROP FUNCTION win"},
		[]driver.Value{"procedure", "public", "touch()", "touch", false, nil, "DROP PROCEDURE touch"},
	))

	objects, err := postgresWriter(platform.Postgres).CleanupCatalog().Objects(c.Context(), db.SQL, "")

	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []dbreset.CleanupObject{
		{Kind: "sequence", Schema: "public", Table: "users", Name: "users_id_seq", SelectorName: "users_id_seq",
			Implicit: true, Statement: "DROP SEQUENCE users_id_seq"},
		{Kind: "foreign_table", Schema: "public", Name: "remote", SelectorName: "remote", Statement: "DROP FOREIGN TABLE remote"},
		{Kind: "aggregate", Schema: "public", Name: "total(integer)", SelectorName: "total", Statement: "DROP AGGREGATE total"},
		{Kind: "collation", Schema: "public", Name: "ci", SelectorName: "ci", Statement: "DROP COLLATION ci"},
		{Kind: "function", Schema: "public", Name: "win()", SelectorName: "win", Statement: "DROP FUNCTION win"},
		{Kind: "procedure", Schema: "public", Name: "touch()", SelectorName: "touch", Statement: "DROP PROCEDURE touch"},
	})
}

func TestCleanupCatalog_Objects_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, runtimeRows(
		[]driver.Value{"extension", "public", "hstore", "hstore", false, nil, "DROP EXTENSION hstore"},
	))

	objects, err := postgresWriter(platform.Postgres).CleanupCatalog().Objects(c.Context(), db.SQL, "public")

	c.Assert(err, qt.ErrorMatches, `inspect PostgreSQL runtime objects: unsupported object kind "extension"`)
	c.Assert(objects, qt.IsNil)
}

// viewRows answers the view dependency read with rows.
func viewRows(rows ...[]driver.Value) dbtest.QueryHandler {
	return func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"relname", "relkind", "depth"}, Rows: rows}, nil
	}
}

func TestCleanupCatalog_ViewDepths(t *testing.T) {
	tests := []struct {
		name string
		rows [][]driver.Value
		want []dbreset.ViewDepth
	}{
		{
			name: "views and materialized views",
			rows: [][]driver.Value{{"mv_users", "m", int64(1)}, {"v_users", "v", int64(0)}},
			want: []dbreset.ViewDepth{{Kind: "materialized_view", Name: "mv_users", Depth: 1}, {Kind: "view", Name: "v_users"}},
		},
		{name: "a schema with no view", rows: nil, want: make([]dbreset.ViewDepth, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, viewRows(test.rows...))

			depths, err := postgresWriter(platform.Postgres).CleanupCatalog().ViewDepths(c.Context(), db.SQL, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(depths, qt.DeepEquals, test.want)
		})
	}
}

// recordedStatements is an executor that records what it is asked to run.
type recordedStatements struct {
	statements []string
}

func (r *recordedStatements) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	r.statements = append(r.statements, statement)
	return nil
}

func TestCleanupCatalog_LockRelations(t *testing.T) {
	tests := []struct {
		name      string
		relations []dbreset.Relation
		want      []string
	}{
		{
			name: "every relation once, in name order",
			relations: []dbreset.Relation{
				{Schema: "tenant", Name: "users"}, {Schema: "public", Name: "remote_users"}, {Schema: "tenant", Name: "users"},
			},
			want: []string{`LOCK TABLE "public"."remote_users", "tenant"."users" IN ACCESS EXCLUSIVE MODE`},
		},
		{name: "no relation", relations: nil, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			executor := &recordedStatements{}

			err := postgresWriter(platform.Postgres).CleanupCatalog().LockRelations(c.Context(), executor, test.relations)

			c.Assert(err, qt.IsNil)
			c.Assert(executor.statements, qt.DeepEquals, test.want)
		})
	}
}
