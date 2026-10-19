package postgres_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
)

// dependentQueries records what the dependents read was asked, and answers
// it with rows.
type dependentQueries struct {
	queries []string
	args    [][]any
}

func (d *dependentQueries) answer(rows ...[]driver.Value) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		d.queries = append(d.queries, query)
		values := make([]any, 0, len(args))
		for _, arg := range args {
			values = append(values, arg.Value)
		}
		d.args = append(d.args, values)
		return dbtest.QueryResult{Columns: []string{"relname", "relname", "relkind"}, Rows: rows}, nil
	}
}

// TestCleanupDependents_HappyPath pins the dependents of the named relations
// with each relkind spelled for a diagnostic, and the names bound to one
// placeholder each.
func TestCleanupDependents_HappyPath(t *testing.T) {
	c := qt.New(t)
	recorded := &dependentQueries{}
	db := dbtest.Open(c, recorded.answer(
		[]driver.Value{"users", "active_users", "v"},
		[]driver.Value{"users", "user_totals", "m"},
		[]driver.Value{"posts", "posts_copy", "r"},
		[]driver.Value{"posts", "posts_by_month", "p"},
		[]driver.Value{"posts", "posts_seq", "S"},
	))

	dependents, err := postgresWriter(platform.CockroachDB).CleanupDependents(c.Context(), db.SQL, []string{"users", "posts"})

	c.Assert(err, qt.IsNil)
	c.Assert(dependents, qt.DeepEquals, []dbreset.Dependent{
		{Source: "users", Name: "active_users", Kind: "view"},
		{Source: "users", Name: "user_totals", Kind: "materialized view"},
		{Source: "posts", Name: "posts_copy", Kind: "table"},
		{Source: "posts", Name: "posts_by_month", Kind: "table"},
		{Source: "posts", Name: "posts_seq", Kind: "object"},
	})
	c.Assert(recorded.queries, qt.HasLen, 1)
	c.Assert(recorded.queries[0], qt.Contains, "AND source.relname IN ($1, $2)")
	c.Assert(recorded.queries[0], qt.Contains, "WHERE n.nspname = current_schema()")
	c.Assert(recorded.args, qt.DeepEquals, [][]any{{"users", "posts"}})
}

// TestCleanupDependents_AsksNothingForNoNames pins that no names reach no
// catalog: an empty IN list is a syntax error on the server.
func TestCleanupDependents_AsksNothingForNoNames(t *testing.T) {
	c := qt.New(t)
	recorded := &dependentQueries{}
	db := dbtest.Open(c, recorded.answer())

	dependents, err := postgresWriter(platform.Postgres).CleanupDependents(c.Context(), db.SQL, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(dependents, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

func TestCleanupDependents_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("relation pg_rewrite does not exist")
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, refused
	})

	dependents, err := postgresWriter(platform.YugabyteDB).CleanupDependents(c.Context(), db.SQL, []string{"users"})

	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(dependents, qt.IsNil)
}
