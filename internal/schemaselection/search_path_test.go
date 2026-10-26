package schemaselection_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/schemaselection"
)

// searchPathServer answers current_schemas(false) with path and records each
// statement it is sent, with its arguments, in order.
type searchPathServer struct {
	path []string
	mu   sync.Mutex
	sent []string
}

func (s *searchPathServer) record(kind, query string, args []driver.NamedValue) {
	values := make([]any, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, fmt.Sprintf("%s %s %v", kind, query, values))
}

func (s *searchPathServer) query(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	s.record("query", query, args)
	rows := make([][]driver.Value, 0, len(s.path))
	for _, schema := range s.path {
		rows = append(rows, []driver.Value{schema})
	}
	return dbtest.QueryResult{Columns: []string{"s"}, Rows: rows}, nil
}

func (s *searchPathServer) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	s.record("exec", query, args)
	return driver.RowsAffected(0), nil
}

const searchPathQuery = "SELECT s FROM unnest(current_schemas(false)) WITH ORDINALITY AS path(s, n) ORDER BY n"

// TestResolveSearchPath_HappyPath pins the question a session is asked: every
// schema an unqualified name resolves in, in the order the server searches
// them, which on `search_path=reporting,public` is both.
func TestResolveSearchPath_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := &searchPathServer{path: []string{"reporting", "public"}}
	db := dbtest.OpenWithExec(t, server.query, server.exec)

	path, err := schemaselection.FromURL("postgres://localhost/app?search_path=reporting,public").ResolveSearchPath(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.DeepEquals, []string{"reporting", "public"})
	c.Assert(server.sent, qt.DeepEquals, []string{"query " + searchPathQuery + " []"})
}

// TestResolveCarriedSearchPath_HappyPath pins the path of a connection that
// could not send its selection: the URL's search_path is set with is_local
// inside a transaction, the path is read there, and the transaction is rolled
// back, so a pooled backend keeps no setting. Read outside the transaction,
// the answer is the pooler's default path.
func TestResolveCarriedSearchPath_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := &searchPathServer{path: []string{"reporting", "public"}}
	db := dbtest.OpenWithExec(t, server.query, server.exec)

	path, err := schemaselection.FromURL("postgres://localhost/app?search_path=reporting,public").ResolveCarriedSearchPath(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(path, qt.DeepEquals, []string{"reporting", "public"})
	c.Assert(server.sent, qt.DeepEquals, []string{
		"exec SELECT set_config($1, $2, true) [search_path reporting,public]",
		"query " + searchPathQuery + " []",
	})
	c.Assert(db.BeginCount(), qt.Equals, 1)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
	c.Assert(db.CommitCount(), qt.Equals, 0)
}

// TestResolveSearchPath_FailurePath pins a server that cannot answer: the
// error names the question, and no path is returned.
func TestResolveSearchPath_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, errors.New("function current_schemas(boolean) does not exist")
	})

	path, err := schemaselection.FromURL("postgres://localhost/app").ResolveSearchPath(c.Context(), db.SQL)

	c.Assert(err, qt.ErrorMatches, `failed to resolve PostgreSQL search path: function current_schemas\(boolean\) does not exist`)
	c.Assert(path, qt.IsNil)
}
