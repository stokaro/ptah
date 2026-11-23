package mysqlconnect_test

import (
	"database/sql/driver"
	"errors"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/connector"
	"ptah.run/internal/connector/mysqlconnect"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/mysqlname"
)

// describeServer answers the banner, folds for whether the server folds
// names, and, for SELECT DATABASE(), selected. A non-nil foldsErr fails the
// folding read. It records every query it is asked.
type describeServer struct {
	selected driver.Value
	folds    int64
	foldsErr error
	queries  []string
}

func (s *describeServer) answer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	s.queries = append(s.queries, query)
	results := map[string]dbtest.QueryResult{
		"SELECT VERSION()":        {Columns: []string{"VERSION()"}, Rows: [][]driver.Value{{"8.4.13"}}},
		"SELECT DATABASE()":       {Columns: []string{"DATABASE()"}, Rows: [][]driver.Value{{s.selected}}},
		mysqlname.FoldsNamesQuery: {Columns: []string{"folds"}, Rows: [][]driver.Value{{s.folds}}},
	}
	errs := map[string]error{mysqlname.FoldsNamesQuery: s.foldsErr}
	return results[query], errs[query]
}

// describe runs Describe for a URL whose path is path.
func describe(c *qt.C, server *describeServer, path string) (catalog.ServerInfo, error) {
	c.Helper()
	db := dbtest.Open(c, server.answer)
	return mysqlconnect.Service{}.Describe(c.Context(), &connector.Pool{DB: db.SQL},
		connector.Request{Parsed: &url.URL{Scheme: "mysql", Host: "localhost", Path: path}}, catalog.ServerInfo{})
}

// The schema is the name the server gives the database the session selected.
// A server started with lower_case_table_names=1 folds the name a URL spells,
// and every catalog row names the folded database, so the URL's spelling keys
// a description by a name no object carries. A URL naming no database is a
// whole server and selects nothing. Whether the server folds database names is
// read on every connection, as the server answers it.
func TestDescribe_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		selected    driver.Value
		folds       int64
		wantSchema  string
		wantWhole   bool
		wantFolding catalog.NameFolding
		wantQueries []string
	}{
		{
			name: "a name the server folds", path: "/R_LC", selected: "r_lc", folds: 1, wantSchema: "r_lc",
			wantFolding: catalog.NameFoldingFolds,
			wantQueries: []string{"SELECT VERSION()", mysqlname.FoldsNamesQuery, "SELECT DATABASE()"},
		},
		{
			name: "a name the server keeps", path: "/App", selected: "App", wantSchema: "App",
			wantFolding: catalog.NameFoldingExact,
			wantQueries: []string{"SELECT VERSION()", mysqlname.FoldsNamesQuery, "SELECT DATABASE()"},
		},
		{
			name: "a whole server that folds", path: "/", folds: 1, wantWhole: true, wantFolding: catalog.NameFoldingFolds,
			wantQueries: []string{"SELECT VERSION()", mysqlname.FoldsNamesQuery},
		},
		{
			name: "a whole server that keeps names apart", path: "/", wantWhole: true, wantFolding: catalog.NameFoldingExact,
			wantQueries: []string{"SELECT VERSION()", mysqlname.FoldsNamesQuery},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &describeServer{selected: test.selected, folds: test.folds}

			info, err := describe(c, server, test.path)

			c.Assert(err, qt.IsNil)
			c.Assert(info.Schema, qt.Equals, test.wantSchema)
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, test.wantSchema)
			c.Assert(info.WholeServer, qt.Equals, test.wantWhole)
			c.Assert(info.NameFolding, qt.Equals, test.wantFolding)
			c.Assert(server.queries, qt.DeepEquals, test.wantQueries)
		})
	}
}

// A session that selected no database although the URL names one is refused
// rather than read as a whole server.
func TestDescribe_FailurePath(t *testing.T) {
	t.Run("no database selected", func(t *testing.T) {
		c := qt.New(t)
		server := &describeServer{selected: nil}

		_, err := describe(c, server, "/app")

		c.Assert(err, qt.ErrorMatches, "the MySQL/MariaDB session selected no database although the URL names one")
	})
	// A connection that could not learn whether the server folds names is
	// refused rather than taken to compare names as bytes, which on a server
	// that folds drops a revoke that spells the database in another case.
	t.Run("the folding read fails", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("refused")
		server := &describeServer{selected: "app", foldsErr: refused}

		_, err := describe(c, server, "/app")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, "failed to read whether the MySQL/MariaDB server folds database names: .*")
	})
}
