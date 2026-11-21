package mysqlconnect_test

import (
	"database/sql/driver"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/connector"
	"ptah.run/internal/connector/mysqlconnect"
	"ptah.run/internal/dbschema/dbtest"
)

// describeServer answers the banner and, for SELECT DATABASE(), selected.
// It records every query it is asked.
type describeServer struct {
	selected driver.Value
	queries  []string
}

func (s *describeServer) answer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	s.queries = append(s.queries, query)
	results := map[string]dbtest.QueryResult{
		"SELECT VERSION()":  {Columns: []string{"VERSION()"}, Rows: [][]driver.Value{{"8.4.13"}}},
		"SELECT DATABASE()": {Columns: []string{"DATABASE()"}, Rows: [][]driver.Value{{s.selected}}},
	}
	return results[query], nil
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
// whole server and asks nothing.
func TestDescribe_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		selected    driver.Value
		wantSchema  string
		wantWhole   bool
		wantQueries []string
	}{
		{
			name: "a name the server folds", path: "/R_LC", selected: "r_lc", wantSchema: "r_lc",
			wantQueries: []string{"SELECT VERSION()", "SELECT DATABASE()"},
		},
		{
			name: "a name the server keeps", path: "/App", selected: "App", wantSchema: "App",
			wantQueries: []string{"SELECT VERSION()", "SELECT DATABASE()"},
		},
		{
			name: "a whole server", path: "/", wantWhole: true,
			wantQueries: []string{"SELECT VERSION()"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &describeServer{selected: test.selected}

			info, err := describe(c, server, test.path)

			c.Assert(err, qt.IsNil)
			c.Assert(info.Schema, qt.Equals, test.wantSchema)
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, test.wantSchema)
			c.Assert(info.WholeServer, qt.Equals, test.wantWhole)
			c.Assert(server.queries, qt.DeepEquals, test.wantQueries)
		})
	}
}

// A session that selected no database although the URL names one is refused
// rather than read as a whole server.
func TestDescribe_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := &describeServer{selected: nil}

	_, err := describe(c, server, "/app")

	c.Assert(err, qt.ErrorMatches, "the MySQL/MariaDB session selected no database although the URL names one")
}
