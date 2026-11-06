package postgres

// White-box testing required: the property under test is which relations the
// reader's statements name, and a fake server answers a statement the same way
// whatever it selects from. Whether a role's privileges change the answer takes
// a live server, which integration/schema_read_role_privileges_e2e_test.go has.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// recordingEmptyServer answers a read as a database holding nothing, and records
// each statement without its comments. The two probes a read needs a row from
// answer that the relation is there and pg_authid is not readable.
func recordingEmptyServer(asked *[]string) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*asked = append(*asked, stripSQLComments(query))
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
		case strings.Contains(query, "has_table_privilege"):
			return dbtest.QueryResult{Columns: []string{"readable"}, Rows: [][]driver.Value{{false}}}, nil
		default:
			return dbtest.QueryResult{}, nil
		}
	}
}

// TestReadSchemaContext_AsksInformationSchemaNothing reads a whole schema and
// looks at every statement. information_schema answers only for what the
// connecting role holds a privilege on, so one read through it brings back a
// comparison that reports two different databases as synced. A new read that
// reaches for it fails here, whichever kind of object it describes.
func TestReadSchemaContext_AsksInformationSchemaNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "PostgreSQL 13", dialect: platform.Postgres, caps: capability.Postgres13()},
		{name: "PostgreSQL 16", dialect: platform.Postgres, caps: capability.Postgres16()},
		{name: "PostgreSQL 18", dialect: platform.Postgres, caps: capability.Postgres18()},
		{name: "YugabyteDB 2024", dialect: platform.YugabyteDB, caps: capability.YugabyteDB24()},
		{name: "YugabyteDB 2025", dialect: platform.YugabyteDB, caps: capability.YugabyteDB25()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []string
			db := dbtest.Open(c, recordingEmptyServer(&asked))
			reader := NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", test.dialect, test.caps)

			_, err := reader.ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)

			// The floor: a read that stopped early would pass with no statement
			// left to look at. Tables, columns, constraints, views and grants
			// are the reads the assertion below is about.
			c.Assert(asked, qt.Any(qt.Contains), "AS row_stats_unknown")
			c.Assert(asked, qt.Any(qt.Contains), "AS udt_schema")
			c.Assert(asked, qt.Any(qt.Contains), "tc.constraint_name")
			c.Assert(asked, qt.Any(qt.Contains), "pg_get_viewdef")
			c.Assert(asked, qt.Any(qt.Contains), "AS with_option")
			c.Assert(asked, qt.Not(qt.Any(qt.Contains)), "information_schema")
		})
	}
}
