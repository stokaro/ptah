package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// commentedEnumCatalog answers a full schema read with one enum whose type
// carries comment, repeated on each of its value rows as the query returns it.
func commentedEnumCatalog(comment string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "SELECT EXISTS") {
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
		}
		if strings.Contains(query, "has_table_privilege") {
			return dbtest.QueryResult{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}}, nil
		}
		if strings.Contains(query, "pg_enum") {
			return dbtest.QueryResult{
				Columns: []string{"enum_name", "enum_value", "type_comment"},
				Rows:    [][]driver.Value{{"mood", "ok", comment}, {"mood", "bad", comment}},
			}, nil
		}
		return dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}, nil
	}
}

// An enum type's comment is read with its values, so a declared comment
// compares against what the server holds (stokaro/ptah#3646).
func TestReadSchemaContext_EnumCommentHappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, commentedEnumCatalog("feelings"))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	schema, err := reader.ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.Enums, qt.HasLen, 1)
	c.Assert(schema.Enums[0].Values, qt.DeepEquals, []string{"ok", "bad"})
	c.Assert(schema.Enums[0].Comment, qt.Equals, "feelings")
}
