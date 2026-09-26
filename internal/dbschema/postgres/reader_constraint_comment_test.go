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

// commentedConstraintCatalog answers a full schema read with a CHECK
// constraint from the information_schema read and an EXCLUDE constraint from
// the pg_constraint read, each carrying comment.
func commentedConstraintCatalog(comment string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
		case strings.Contains(query, "has_table_privilege"):
			return dbtest.QueryResult{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}}, nil
		case strings.Contains(query, "information_schema.table_constraints AS tc"):
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema", "table_name", "constraint_name", "constraint_type", "columns",
					"foreign_schema", "foreign_table", "foreign_columns", "delete_rule", "update_rule",
					"deferrable", "deferred", "check_clause", "definition", "comment",
				},
				Rows: [][]driver.Value{{
					"public", "orders", "ck_total", "CHECK", "total", "", "", "", "", "",
					false, false, "total > 0", "CHECK ((total > 0))", comment,
				}},
			}, nil
		case strings.Contains(query, "c.contype IN ('x')"):
			return dbtest.QueryResult{
				Columns: []string{
					"schema_name", "constraint_name", "table_name", "constraint_type",
					"constraint_definition", "required_extensions", "constraint_comment",
				},
				Rows: [][]driver.Value{{
					"public", "ex_room", "bookings", "x", "EXCLUDE USING gist (room WITH =)", "[]", comment,
				}},
			}, nil
		}
		return dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}, nil
	}
}

// A constraint's comment is read with the constraint, from both reads that
// report constraints, so a declared comment compares against what the server
// holds (stokaro/ptah#3678).
func TestReadSchemaContext_ConstraintCommentHappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, commentedConstraintCatalog("a total is positive"))
	reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

	schema, err := reader.ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	comments := make(map[string]string, len(schema.Constraints))
	for _, constraint := range schema.Constraints {
		comments[constraint.Name] = constraint.Comment
	}
	c.Assert(comments, qt.DeepEquals, map[string]string{
		"ck_total": "a total is positive",
		"ex_room":  "a total is positive",
	})
}
