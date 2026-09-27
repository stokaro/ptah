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

// deferredExcludeCatalog answers a full schema read with one EXCLUDE from the
// pg_constraint read, deferrable and deferring as given, spelled the way
// pg_get_constraintdef prints it.
func deferredExcludeCatalog(deferrable, deferred bool, definition string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return dbtest.QueryResult{Columns: []string{"exists"}, Rows: [][]driver.Value{{true}}}, nil
		case strings.Contains(query, "has_table_privilege"):
			return dbtest.QueryResult{Columns: []string{"has_table_privilege"}, Rows: [][]driver.Value{{false}}}, nil
		case strings.Contains(query, "c.contype IN ('x')"):
			return dbtest.QueryResult{
				Columns: []string{
					"schema_name", "constraint_name", "table_name", "constraint_type",
					"constraint_definition", "required_extensions", "constraint_comment",
					"condeferrable", "condeferred",
				},
				Rows: [][]driver.Value{{
					"public", "ex_r", "slots", "x", definition, "[]", "", deferrable, deferred,
				}},
			}, nil
		}
		return dbtest.QueryResult{Columns: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}, nil
	}
}

// TestReadSchemaContext_ExcludeDeferral reads the deferral of an EXCLUDE from
// pg_constraint, as the read of every other constraint does, and keeps the
// clause pg_get_constraintdef appends out of the predicate.
func TestReadSchemaContext_ExcludeDeferral(t *testing.T) {
	tests := []struct {
		name          string
		deferrable    bool
		deferred      bool
		definition    string
		wantInitially string
	}{
		{name: "not deferrable", definition: "EXCLUDE USING btree (r WITH =) WHERE ((r > 0))"},
		{
			name: "deferrable", deferrable: true, wantInitially: "immediate",
			definition: "EXCLUDE USING btree (r WITH =) WHERE ((r > 0)) DEFERRABLE",
		},
		{
			name: "deferred", deferrable: true, deferred: true, wantInitially: "deferred",
			definition: "EXCLUDE USING btree (r WITH =) WHERE ((r > 0)) DEFERRABLE INITIALLY DEFERRED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, deferredExcludeCatalog(test.deferrable, test.deferred, test.definition))
			reader := postgres.NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())

			schema, err := reader.ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Constraints, qt.HasLen, 1)
			c.Assert(schema.Constraints[0].Deferrable, qt.Equals, test.deferrable)
			c.Assert(schema.Constraints[0].Initially, qt.Equals, test.wantInitially)
			c.Assert(*schema.Constraints[0].WhereCondition, qt.Equals, "(r > 0)")
		})
	}
}
