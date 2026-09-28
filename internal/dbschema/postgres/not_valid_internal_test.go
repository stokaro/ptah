package postgres

// White-box testing required: postgresNotValidFromDefinition is unexported, and
// a fake server can only hand back the definition text it parses, so the parse
// is pinned here against the text each server printed, and the constraint read
// is driven directly to show the flag reaches the constraint. The live tests in
// integration/ read the clause off a real server.

import (
	"database/sql/driver"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestPostgresNotValidFromDefinition pins the parse against the definitions
// pg_get_constraintdef printed on PostgreSQL 18.6, CockroachDB v26.3.2 and
// YugabyteDB 2026.1.2 (stokaro/ptah#3853).
func TestPostgresNotValidFromDefinition(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       bool
	}{
		{name: "a CHECK added NOT VALID", definition: "CHECK ((a < 100)) NOT VALID", want: true},
		{name: "a validated CHECK", definition: "CHECK ((a > 0))"},
		{
			name:       "a foreign key with every clause before NOT VALID",
			definition: "FOREIGN KEY (p) REFERENCES odp(id) ON DELETE SET NULL (p) DEFERRABLE INITIALLY DEFERRED NOT VALID",
			want:       true,
		},
		{name: "a validated foreign key", definition: "FOREIGN KEY (p) REFERENCES nvp(id)"},
		{
			name:       "a NOT ENFORCED CHECK, which PostgreSQL 18.6 records unvalidated and prints without NOT VALID",
			definition: "CHECK ((a > 0)) NOT ENFORCED",
		},
		{name: "a quoted referenced table that spells the clause", definition: `FOREIGN KEY (p) REFERENCES "x NOT VALID"(id)`},
		{name: "a string in the condition that spells the clause", definition: `CHECK ((a <> ' NOT VALID'::text))`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(postgresNotValidFromDefinition(test.definition), qt.Equals, test.want)
		})
	}
}

// TestReadBasicConstraintsForSchema_ReadsNotValid reads the clause off the
// definition the constraint query returns, for a CHECK and a foreign key, as
// PostgreSQL 18.6 prints them.
func TestReadBasicConstraintsForSchema_ReadsNotValid(t *testing.T) {
	c := qt.New(t)
	columns := []string{
		"table_schema", "table_name", "constraint_name", "constraint_type", "columns", "foreign_schema",
		"foreign_table", "foreign_columns", "delete_rule", "update_rule", "deferrable", "deferred",
		"check_clause", "definition", "comment",
	}
	// The constraint read sends one query, so every query gets these rows.
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{
			{"public", "c", "c_a", "CHECK", "a", "", "", "", "", "", false, false, "(a < 100)",
				"CHECK ((a < 100)) NOT VALID", ""},
			{"public", "c", "c_b", "CHECK", "b", "", "", "", "", "", false, false, "(b > 0)",
				"CHECK ((b > 0))", ""},
			{"public", "c", "c_p", "FOREIGN KEY", "p", "public", "p", "id", "NO ACTION", "NO ACTION", false, false, "",
				"FOREIGN KEY (p) REFERENCES p(id) NOT VALID", ""},
		}}, nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres18())

	constraints, err := reader.readBasicConstraintsForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(notValidOf(constraints), qt.DeepEquals, []string{
		"c_a not_valid=true", "c_b not_valid=false", "c_p not_valid=true",
	})
}

func notValidOf(constraints []catalog.Constraint) []string {
	described := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		described = append(described, constraint.Name+" not_valid="+strconv.FormatBool(constraint.NotValid))
	}
	return described
}
