package postgres

// White-box testing required: postgresNotEnforcedFromDefinition and
// postgresMatchFromDefinition are unexported, and a fake server can only hand
// back the definition text they parse, so the parse is pinned here against
// the text each server printed. The live tests in integration/ read the
// clauses off a real server.

import (
	"database/sql/driver"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestPostgresEnforcementAndMatchFromDefinition pins the parse against the
// definitions pg_get_constraintdef printed on PostgreSQL 18.6, CockroachDB
// v26.3.2 and YugabyteDB 2026.1.2 (stokaro/ptah#3853).
func TestPostgresEnforcementAndMatchFromDefinition(t *testing.T) {
	tests := []struct {
		name            string
		definition      string
		wantNotEnforced bool
		wantMatch       string
	}{
		{
			name:            "a CHECK not enforced",
			definition:      "CHECK ((a > 0)) NOT ENFORCED",
			wantNotEnforced: true,
		},
		{
			name:       "a CHECK enforced",
			definition: "CHECK ((a > 0))",
		},
		{
			name:            "a foreign key with every clause",
			definition:      "FOREIGN KEY (a) REFERENCES p(id) MATCH FULL DEFERRABLE NOT ENFORCED",
			wantNotEnforced: true,
			wantMatch:       "FULL",
		},
		{
			name:       "MATCH FULL before an action",
			definition: "FOREIGN KEY (a, b) REFERENCES p(id, k) MATCH FULL ON DELETE CASCADE",
			wantMatch:  "FULL",
		},
		{
			name:       "MATCH FULL alone",
			definition: "FOREIGN KEY (a, b) REFERENCES p(id, k) MATCH FULL",
			wantMatch:  "FULL",
		},
		{
			name:       "MATCH PARTIAL",
			definition: "FOREIGN KEY (a) REFERENCES p(id) MATCH PARTIAL",
			wantMatch:  "PARTIAL",
		},
		{
			name:       "MATCH SIMPLE, which the server does not print",
			definition: "FOREIGN KEY (a) REFERENCES p(id) ON DELETE CASCADE",
		},
		{
			name:       "a quoted referenced table that spells the clause",
			definition: `FOREIGN KEY (a) REFERENCES "p MATCH FULL"(id)`,
		},
		{
			name:       "a quoted column that spells the keyword before the reference",
			definition: `FOREIGN KEY (" REFERENCES x(y) MATCH FULL ") REFERENCES p(id)`,
		},
		{
			name:       "a definition that is not a foreign key",
			definition: "UNIQUE (a)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(postgresNotEnforcedFromDefinition(test.definition), qt.Equals, test.wantNotEnforced)
			c.Assert(postgresMatchFromDefinition(test.definition), qt.Equals, test.wantMatch)
		})
	}
}

// TestReadBasicConstraintsForSchema_ReadsEnforcementAndMatch reads the clauses
// off the definition the constraint query returns, for a CHECK and a foreign
// key, as PostgreSQL 18.6 prints them.
func TestReadBasicConstraintsForSchema_ReadsEnforcementAndMatch(t *testing.T) {
	c := qt.New(t)
	columns := []string{
		"table_schema", "table_name", "constraint_name", "constraint_type", "columns", "foreign_schema",
		"foreign_table", "foreign_columns", "delete_rule", "update_rule", "deferrable", "deferred",
		"check_clause", "definition", "comment",
	}
	// The constraint read sends one query, so every query gets these rows.
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{
			{"public", "c", "c_a_check", "CHECK", "a", "", "", "", "", "", false, false, "(a > 0)",
				"CHECK ((a > 0)) NOT ENFORCED", ""},
			{"public", "c", "c_p_fkey", "FOREIGN KEY", "p", "public", "p", "id", "NO ACTION", "NO ACTION", true, false, "",
				"FOREIGN KEY (p) REFERENCES p(id) MATCH FULL DEFERRABLE NOT ENFORCED", ""},
		}}, nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres18())

	constraints, err := reader.readBasicConstraintsForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(clausesOf(constraints), qt.DeepEquals, []string{
		"c_a_check not_enforced=true match=", "c_p_fkey not_enforced=true match=FULL",
	})
}

func clausesOf(constraints []catalog.Constraint) []string {
	described := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		described = append(described,
			constraint.Name+" not_enforced="+strconv.FormatBool(constraint.NotEnforced)+" match="+constraint.Match)
	}
	return described
}
