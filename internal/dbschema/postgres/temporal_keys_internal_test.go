package postgres

// White-box testing required: postgresTemporalFromDefinition is unexported, and
// a fake server can only hand back the definition text it parses, so the parse
// is pinned here against the text PostgreSQL 18.6 printed.
// integration/temporal_keys_e2e_test.go reads the keys off a real server and
// checks them against pg_constraint.conperiod.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestPostgresTemporalFromDefinition pins the parse against the definitions
// pg_get_constraintdef printed for keys whose pg_constraint.conperiod is true
// (stokaro/ptah#6330).
func TestPostgresTemporalFromDefinition(t *testing.T) {
	tests := []struct {
		name            string
		constraintType  string
		definition      string
		wantWithoutOver bool
		wantPeriod      bool
	}{
		{
			name: "a primary key", constraintType: "PRIMARY KEY",
			definition:      "PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
			wantWithoutOver: true,
		},
		{
			name: "a unique constraint of three columns", constraintType: "UNIQUE",
			definition:      "UNIQUE (id, k, valid_at WITHOUT OVERLAPS)",
			wantWithoutOver: true,
		},
		{
			name: "a unique constraint with an INCLUDE payload", constraintType: "UNIQUE",
			definition:      "UNIQUE (id, valid_at WITHOUT OVERLAPS) INCLUDE (note)",
			wantWithoutOver: true,
		},
		{
			name: "a foreign key", constraintType: "FOREIGN KEY",
			definition: "FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)",
			wantPeriod: true,
		},
		{
			name: "a foreign key with actions", constraintType: "FOREIGN KEY",
			definition: "FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at) ON DELETE RESTRICT",
			wantPeriod: true,
		},
		{
			name: "a plain primary key", constraintType: "PRIMARY KEY",
			definition: "PRIMARY KEY (id, valid_at)",
		},
		{
			name: "a plain foreign key", constraintType: "FOREIGN KEY",
			definition: "FOREIGN KEY (pid, valid_at) REFERENCES p(id, valid_at)",
		},
		{
			name: "a foreign key over a column named period", constraintType: "FOREIGN KEY",
			definition: "FOREIGN KEY (a, period) REFERENCES p(a, period)",
		},
		{
			name: "a quoted column that spells the clause", constraintType: "UNIQUE",
			definition: `UNIQUE ("x WITHOUT OVERLAPS)", b)`,
		},
		{
			name: "a quoted column that spells the marker", constraintType: "FOREIGN KEY",
			definition: `FOREIGN KEY (a, "b, PERIOD c") REFERENCES p(a, b)`,
		},
		{
			name: "a CHECK", constraintType: "CHECK",
			definition: "CHECK ((a > 0))",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			withoutOverlaps, period := postgresTemporalFromDefinition(test.constraintType, test.definition)

			c.Assert(withoutOverlaps, qt.Equals, test.wantWithoutOver)
			c.Assert(period, qt.Equals, test.wantPeriod)
		})
	}
}

// TestReadBasicConstraintsForSchema_ReadsTemporalKeys reads the clauses off the
// definition the constraint query returns, as PostgreSQL 18.6 prints them.
func TestReadBasicConstraintsForSchema_ReadsTemporalKeys(t *testing.T) {
	c := qt.New(t)
	columns := []string{
		"table_schema", "table_name", "constraint_name", "constraint_type", "columns", "foreign_schema",
		"foreign_table", "foreign_columns", "delete_rule", "update_rule", "deferrable", "deferred",
		"check_clause", "definition", "comment", "referenced_index",
	}
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{
			{"public", "p", "p_pkey", "PRIMARY KEY", "id,valid_at", "", "", "", "", "", false, false, "",
				"PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)", "", ""},
			{"public", "c", "c_fk", "FOREIGN KEY", "pid,valid_at", "public", "p", "id,valid_at", "NO ACTION", "NO ACTION",
				false, false, "", "FOREIGN KEY (pid, PERIOD valid_at) REFERENCES p(id, PERIOD valid_at)", "", "p_pkey"},
		}}, nil
	})
	reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres18())

	constraints, err := reader.readBasicConstraintsForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(constraints, qt.HasLen, 2)
	c.Assert(constraints[0].Name, qt.Equals, "p_pkey")
	c.Assert(constraints[0].WithoutOverlaps, qt.IsTrue)
	c.Assert(constraints[0].Period, qt.IsFalse)
	c.Assert(constraints[1].Name, qt.Equals, "c_fk")
	c.Assert(constraints[1].Period, qt.IsTrue)
	c.Assert(constraints[1].WithoutOverlaps, qt.IsFalse)
}
