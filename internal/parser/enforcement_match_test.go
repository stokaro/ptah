package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// constraintClauses is what one CREATE TABLE's constraints say about
// enforcement and the MATCH type, as `<where>: <clause>` in the order the
// statement holds them.
func constraintClauses(c *qt.C, dialect, sql string) []string {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	table, ok := statements.Statements[len(statements.Statements)-1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	var clauses []string
	for _, column := range table.Columns {
		clauses = append(clauses, describeEnforcement("column "+column.Name+" CHECK", column.Check != "", column.CheckNotEnforced))
		if column.ForeignKey != nil {
			clauses = append(clauses, describeReference("column "+column.Name+" REFERENCES", column.ForeignKey))
		}
	}
	for _, constraint := range table.Constraints {
		switch constraint.Type {
		case ast.CheckConstraint:
			clauses = append(clauses, describeEnforcement("CHECK ("+constraint.Expression+")", true, constraint.NotEnforced))
		case ast.ForeignKeyConstraint:
			clauses = append(clauses, describeReference("FOREIGN KEY", constraint.Reference))
		}
	}
	return compact(clauses)
}

func describeEnforcement(where string, present, notEnforced bool) string {
	switch {
	case !present:
		return ""
	case notEnforced:
		return where + ": NOT ENFORCED"
	default:
		return where + ": ENFORCED"
	}
}

func describeReference(where string, reference *ast.ForeignKeyRef) string {
	described := describeEnforcement(where, true, reference.NotEnforced)
	if reference.Match != "" {
		described += " MATCH " + reference.Match
	}
	return described
}

func compact(values []string) []string {
	var kept []string
	for _, value := range values {
		if value != "" {
			kept = append(kept, value)
		}
	}
	return kept
}

// TestParse_EnforcementAndMatch_HappyPath reads `NOT ENFORCED` and the MATCH
// type onto the constraint they follow (stokaro/ptah#3853). Each row's SQL was
// run on its server: PostgreSQL 18.6, MySQL 8.4.11 and 9.7.2, CockroachDB
// v26.3.2, and each server recorded what the row expects.
func TestParse_EnforcementAndMatch_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "a table CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT ENFORCED);",
			want:    []string{"CHECK (a > 0): NOT ENFORCED"},
		},
		{
			name:    "a column CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) NOT ENFORCED);",
			want:    []string{"column a CHECK: NOT ENFORCED"},
		},
		{
			name:    "a column's second CHECK alone",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, b int CHECK (a > 0) CHECK (b > 0) NOT ENFORCED);",
			want:    []string{"column b CHECK: ENFORCED", "CHECK (b > 0): NOT ENFORCED"},
		},
		{
			name:    "a column CHECK written ENFORCED",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) ENFORCED);",
			want:    []string{"column a CHECK: ENFORCED"},
		},
		{
			name:    "a table foreign key, with MATCH FULL and a deferral clause",
			dialect: platform.Postgres,
			sql: "CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p (id, k) MATCH FULL " +
				"ON DELETE CASCADE NOT ENFORCED DEFERRABLE);",
			want: []string{"FOREIGN KEY: NOT ENFORCED MATCH FULL"},
		},
		{
			name:    "a column's REFERENCES",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) MATCH FULL NOT ENFORCED);",
			want:    []string{"column a REFERENCES: NOT ENFORCED MATCH FULL"},
		},
		{
			name:    "MATCH SIMPLE is what a key is without the clause",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) MATCH SIMPLE);",
			want:    []string{"column a REFERENCES: ENFORCED"},
		},
		{
			name:    "MATCH FULL on CockroachDB",
			dialect: platform.CockroachDB,
			sql:     "CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p (id, k) MATCH FULL);",
			want:    []string{"FOREIGN KEY: ENFORCED MATCH FULL"},
		},
		{
			name:    "a MySQL CHECK",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) NOT ENFORCED, b int, CONSTRAINT c_b CHECK (b > 0) NOT ENFORCED);",
			want:    []string{"column a CHECK: NOT ENFORCED", "CHECK (b > 0): NOT ENFORCED"},
		},
		{
			name:    "MySQL keeps the last enforcement clause",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_a CHECK (a > 0) NOT ENFORCED ENFORCED);",
			want:    []string{"CHECK (a > 0): ENFORCED"},
		},
		{
			name:    "MySQL MATCH PARTIAL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, b int, CONSTRAINT c_ab FOREIGN KEY (a, b) REFERENCES p (id, k) MATCH PARTIAL);",
			want:    []string{"FOREIGN KEY: ENFORCED MATCH PARTIAL"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := constraintClauses(c, test.dialect, test.sql)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
