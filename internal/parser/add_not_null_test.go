package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestParseAddNotNull_HappyPath reads PostgreSQL 18's spelling of SET NOT NULL as the
// same action, with the name it states, which is what the statement does on 18.6.
func TestParseAddNotNull_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		want    ast.AlterColumnOperation
	}{
		{
			name:    "a name",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			dialect: platform.Postgres,
			want:    ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull, NotNullConstraintName: "nn"},
		},
		{
			name:    "no name",
			sql:     "ALTER TABLE t ADD NOT NULL a;",
			dialect: platform.Postgres,
			want:    ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull},
		},
		{
			name:    "a quoted name and column, as a plan writes them",
			sql:     `ALTER TABLE "t" ADD CONSTRAINT "Nn" NOT NULL "A";`,
			dialect: platform.Postgres,
			want:    ast.AlterColumnOperation{ColumnName: `"A"`, Action: ast.AlterColumnSetNotNull, NotNullConstraintName: `"Nn"`},
		},
		{
			name: "a document of no dialect",
			sql:  "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			want: ast.AlterColumnOperation{ColumnName: "a", Action: ast.AlterColumnSetNotNull, NotNullConstraintName: "nn"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			alter, ok := statements.Statements[0].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(alter.Operations, qt.HasLen, 1)
			got, ok := alter.Operations[0].(*ast.AlterColumnOperation)
			c.Assert(ok, qt.IsTrue)
			c.Assert(*got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseAddNotNull_BesideOtherClauses keeps the clause one operation of its statement,
// in its place.
func TestParseAddNotNull_BesideOtherClauses(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("ALTER TABLE t DROP COLUMN b, ADD CONSTRAINT nn NOT NULL a, ADD CONSTRAINT ck CHECK (a > 0), ADD COLUMN c int;",
		parser.WithDialect(platform.Postgres)).Parse()

	c.Assert(err, qt.IsNil)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Operations, qt.HasLen, 4)
	_, addsNotNull := alter.Operations[1].(*ast.AlterColumnOperation)
	c.Assert(addsNotNull, qt.IsTrue)
	_, addsCheck := alter.Operations[2].(*ast.AddConstraintOperation)
	c.Assert(addsCheck, qt.IsTrue)
	_, addsColumn := alter.Operations[3].(*ast.AddColumnOperation)
	c.Assert(addsColumn, qt.IsTrue)
}

// TestParseAddNotNull_FailurePath refuses the attributes the model has no field for, and
// the clause on a dialect whose grammar lacks it.
func TestParseAddNotNull_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		wantErr string
	}{
		{
			name:    "NOT VALID",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a NOT VALID;",
			dialect: platform.Postgres,
			wantErr: `line 1, column 44: unsupported SQL statement: NOT`,
		},
		{
			name:    "NO INHERIT",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a NO INHERIT;",
			dialect: platform.Postgres,
			wantErr: `line 1, column 44: unsupported SQL statement: NO`,
		},
		{
			name:    "no column",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL;",
			dialect: platform.Postgres,
			wantErr: `.*expected column name after NOT NULL.*`,
		},
		{
			name:    "a named clause on MySQL",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			dialect: platform.MySQL,
			wantErr: `.*unsupported constraint type: NOT`,
		},
		{
			name:    "a named clause on CockroachDB",
			sql:     "ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			dialect: platform.CockroachDB,
			wantErr: `.*unsupported constraint type: NOT`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
