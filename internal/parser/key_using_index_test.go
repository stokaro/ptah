package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// keyUsingIndex is what a parse of `ADD ... USING INDEX` records of the key.
type keyUsingIndex struct {
	Type       ast.ConstraintType
	Name       string
	UsingIndex string
	Columns    []string
	Deferrable bool
	Initially  string
}

// TestParseKeyUsingIndex_HappyPath pins the key PostgreSQL's `ALTER TABLE ...
// ADD [CONSTRAINT name] UNIQUE | PRIMARY KEY USING INDEX index` declares: the
// index it is made of, and no columns, since they are the index's. PostgreSQL
// 18.6 runs every statement, on a table with the unique index.
func TestParseKeyUsingIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		want    keyUsingIndex
	}{
		{
			name:    "a named unique key",
			sql:     "ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;",
			dialect: platform.Postgres,
			want:    keyUsingIndex{Type: ast.UniqueConstraint, Name: "t_a_key", UsingIndex: "t_a"},
		},
		{
			name:    "an unnamed unique key",
			sql:     "ALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			dialect: platform.Postgres,
			want:    keyUsingIndex{Type: ast.UniqueConstraint, UsingIndex: "t_a"},
		},
		{
			name:    "a primary key",
			sql:     "ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_a;",
			dialect: platform.Postgres,
			want:    keyUsingIndex{Type: ast.PrimaryKeyConstraint, Name: "t_pkey", UsingIndex: "t_a"},
		},
		{
			name:    "a deferrable key",
			sql:     "ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX t_a DEFERRABLE INITIALLY DEFERRED;",
			dialect: platform.Postgres,
			want:    keyUsingIndex{Type: ast.UniqueConstraint, Name: "k", UsingIndex: "t_a", Deferrable: true, Initially: "deferred"},
		},
		{
			name:    "a quoted index name",
			sql:     `ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX "T_A";`,
			dialect: platform.Postgres,
			want:    keyUsingIndex{Type: ast.UniqueConstraint, Name: "k", UsingIndex: `"T_A"`},
		},
		{
			name: "a document of no dialect",
			sql:  "ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX t_a;",
			want: keyUsingIndex{Type: ast.UniqueConstraint, Name: "k", UsingIndex: "t_a"},
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
			add, ok := alter.Operations[0].(*ast.AddConstraintOperation)
			c.Assert(ok, qt.IsTrue)
			got := add.Constraint
			c.Assert(keyUsingIndex{
				Type: got.Type, Name: got.Name, UsingIndex: got.UsingIndex, Columns: got.Columns,
				Deferrable: got.Deferrable, Initially: got.Initially,
			}, qt.DeepEquals, test.want)
		})
	}
}

// TestParseKeyUsingIndex_FailurePath pins the refusals of a key made of an
// existing index where PostgreSQL 18.6 refuses it, and on a dialect whose
// grammar lacks the clause.
func TestParseKeyUsingIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
		wantErr string
	}{
		{
			name:    "in CREATE TABLE",
			sql:     "CREATE TABLE t (a int, CONSTRAINT k UNIQUE USING INDEX t_a);",
			dialect: platform.Postgres,
			wantErr: `line 1, column 44: UNIQUE USING INDEX: an existing index becomes a key only in ALTER TABLE \.\.\. ADD, .*`,
		},
		{
			name:    "a qualified index name",
			sql:     "ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX public.t_a;",
			dialect: platform.Postgres,
			wantErr: `line 1, column 57: UNIQUE USING INDEX: the index is named without its schema, .*`,
		},
		{
			name:    "NOT VALID",
			sql:     "ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX t_a NOT VALID;",
			dialect: platform.Postgres,
			wantErr: `line 1, column 55: NOT VALID: UNIQUE constraints cannot be marked NOT VALID`,
		},
		{
			name:    "a unique key on MySQL",
			sql:     "ALTER TABLE t ADD CONSTRAINT k UNIQUE USING INDEX t_a;",
			dialect: platform.MySQL,
			wantErr: `line 1, column 39: UNIQUE USING INDEX: a key made of an existing index is PostgreSQL's clause, ` +
				`which the mysql dialect does not take; list the key's columns`,
		},
		{
			name:    "a primary key on MySQL",
			sql:     "ALTER TABLE t ADD PRIMARY KEY USING INDEX t_a;",
			dialect: platform.MySQL,
			wantErr: `line 1, column 31: PRIMARY KEY USING INDEX: a key made of an existing index is PostgreSQL's clause, .*`,
		},
		{
			name:    "a unique key on CockroachDB",
			sql:     "ALTER TABLE t ADD UNIQUE USING INDEX t_a;",
			dialect: platform.CockroachDB,
			wantErr: `line 1, column 26: UNIQUE USING INDEX: a key made of an existing index is PostgreSQL's clause, ` +
				`which the cockroachdb dialect does not take; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
