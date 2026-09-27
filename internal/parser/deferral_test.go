package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// deferralOf answers the deferral the one foreign key sql declares carries,
// at table level, on a column, or in ALTER TABLE ... ADD CONSTRAINT.
func deferralOf(c *qt.C, dialect, sql string) ast.ForeignKeyRef {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	var references []*ast.ForeignKeyRef
	for _, statement := range statements.Statements {
		switch node := statement.(type) {
		case *ast.CreateTableNode:
			for _, column := range node.Columns {
				if column.ForeignKey != nil {
					references = append(references, column.ForeignKey)
				}
			}
			for _, constraint := range node.Constraints {
				if constraint.Reference != nil {
					references = append(references, constraint.Reference)
				}
			}
		case *ast.AlterTableNode:
			for _, operation := range node.Operations {
				if add, ok := operation.(*ast.AddConstraintOperation); ok && add.Constraint.Reference != nil {
					references = append(references, add.Constraint.Reference)
				}
			}
		}
	}
	c.Assert(references, qt.HasLen, 1)
	return ast.ForeignKeyRef{Deferrable: references[0].Deferrable, Initially: references[0].Initially}
}

// TestParse_ForeignKeyDeferral_HappyPath reads the deferral clauses after a
// foreign key onto its reference. Each PostgreSQL row was run on PostgreSQL
// 18.6, and pg_constraint's condeferrable and condeferred say what the row
// wants; the SQLite rows were accepted by SQLite 3.51.
func TestParse_ForeignKeyDeferral_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    ast.ForeignKeyRef
	}{
		{
			name:    "a table-level key, deferred",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED);",
			want:    ast.ForeignKeyRef{Deferrable: true, Initially: "deferred"},
		},
		{
			name:    "INITIALLY DEFERRED alone",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) INITIALLY DEFERRED);",
			want:    ast.ForeignKeyRef{Deferrable: true, Initially: "deferred"},
		},
		{
			name:    "the timing first",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) INITIALLY IMMEDIATE DEFERRABLE);",
			want:    ast.ForeignKeyRef{Deferrable: true, Initially: "immediate"},
		},
		{
			name:    "a column's key after its actions, before another column",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) ON DELETE CASCADE DEFERRABLE, b int);",
			want:    ast.ForeignKeyRef{Deferrable: true},
		},
		{
			name:    "NOT DEFERRABLE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_fk FOREIGN KEY (a) REFERENCES p (id) NOT DEFERRABLE);",
			want:    ast.ForeignKeyRef{},
		},
		{
			name:    "DEFERRABLE twice",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE DEFERRABLE);",
			want:    ast.ForeignKeyRef{Deferrable: true},
		},
		{
			name:    "ALTER TABLE adds a deferred key",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT c_fk FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED;",
			want:    ast.ForeignKeyRef{Deferrable: true, Initially: "deferred"},
		},
		{
			name:    "a column's key on SQLite",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) DEFERRABLE INITIALLY DEFERRED);",
			want:    ast.ForeignKeyRef{Deferrable: true, Initially: "deferred"},
		},
		{
			name:    "a table-level key on SQLite",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) NOT DEFERRABLE INITIALLY IMMEDIATE);",
			want:    ast.ForeignKeyRef{Initially: "immediate"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(deferralOf(c, test.dialect, test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestParse_KeyDeferralThatChangesNothing_HappyPath reads the clauses that
// say what a key or a CHECK is without them. PostgreSQL 18.6 accepts each
// row and records the constraint as not deferrable.
func TestParse_KeyDeferralThatChangesNothing_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		columns     []string
		constraints int
	}{
		{
			name:        "a table-level UNIQUE",
			sql:         "CREATE TABLE c (a int, UNIQUE (a) NOT DEFERRABLE INITIALLY IMMEDIATE);",
			columns:     []string{"a"},
			constraints: 1,
		},
		{name: "a column's UNIQUE", sql: "CREATE TABLE c (a int UNIQUE NOT DEFERRABLE, b int);", columns: []string{"a", "b"}},
		{
			name:        "a named column UNIQUE",
			sql:         "CREATE TABLE c (a int CONSTRAINT u UNIQUE NOT DEFERRABLE);",
			columns:     []string{"a"},
			constraints: 1,
		},
		{name: "a column's PRIMARY KEY", sql: "CREATE TABLE c (a int PRIMARY KEY INITIALLY IMMEDIATE);", columns: []string{"a"}},
		{
			name:        "an EXCLUDE",
			sql:         "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =) NOT DEFERRABLE);",
			columns:     []string{"r"},
			constraints: 1,
		},
		{name: "a CHECK", sql: "CREATE TABLE c (a int, CHECK (a > 0) NOT DEFERRABLE);", columns: []string{"a"}, constraints: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(columnNames(table), qt.DeepEquals, test.columns)
			c.Assert(table.Constraints, qt.HasLen, test.constraints)
		})
	}
}

// TestParse_Deferral_FailurePath refuses a clause the model cannot carry, one
// the server refuses, and one in a place the server does not take it. The
// PostgreSQL messages quote PostgreSQL 18.6; MySQL 8.4 and MariaDB 11.8 answer
// ERROR 1064 to every clause, and SQLite 3.51 to the SQLite rows.
func TestParse_Deferral_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a deferrable UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) DEFERRABLE);",
			wantErr: `DEFERRABLE UNIQUE at position 34: a deferrable UNIQUE is not modeled yet \(stokaro/ptah#3824\).*`,
		},
		{
			name:    "a deferred EXCLUDE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =) INITIALLY DEFERRED);",
			wantErr: `DEFERRABLE EXCLUDE at position 54: a deferrable EXCLUDE is not modeled yet.*`,
		},
		{
			name:    "a deferrable table-level PRIMARY KEY",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CONSTRAINT c_pk PRIMARY KEY (a) DEFERRABLE);",
			wantErr: `DEFERRABLE PRIMARY KEY at position 55: .*`,
		},
		{
			name:    "a column's deferrable UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int UNIQUE DEFERRABLE);",
			wantErr: `DEFERRABLE UNIQUE at position 29: .*`,
		},
		{
			name:    "a column's deferrable PRIMARY KEY",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int PRIMARY KEY DEFERRABLE);",
			wantErr: `DEFERRABLE PRIMARY KEY at position 34: .*`,
		},
		{
			name:    "ALTER TABLE adds a deferrable UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT u UNIQUE (a) DEFERRABLE;",
			wantErr: `DEFERRABLE UNIQUE at position 66: .*`,
		},
		{
			name:    "a deferrable CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) DEFERRABLE);",
			wantErr: `deferral clause at position 37: CHECK constraints cannot be marked DEFERRABLE`,
		},
		{
			name:    "after NOT NULL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int NOT NULL DEFERRABLE);",
			wantErr: `misplaced deferral clause at position 31: .*`,
		},
		{
			name:    "on a column with no constraint",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int DEFERRABLE);",
			wantErr: `misplaced deferral clause at position 22: .*`,
		},
		{
			name:    "DEFERRABLE beside NOT DEFERRABLE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE NOT DEFERRABLE);",
			wantErr: `deferral clause at position 57: conflicting constraint properties`,
		},
		{
			name:    "INITIALLY DEFERRED beside NOT DEFERRABLE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) INITIALLY DEFERRED NOT DEFERRABLE);",
			wantErr: `deferral clause at position 34: constraint declared INITIALLY DEFERRED must be DEFERRABLE`,
		},
		{
			name:    "two timings",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) INITIALLY DEFERRED INITIALLY IMMEDIATE);",
			wantErr: `INITIALLY at position 76: conflicting constraint properties`,
		},
		{
			name:    "INITIALLY with no timing",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) INITIALLY LATER);",
			wantErr: `expected DEFERRED or IMMEDIATE after INITIALLY at position 67`,
		},
		{
			name:    "MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) NOT DEFERRABLE);",
			wantErr: `deferral clause at position 34: MySQL and MariaDB have no deferrable constraints.*`,
		},
		{
			name:    "MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE);",
			wantErr: `deferral clause at position 57: MySQL and MariaDB have no deferrable constraints.*`,
		},
		{
			name:    "an index, read with no dialect",
			dialect: "",
			sql:     "CREATE TABLE c (a int, KEY k (a) NOT DEFERRABLE);",
			wantErr: `deferral clause at position 33: an index takes none`,
		},
		{
			name:    "SQLite INITIALLY with no DEFERRABLE",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) INITIALLY DEFERRED);",
			wantErr: `INITIALLY at position 57: SQLite takes it only after \[NOT\] DEFERRABLE`,
		},
		{
			name:    "SQLite after a UNIQUE",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) NOT DEFERRABLE);",
			wantErr: `deferral clause at position 34: SQLite takes one only after a foreign key, not after UNIQUE`,
		},
		{
			name:    "SQLite after a CHECK",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT DEFERRABLE);",
			wantErr: `deferral clause at position 37: SQLite takes one only after a foreign key, not after CHECK`,
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

// TestParse_UnreadClauseAfterATableElement_FailurePath refuses a word after a
// table element instead of reading it as the start of a column. Read that way,
// each row gains a column named after its first word, with no type, and loses
// the clause: `UNIQUE (a) DEFERRABLE` is a column `deferrable`
// (stokaro/ptah#3818).
func TestParse_UnreadClauseAfterATableElement_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "NOT VALID after a CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT VALID);",
			wantErr: `unexpected NOT after a table element at position 37: expected ',' or '\)'`,
		},
		{
			name:    "MATCH FULL after a foreign key",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH FULL);",
			wantErr: `unexpected MATCH after a table element at position 57: expected ',' or '\)'`,
		},
		{
			name:    "NO INHERIT after a CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NO INHERIT);",
			wantErr: `unexpected NO after a table element at position 37: expected ',' or '\)'`,
		},
		{
			name:    "a tablespace after an index, read with no dialect",
			dialect: "",
			sql:     "CREATE TABLE c (a int, KEY k (a) USING INDEX TABLESPACE x);",
			wantErr: `unexpected USING after a table element at position 33: expected ',' or '\)'`,
		},
		{
			name:    "NOT ENFORCED after a MySQL CHECK",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, CONSTRAINT ck CHECK (a > 0) NOT ENFORCED);",
			wantErr: `unexpected NOT after a table element at position 51: expected ',' or '\)'`,
		},
		{
			name:    "an index option after a MySQL key",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) INVISIBLE);",
			wantErr: `unexpected INVISIBLE after a table element at position 33: expected ',' or '\)'`,
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

// columnNames lists the columns table declares, in order.
func columnNames(table *ast.CreateTableNode) []string {
	names := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		names = append(names, column.Name)
	}
	return names
}
