package parser_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// modifyColumnParents declares the tables the rows modify and reference.
const modifyColumnParents = "CREATE TABLE p (id INT PRIMARY KEY);\nCREATE TABLE c (id INT, a INT);\n"

// TestParseModifyColumn_MySQLFamilyFailurePath refuses what the MySQL family
// refuses in the column definition of ALTER TABLE ... MODIFY, rather than
// reading a foreign key or a constraint the server never builds
// (stokaro/ptah#3759). Measured on MySQL 8.4.11, 9.7.2 and 26.7.0 and on
// MariaDB 10.11.19, 11.8.9 and 12.3.3: each row answers ERROR 1064 (42000). No
// engine takes REFERENCES there, and MariaDB takes CONSTRAINT before nothing.
func TestParseModifyColumn_MySQLFamilyFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a reference on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY a INT REFERENCES p(id);",
			wantErr: "REFERENCES at position 96 in ALTER TABLE ... MODIFY: mariadb takes no REFERENCES " +
				"clause in a MODIFY column definition, and answers ERROR 1064 (42000) to one; add the key " +
				"with ALTER TABLE ... ADD FOREIGN KEY",
		},
		{
			name:    "a reference after MODIFY COLUMN on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY COLUMN a INT REFERENCES p(id);",
			wantErr: "REFERENCES at position 103 in ALTER TABLE ... MODIFY: mariadb takes no REFERENCES " +
				"clause in a MODIFY column definition, and answers ERROR 1064 (42000) to one; add the key " +
				"with ALTER TABLE ... ADD FOREIGN KEY",
		},
		{
			name:    "a lower-case reference on MariaDB",
			dialect: platform.MariaDB,
			sql:     "alter table c modify a int references p(id);",
			wantErr: "REFERENCES at position 96 in ALTER TABLE ... MODIFY: mariadb takes no REFERENCES " +
				"clause in a MODIFY column definition, and answers ERROR 1064 (42000) to one; add the key " +
				"with ALTER TABLE ... ADD FOREIGN KEY",
		},
		{
			name:    "a named reference on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY a INT CONSTRAINT fk REFERENCES p(id);",
			wantErr: "CONSTRAINT fk at position 96 is followed by REFERENCES: in a MODIFY column definition, " +
				"mariadb accepts no CONSTRAINT, and answers ERROR 1064 (42000) to this; drop CONSTRAINT fk",
		},
		{
			name:    "a reference after CONSTRAINT without a name on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY a INT CONSTRAINT REFERENCES p(id);",
			wantErr: "CONSTRAINT at position 96 is followed by REFERENCES, not by a name: in a MODIFY column " +
				"definition, mariadb accepts no CONSTRAINT, and answers ERROR 1064 (42000) to this; drop the " +
				"CONSTRAINT keyword",
		},
		{
			name:    "a named check on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY a INT CONSTRAINT ck CHECK (a > 0);",
			wantErr: "CONSTRAINT ck at position 96 is followed by CHECK: in a MODIFY column definition, " +
				"mariadb accepts no CONSTRAINT, and answers ERROR 1064 (42000) to this; drop CONSTRAINT ck",
		},
		{
			name:    "a check after CONSTRAINT without a name on MariaDB",
			dialect: platform.MariaDB,
			sql:     "ALTER TABLE c MODIFY a INT CONSTRAINT CHECK (a > 0);",
			wantErr: "CONSTRAINT at position 96 is followed by CHECK, not by a name: in a MODIFY column " +
				"definition, mariadb accepts no CONSTRAINT, and answers ERROR 1064 (42000) to this; drop the " +
				"CONSTRAINT keyword",
		},
		{
			name:    "a reference on MySQL",
			dialect: platform.MySQL,
			sql:     "ALTER TABLE c MODIFY a INT REFERENCES p(id);",
			wantErr: "REFERENCES at position 96 in ALTER TABLE ... MODIFY: mysql takes no REFERENCES " +
				"clause in a MODIFY column definition, and answers ERROR 1064 (42000) to one; add the key " +
				"with ALTER TABLE ... ADD FOREIGN KEY",
		},
		{
			name:    "a named reference on MySQL",
			dialect: platform.MySQL,
			sql:     "ALTER TABLE c MODIFY a INT CONSTRAINT fk REFERENCES p(id);",
			wantErr: "CONSTRAINT fk at position 96 is followed by REFERENCES: in a MODIFY column definition, " +
				"mysql accepts CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to " +
				"this; drop CONSTRAINT fk",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				modifyColumnParents+test.sql,
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(result, qt.IsNil)
		})
	}
}

// TestParseModifyColumnCheck_HappyPath keeps the refusal off the CHECK each
// engine takes in a MODIFY. Measured on MySQL 8.4.11, 9.7.2 and 26.7.0, a named
// check is built as `ck` and one after CONSTRAINT without a name as `c_chk_1`;
// MariaDB 10.11.19, 11.8.9 and 12.3.3 build the bare CHECK.
func TestParseModifyColumnCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantName string
	}{
		{
			name:     "a named check on MySQL",
			dialect:  platform.MySQL,
			sql:      "ALTER TABLE c MODIFY a INT CONSTRAINT ck CHECK (a > 0);",
			wantName: "ck",
		},
		{
			name:     "a check after CONSTRAINT without a name on MySQL",
			dialect:  platform.MySQL,
			sql:      "ALTER TABLE c MODIFY a INT CONSTRAINT CHECK (a > 0);",
			wantName: "",
		},
		{
			name:     "a bare check on MariaDB",
			dialect:  platform.MariaDB,
			sql:      "ALTER TABLE c MODIFY a INT CHECK (a > 0);",
			wantName: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				modifyColumnParents+test.sql,
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 3)
			alter, ok := result.Statements[2].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(alter.Operations, qt.HasLen, 1)
			modify, ok := alter.Operations[0].(*ast.ModifyColumnOperation)
			c.Assert(ok, qt.IsTrue)
			c.Assert(modify.Column.Check, qt.Equals, "a > 0")
			c.Assert(modify.Column.CheckName, qt.Equals, test.wantName)
		})
	}
}

// TestParseAddColumnReferenceAfterModify_MariaDBHappyPath keeps the MODIFY rule
// inside the MODIFY. Measured on MariaDB 10.11.19, 11.8.9 and 12.3.3, `ALTER
// TABLE c MODIFY a int NOT NULL, ADD COLUMN b int REFERENCES p(id)` builds the
// foreign key on `b`, and so does the named spelling, under its name. A rule
// that stayed on after the MODIFY would refuse both.
func TestParseAddColumnReferenceAfterModify_MariaDBHappyPath(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
	}{
		{
			name:     "a reference",
			sql:      "ALTER TABLE c MODIFY a INT NOT NULL, ADD COLUMN b INT REFERENCES p(id);",
			wantName: "",
		},
		{
			name:     "a named reference",
			sql:      "ALTER TABLE c MODIFY a INT NOT NULL, ADD COLUMN b INT CONSTRAINT fk REFERENCES p(id);",
			wantName: "fk",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				modifyColumnParents+test.sql,
				parser.WithDialect(platform.MariaDB),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 3)
			alter, ok := result.Statements[2].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(alter.Operations, qt.HasLen, 2)
			add, ok := alter.Operations[1].(*ast.AddColumnOperation)
			c.Assert(ok, qt.IsTrue)
			c.Assert(add.Column.ForeignKey, qt.IsNotNil)
			c.Assert(add.Column.ForeignKey.Name, qt.Equals, test.wantName)
			c.Assert(add.Column.ForeignKey.Table, qt.Equals, "p")
			c.Assert(add.Column.ForeignKey.Column, qt.Equals, "id")
		})
	}
}

// TestParseModifyColumnReference_NoDialectHappyPath keeps the rule on the MySQL
// family. A document read with no dialect is not written for one engine, and
// the refusal belongs to the engine that would refuse the statement.
func TestParseModifyColumnReference_NoDialectHappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := parser.NewParser(modifyColumnParents + "ALTER TABLE c MODIFY a INT REFERENCES p(id);").Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 3)
	alter, ok := result.Statements[2].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Operations, qt.HasLen, 1)
	modify, ok := alter.Operations[0].(*ast.ModifyColumnOperation)
	c.Assert(ok, qt.IsTrue)
	c.Assert(modify.Column.ForeignKey, qt.IsNotNil)
	c.Assert(modify.Column.ForeignKey.Table, qt.Equals, "p")
}
