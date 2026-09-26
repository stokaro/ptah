package parser_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// namedColumnConstraintParents is the table the rows' references point at.
const namedColumnConstraintParents = "CREATE TABLE p (id INT PRIMARY KEY);\n"

// TestParseNamedColumnConstraint_MySQLFamilyFailurePath refuses a named column
// constraint where the engine refuses it, rather than reading a constraint the
// server would never build (stokaro/ptah#3745). Measured on MySQL 8.4.11,
// MySQL 26.7.0 and MariaDB 11.8.9: each row answers ERROR 1064 (42000). MySQL
// takes a name on a column only before CHECK, and MariaDB only before
// REFERENCES, as each does with CONSTRAINT written without a name.
func TestParseNamedColumnConstraint_MySQLFamilyFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a unique key on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT uq UNIQUE);",
			wantErr: "CONSTRAINT uq at position 67 is followed by UNIQUE: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT uq",
		},
		{
			name:    "a unique key with KEY on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT uq UNIQUE KEY);",
			wantErr: "CONSTRAINT uq at position 67 is followed by UNIQUE: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT uq",
		},
		{
			name:    "a primary key on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT CONSTRAINT pk PRIMARY KEY);",
			wantErr: "CONSTRAINT pk at position 60 is followed by PRIMARY: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT pk",
		},
		{
			name:    "a reference on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT fk REFERENCES p(id));",
			wantErr: "CONSTRAINT fk at position 67 is followed by REFERENCES: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT fk",
		},
		{
			name:    "a NOT NULL on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT nn NOT NULL);",
			wantErr: "CONSTRAINT nn at position 67 is followed by NOT: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT nn",
		},
		{
			name:    "a default on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT df DEFAULT 1);",
			wantErr: "CONSTRAINT df at position 67 is followed by DEFAULT: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT df",
		},
		{
			name:    "a NULL on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT nl NULL);",
			wantErr: "CONSTRAINT nl at position 67 is followed by NULL: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT nl",
		},
		{
			name:    "an attribute after the symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT x COMMENT 'c');",
			wantErr: "CONSTRAINT x at position 67 is followed by COMMENT: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT x",
		},
		{
			name:    "a quoted symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT `uq` UNIQUE);",
			wantErr: "CONSTRAINT `uq` at position 67 is followed by UNIQUE: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT `uq`",
		},
		{
			name:    "a lower-case keyword and kind on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT constraint uq unique);",
			wantErr: "CONSTRAINT uq at position 67 is followed by UNIQUE: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT uq",
		},
		{
			name:    "a unique key ALTER TABLE adds on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT);\nALTER TABLE c ADD COLUMN a INT CONSTRAINT uq UNIQUE;",
			wantErr: "CONSTRAINT uq at position 93 is followed by UNIQUE: on a column, mysql accepts " +
				"CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT uq",
		},
		{
			name:    "a NOT NULL MODIFY states on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT);\nALTER TABLE c MODIFY a INT CONSTRAINT nn NOT NULL;",
			wantErr: "CONSTRAINT nn at position 96 is followed by NOT: in a MODIFY column definition, " +
				"mysql accepts CONSTRAINT with a name only before CHECK, and answers ERROR 1064 (42000) " +
				"to this; drop CONSTRAINT nn",
		},
		{
			name:    "a unique key on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT uq UNIQUE);",
			wantErr: "CONSTRAINT uq at position 67 is followed by UNIQUE: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT uq",
		},
		{
			name:    "a primary key on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT CONSTRAINT pk PRIMARY KEY);",
			wantErr: "CONSTRAINT pk at position 60 is followed by PRIMARY: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT pk",
		},
		{
			name:    "a check on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT ck CHECK (a > 0));",
			wantErr: "CONSTRAINT ck at position 67 is followed by CHECK: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT ck",
		},
		{
			name:    "a NOT NULL on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT nn NOT NULL);",
			wantErr: "CONSTRAINT nn at position 67 is followed by NOT: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT nn",
		},
		{
			name:    "a default on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT df DEFAULT 1);",
			wantErr: "CONSTRAINT df at position 67 is followed by DEFAULT: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT df",
		},
		{
			name:    "a NULL on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT nl NULL);",
			wantErr: "CONSTRAINT nl at position 67 is followed by NULL: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT nl",
		},
		{
			name:    "an index keyword after the symbol on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT x KEY);",
			wantErr: "CONSTRAINT x at position 67 is followed by KEY: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT x",
		},
		{
			name:    "a check ALTER TABLE adds on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT);\nALTER TABLE c ADD COLUMN a INT CONSTRAINT ck CHECK (a > 0);",
			wantErr: "CONSTRAINT ck at position 93 is followed by CHECK: on a column, mariadb accepts " +
				"CONSTRAINT with a name only before REFERENCES, and answers ERROR 1064 (42000) to this; " +
				"drop CONSTRAINT ck",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				namedColumnConstraintParents+test.sql,
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(result, qt.IsNil)
		})
	}
}

// TestParseNamedColumnCheck_HappyPath keeps the refusal off the one kind MySQL
// takes a name before: measured on MySQL 8.4.11 and 26.7.0, `a int CONSTRAINT
// ck CHECK (a > 0)` builds a check named `ck`, and so does PostgreSQL 18.6. A
// refusal that took every named column constraint on the MySQL family would
// fail the first row.
func TestParseNamedColumnCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "MySQL", dialect: platform.MySQL},
		{name: "PostgreSQL", dialect: platform.Postgres},
		{name: "no dialect", dialect: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				namedColumnConstraintParents+"CREATE TABLE c (id INT, a INT CONSTRAINT ck CHECK (a > 0));",
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 2)
			table, ok := result.Statements[1].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 2)
			c.Assert(table.Columns[1].CheckName, qt.Equals, "ck")
			c.Assert(table.Columns[1].Check, qt.Equals, "a > 0")
		})
	}
}

// TestParseNamedColumnReference_HappyPath is the control on the other side:
// MariaDB takes a name before a column REFERENCES and builds the foreign key
// under it, and so does PostgreSQL 18.6. A refusal that swapped the two MySQL
// engines would fail the first row here and the MySQL row of the test above.
func TestParseNamedColumnReference_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "MariaDB", dialect: platform.MariaDB},
		{name: "PostgreSQL", dialect: platform.Postgres},
		{name: "no dialect", dialect: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				namedColumnConstraintParents+"CREATE TABLE c (id INT, a INT CONSTRAINT fk REFERENCES p(id));",
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 2)
			table, ok := result.Statements[1].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 2)
			c.Assert(table.Columns[1].ForeignKey, qt.IsNotNil)
			c.Assert(table.Columns[1].ForeignKey.Name, qt.Equals, "fk")
			c.Assert(table.Columns[1].ForeignKey.Table, qt.Equals, "p")
			c.Assert(table.Columns[1].ForeignKey.Column, qt.Equals, "id")
		})
	}
}

// TestParseNamedColumnKey_PostgresHappyPath keeps the refusal off PostgreSQL,
// which takes a name before a column UNIQUE and PRIMARY KEY: measured on 18.6,
// pg_constraint holds `uq` and `pk`. Each is read as the table constraint it
// describes.
func TestParseNamedColumnKey_PostgresHappyPath(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantType ast.ConstraintType
	}{
		{
			name:     "a unique key",
			sql:      "CREATE TABLE c (id INT, a INT CONSTRAINT uq UNIQUE);",
			wantName: "uq",
			wantType: ast.UniqueConstraint,
		},
		{
			name:     "a primary key",
			sql:      "CREATE TABLE c (id INT, a INT CONSTRAINT pk PRIMARY KEY);",
			wantName: "pk",
			wantType: ast.PrimaryKeyConstraint,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(
				namedColumnConstraintParents+test.sql,
				parser.WithDialect(platform.Postgres),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 2)
			table, ok := result.Statements[1].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Name, qt.Equals, test.wantName)
			c.Assert(table.Constraints[0].Type, qt.Equals, test.wantType)
			c.Assert(table.Constraints[0].Columns, qt.DeepEquals, []string{"a"})
		})
	}
}

// TestParseNamedColumnNotNull_PostgresHappyPath keeps the refusal off a named
// NOT NULL on PostgreSQL, which 18.6 records in pg_constraint as `nn`.
func TestParseNamedColumnNotNull_PostgresHappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := parser.NewParser(
		namedColumnConstraintParents+"CREATE TABLE c (id INT, a INT CONSTRAINT nn NOT NULL);",
		parser.WithDialect(platform.Postgres),
	).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(result.Statements, qt.HasLen, 2)
	table, ok := result.Statements[1].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Columns, qt.HasLen, 2)
	c.Assert(table.Columns[1].NotNullConstraintName, qt.Equals, "nn")
	c.Assert(table.Columns[1].Nullable, qt.IsFalse)
}
