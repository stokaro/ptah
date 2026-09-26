package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// constraintWithoutSymbolParents are the tables the rows' keys reference.
const constraintWithoutSymbolParents = "CREATE TABLE p (id INT PRIMARY KEY);\n"

// TestParseConstraintWithoutSymbol_HappyPath reads a CONSTRAINT written without
// a symbol as the same clause written without the keyword, on the engine that
// accepts the spelling (stokaro/ptah#3730). Each row was measured on MySQL
// 8.4.11 and MariaDB 11.8.9: the server builds the same catalog from both
// spellings, names included.
func TestParseConstraintWithoutSymbol_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		with    string
		without string
	}{
		{
			name:    "a foreign key on MySQL",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, p_id INT, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id INT, p_id INT, FOREIGN KEY (p_id) REFERENCES p(id));",
		},
		{
			name:    "a foreign key on MariaDB",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id INT, p_id INT, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id INT, p_id INT, FOREIGN KEY (p_id) REFERENCES p(id));",
		},
		{
			name:    "a foreign key naming its index",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, p_id INT, CONSTRAINT FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id INT, p_id INT, FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
		},
		{
			name:    "a comment between the keyword and the kind",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, p_id INT, CONSTRAINT /* no symbol */ FOREIGN KEY (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id INT, p_id INT, FOREIGN KEY (p_id) REFERENCES p(id));",
		},
		{
			name:    "a lower-case keyword and kind",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, p_id INT, constraint foreign key (p_id) references p(id));",
			without: "CREATE TABLE c (id INT, p_id INT, foreign key (p_id) references p(id));",
		},
		{
			name:    "a primary key",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, CONSTRAINT PRIMARY KEY (id));",
			without: "CREATE TABLE c (id INT, PRIMARY KEY (id));",
		},
		{
			name:    "a unique key",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, a INT, CONSTRAINT UNIQUE (a));",
			without: "CREATE TABLE c (id INT, a INT, UNIQUE (a));",
		},
		{
			name:    "a unique key naming its index",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id INT, a INT, CONSTRAINT UNIQUE KEY uq (a));",
			without: "CREATE TABLE c (id INT, a INT, UNIQUE KEY uq (a));",
		},
		{
			name:    "a unique key with a bare index name",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, a INT, CONSTRAINT UNIQUE uq (a));",
			without: "CREATE TABLE c (id INT, a INT, UNIQUE uq (a));",
		},
		{
			name:    "a unique key with an access method",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id INT, a INT, CONSTRAINT UNIQUE KEY USING HASH (a));",
			without: "CREATE TABLE c (id INT, a INT, UNIQUE KEY USING HASH (a));",
		},
		{
			name:    "a check",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, a INT, CONSTRAINT CHECK (a > 0));",
			without: "CREATE TABLE c (id INT, a INT, CHECK (a > 0));",
		},
		{
			name:    "a foreign key ALTER TABLE adds",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD CONSTRAINT FOREIGN KEY (a) REFERENCES p(id);",
			without: "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD FOREIGN KEY (a) REFERENCES p(id);",
		},
		{
			name:    "a unique key ALTER TABLE adds",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD CONSTRAINT UNIQUE (a);",
			without: "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD UNIQUE (a);",
		},
		{
			name:    "a check on a column, on MySQL",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT, a INT CONSTRAINT CHECK (a > 0));",
			without: "CREATE TABLE c (id INT, a INT CHECK (a > 0));",
		},
		{
			name:    "a reference on a column, on MariaDB",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id INT, a INT CONSTRAINT REFERENCES p(id));",
			without: "CREATE TABLE c (id INT, a INT REFERENCES p(id));",
		},
		{
			name:    "a check on a column ALTER TABLE adds, on MySQL",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id INT);\nALTER TABLE c ADD COLUMN a INT CONSTRAINT CHECK (a > 0);",
			without: "CREATE TABLE c (id INT);\nALTER TABLE c ADD COLUMN a INT CHECK (a > 0);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			with, err := parser.NewParser(constraintWithoutSymbolParents+test.with, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			without, err := parser.NewParser(constraintWithoutSymbolParents+test.without, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)

			c.Assert(with, qt.DeepEquals, without)
		})
	}
}

// TestParseConstraintWithoutSymbol_WordBeforeAKindIsTheSymbol reads a word that
// opens a kind as the symbol when a kind follows it, so a symbol spelled like a
// keyword the engine does not reserve keeps its name. Each row was measured:
// PostgreSQL 18.6 names the unique constraint `key`, and MySQL 8.4.11 and
// MariaDB 11.8.9 name the check `exclude`.
func TestParseConstraintWithoutSymbol_WordBeforeAKindIsTheSymbol(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantName string
		wantType ast.ConstraintType
	}{
		{
			name:     "KEY names a unique constraint on PostgreSQL",
			dialect:  platform.Postgres,
			sql:      "CREATE TABLE c (a INT, CONSTRAINT key UNIQUE (a));",
			wantName: "key",
			wantType: ast.UniqueConstraint,
		},
		{
			name:     "EXCLUDE names a check on MySQL",
			dialect:  platform.MySQL,
			sql:      "CREATE TABLE c (a INT, CONSTRAINT exclude CHECK (a > 0));",
			wantName: "exclude",
			wantType: ast.CheckConstraint,
		},
		{
			name:     "EXCLUDE names a check on MariaDB",
			dialect:  platform.MariaDB,
			sql:      "CREATE TABLE c (a INT, CONSTRAINT exclude CHECK (a > 0));",
			wantName: "exclude",
			wantType: ast.CheckConstraint,
		},
		{
			name:     "a quoted keyword names a foreign key on MySQL",
			dialect:  platform.MySQL,
			sql:      "CREATE TABLE c (a INT, CONSTRAINT `FOREIGN` FOREIGN KEY (a) REFERENCES p(id));",
			wantName: "`FOREIGN`",
			wantType: ast.ForeignKeyConstraint,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(constraintWithoutSymbolParents+test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.HasLen, 2)
			table, ok := result.Statements[1].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].Name, qt.Equals, test.wantName)
			c.Assert(table.Constraints[0].Type, qt.Equals, test.wantType)
		})
	}
}

// TestParseConstraintWithoutSymbol_FailurePath refuses a CONSTRAINT without a
// symbol where the engine refuses it, and says why, rather than reading the
// kind as a symbol and failing on what follows. PostgreSQL 18.6 answers each
// symbol-less row with a syntax error, and MySQL 8.4.11 and MariaDB 11.8.9 with
// ERROR 1064; a CONSTRAINT before an index keyword is refused by all three,
// with a symbol or without one.
func TestParseConstraintWithoutSymbol_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "a foreign key on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, p_id INT, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			wantErr: `CONSTRAINT at position 71 is followed by FOREIGN, not by a name: postgres requires a name ` +
				`after CONSTRAINT, and only MySQL and MariaDB accept the keyword without one; name the ` +
				`constraint, or drop the CONSTRAINT keyword`,
		},
		{
			name:    "a unique key on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT UNIQUE (a));",
			wantErr: `CONSTRAINT at position 68 is followed by UNIQUE, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a primary key on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, CONSTRAINT PRIMARY KEY (id));",
			wantErr: `CONSTRAINT at position 61 is followed by PRIMARY, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a check on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT CHECK (a > 0));",
			wantErr: `CONSTRAINT at position 68 is followed by CHECK, not by a name: postgres requires a name .*`,
		},
		{
			name:    "an exclusion constraint on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT EXCLUDE USING btree (a WITH =));",
			wantErr: `CONSTRAINT at position 68 is followed by EXCLUDE, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a foreign key ALTER TABLE adds on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD CONSTRAINT FOREIGN KEY (a) REFERENCES p(id);",
			wantErr: `CONSTRAINT at position 87 is followed by FOREIGN, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a check on a column on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT CHECK (a > 0));",
			wantErr: `CONSTRAINT at position 67 is followed by CHECK, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a NOT NULL on a column on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT NOT NULL);",
			wantErr: `CONSTRAINT at position 67 is followed by NOT, not by a name: postgres requires a name .*`,
		},
		{
			name:    "a foreign key in a document with no dialect",
			dialect: "",
			sql:     "CREATE TABLE c (id INT, p_id INT, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			wantErr: `CONSTRAINT at position 71 is followed by FOREIGN, not by a name: a dialect-neutral document ` +
				`requires a name after CONSTRAINT, .*`,
		},
		{
			name:    "an exclusion constraint on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT EXCLUDE USING btree (a WITH =));",
			wantErr: `CONSTRAINT at position 68 is followed by EXCLUDE, not by a name: mysql accepts CONSTRAINT ` +
				`without a name only before PRIMARY KEY, UNIQUE, FOREIGN KEY and CHECK`,
		},
		{
			name:    "a reference on a column on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT REFERENCES p(id));",
			wantErr: `CONSTRAINT at position 67 is followed by REFERENCES, not by a name: on a column, mysql ` +
				`accepts CONSTRAINT without a name only before CHECK, and answers ERROR 1064 \(42000\) to ` +
				`this; drop the CONSTRAINT keyword`,
		},
		{
			name:    "a unique key on a column on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT UNIQUE);",
			wantErr: `CONSTRAINT at position 67 is followed by UNIQUE, not by a name: on a column, mysql .*`,
		},
		{
			name:    "a primary key on a column on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT CONSTRAINT PRIMARY KEY);",
			wantErr: `CONSTRAINT at position 60 is followed by PRIMARY, not by a name: on a column, mysql .*`,
		},
		{
			name:    "a NOT NULL on a column on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT NOT NULL);",
			wantErr: `CONSTRAINT at position 67 is followed by NOT, not by a name: on a column, mysql .*`,
		},
		{
			name:    "a default on a column on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT DEFAULT 1);",
			wantErr: `CONSTRAINT at position 67 is followed by DEFAULT, not by a name: on a column, mysql .*`,
		},
		{
			name:    "a check on a column on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT CONSTRAINT CHECK (a > 0));",
			wantErr: `CONSTRAINT at position 67 is followed by CHECK, not by a name: on a column, mariadb ` +
				`accepts CONSTRAINT without a name only before REFERENCES, and answers ERROR 1064 \(42000\) ` +
				`to this; drop the CONSTRAINT keyword`,
		},
		{
			name:    "a check on a column ALTER TABLE adds, on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT);\nALTER TABLE c ADD COLUMN a INT CONSTRAINT CHECK (a > 0);",
			wantErr: `CONSTRAINT at position 93 is followed by CHECK, not by a name: on a column, mariadb .*`,
		},
		{
			name:    "KEY without a symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT KEY (a));",
			wantErr: `CONSTRAINT at position 68 stands before KEY, which declares an index: an index takes no ` +
				`CONSTRAINT keyword, and MySQL and MariaDB answer ERROR 1064 \(42000\) to one; drop the ` +
				`CONSTRAINT keyword`,
		},
		{
			name:    "KEY with a symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT k KEY (a));",
			wantErr: `CONSTRAINT at position 68 stands before KEY, which declares an index: .*`,
		},
		{
			name:    "INDEX without a symbol on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT INDEX (a));",
			wantErr: `CONSTRAINT at position 68 stands before INDEX, which declares an index: .*`,
		},
		{
			name:    "FULLTEXT without a symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a TEXT, CONSTRAINT FULLTEXT (a));",
			wantErr: `CONSTRAINT at position 69 stands before FULLTEXT, which declares an index: .*`,
		},
		{
			name:    "SPATIAL with a symbol on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, g GEOMETRY NOT NULL, CONSTRAINT s SPATIAL (g));",
			wantErr: `CONSTRAINT at position 82 stands before SPATIAL, which declares an index: .*`,
		},
		{
			name:    "KEY without a symbol ALTER TABLE adds on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id INT, a INT);\nALTER TABLE c ADD CONSTRAINT KEY (a);",
			wantErr: `CONSTRAINT at position 87 stands before KEY, which declares an index: .*`,
		},
		{
			name:    "KEY with a symbol on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (id INT, a INT, CONSTRAINT k KEY (a));",
			wantErr: `CONSTRAINT at position 68 stands before KEY, which declares an index: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := parser.NewParser(constraintWithoutSymbolParents+test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.IsNil)
		})
	}
}
