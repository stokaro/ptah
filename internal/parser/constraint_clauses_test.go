package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// parsed answers the statements sql parses to under dialect, with the MySQL
// owner's column options read.
func parsed(c *qt.C, dialect, sql string) []ast.Node {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect), withMySQLOwners()).Parse()
	c.Assert(err, qt.IsNil)
	return statements.Statements
}

// TestParse_ClauseThatChangesNothing_HappyPath reads a clause after a table
// element that builds what the element builds without it, and pins that by
// comparing the parse with the parse of the same statement without the clause.
// Each row was run on the server its dialect names: PostgreSQL 18.6, MySQL 8.4
// and 26.7, MariaDB 11.8 and 12.3. Unread, each is refused as `unexpected
// <word> after a table element` (stokaro/ptah#3826).
func TestParse_ClauseThatChangesNothing_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		without string
	}{
		{
			// PostgreSQL 18.6 records the constraint validated.
			name:    "NOT VALID after a CHECK in CREATE TABLE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT VALID);",
			without: "CREATE TABLE c (a int, CHECK (a > 0));",
		},
		{
			name:    "NOT VALID after a foreign key in CREATE TABLE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) NOT VALID);",
			without: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id));",
		},
		{
			name:    "ENFORCED after a CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) ENFORCED);",
			without: "CREATE TABLE c (a int, CHECK (a > 0));",
		},
		{
			// ALTER TABLE ... ADD reads a constraint under its own rule for NOT
			// VALID, and a CREATE TABLE after it is read under CREATE TABLE's.
			name:    "NOT VALID in a CREATE TABLE after an ALTER TABLE",
			dialect: platform.Postgres,
			sql:     "ALTER TABLE a ADD CONSTRAINT a_ck CHECK (x > 0);\nCREATE TABLE c (a int, CHECK (a > 0) NOT VALID);",
			without: "ALTER TABLE a ADD CONSTRAINT a_ck CHECK (x > 0);\nCREATE TABLE c (a int, CHECK (a > 0));",
		},
		{
			name:    "ENFORCED after a foreign key, beside a deferral clause",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ENFORCED DEFERRABLE NOT VALID);",
			without: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) DEFERRABLE);",
		},
		{
			name:    "ENFORCED after a column's CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) ENFORCED NOT NULL, b int);",
			without: "CREATE TABLE c (a int CHECK (a > 0) NOT NULL, b int);",
		},
		{
			name:    "ENFORCED after a column's REFERENCES",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) ENFORCED);",
			without: "CREATE TABLE c (a int REFERENCES p (id));",
		},
		{
			name:    "ENFORCED after a CHECK ALTER TABLE adds",
			dialect: platform.Postgres,
			sql:     "ALTER TABLE c ADD CONSTRAINT c_ck CHECK (a > 0) ENFORCED;",
			without: "ALTER TABLE c ADD CONSTRAINT c_ck CHECK (a > 0);",
		},
		{
			name:    "MATCH SIMPLE before the actions",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE);",
			without: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE);",
		},
		{
			name:    "MATCH SIMPLE on a column's REFERENCES",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) MATCH SIMPLE);",
			without: "CREATE TABLE c (a int REFERENCES p (id));",
		},
		{
			// MySQL 8.4 and 26.7 keep ON DELETE beside MATCH SIMPLE.
			name:    "MATCH SIMPLE on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE ON DELETE CASCADE);",
			without: "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE);",
		},
		{
			name:    "ENFORCED after a MySQL CHECK",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, CONSTRAINT ck CHECK (a > 0) ENFORCED);",
			without: "CREATE TABLE c (a int, CONSTRAINT ck CHECK (a > 0));",
		},
		{
			name:    "ENFORCED after a MySQL column CHECK",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) ENFORCED);",
			without: "CREATE TABLE c (a int CHECK (a > 0));",
		},
		{
			// MariaDB 11.8 and 12.3 build `KEY k USING BTREE (a) USING HASH` as
			// HASH: the later clause wins.
			name:    "USING after a key's parts",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, KEY k USING BTREE (a) USING HASH);",
			without: "CREATE TABLE c (a int, KEY k USING HASH (a));",
		},
		{
			name:    "USING after a UNIQUE key's parts",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, UNIQUE KEY k (a) USING HASH);",
			without: "CREATE TABLE c (a int, UNIQUE KEY k USING HASH (a));",
		},
		{
			name:    "USING BTREE after a key's parts",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k USING HASH (a) USING BTREE);",
			without: "CREATE TABLE c (a int, KEY k (a));",
		},
		{
			name:    "USING BTREE after a primary key's parts",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, PRIMARY KEY (a) USING BTREE);",
			without: "CREATE TABLE c (a int, PRIMARY KEY (a));",
		},
		{
			name:    "VISIBLE",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) VISIBLE);",
			without: "CREATE TABLE c (a int, KEY k (a));",
		},
		{
			name:    "NOT IGNORED on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, UNIQUE KEY k (a) NOT IGNORED);",
			without: "CREATE TABLE c (a int, UNIQUE KEY k (a));",
		},
		{
			name:    "index options after a FULLTEXT parser",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (b text, FULLTEXT KEY f (b) WITH PARSER ngram VISIBLE);",
			without: "CREATE TABLE c (b text, FULLTEXT KEY f (b) WITH PARSER ngram);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := parsed(c, test.dialect, test.sql)

			c.Assert(got, qt.DeepEquals, parsed(c, test.dialect, test.without))
		})
	}
}

// TestParse_UnmodeledClause_FailurePath refuses by name a clause that declares
// something the model has no field for, and a clause the dialect's server
// refuses. Every accepted clause was run on its server: PostgreSQL 18.6,
// MySQL 8.4 and 26.7, MariaDB 11.8 and 12.3.
func TestParse_UnmodeledClause_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			name:    "ENFORCED after a UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) ENFORCED);",
			wantErr: `line 1, column 35: ENFORCED: UNIQUE constraints cannot be marked ENFORCED`,
		},
		{
			name:    "ENFORCED after an EXCLUDE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =) ENFORCED);",
			wantErr: `(?s).*PostgreSQL EXCLUDE: unrepresented or unexpected clause ENFORCED.*`,
		},
		{
			name:    "ENFORCED after an index",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) ENFORCED);",
			wantErr: `line 1, column 34: ENFORCED: an index takes no ENFORCED clause`,
		},
		{
			name:    "ENFORCED after NOT NULL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int NOT NULL ENFORCED);",
			wantErr: `line 1, column 32: misplaced ENFORCED clause: .*`,
		},
		{
			name:    "ENFORCED after a column's UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int UNIQUE ENFORCED);",
			wantErr: `line 1, column 30: misplaced ENFORCED clause: .*`,
		},
		{
			name:    "ENFORCED after a foreign key on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) ENFORCED);",
			wantErr: `line 1, column 58: ENFORCED: the mysql dialect takes no ENFORCED clause after a FOREIGN KEY`,
		},
		{
			name:    "ENFORCED on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, CONSTRAINT ck CHECK (a > 0) ENFORCED);",
			wantErr: `line 1, column 52: ENFORCED: the mariadb dialect takes no ENFORCED clause after a CHECK`,
		},
		{
			name:    "NOT VALID after a UNIQUE",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) NOT VALID);",
			wantErr: `line 1, column 35: NOT VALID: UNIQUE constraints cannot be marked NOT VALID`,
		},
		{
			name:    "NOT VALID after a column's CHECK",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) NOT VALID);",
			wantErr: `line 1, column 37: NOT VALID: it follows a table constraint, not a column`,
		},
		{
			name:    "NOT VALID on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, CONSTRAINT ck CHECK (a > 0) NOT VALID);",
			wantErr: `line 1, column 52: NOT VALID: the mysql dialect takes no NOT VALID clause`,
		},
		{
			name:    "NOT ENFORCED twice",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT ENFORCED ENFORCED);",
			wantErr: "line 1, column 51: NOT ENFORCED or ENFORCED: a constraint takes one of them, and PostgreSQL 18.6 answers " +
				"`multiple ENFORCED/NOT ENFORCED clauses not allowed`",
		},
		{
			name:    "NOT ENFORCED twice on a column",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int CHECK (a > 0) ENFORCED NOT ENFORCED);",
			wantErr: "line 1, column 46: NOT ENFORCED or ENFORCED: .*",
		},
		{
			name:    "NOT ENFORCED after a foreign key on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) NOT ENFORCED);",
			wantErr: `line 1, column 58: NOT ENFORCED: the mysql dialect takes no NOT ENFORCED clause after a FOREIGN KEY`,
		},
		{
			name:    "NOT ENFORCED on CockroachDB",
			dialect: platform.CockroachDB,
			sql:     "CREATE TABLE c (a int, CHECK (a > 0) NOT ENFORCED);",
			wantErr: `line 1, column 38: NOT ENFORCED: the cockroachdb dialect takes no NOT ENFORCED clause after a CHECK`,
		},
		{
			name:    "MATCH FULL on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p (id, k) MATCH FULL);",
			wantErr: `line 1, column 71: .*MATCH FULL: MariaDB 11.8.9 accepts the clause and records NONE, so the key it builds ` +
				`is MATCH SIMPLE; declare the key without the clause`,
		},
		{
			name:    "MATCH PARTIAL on SQLite",
			dialect: platform.SQLite,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) MATCH PARTIAL);",
			wantErr: `line 1, column 41: .*MATCH PARTIAL: SQLite 3.51 accepts the clause and records NONE.*`,
		},
		{
			name:    "MATCH PARTIAL on PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, b int, FOREIGN KEY (a, b) REFERENCES p (id, k) MATCH PARTIAL);",
			wantErr: "line 1, column 71: .*MATCH PARTIAL: the postgres dialect's server does not implement it, and " +
				"PostgreSQL 18.6 answers `MATCH PARTIAL not yet implemented`",
		},
		{
			name:    "MATCH PARTIAL on CockroachDB",
			dialect: platform.CockroachDB,
			sql:     "CREATE TABLE c (a int REFERENCES p (id) MATCH PARTIAL);",
			wantErr: `line 1, column 41: .*MATCH PARTIAL: the cockroachdb dialect's server does not implement it.*`,
		},
		{
			name:    "MATCH with no type",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH ON DELETE CASCADE);",
			wantErr: `line 1, column 64: .*expected SIMPLE, FULL or PARTIAL after MATCH`,
		},
		{
			name:    "MATCH on SQL Server",
			dialect: platform.SQLServer,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) REFERENCES p (id) MATCH SIMPLE);",
			wantErr: `line 1, column 58: .*MATCH: the sqlserver dialect takes no MATCH clause`,
		},
		{
			name:    "an invisible primary key",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, PRIMARY KEY (a) INVISIBLE);",
			wantErr: `line 1, column 40: INVISIBLE: MySQL 8.4.11 answers ERROR 3522, a primary key index cannot be invisible`,
		},
		{
			name:    "an ignored primary key",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, PRIMARY KEY (a) IGNORED);",
			wantErr: `line 1, column 40: IGNORED: MariaDB 11.8.9 answers ERROR 4174, a primary key cannot be marked as IGNORE`,
		},
		{
			name:    "INVISIBLE on MariaDB",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (a int, KEY k (a) INVISIBLE);",
			wantErr: `line 1, column 34: INVISIBLE: it is MySQL's clause, and MariaDB 11.8.9 answers ERROR 1064; MariaDB spells it IGNORED`,
		},
		{
			name:    "IGNORED on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) IGNORED);",
			wantErr: `line 1, column 34: IGNORED: it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064`,
		},
		{
			name:    "NOT IGNORED on MySQL",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) NOT IGNORED);",
			wantErr: `line 1, column 34: NOT IGNORED: it is MariaDB's clause, and MySQL 8.4 answers ERROR 1064`,
		},
		{
			name:    "an engine attribute",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) ENGINE_ATTRIBUTE = '{}');",
			wantErr: `line 1, column 34: ENGINE_ATTRIBUTE: an engine attribute of an index is not modeled.*`,
		},
		{
			name:    "an option after other options",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, KEY k (a) VISIBLE USING BTREE COMMENT 'x' ENGINE_ATTRIBUTE = '{}');",
			wantErr: `line 1, column 66: ENGINE_ATTRIBUTE: .*`,
		},
		{
			name:    "an index option after a foreign key's columns",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (a int, FOREIGN KEY (a) VISIBLE REFERENCES p (id));",
			wantErr: `line 1, column 40: expected REFERENCES after FOREIGN KEY: expected 'REFERENCES', got 'VISIBLE'`,
		},
		{
			name:    "USING on a FULLTEXT index",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (b text, FULLTEXT KEY f (b) USING BTREE);",
			wantErr: `line 1, column 44: USING: a FULLTEXT index takes no access method`,
		},
		{
			name:    "a tablespace after a key still refused",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE c (a int, UNIQUE (a) USING INDEX TABLESPACE pg_default);",
			wantErr: `line 1, column 35: UNIQUE USING INDEX TABLESPACE: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect), parser.WithSQL(exclusionParserRuntime(c).SQL())).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
