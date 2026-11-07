package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestParser_ViewCheckOption_HappyPath pins that a view's trailing WITH
// [CASCADED | LOCAL] CHECK OPTION is read into WithCheck and LocalCheck and
// left out of the body, in every spelling the engines take. Kept in the body,
// the clause made the declared body differ from the one the server reports,
// which stores the clause apart, so every plan replaced the view again.
//
// The last rows pin the words where they are not the clause: in a string
// literal, a quoted identifier, a comment or a subquery, the body keeps them.
func TestParser_ViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		sql       string
		wantBody  string
		wantCheck bool
		wantLocal bool
	}{
		{
			name:      "the clause without a level",
			dialect:   platform.Postgres,
			sql:       "CREATE VIEW v AS SELECT id FROM t WHERE n > 0 WITH CHECK OPTION;",
			wantBody:  "SELECT id FROM t WHERE n > 0",
			wantCheck: true,
		},
		{
			name:      "the cascaded level",
			dialect:   platform.MySQL,
			sql:       "CREATE VIEW v AS SELECT id FROM t WHERE n > 0 WITH CASCADED CHECK OPTION;",
			wantBody:  "SELECT id FROM t WHERE n > 0",
			wantCheck: true,
		},
		{
			name:      "the local level",
			dialect:   platform.MariaDB,
			sql:       "CREATE VIEW v AS SELECT id FROM t WHERE n > 0 WITH LOCAL CHECK OPTION;",
			wantBody:  "SELECT id FROM t WHERE n > 0",
			wantCheck: true,
			wantLocal: true,
		},
		{
			name:      "lower case with comments between the words and after the clause",
			dialect:   platform.Postgres,
			sql:       "create view v as select id from t where n > 0\nwith /* level */ local -- note\ncheck\toption /* end */;",
			wantBody:  "select id from t where n > 0",
			wantCheck: true,
			wantLocal: true,
		},
		{
			name:      "no dialect named",
			sql:       "CREATE VIEW v AS SELECT id FROM t WITH CHECK OPTION",
			wantBody:  "SELECT id FROM t",
			wantCheck: true,
		},
		{
			name:      "a replace with a column list and options",
			dialect:   platform.Postgres,
			sql:       "CREATE OR REPLACE VIEW v (a) WITH (security_barrier) AS SELECT id FROM t WITH LOCAL CHECK OPTION;",
			wantBody:  "SELECT id AS a FROM t",
			wantCheck: true,
			wantLocal: true,
		},
		{
			name:      "a body that opens with a common table expression",
			dialect:   platform.Postgres,
			sql:       "CREATE VIEW v AS WITH x AS (SELECT 1 AS a) SELECT a FROM x WITH CASCADED CHECK OPTION;",
			wantBody:  "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
			wantCheck: true,
		},
		{
			name:      "the SQL Server spelling",
			dialect:   platform.SQLServer,
			sql:       "CREATE VIEW dbo.v AS SELECT id FROM dbo.t WHERE n > 0 WITH CHECK OPTION;",
			wantBody:  "SELECT id FROM dbo.t WHERE n > 0",
			wantCheck: true,
		},
		{
			name:     "no clause",
			dialect:  platform.Postgres,
			sql:      "CREATE VIEW v AS SELECT id FROM t;",
			wantBody: "SELECT id FROM t",
		},
		{
			name:     "the words in a string literal",
			dialect:  platform.Postgres,
			sql:      "CREATE VIEW v AS SELECT id, 'WITH CHECK OPTION' AS note FROM t WHERE note <> 'WITH LOCAL CHECK OPTION';",
			wantBody: "SELECT id, 'WITH CHECK OPTION' AS note FROM t WHERE note <> 'WITH LOCAL CHECK OPTION'",
		},
		{
			name:     "the words in a quoted identifier",
			dialect:  platform.Postgres,
			sql:      `CREATE VIEW v AS SELECT id AS "WITH CHECK OPTION" FROM t AS "WITH LOCAL CHECK OPTION";`,
			wantBody: `SELECT id AS "WITH CHECK OPTION" FROM t AS "WITH LOCAL CHECK OPTION"`,
		},
		{
			name:     "the words in a trailing comment",
			dialect:  platform.Postgres,
			sql:      "CREATE VIEW v AS SELECT id FROM t -- WITH CHECK OPTION\n;",
			wantBody: "SELECT id FROM t -- WITH CHECK OPTION",
		},
		{
			name:     "the words in a subquery",
			dialect:  platform.Postgres,
			sql:      "CREATE VIEW v AS SELECT id FROM t WHERE id IN (SELECT id FROM u WITH CHECK OPTION);",
			wantBody: "SELECT id FROM t WHERE id IN (SELECT id FROM u WITH CHECK OPTION)",
		},
		{
			name:     "a level the clause does not take",
			dialect:  platform.Postgres,
			sql:      "CREATE VIEW v AS SELECT id FROM t WITH STRICT CHECK OPTION;",
			wantBody: "SELECT id FROM t WITH STRICT CHECK OPTION",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			view, ok := statements.Statements[0].(*ast.CreateViewNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(view.Body, qt.Equals, test.wantBody)
			c.Assert(view.WithCheck, qt.Equals, test.wantCheck)
			c.Assert(view.LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

// TestParser_ViewCheckOption_FailurePath pins that a check option with no query
// before it is a view without a body, not a body of three words.
func TestParser_ViewCheckOption_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "the clause alone", sql: "CREATE VIEW v AS WITH CHECK OPTION;"},
		{name: "the clause with a level, in lower case", sql: "create view v as with local check option;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.ErrorMatches, `line 1, column \d+: expected view body after AS`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
