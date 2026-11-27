package sqlqualifier_test

import (
	"cmp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlqualifier"
)

// TestBody_HappyPath pins the bodies a server prints, as the MySQL family and
// ClickHouse print them. What is not the database's qualifier stays: another
// database, a name that only ends in the database's name, and a string literal.
func TestBody_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "the MySQL family's print", body: "select `dev`.`t`.`id` AS `id`,`dev`.`t`.`a` AS `a` from `dev`.`t`", want: "select `t`.`id` AS `id`,`t`.`a` AS `a` from `t`"},
		{name: "a bare qualifier", body: "SELECT id, a FROM dev.t", want: "SELECT id, a FROM t"},
		{name: "another database", body: "select `other`.`t`.`id` AS `id` from `other`.`t`", want: "select `other`.`t`.`id` AS `id` from `other`.`t`"},
		{name: "a name ending in the database's name", body: "SELECT id FROM my_dev.t", want: "SELECT id FROM my_dev.t"},
		{name: "a string literal", body: "SELECT 'dev.t' AS x, 'it''s dev.t' AS y FROM dev.t", want: "SELECT 'dev.t' AS x, 'it''s dev.t' AS y FROM t"},
		{name: "an alias spelled like the database", body: "SELECT dev.id FROM dev.users AS dev", want: "SELECT dev.id FROM users AS dev"},
		{name: "an alias spelled like the database, without the keyword", body: "select `dev`.`id` AS `id` from `dev`.`users` `dev`", want: "select `dev`.`id` AS `id` from `users` `dev`"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlqualifier.ViewBody(test.body, "mysql", "dev", ""), qt.Equals, test.want)
		})
	}
}

// TestRespellViewBody_HappyPath respells every database qualifier the answer
// names otherwise, and leaves what is not a qualifier as written: an alias in
// front of a column, a table in front of a column, and a string literal.
func TestRespellViewBody_HappyPath(t *testing.T) {
	stored := map[string]string{"APP4": "app4", "App4": "app4"}
	lower := func(name string) string { return cmp.Or(stored[name], name) }
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "bare qualifier", body: "SELECT id FROM APP4.t", want: "SELECT id FROM `app4`.t"},
		{name: "quoted qualifier", body: "select `App4`.`t`.`id` AS `id` from `App4`.`t`", want: "select `app4`.`t`.`id` AS `id` from `app4`.`t`"},
		{name: "another database", body: "SELECT a.id FROM APP4.t JOIN crm.a a ON a.id = t.id", want: "SELECT a.id FROM `app4`.t JOIN crm.a a ON a.id = t.id"},
		{name: "alias spelled as the database", body: "SELECT APP4.id FROM t APP4", want: "SELECT APP4.id FROM t APP4"},
		{name: "string literal", body: "SELECT 'APP4.t' AS s FROM t", want: "SELECT 'APP4.t' AS s FROM t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlqualifier.RespellViewBody(test.body, "mysql", lower), qt.Equals, test.want)
		})
	}
}

// TestExpression_HappyPath pins the column defaults and virtual columns the
// MariaDB and Oracle catalogs store, read in the database or account they
// name. A sequence or function of that schema loses its qualifier, and another
// schema's, a part after a dot, a name only ending in the schema's name and a
// string literal stay.
func TestExpression_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, schema, expression, want string
	}{
		{name: "MariaDB nextval", dialect: "mariadb", schema: "app", expression: "nextval(`app`.`s`)", want: "nextval(`s`)"},
		{name: "MariaDB lastval", dialect: "mariadb", schema: "app", expression: "lastval(`app`.`s`)", want: "lastval(`s`)"},
		{name: "MariaDB setval", dialect: "mariadb", schema: "app", expression: "setval(`app`.`s`,5,1,0)", want: "setval(`s`,5,1,0)"},
		{name: "MariaDB inside an operator", dialect: "mariadb", schema: "app", expression: "(nextval(`app`.`s`) + 1)", want: "(nextval(`s`) + 1)"},
		{name: "MariaDB another database", dialect: "mariadb", schema: "app", expression: "nextval(`other`.`s`)", want: "nextval(`other`.`s`)"},
		{name: "MariaDB a name ending in the database's", dialect: "mariadb", schema: "app", expression: "nextval(`myapp`.`s`)", want: "nextval(`myapp`.`s`)"},
		{name: "MariaDB a sequence named like the database", dialect: "mariadb", schema: "app", expression: "nextval(`app`.`app`)", want: "nextval(`app`)"},
		{name: "MariaDB a string literal", dialect: "mariadb", schema: "app", expression: "'app.s'", want: "'app.s'"},
		{name: "MariaDB case is kept apart", dialect: "mariadb", schema: "app", expression: "nextval(`APP`.`s`)", want: "nextval(`APP`.`s`)"},
		{name: "Oracle sequence", dialect: "oracle", schema: "APP", expression: `"APP"."SEQ"."NEXTVAL"`, want: `"SEQ"."NEXTVAL"`},
		{name: "Oracle sequence written bare", dialect: "oracle", schema: "APP", expression: "app.seq.nextval", want: "seq.nextval"},
		{name: "Oracle sequence of a quoted lower-case account", dialect: "oracle", schema: "APP", expression: `"app"."SEQ"."NEXTVAL"`, want: `"app"."SEQ"."NEXTVAL"`},
		{name: "Oracle sequence in an operator", dialect: "oracle", schema: "APP", expression: `"APP"."SEQ"."NEXTVAL"+1`, want: `"SEQ"."NEXTVAL"+1`},
		{name: "Oracle function", dialect: "oracle", schema: "APP", expression: `"APP"."DBL"("N")`, want: `"DBL"("N")`},
		{name: "Oracle function of another account", dialect: "oracle", schema: "APP", expression: `"CRM"."DBL"("N")`, want: `"CRM"."DBL"("N")`},
		{name: "Oracle package function", dialect: "oracle", schema: "APP", expression: `"APP"."PKG"."F"("N")`, want: `"PKG"."F"("N")`},
		{name: "Oracle sequence named like the account", dialect: "oracle", schema: "APP", expression: `"APP"."APP"."NEXTVAL"`, want: `"APP"."NEXTVAL"`},
		{name: "Oracle column", dialect: "oracle", schema: "APP", expression: `"APP"*2`, want: `"APP"*2`},
		{name: "Oracle string literal", dialect: "oracle", schema: "APP", expression: `'APP.SEQ.NEXTVAL'`, want: `'APP.SEQ.NEXTVAL'`},
		{name: "no schema", dialect: "oracle", schema: "", expression: `"APP"."SEQ"."NEXTVAL"`, want: `"APP"."SEQ"."NEXTVAL"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlqualifier.Expression(test.expression, test.dialect, test.schema), qt.Equals, test.want)
		})
	}
}
