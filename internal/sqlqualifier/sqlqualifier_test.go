package sqlqualifier_test

import (
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
