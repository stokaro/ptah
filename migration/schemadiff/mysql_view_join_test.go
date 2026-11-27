package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A view whose body joins tables is stored by MySQL and MariaDB with the
// joins in parentheses, each ON condition in parentheses, a table alias
// without AS and a join keyword without INNER, CROSS or OUTER. The held bodies
// are those MySQL 26.10 and MariaDB 12.3 store, read from one database, so
// without its qualifier. A declaration of the same query plans nothing
// (stokaro/ptah#6757).
func TestMySQLViewJoin_MatchesTheStoredJoin(t *testing.T) {
	for _, test := range []struct{ name, dialect, declared, held string }{
		{
			name: "join with an alias", dialect: "mysql",
			declared: "SELECT t.id, a.name FROM t JOIN u a ON a.id = t.id",
			held:     "select `t`.`id` AS `id`,`a`.`name` AS `name` from (`t` join `u` `a` on((`a`.`id` = `t`.`id`)))",
		},
		{
			name: "mariadb join with an alias", dialect: "mariadb",
			declared: "SELECT t.id, a.name FROM t JOIN u a ON a.id = t.id",
			held:     "select `t`.`id` AS `id`,`a`.`name` AS `name` from (`t` join `u` `a` on(`a`.`id` = `t`.`id`))",
		},
		{
			name: "inner join with AS and two conditions", dialect: "mysql",
			declared: "SELECT t.id FROM t INNER JOIN u AS a ON a.id = t.id AND a.name = 'x'",
			held:     "select `t`.`id` AS `id` from (`t` join `u` `a` on(((`a`.`id` = `t`.`id`) and (`a`.`name` = 'x'))))",
		},
		{
			name: "mariadb inner join with AS and two conditions", dialect: "mariadb",
			declared: "SELECT t.id FROM t INNER JOIN u AS a ON a.id = t.id AND a.name = 'x'",
			held:     "select `t`.`id` AS `id` from (`t` join `u` `a` on(`a`.`id` = `t`.`id` and `a`.`name` = 'x'))",
		},
		{
			name: "two outer joins", dialect: "mysql",
			declared: "SELECT t.id FROM t LEFT OUTER JOIN u ON u.id = t.id LEFT JOIN w ON w.id = u.id",
			held: "select `t`.`id` AS `id` from ((`t` left join `u` on((`u`.`id` = `t`.`id`))) " +
				"left join `w` on((`w`.`id` = `u`.`id`)))",
		},
		{
			name: "a condition with its own group", dialect: "mysql",
			declared: "SELECT t.id FROM t JOIN u ON (u.id = t.id OR u.id = t.x) AND t.x > 1",
			held: "select `t`.`id` AS `id` from (`t` join `u` on((((`u`.`id` = `t`.`id`) or (`u`.`id` = `t`.`x`)) " +
				"and (`t`.`x` > 1))))",
		},
		{
			name: "mariadb a condition with its own group", dialect: "mariadb",
			declared: "SELECT t.id FROM t JOIN u ON (u.id = t.id OR u.id = t.x) AND t.x > 1",
			held:     "select `t`.`id` AS `id` from (`t` join `u` on((`u`.`id` = `t`.`id` or `u`.`id` = `t`.`x`) and `t`.`x` > 1))",
		},
		{
			name: "cross join", dialect: "mysql",
			declared: "SELECT t.id FROM t CROSS JOIN u",
			held:     "select `t`.`id` AS `id` from (`t` join `u`)",
		},
		{
			name: "a join nested on the right", dialect: "mysql",
			declared: "SELECT t.id FROM t JOIN (u JOIN w ON w.id = u.id) ON u.id = t.id",
			held: "select `t`.`id` AS `id` from (`t` join (`u` join `w` on((`w`.`id` = `u`.`id`))) " +
				"on((`u`.`id` = `t`.`id`)))",
		},
		{
			name: "a join and a where clause", dialect: "mysql",
			declared: "SELECT t.id FROM t JOIN u ON u.id = t.id WHERE t.x = 1",
			held:     "select `t`.`id` AS `id` from (`t` join `u` on((`u`.`id` = `t`.`id`))) where (`t`.`x` = 1)",
		},
		{
			name: "a join to another database", dialect: "mysql",
			declared: "SELECT t.id, a.name FROM t JOIN crm6.accounts a ON a.id = t.id",
			held:     "select `t`.`id` AS `id`,`a`.`name` AS `name` from (`t` join `crm6`.`accounts` `a` on((`a`.`id` = `t`.`id`)))",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := mysqlLocalViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlLocalOptions(test.dialect), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// A declared join that asks another query than the stored one is still a
// change: another join kind, a condition grouped another way, an outer join
// nested another way, and a join the server reorders, which the comparison
// does not read.
func TestMySQLViewJoin_KeepsDifferentQueries(t *testing.T) {
	for _, test := range []struct{ name, dialect, declared, held string }{
		{
			name: "left join against a join", dialect: "mysql",
			declared: "SELECT t.id FROM t LEFT JOIN u ON u.id = t.id",
			held:     "select `t`.`id` AS `id` from (`t` join `u` on((`u`.`id` = `t`.`id`)))",
		},
		{
			name: "a condition grouped another way", dialect: "mariadb",
			declared: "SELECT t.id FROM t JOIN u ON u.id = t.id OR (u.id = t.x AND t.x > 1)",
			held:     "select `t`.`id` AS `id` from (`t` join `u` on((`u`.`id` = `t`.`id` or `u`.`id` = `t`.`x`) and `t`.`x` > 1))",
		},
		{
			name: "a group the declaration keeps against a condition without it", dialect: "mariadb",
			declared: "SELECT t.id FROM t JOIN u ON (u.id = t.id OR u.id = t.x) AND t.x > 1",
			held:     "select `t`.`id` AS `id` from (`t` join `u` on(`u`.`id` = `t`.`id` or `u`.`id` = `t`.`x` and `t`.`x` > 1))",
		},
		{
			name: "an outer join nested on the right", dialect: "mysql",
			declared: "SELECT t.id FROM t LEFT JOIN (u JOIN w ON w.id = u.id) ON u.id = t.id",
			held: "select `t`.`id` AS `id` from ((`t` left join `u` on((`u`.`id` = `t`.`id`))) " +
				"join `w` on((`w`.`id` = `u`.`id`)))",
		},
		{
			name: "another alias", dialect: "mysql",
			declared: "SELECT t.id FROM t JOIN u AS b ON b.id = t.id",
			held:     "select `t`.`id` AS `id` from (`t` join `u` `a` on((`a`.`id` = `t`.`id`)))",
		},
		{
			name: "a right join the server writes as a left join", dialect: "mysql",
			declared: "SELECT t.id FROM t RIGHT JOIN u ON u.id = t.id",
			held:     "select `t`.`id` AS `id` from (`u` left join `t` on((`u`.`id` = `t`.`id`)))",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := mysqlLocalViewBodies(test.declared, test.held)

			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlLocalOptions(test.dialect), must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ViewsModified, qt.HasLen, 1)
		})
	}
}

// PostgreSQL states no join spelling of its own, so the join fold does not
// read its bodies: a declaration in MySQL's stored form is still a change.
func TestMySQLViewJoin_IsMySQLsAlone(t *testing.T) {
	c := qt.New(t)
	desired, held := mysqlLocalViewBodies(
		"SELECT t.id FROM t JOIN u ON u.id = t.id",
		"SELECT t.id FROM (t JOIN u ON ((u.id = t.id)))",
	)

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, held, mysqlWholeServerOptions("postgres"), must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsModified, qt.HasLen, 1)
}
