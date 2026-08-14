package atlasschema

// White-box testing required: rescopeStatementsForDevDatabase is the step that
// keeps a dev-database rehearsal inside the dev database, and it runs between
// two package-private stages (plan preparation and the rehearsal core) that no
// exported entry point exposes separately. Driving it through `schema apply`
// would need a live MySQL server for every row; the rewrite and the refusal are
// pure string work and are asserted directly here. The end-to-end property —
// a failed apply leaves the target byte-identical — is covered live in
// simulate_mysql_live_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func TestRescopeStatementsForDevDatabase(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		target     string
		dev        string
		statements []string
		assert     func(c *qt.C, got []string, err error)
	}{
		{
			name:       "mysql qualified create table moves to the dev database",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `app`.`users` (\n  `id` int PRIMARY KEY\n)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE `appdev`.`users` (\n  `id` int PRIMARY KEY\n)"})
			},
		},
		{
			name:    "mysql rewrites every qualifier in one statement",
			dialect: platform.MySQL,
			target:  "app",
			dev:     "appdev",
			statements: []string{
				"ALTER TABLE `app`.`posts` ADD CONSTRAINT `fk` FOREIGN KEY (`user_id`) REFERENCES `app`.`users` (`id`)",
			},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{
					"ALTER TABLE `appdev`.`posts` ADD CONSTRAINT `fk` FOREIGN KEY (`user_id`) REFERENCES `appdev`.`users` (`id`)",
				})
			},
		},
		{
			name:       "mysql quotes a bare qualifier on the way to the dev database",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"DROP TABLE IF EXISTS app.users"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"DROP TABLE IF EXISTS `appdev`.users"})
			},
		},
		{
			name:       "mysql rewrites a session database switch",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"USE app"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"USE `appdev`"})
			},
		},
		{
			name:       "mysql rewrites a schema creation naming the target",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE SCHEMA IF NOT EXISTS `app`"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE SCHEMA IF NOT EXISTS `appdev`"})
			},
		},
		{
			name:       "mysql refuses a third database the rewrite cannot claim",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `elsewhere`.`users` (`id` int)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(got, qt.IsNil)
				c.Assert(IsDevScopeEscape(err), qt.IsTrue, qt.Commentf("error: %v", err))
				c.Assert(err, qt.ErrorMatches,
					`dev database simulation refused: statement 1 names schema "elsewhere", but the dev database is "appdev"\..*`)
			},
		},
		{
			name:       "mysql refuses a session switch to a third database",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"USE elsewhere"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(got, qt.IsNil)
				c.Assert(IsDevScopeEscape(err), qt.IsTrue, qt.Commentf("error: %v", err))
			},
		},
		{
			name:       "mysql reports the offending statement index",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `app`.`a` (`id` int)", "CREATE TABLE `elsewhere`.`b` (`id` int)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(got, qt.IsNil)
				var scopeErr *DevScopeError
				c.Assert(err, qt.ErrorAs, &scopeErr)
				c.Assert(scopeErr.StatementIndex, qt.Equals, 2)
				c.Assert(scopeErr.Schema, qt.Equals, "elsewhere")
			},
		},
		{
			name:       "mysql refuses a double-quoted foreign schema rather than missing it",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{`CREATE TABLE "elsewhere"."users" ("id" int)`},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(got, qt.IsNil)
				c.Assert(IsDevScopeEscape(err), qt.IsTrue, qt.Commentf("error: %v", err))
			},
		},
		{
			name:       "mysql leaves a decimal literal alone",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"ALTER TABLE `app`.`t` ADD COLUMN `score` decimal(10,2) NOT NULL DEFAULT 1.5"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{
					"ALTER TABLE `appdev`.`t` ADD COLUMN `score` decimal(10,2) NOT NULL DEFAULT 1.5",
				})
			},
		},
		{
			name:       "mysql leaves an unreserved column named database alone",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE t (database varchar(10))"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE t (database varchar(10))"})
			},
		},
		{
			name:       "mysql leaves a string literal spelling the target alone",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"INSERT INTO `app`.`audit` (`note`) VALUES ('app.users was migrated')"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{
					"INSERT INTO `appdev`.`audit` (`note`) VALUES ('app.users was migrated')",
				})
			},
		},
		{
			name:       "mariadb is rescoped like mysql",
			dialect:    platform.MariaDB,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `app`.`users` (`id` int)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE `appdev`.`users` (`id` int)"})
			},
		},
		{
			name:       "clickhouse is rescoped like mysql",
			dialect:    platform.ClickHouse,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `app`.`users` (`id` Int32) ENGINE = MergeTree ORDER BY `id`"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{
					"CREATE TABLE `appdev`.`users` (`id` Int32) ENGINE = MergeTree ORDER BY `id`",
				})
			},
		},
		{
			name:       "postgres statements are left exactly as planned",
			dialect:    platform.Postgres,
			target:     "public",
			dev:        "public",
			statements: []string{`CREATE TABLE "public"."users" ("id" int PRIMARY KEY NOT NULL)`},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				// A PostgreSQL schema is a namespace inside the connected
				// database, so the qualified plan already runs where the dev
				// connection points. Measured 2026-08-07 on live PostgreSQL 17:
				// the rehearsal created public.users in the dev database.
				c.Assert(got, qt.DeepEquals, []string{`CREATE TABLE "public"."users" ("id" int PRIMARY KEY NOT NULL)`})
			},
		},
		{
			name:       "sqlite statements are left exactly as planned",
			dialect:    platform.SQLite,
			target:     "",
			dev:        "",
			statements: []string{"CREATE TABLE sim_added (id INTEGER PRIMARY KEY)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE sim_added (id INTEGER PRIMARY KEY)"})
			},
		},
		{
			name:       "mysql dev connection must name a database",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "  ",
			statements: []string{"CREATE TABLE `app`.`users` (`id` int)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(got, qt.IsNil)
				c.Assert(err, qt.ErrorMatches, `--dev-url must name a database: .*`)
			},
		},
		{
			name:       "mysql unqualified statements need no rewrite",
			dialect:    platform.MySQL,
			target:     "app",
			dev:        "appdev",
			statements: []string{"CREATE TABLE `users` (`id` int)"},
			assert: func(c *qt.C, got []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE `users` (`id` int)"})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := rescopeStatementsForDevDatabase(test.statements, test.dialect, test.target, test.dev)
			test.assert(c, got, err)
		})
	}
}

func TestSchemaScopeNamesDatabase(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "mysql", dialect: platform.MySQL, want: true},
		{name: "mariadb", dialect: platform.MariaDB, want: true},
		{name: "clickhouse", dialect: platform.ClickHouse, want: true},
		{name: "postgres", dialect: platform.Postgres, want: false},
		{name: "cockroachdb", dialect: platform.CockroachDB, want: false},
		{name: "sqlite", dialect: platform.SQLite, want: false},
		{name: "sqlserver", dialect: platform.SQLServer, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaScopeNamesDatabase(test.dialect), qt.Equals, test.want)
		})
	}
}
