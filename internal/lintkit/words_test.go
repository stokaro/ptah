package lintkit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// The table an ALTER TABLE alters is read past ONLY and IF EXISTS, as the
// author spelled it and normalized for comparison.
func TestAlterTableReference(t *testing.T) {
	for _, test := range []struct {
		sql  string
		want lintkit.TableReference
	}{
		{"ALTER TABLE users ADD COLUMN a int", lintkit.TableReference{Name: "users", Normalized: "USERS"}},
		{`ALTER TABLE IF EXISTS ONLY public."Users" ADD COLUMN a int`, lintkit.TableReference{Name: `public."Users"`, Normalized: "PUBLIC.USERS"}},
		{"DROP TABLE users", lintkit.TableReference{}},
	} {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)
			statement := statements(test.sql)[0]
			c.Assert(lintkit.AlterTableReference(statement.Words, statement.SourceWords), qt.DeepEquals, test.want)
		})
	}
}

// Every unique index a file builds is found, in file order, and YQL's own
// spelling only where the target is YDB.
func TestUniqueIndexSites(t *testing.T) {
	const sql = "CREATE UNIQUE INDEX CONCURRENTLY users_email ON users (email);\n" +
		"ALTER TABLE users ADD CONSTRAINT users_name UNIQUE (first, last), ADD COLUMN a int;\n" +
		"CREATE INDEX plain ON users (a);\n"
	c := qt.New(t)

	sites := lintkit.UniqueIndexSites(statements(sql), "postgres")

	c.Assert(sites, qt.HasLen, 2)
	c.Assert([]any{sites[0].Statement, sites[0].Name, sites[0].Table.Normalized, sites[0].Columns, sites[0].Concurrently, sites[0].Label()},
		qt.DeepEquals, []any{0, "users_email", "USERS", []string{"email"}, true, "CREATE UNIQUE INDEX users_email"})
	c.Assert([]any{sites[1].Statement, sites[1].Name, sites[1].Table.Normalized, sites[1].Columns},
		qt.DeepEquals, []any{1, "users_name", "USERS", []string{"first", "last"}})

	yql := lintscan.Split("ALTER TABLE users ADD INDEX by_email GLOBAL UNIQUE SYNC ON (email);", lintscan.ForDialect("ydb"))
	c.Assert(lintkit.UniqueIndexSites(yql, "postgres"), qt.HasLen, 0)
	c.Assert(lintkit.UniqueIndexSites(yql, "ydb"), qt.HasLen, 1)
}

func TestNormalizeIdent(t *testing.T) {
	for _, test := range []struct{ word, want string }{{"users", "USERS"}, {`"Users"`, "USERS"}, {"`users`", "USERS"}} {
		t.Run(test.word, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.NormalizeIdent(test.word), qt.Equals, test.want)
		})
	}
}
