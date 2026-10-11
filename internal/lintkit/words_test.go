package lintkit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
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

// Every unique index a file builds is found, in file order: the spellings the
// package reads, and the sites a dialect's owner reported as effects.
func TestUniqueIndexSites(t *testing.T) {
	const sql = "CREATE UNIQUE INDEX CONCURRENTLY users_email ON users (email);\n" +
		"ALTER TABLE users ADD CONSTRAINT users_name UNIQUE (first, last), ADD COLUMN a int;\n" +
		"CREATE INDEX plain ON users (a);\n"
	c := qt.New(t)

	sites := lintkit.UniqueIndexSites(statements(sql))

	c.Assert(sites, qt.HasLen, 2)
	c.Assert([]any{sites[0].Statement, sites[0].Name, sites[0].Table.Normalized, sites[0].Columns, sites[0].Concurrently, sites[0].Label()},
		qt.DeepEquals, []any{0, "users_email", "USERS", []string{"email"}, true, "CREATE UNIQUE INDEX users_email"})
	c.Assert([]any{sites[1].Statement, sites[1].Name, sites[1].Table.Normalized, sites[1].Columns},
		qt.DeepEquals, []any{1, "users_name", "USERS", []string{"first", "last"}})
}

// A site an owner reported carries the owner's words, and its table is folded
// the way the package folds a reference it reads itself, so the two compare.
func TestUniqueIndexSites_Effects(t *testing.T) {
	c := qt.New(t)
	yql := lintscan.Split("ALTER TABLE `dir/Users` ADD INDEX by_email GLOBAL UNIQUE SYNC ON (email);", lintscan.ForDialect("ydb"))
	c.Assert(lintkit.UniqueIndexSites(yql), qt.HasLen, 0)
	yql[0].Effects = []lintext.Effect{{
		Fact: lintext.FactUniqueIndex, Object: "by_email", Table: "`dir/Users`", Columns: []string{"email"},
		Clause: "ADD INDEX", Failure: "YDB: Duplicate key found",
	}}

	sites := lintkit.UniqueIndexSites(yql)

	c.Assert(sites, qt.DeepEquals, []lintkit.UniqueIndexSite{{
		Statement: 0, Name: "by_email", Table: lintkit.TableReference{Name: "`dir/Users`", Normalized: "DIR/USERS"},
		Columns: []string{"email"}, Spelled: "ADD INDEX", Failure: "YDB: Duplicate key found",
	}})
	c.Assert(sites[0].Table.Normalized, qt.Equals, lintkit.AlterTableReference(yql[0].Words, yql[0].SourceWords).Normalized)
}

// A reference as written folds part by part; a dot inside quotes belongs to
// its part.
func TestNormalizeReference(t *testing.T) {
	for _, test := range []struct{ reference, want string }{
		{"users", "USERS"},
		{`public."Users"`, "PUBLIC.USERS"},
		{"`a.b`.c", "A.B.C"},
		{"`dir/users`", "DIR/USERS"},
	} {
		t.Run(test.reference, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.NormalizeReference(test.reference), qt.Equals, test.want)
		})
	}
}

func TestNormalizeIdent(t *testing.T) {
	for _, test := range []struct{ word, want string }{{"users", "USERS"}, {`"Users"`, "USERS"}, {"`users`", "USERS"}} {
		t.Run(test.word, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintkit.NormalizeIdent(test.word), qt.Equals, test.want)
		})
	}
}
