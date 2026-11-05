package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// parsed is the nodes sql parses into as PostgreSQL.
func parsed(c *qt.C, sql string) []ast.Node {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres), parser.WithSQL(sqlext.None())).Parse()
	c.Assert(err, qt.IsNil)
	return statements.Statements
}

// followAll follows every node of sql and returns each statement's answer.
func followAll(c *qt.C, follower *sqlschema.NameFollower, sql string) []sqlschema.StatementNames {
	c.Helper()
	var answers []sqlschema.StatementNames
	for _, node := range parsed(c, sql) {
		answers = append(answers, follower.Follow(node))
	}
	return answers
}

// TestNewNameFollower holds that a follower exists for the dialects whose
// descriptor says their server cuts long names, and for no other.
func TestNewNameFollower(t *testing.T) {
	rows := []struct {
		dialect string
		cuts    bool
	}{
		{dialect: platform.Postgres, cuts: true},
		{dialect: platform.YugabyteDB, cuts: true},
		{dialect: platform.CockroachDB},
		{dialect: platform.MySQL},
		{dialect: ""},
	}
	for _, row := range rows {
		t.Run(row.dialect, func(t *testing.T) {
			c := qt.New(t)

			follower, cuts := sqlschema.NewNameFollower(row.dialect)

			c.Assert(cuts, qt.Equals, row.cuts)
			c.Assert(follower != nil, qt.Equals, row.cuts)
		})
	}
}

// TestNameFollower_Follow holds that each statement answers for itself: the
// name it cuts, the collision it causes with what an earlier statement
// declared, and whether it declares names at all.
func TestNameFollower_Follow(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 63)
	follower, _ := sqlschema.NewNameFollower(platform.Postgres)

	answers := followAll(c, follower, "CREATE TABLE t (a int, b int);\n"+
		`CREATE INDEX "`+long+`_alpha" ON t (a);`+"\n"+
		`CREATE INDEX IF NOT EXISTS "`+long+`_beta" ON t (b);`+"\n"+
		`CREATE INDEX "`+long+`_gamma" ON t (b);`+"\n"+
		"DROP TABLE t;\n")

	scope := `schema "public"`
	c.Assert(answers, qt.DeepEquals, []sqlschema.StatementNames{
		{Declares: true},
		{Declares: true, Cut: []sqlschema.StoredName{{Kind: "index", Scope: scope, Written: long + "_alpha", Stored: long, Limit: 63}}},
		{Declares: true, Collisions: []sqlschema.NameCollision{{
			Name:   sqlschema.StoredName{Kind: "index", Scope: scope, Written: long + "_beta", Stored: long, Limit: 63},
			HeldBy: "index", HeldAs: long + "_alpha",
		}}},
		{Declares: true, Collisions: []sqlschema.NameCollision{{
			Name:   sqlschema.StoredName{Kind: "index", Scope: scope, Written: long + "_gamma", Stored: long, Limit: 63},
			HeldBy: "index", HeldAs: long + "_alpha", Refused: true,
		}}},
		{},
	})
}

// TestNameFollower_Clone holds that a clone starts from the names the
// follower holds and that following on it leaves the follower as it was.
func TestNameFollower_Clone(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 63)
	follower, _ := sqlschema.NewNameFollower(platform.Postgres)
	followAll(c, follower, "CREATE TABLE t (a int, b int);\n"+`CREATE INDEX "`+long+`_alpha" ON t (a);`)
	clone := follower.Clone()

	onClone := followAll(c, clone, `DROP INDEX "`+long+`";`+"\n"+`CREATE INDEX "`+long+`_beta" ON t (b);`)
	onFollower := followAll(c, follower, `CREATE INDEX "`+long+`_beta" ON t (b);`)

	c.Assert(onClone[1].Collisions, qt.HasLen, 0)
	c.Assert(onFollower[0].Collisions, qt.HasLen, 1)
}

// TestLocateName_HappyPath holds where a name is found: in the statement's
// identifiers, folded as the server folds a bare word, past a literal or a
// comment that spells it.
func TestLocateName_HappyPath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		written string
		want    int
	}{
		{name: "a bare word folded", sql: "CREATE INDEX Idx_A ON t (a)", written: "idx_a", want: 13},
		{name: "a quoted name", sql: `CREATE INDEX "Idx_A" ON t (a)`, written: "Idx_A", want: 13},
		{name: "a quoted name holding a quote", sql: `CREATE INDEX "a""b" ON t (a)`, written: `a"b`, want: 13},
		{name: "past a literal", sql: "CREATE TABLE t (a text DEFAULT 'ck', CONSTRAINT ck CHECK (a <> ''))", written: "ck", want: 48},
		{name: "past a comment", sql: "CREATE INDEX -- ix\n ix ON t (a)", written: "ix", want: 20},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			offset, found := sqlschema.LocateName(platform.Postgres, row.sql, row.written)

			c.Assert(found, qt.IsTrue)
			c.Assert(offset, qt.Equals, row.want)
		})
	}
}

// TestLocateName_FailurePath holds that a name only a literal, a comment or
// another case spells is not found.
func TestLocateName_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		written string
	}{
		{name: "only in a literal", sql: "CREATE TABLE t (a text DEFAULT 'ck')", written: "ck"},
		{name: "only in a comment", sql: "CREATE TABLE t (a int) -- ck", written: "ck"},
		{name: "a quoted name in another case", sql: `CREATE INDEX "Idx" ON t (a)`, written: "idx"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			offset, found := sqlschema.LocateName(platform.Postgres, row.sql, row.written)

			c.Assert(found, qt.IsFalse)
			c.Assert(offset, qt.Equals, 0)
		})
	}
}
