package mytype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
	"ptah.run/internal/lintscan"
)

// A spelling in a statement and the server's own spelling of the same type
// store a value the same way; a different width or sign does not.
func TestSameStorage(t *testing.T) {
	for _, test := range []struct {
		written, server string
		want            bool
	}{
		{"INT(11)", "int", true},
		{"INTEGER UNSIGNED", "int unsigned", true},
		{"VARCHAR(20) CHARACTER SET utf8", "varchar(20)", true},
		{"BIGINT", "int", false},
		{"INT UNSIGNED", "int", false},
		{"DECIMAL(10,2)", "decimal(12,2)", false},
	} {
		t.Run(test.written, func(t *testing.T) {
			c := qt.New(t)
			words := lintscan.Split("ALTER TABLE t MODIFY a "+test.written, lintscan.ForDialect("mysql"))[0].Words
			written, _, ok := mytype.ParseAt(words, 5, "utf8mb4")
			c.Assert(ok, qt.IsTrue)
			server, ok := mytype.ParseSpelling(test.server, "utf8mb3")
			c.Assert(ok, qt.IsTrue)
			c.Assert(written.SameStorage(server), qt.Equals, test.want)
		})
	}
}

// An ENUM or SET list reads with its quoting resolved and spells back the way
// the DDL writes it.
func TestMemberListAt(t *testing.T) {
	c := qt.New(t)
	words := lintscan.Split(`ALTER TABLE t MODIFY a ENUM('a', 'it''s', 'c  ') NOT NULL`, lintscan.ForDialect("mysql"))[0].Words

	list, next, ok := mytype.MemberListAt(words, 5)

	c.Assert(ok, qt.IsTrue)
	c.Assert([]any{list.Kind, list.Members, words[next]}, qt.DeepEquals, []any{mytype.Enum, []string{"a", "it's", "c"}, "NOT"})
	c.Assert(mytype.QuoteMembers(list.Members), qt.Equals, `'a', 'it''s', 'c'`)
	c.Assert(list.Kind.String(), qt.Equals, "ENUM")
}

// Every MODIFY and CHANGE with a type is found, and the table's character set
// at a statement follows the earlier statements of the file that change it.
func TestChangeSitesAndTableCharset(t *testing.T) {
	c := qt.New(t)
	statements := lintscan.Split("ALTER TABLE t MODIFY a VARCHAR(10), CHANGE b c TEXT;\n"+
		"ALTER TABLE t CONVERT TO CHARACTER SET utf8;\n"+
		"ALTER TABLE t MODIFY d INT;\n", lintscan.ForDialect("mysql"))

	sites := mytype.ChangeSites(statements)

	c.Assert(sites, qt.HasLen, 3)
	c.Assert([]any{sites[0].OldName, sites[0].NewName, sites[1].OldName, sites[1].NewName, sites[2].Statement}, qt.DeepEquals,
		[]any{"a", "a", "b", "c", 2})
	table := lintkit.TableReference{Name: "t", Normalized: "T"}
	c.Assert(mytype.TableCharsetBefore(statements, 0, table, "latin1"), qt.Equals, "latin1")
	c.Assert(mytype.TableCharsetBefore(statements, 2, table, "latin1"), qt.Equals, "utf8mb3")
}
