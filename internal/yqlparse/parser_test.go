package yqlparse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/yqlparse"
)

func TestParseIncompleteDeclarations(t *testing.T) {
	for _, text := range []string{
		"--!ansi_lexer\nCREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id));",
		"CREATE", "CREATE TABLE", "CREATE TABLE t (", "CREATE TABLE t (id Decimal(",
		"CREATE TABLE t (id Int64 DEFAULT (", "CREATE TABLE t (id String DEFAULT 'unterminated",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)) WITH (STORE = )",
		"CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id)); /* unterminated",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			statements, err := yqlparse.Parse(text)
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
		})
	}
}
