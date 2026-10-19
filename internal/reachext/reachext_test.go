package reachext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/reachext"
)

func TestCallableName_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		token lexer.Token
		want  string
	}{
		{name: "a bare name", token: lexer.Token{Type: lexer.TokenIdentifier, Value: "pg_read_file"}, want: "PG_READ_FILE"},
		{name: "a backticked name", token: lexer.Token{Type: lexer.TokenIdentifier, Value: "`load_file`"}, want: "LOAD_FILE"},
		{name: "a bracketed name", token: lexer.Token{Type: lexer.TokenIdentifier, Value: "[xp_cmdshell]"}, want: "XP_CMDSHELL"},
		{name: "a double-quoted name", token: lexer.Token{Type: lexer.TokenString, Value: `"lo_import"`}, want: "LO_IMPORT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := reachext.CallableName(test.token)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestCallableName_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		token lexer.Token
	}{
		{name: "a single-quoted string", token: lexer.Token{Type: lexer.TokenString, Value: "'pg_read_file'"}},
		{name: "an operator", token: lexer.Token{Type: lexer.TokenOperator, Value: "("}},
		{name: "an empty quoted name", token: lexer.Token{Type: lexer.TokenIdentifier, Value: "``"}},
		{name: "an empty double-quoted name", token: lexer.Token{Type: lexer.TokenString, Value: `""`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := reachext.CallableName(test.token)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
