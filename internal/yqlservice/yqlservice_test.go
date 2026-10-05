package yqlservice_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/yqlservice"
)

// opensPing recognizes a made-up statement, `PING <name>`, standing in for one
// of Ptah's own.
func opensPing(tokens []lexer.Token) bool {
	return tokens[0].MatchIdentifierValue("PING")
}

// A statement found alone, or after the definitions a migration carries into
// every query, is located with the path prefix in effect for it.
func TestLocate_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantFirst  string
		wantPrefix string
	}{
		{name: "alone", text: "PING `a/b`", wantFirst: "PING"},
		{name: "terminated", text: "PING `a/b`;\n", wantFirst: "PING"},
		{name: "lower case", text: "ping x", wantFirst: "ping"},
		{name: "after the translation setting", text: "--!syntax_v1\nPING x", wantFirst: "PING"},
		{name: "after a prefix in parentheses", text: "PRAGMA TablePathPrefix('/local/r');\nPING x",
			wantFirst: "PING", wantPrefix: "/local/r"},
		{name: "after a prefix with an equals sign", text: `PRAGMA TablePathPrefix = "/local/r"; PING x`,
			wantFirst: "PING", wantPrefix: "/local/r"},
		{name: "the last prefix wins", text: "PRAGMA TablePathPrefix('/a'); PRAGMA TablePathPrefix('/b'); PING x",
			wantFirst: "PING", wantPrefix: "/b"},
		{name: "after other definitions", text: "PRAGMA OrderedColumns; DECLARE $p AS Int64; $n = 1; PING x",
			wantFirst: "PING"},
		{name: "after a comment", text: "-- why\n/* more */ PING x", wantFirst: "PING"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			located, recognized, err := yqlservice.Locate(test.text, opensPing, "test service")
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(located.Tokens[0].Value, qt.Equals, test.wantFirst)
			c.Assert(located.Tokens[1].Value, qt.Not(qt.Equals), "")
			c.Assert(located.PathPrefix, qt.Equals, test.wantPrefix)
		})
	}
}

// Text holding no such statement is not recognized and reports no error, the
// words inside a string or a comment included.
func TestLocate_NotRecognized(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "another statement", text: "SELECT 1"},
		{name: "in a string", text: "SELECT 'PING x'"},
		{name: "in a comment", text: "-- PING x\nSELECT 1"},
		{name: "in a block comment", text: "/* PING x */ SELECT 1"},
		{name: "not first", text: "SELECT PING FROM t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			located, recognized, err := yqlservice.Locate(test.text, opensPing, "test service")
			c.Assert(err, qt.IsNil)
			c.Assert(recognized, qt.IsFalse)
			c.Assert(located, qt.DeepEquals, yqlservice.Located{})
		})
	}
}

// A statement the service has to take alone is refused where it shares its
// query with anything but definitions, and where the prefix in effect for it
// cannot be read.
func TestLocate_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "two statements", text: "PING a; PING b",
			wantErr: `a query runs one PING B statement, and this one holds two; put each in a query of its own`},
		{name: "not last", text: "PING a; SELECT 1",
			wantErr: `PING A is followed by another statement in its query, and Ptah runs it through YDB's test service, which takes it alone`},
		{name: "after a data statement", text: "UPSERT INTO t (id) VALUES (1); PING a",
			wantErr: `PING A shares its query with UPSERT INTO T, and Ptah runs it through YDB's test service, which takes it alone`},
		{name: "a prefix Ptah cannot read", text: "PRAGMA TablePathPrefix('/a', '/b'); PING a",
			wantErr: `PING A follows a TablePathPrefix pragma Ptah cannot read; write it as PRAGMA TablePathPrefix\("/database/path"\)`},
		{name: "a prefix that is not a string", text: "PRAGMA TablePathPrefix($p); PING a",
			wantErr: `PING A follows a TablePathPrefix pragma whose path is not a plain string`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			located, recognized, err := yqlservice.Locate(test.text, opensPing, "test service")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(recognized, qt.IsTrue)
			c.Assert(located, qt.DeepEquals, yqlservice.Located{})
		})
	}
}

// A YQL string reads back as the text it denotes, in either quote style, with
// the escapes ydbtype.StringLiteral writes.
func TestStringValue_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{name: "single quotes", token: `'abc'`, want: "abc"},
		{name: "double quotes", token: `"abc"`, want: "abc"},
		{name: "Utf8 suffix", token: `'abc'u`, want: "abc"},
		{name: "empty", token: `''`, want: ""},
		{name: "the other quote bare", token: `"it's"`, want: "it's"},
		{name: "escapes", token: `'a\\b\'c\"d\ne\rf\tg'`, want: "a\\b'c\"d\ne\rf\tg"},
		{name: "a byte", token: `'\x00\x7f'`, want: "\x00\x7f"},
		{name: "non-ASCII", token: `'привет'`, want: "привет"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := yqlservice.StringValue(lexer.Token{Type: lexer.TokenString, Value: test.token})
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// What is not such a string is refused rather than read as one.
func TestStringValue_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		token lexer.Token
	}{
		{name: "an identifier", token: lexer.Token{Type: lexer.TokenIdentifier, Value: "abc"}},
		{name: "a raw string", token: lexer.Token{Type: lexer.TokenString, Value: "@@abc@@"}},
		{name: "unclosed", token: lexer.Token{Type: lexer.TokenString, Value: `'abc`}},
		{name: "mixed quotes", token: lexer.Token{Type: lexer.TokenString, Value: `'abc"`}},
		{name: "a bare quote inside", token: lexer.Token{Type: lexer.TokenString, Value: `'a'b'`}},
		{name: "an unknown escape", token: lexer.Token{Type: lexer.TokenString, Value: `'\q'`}},
		{name: "a short byte", token: lexer.Token{Type: lexer.TokenString, Value: `'\x4'`}},
		{name: "a bad byte", token: lexer.Token{Type: lexer.TokenString, Value: `'\xzz'`}},
		{name: "a trailing backslash", token: lexer.Token{Type: lexer.TokenString, Value: `'a\'`}},
		{name: "another suffix", token: lexer.Token{Type: lexer.TokenString, Value: `'abc'y`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := yqlservice.StringValue(test.token)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// A path resolves against the prefix in effect, or the database root, and an
// absolute one stands on its own.
func TestAbsolute_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		prefix   string
		database string
		want     string
	}{
		{name: "at the root", path: "t", database: "/local", want: "/local/t"},
		{name: "a database without a slash", path: "d/t", database: "local", want: "/local/d/t"},
		{name: "under a prefix", path: "t", prefix: "/local/.ptah/r", database: "/local", want: "/local/.ptah/r/t"},
		{name: "absolute", path: "/local/x/t", prefix: "/local/r", database: "/local", want: "/local/x/t"},
		{name: "cleaned", path: "/local/x/../t", database: "/local", want: "/local/t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := yqlservice.Absolute(test.path, test.prefix, test.database)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A relative prefix is refused, as YDB refuses it.
func TestAbsolute_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := yqlservice.Absolute("t", "relp", "/local")
	c.Assert(err, qt.ErrorMatches, `path relp/t is not in database /local: the TablePathPrefix "relp" is relative, and YDB reads a prefix only as an absolute path`)
	c.Assert(got, qt.Equals, "")
}

// Within answers for a path strictly below the root, and not for the root
// itself, a sibling sharing its prefix, or a path that climbs out.
func TestWithin(t *testing.T) {
	tests := []struct {
		name         string
		absolute     string
		root         string
		wantRelative string
		wantInside   bool
	}{
		{name: "below", absolute: "/local/a/t", root: "/local", wantRelative: "a/t", wantInside: true},
		{name: "below a realm", absolute: "/local/.ptah/r/t", root: "/local/.ptah/r", wantRelative: "t", wantInside: true},
		{name: "the root", absolute: "/local", root: "/local"},
		{name: "a sibling with the same prefix", absolute: "/localx/t", root: "/local"},
		{name: "climbing out", absolute: "/local/r/../../etc/t", root: "/local/r"},
		{name: "outside a realm", absolute: "/local/t", root: "/local/.ptah/r"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			relative, inside := yqlservice.Within(test.absolute, test.root)
			c.Assert(inside, qt.Equals, test.wantInside)
			c.Assert(relative, qt.Equals, test.wantRelative)
		})
	}
}
