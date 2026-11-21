package dialectlexer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// switchAnswers is what the dialect switch Options replaced answered for each
// canonical name, written out as it stood: backslash escapes on MySQL,
// MariaDB and ClickHouse; bracketed names and no # comments on SQL Server;
// the E'...' prefix on the PostgreSQL family and PostgreSQL's own string
// constants on PostgreSQL alone; the -- rule and the executable comments of
// MySQL and MariaDB; the YQL scanner alone on YDB; and SQL-standard strings on
// every dialect the shared lexer reads.
//
// SQLite differs from the switch, deliberately: its descriptor states
// bracketed names, which the switch did not give it, and which SQLite 3.51
// reads (see internal/builtindialects/sqlite.go). So does the PostgreSQL
// family, whose descriptors state nested block comments, which no switch read
// and which PostgreSQL 18.6, CockroachDB v26.3.5 and YugabyteDB 2026.1.2 read,
// and so does SQL Server, whose descriptor states them too: SQL Server 2025
// ends `/* /* */ ' */` at the second `*/`, not the first.
var switchAnswers = map[string]lexer.Options{
	platform.Postgres: {
		StandardStrings: true, PostgreSQLEscapeStrings: true, PostgreSQLStringConstants: true, NestedBlockComments: true,
		LineCommentsDeclared: true,
	},
	platform.CockroachDB: {StandardStrings: true, PostgreSQLEscapeStrings: true, NestedBlockComments: true, LineCommentsDeclared: true},
	platform.YugabyteDB:  {StandardStrings: true, PostgreSQLEscapeStrings: true, NestedBlockComments: true, LineCommentsDeclared: true},
	platform.MySQL: {
		StandardStrings: true, BackslashEscapes: true, LineCommentsDeclared: true, LineComments: []string{"#"},
		RequireWhitespaceAfterDashDash: true, ExecutableComments: lexer.ExecutableCommentsMySQL,
	},
	platform.MariaDB: {
		StandardStrings: true, BackslashEscapes: true, LineCommentsDeclared: true, LineComments: []string{"#"},
		RequireWhitespaceAfterDashDash: true, ExecutableComments: lexer.ExecutableCommentsMariaDB,
	},
	platform.ClickHouse: {StandardStrings: true, BackslashEscapes: true, LineCommentsDeclared: true, LineComments: []string{"#", "//"}},
	platform.SQLite:     {StandardStrings: true, BracketIdentifiers: true, LineCommentsDeclared: true},
	platform.SQLServer: {
		StandardStrings: true, BracketIdentifiers: true, DisableHashComments: true, NestedBlockComments: true, LineCommentsDeclared: true,
	},
	platform.Oracle: {StandardStrings: true, LineCommentsDeclared: true},
}

// TestOptions_AgreesWithTheSwitchItReplaces pins Options for every spelling
// the process registry accepts, in the case and padding a caller may pass,
// against what the switch answered for the dialect the spelling selects.
func TestOptions_AgreesWithTheSwitchItReplaces(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Dialects().Dialects(), qt.HasLen, len(switchAnswers))
	for _, spelling := range platform.DialectSpellings() {
		for _, written := range []string{spelling, " " + spelling + " ", strings.ToUpper(spelling)} {
			t.Run(written, func(t *testing.T) {
				c := qt.New(t)
				want, found := switchAnswers[platform.NormalizeDialect(spelling)]
				c.Assert(found, qt.IsTrue)
				c.Assert(dialectlexer.Options(written), qt.DeepEquals, want)
			})
		}
	}
}

// TestOptions_NoDialect reads an empty or unknown dialect with SQL-standard
// strings and nothing else, as the switch did.
func TestOptions_NoDialect(t *testing.T) {
	for _, dialect := range []string{"", "nonesuch"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dialectlexer.Options(dialect), qt.DeepEquals, lexer.Options{StandardStrings: true})
		})
	}
}

func TestFromDescriptor_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		lexical descriptor.Lexical
		want    lexer.Options
	}{
		{
			name:    "no lexical facts",
			lexical: descriptor.Lexical{},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
		{
			name: "every shared-lexer fact",
			lexical: descriptor.Lexical{
				BackslashEscapes: true, EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true,
				DashCommentNeedsSpace: true, ExecutableComments: descriptor.ExecutableCommentsMBang,
				BracketIdentifiers: true, HashNames: true,
			},
			want: lexer.Options{
				StandardStrings: true, BackslashEscapes: true,
				PostgreSQLEscapeStrings: true, PostgreSQLStringConstants: true,
				RequireWhitespaceAfterDashDash: true, ExecutableComments: lexer.ExecutableCommentsMariaDB,
				BracketIdentifiers: true, DisableHashComments: true, LineCommentsDeclared: true,
			},
		},
		{
			// The lexer reads both forms as one rule, so one of them alone
			// turns on neither.
			name:    "continued strings without Unicode strings",
			lexical: descriptor.Lexical{ContinuedStrings: true},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
		{
			name:    "Unicode strings without continued strings",
			lexical: descriptor.Lexical{UnicodeStrings: true},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
		{
			name:    "the bang executable comments",
			lexical: descriptor.Lexical{ExecutableComments: descriptor.ExecutableCommentsBang},
			want:    lexer.Options{StandardStrings: true, ExecutableComments: lexer.ExecutableCommentsMySQL, LineCommentsDeclared: true},
		},
		{
			// The shared lexer does not read this one; the body scanner of
			// internal/deporder does.
			name:    "a fact only the body scanner reads",
			lexical: descriptor.Lexical{IdentifierBackslashEscapes: true},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
		{
			name:    "the line comment markers, as the whole list",
			lexical: descriptor.Lexical{LineComments: []string{"#", "//"}},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true, LineComments: []string{"#", "//"}},
		},
		{
			// YQL is read by its engine's provider, so no built-in service
			// serves the lexer and its text falls to the shared rules.
			name:    "the YQL lexer, which no built-in service serves",
			lexical: descriptor.Lexical{Lexer: descriptor.LexerYQL},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
		{
			// A registry refuses a lexer the descriptor package does not
			// declare, so only a descriptor nobody validated names one no
			// service serves, and its text is read by the shared rules.
			name:    "a lexer no service serves",
			lexical: descriptor.Lexical{Lexer: "antlr"},
			want:    lexer.Options{StandardStrings: true, LineCommentsDeclared: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := dialectlexer.FromDescriptor(descriptor.Dialect{Name: "acme", Lexical: test.lexical})
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestFromDescriptor_UnvalidatedExecutableComments reads a value no registry
// accepts as no executable comments at all, so every block comment of the
// text stays a comment.
func TestFromDescriptor_UnvalidatedExecutableComments(t *testing.T) {
	c := qt.New(t)
	got := dialectlexer.FromDescriptor(descriptor.Dialect{
		Name:    "acme",
		Lexical: descriptor.Lexical{ExecutableComments: "nonesuch"},
	})
	c.Assert(got, qt.DeepEquals, lexer.Options{StandardStrings: true, LineCommentsDeclared: true})
}
