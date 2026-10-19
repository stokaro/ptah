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
// MySQL and MariaDB; the YQL lexer alone on YDB; and SQL-standard strings on
// every dialect the shared lexer reads.
var switchAnswers = map[string]lexer.Options{
	platform.Postgres: {
		StandardStrings: true, PostgreSQLEscapeStrings: true, PostgreSQLStringConstants: true,
	},
	platform.CockroachDB: {StandardStrings: true, PostgreSQLEscapeStrings: true},
	platform.YugabyteDB:  {StandardStrings: true, PostgreSQLEscapeStrings: true},
	platform.Spanner:     {StandardStrings: true, PostgreSQLEscapeStrings: true},
	platform.MySQL: {
		StandardStrings: true, BackslashEscapes: true,
		RequireWhitespaceAfterDashDash: true, ExecutableComments: lexer.ExecutableCommentsMySQL,
	},
	platform.MariaDB: {
		StandardStrings: true, BackslashEscapes: true,
		RequireWhitespaceAfterDashDash: true, ExecutableComments: lexer.ExecutableCommentsMariaDB,
	},
	platform.ClickHouse: {StandardStrings: true, BackslashEscapes: true},
	platform.SQLite:     {StandardStrings: true},
	platform.SQLServer:  {StandardStrings: true, BracketIdentifiers: true, DisableHashComments: true},
	platform.Oracle:     {StandardStrings: true},
	platform.YDB:        {YQL: true},
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
			want:    lexer.Options{StandardStrings: true},
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
				BracketIdentifiers: true, DisableHashComments: true,
			},
		},
		{
			// The lexer reads both forms as one rule, so one of them alone
			// turns on neither.
			name:    "continued strings without Unicode strings",
			lexical: descriptor.Lexical{ContinuedStrings: true},
			want:    lexer.Options{StandardStrings: true},
		},
		{
			name:    "Unicode strings without continued strings",
			lexical: descriptor.Lexical{UnicodeStrings: true},
			want:    lexer.Options{StandardStrings: true},
		},
		{
			name:    "the bang executable comments",
			lexical: descriptor.Lexical{ExecutableComments: descriptor.ExecutableCommentsBang},
			want:    lexer.Options{StandardStrings: true, ExecutableComments: lexer.ExecutableCommentsMySQL},
		},
		{
			// Neither the shared lexer nor the YQL one reads these two; the
			// body scanner of internal/deporder does.
			name:    "facts only the body scanner reads",
			lexical: descriptor.Lexical{LineComments: []string{"#"}, IdentifierBackslashEscapes: true},
			want:    lexer.Options{StandardStrings: true},
		},
		{
			name:    "the YQL lexer",
			lexical: descriptor.Lexical{Lexer: descriptor.LexerYQL},
			want:    lexer.Options{YQL: true},
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
	c.Assert(got, qt.DeepEquals, lexer.Options{StandardStrings: true})
}
