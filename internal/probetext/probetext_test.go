package probetext_test

import (
	"bytes"
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/probetext"
	"ptah.run/internal/rundiag"
)

// TestFragment_HappyPath holds text whose parentheses, semicolons and quotes
// the server reads where the lexer does: inside a string, a quoted name or a
// comment, or balanced.
func TestFragment_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		text    string
	}{
		{name: "empty", dialect: platform.Postgres, text: ""},
		{name: "balanced parentheses", dialect: platform.SQLServer, text: "(amount + 1) * (amount - 1)"},
		{name: "parenthesis in a string", dialect: platform.SQLServer, text: "note <> N')'"},
		{name: "semicolon in a string", dialect: platform.Postgres, text: "note <> ';'"},
		{name: "semicolon in a block comment", dialect: platform.Postgres, text: "amount > 0 /* ; */"},
		{name: "line comment ended by a line feed", dialect: platform.CockroachDB, text: "amount > 0 -- ) ;\n"},
		{name: "nested comment on SQL Server", dialect: platform.SQLServer, text: "amount > 0 /* a /* ) */ ; */"},
		{name: "parenthesis in a quoted name", dialect: platform.Postgres, text: `"a)b" > 0`},
		{name: "parenthesis in a bracketed name", dialect: platform.SQLServer, text: "[a)b] > 0"},
		{name: "dollar quote where the server reads one", dialect: platform.Postgres, text: "note <> $q$ ) ; $q$"},
		{name: "escaped quote in an escape string", dialect: platform.Postgres, text: `note <> E'\') ;'`},
		{name: "doubled quote in a string", dialect: platform.SQLServer, text: "note <> ') '' ;'"},
		// The probe transaction reads strings as standard, so a backslash
		// before the closing quote is a character.
		{name: "a string ending in a backslash on PostgreSQL", dialect: platform.Postgres, text: `amount >= 0 AND code <> 'C:\'`},
		{name: "a string ending in a backslash on CockroachDB", dialect: platform.CockroachDB, text: `code <> 'C:\'`},
		{name: "a string ending in a backslash on YugabyteDB", dialect: platform.YugabyteDB, text: `code <> 'C:\'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(probetext.Fragment(test.dialect, test.text), qt.IsNil)
		})
	}
}

// TestFragment_FailurePath holds text that could reach past the clause a probe
// writes it in, under at least one way a server of the dialect reads it.
func TestFragment_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		text    string
		wantErr string
	}{
		{
			name:    "closes the template's parentheses",
			dialect: platform.SQLServer,
			text: "amount + 1)); COMMIT; CREATE TABLE dbo.made_by_plan (x int); BEGIN TRANSACTION; " +
				"SAVE TRANSACTION ptah_generated_probe; SELECT ((1",
			wantErr: "its text closes a parenthesis it does not open",
		},
		{
			name:    "closes them with no semicolon",
			dialect: platform.SQLServer,
			text:    "amount + 1)) COMMIT CREATE TABLE dbo.made_by_plan (x int) BEGIN TRANSACTION SELECT ((1",
			wantErr: "its text closes a parenthesis it does not open",
		},
		{
			// A lexer that does not nest comments reads the text from the
			// first `*/` on as a string, and the server as SQL.
			name:    "a nested comment ends where the server ends it",
			dialect: platform.SQLServer,
			text: "amount > 0 /* /* */ ' */ )) COMMIT CREATE TABLE dbo.made_by_comment (x int) " +
				"BEGIN TRANSACTION SELECT ((1 /* ' */",
			wantErr: "its text closes a parenthesis it does not open",
		},
		{
			// The lexer ends the comment at the carriage return and reads a
			// string from there; CockroachDB reads the comment to the line
			// feed and the parentheses after it as SQL.
			name:    "a carriage return does not end a CockroachDB comment",
			dialect: platform.CockroachDB,
			text:    "amount > 0 -- \r'\n)) ; COMMIT ; SELECT ((1 --'",
			wantErr: "its text closes a parenthesis it does not open",
		},
		{
			// PostgreSQL reads `$1` as a parameter, so no string starts
			// there and the parentheses after it are SQL.
			name:    "a dollar tag cannot begin with a digit",
			dialect: platform.Postgres,
			text:    "amount > 0 $1$)); CREATE TABLE public.made_by_plan (x int); $1$",
			wantErr: "its text closes a parenthesis it does not open",
		},
		{
			name:    "leaves a parenthesis open",
			dialect: platform.Postgres,
			text:    "(amount > 0",
			wantErr: "its text leaves a parenthesis open",
		},
		{
			name:    "holds a semicolon",
			dialect: platform.Postgres,
			text:    "amount > 0; SELECT 1",
			wantErr: "its text holds a statement separator",
		},
		{
			name:    "leaves a string open",
			dialect: platform.SQLServer,
			text:    "note <> 'a",
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			name:    "leaves a quoted name open",
			dialect: platform.Postgres,
			text:    `"note <> 0`,
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			name:    "leaves a bracketed name open",
			dialect: platform.SQLServer,
			text:    "[note <> 0",
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			name:    "leaves a block comment open",
			dialect: platform.Postgres,
			text:    "amount > 0 /* note",
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			name:    "leaves a nested comment open",
			dialect: platform.SQLServer,
			text:    "amount > 0 /* a /* b */",
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			// The statement goes on after the text on the same line, so the
			// comment would take its closing parenthesis.
			name:    "ends in a line comment",
			dialect: platform.SQLServer,
			text:    "amount > 0 -- note",
			wantErr: "its text leaves a string, quoted name or comment open",
		},
		{
			name:    "dollar quote where the server reads none",
			dialect: platform.SQLServer,
			text:    "note <> $q$ )) COMMIT SELECT (( $q$",
			wantErr: "its text holds a quote the server does not read as one",
		},
		{
			name:    "backtick quote",
			dialect: platform.Postgres,
			text:    "`a)` > 0",
			wantErr: "its text holds a quote the server does not read as one",
		},
		{
			name:    "executable comment",
			dialect: platform.MySQL,
			text:    "amount > 0 /*! OR 1 */",
			wantErr: "its text holds a character the lexer cannot place",
		},
		{
			name:    "NUL character",
			dialect: platform.Postgres,
			text:    "amount > 0\x00)) ; COMMIT",
			wantErr: "its text is not valid UTF-8 or holds a NUL character",
		},
		{
			name:    "invalid UTF-8",
			dialect: platform.Postgres,
			text:    "amount > 0 \xff",
			wantErr: "its text is not valid UTF-8 or holds a NUL character",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := probetext.Fragment(test.dialect, test.text)
			c.Assert(err, qt.ErrorIs, probetext.ErrUncontained)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestFragments_HappyPath accepts texts each of which [probetext.Fragment]
// accepts, an empty one among them.
func TestFragments_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(probetext.Fragments(platform.Postgres, "amount + 1", "", "amount > 0"), qt.IsNil)
}

// TestFragments_FailurePath reports the first text's refusal alone, so the
// warning names one cause.
func TestFragments_FailurePath(t *testing.T) {
	c := qt.New(t)
	err := probetext.Fragments(platform.Postgres, "amount + 1", "amount)", "'open")
	c.Assert(err, qt.ErrorIs, probetext.ErrUncontained)
	c.Assert(err, qt.ErrorMatches, "its text closes a parenthesis it does not open")
}

// TestStatement_HappyPath holds whole statements: one semicolon may end each.
func TestStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "no semicolon", statement: "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql AS $$ SELECT 1; $$"},
		{name: "one trailing semicolon", statement: "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(probetext.Statement(platform.Postgres, test.statement), qt.IsNil)
		})
	}
}

// TestStatement_FailurePath holds statements with a second statement after the
// first semicolon, or an unbalanced declaration in them.
func TestStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "a second statement",
			statement: "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$; COMMIT",
			wantErr:   "its text holds a statement separator",
		},
		{
			name:      "two semicolons",
			statement: "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;;",
			wantErr:   "its text holds a statement separator",
		},
		{
			name:      "a default closing the argument list",
			statement: "CREATE FUNCTION pg_temp.f(a int DEFAULT 1)) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
			wantErr:   "its text closes a parenthesis it does not open",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := probetext.Statement(platform.Postgres, test.statement)
			c.Assert(err, qt.ErrorIs, probetext.ErrUncontained)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestStatementWithBody_HappyPath accepts the semicolons of a SQL-standard
// body, which only the server can place.
func TestStatementWithBody_HappyPath(t *testing.T) {
	c := qt.New(t)
	statement := "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1; SELECT 2; END;"
	c.Assert(probetext.StatementWithBody(platform.Postgres, statement), qt.IsNil)
}

// TestStatementWithBody_FailurePath still refuses what the lexer can place.
func TestStatementWithBody_FailurePath(t *testing.T) {
	c := qt.New(t)
	err := probetext.StatementWithBody(platform.Postgres, "CREATE FUNCTION pg_temp.f() RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 'a; END;")
	c.Assert(err, qt.ErrorIs, probetext.ErrUncontained)
	c.Assert(err, qt.ErrorMatches, "its text leaves a string, quoted name or comment open")
}

// TestWarn_WritesTheObjectAndTheCauseAtOnce writes the warning when it is
// raised, before the run ends, so it reaches the reader ahead of the plan the
// comparison prints.
func TestWarn_WritesTheObjectAndTheCauseAtOnce(t *testing.T) {
	c := qt.New(t)
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)

	probetext.Warn(ctx, "k", "CHECK ck_amount on t", probetext.Fragment(platform.SQLServer, "amount)"))

	c.Assert(stream.String(), qt.Equals, "warning: CHECK ck_amount on t is compared as written, not in the form "+
		"the server stores: the probe that spells it does not run, because its text closes a parenthesis it does not open\n")
}

// TestWarn_OnceForADeclarationProbedTwice keeps one warning for a key a run
// probes again, as a CHECK put through the server a second time is.
func TestWarn_OnceForADeclarationProbedTwice(t *testing.T) {
	c := qt.New(t)
	var stream bytes.Buffer
	report := rundiag.New(rundiag.Options{Stream: &stream, Listing: rundiag.ListedAtDebugLevel})
	ctx := rundiag.With(context.Background(), report)
	refusal := probetext.Fragment(platform.Postgres, "a;")

	probetext.Warn(ctx, "k", "view v", refusal)
	probetext.Warn(ctx, "k", "view v", refusal)
	report.Flush()

	c.Assert(stream.String(), qt.Equals, "warning: view v is compared as written, not in the form "+
		"the server stores: the probe that spells it does not run, because its text holds a statement separator\n")
}
