package atlasschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/lexer"
)

// PlanEscapeError reports that a pre-planned statement can reach databases or
// files outside the dev database, so replaying it on the dev database would
// not be a sandboxed rehearsal: the "verification" could modify the real
// target (or the host filesystem) and could even pass while the effect landed
// somewhere the comparison never looks.
type PlanEscapeError struct {
	// StatementIndex is the 1-based position of the offending statement.
	StatementIndex int
	// Construct is the SQL construct that escapes the dev database.
	Construct string
	// Reach describes what the construct can touch.
	Reach string
}

func (e *PlanEscapeError) Error() string {
	return fmt.Sprintf(
		"pre-planned migration cannot be verified on a dev database: statement %d uses %s, which %s. "+
			"Replaying it would not be a sandboxed rehearsal — it could modify the real target or the host "+
			"filesystem while the plan is still being verified, so the plan is refused before anything runs. "+
			"Review the statement and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, e.Construct, e.Reach)
}

// IsPlanEscape reports whether err wraps a dev-database escape refusal.
func IsPlanEscape(err error) bool {
	var target *PlanEscapeError
	return errors.As(err, &target)
}

// escapeRule matches one dev-database escape construct. A rule fires only on
// bare (unquoted) SQL keywords: the lexer emits double-quoted identifiers as
// strings and keeps the quotes on backticked ones, and a quoted keyword is an
// ordinary identifier to the database anyway, so a table named "attachment" or
// a string literal containing ATTACH never matches.
type escapeRule struct {
	// leading, when non-empty, requires the statement to start with this
	// keyword.
	leading string
	// keyword is the bare identifier that triggers the rule. Empty matches
	// any statement that satisfies leading and stringArgAfter.
	keyword string
	// prefix, when true, matches identifiers that merely start with keyword.
	prefix bool
	// stringArgAfter, when non-empty, additionally requires a string literal
	// to directly follow one of these keywords (a file path argument).
	stringArgAfter []string
	construct      string
	reach          string
}

// escapeRules enumerate the constructs that break dev-database containment.
// The set is dialect-generic on purpose: a plan file is an untrusted document
// and the reader does not know which engine authored it, so every family's
// escape hatches are refused regardless of the target dialect.
var escapeRules = []escapeRule{
	{
		leading: "ATTACH", keyword: "ATTACH",
		construct: "ATTACH",
		reach:     "attaches another SQLite database file to the session, so the statement can write to databases other than the dev database",
	},
	{
		leading: "DETACH", keyword: "DETACH",
		construct: "DETACH",
		reach:     "manipulates the session's attached SQLite databases rather than the dev database schema",
	},
	{
		leading: "VACUUM", keyword: "INTO",
		construct: "VACUUM INTO",
		reach:     "writes a copy of the database to an arbitrary path on the host filesystem",
	},
	{
		leading: "LOAD", keyword: "INFILE",
		construct: "LOAD DATA INFILE",
		reach:     "reads a file from the database server or the client host",
	},
	{
		keyword:   "OUTFILE",
		construct: "SELECT ... INTO OUTFILE",
		reach:     "writes a file on the database server host",
	},
	{
		keyword:   "DUMPFILE",
		construct: "SELECT ... INTO DUMPFILE",
		reach:     "writes a file on the database server host",
	},
	{
		leading: "COPY", keyword: "PROGRAM",
		construct: "COPY ... PROGRAM",
		reach:     "executes a shell command on the database server host",
	},
	{
		leading: "COPY", stringArgAfter: []string{"FROM", "TO"},
		construct: "COPY with a file path",
		reach:     "reads or writes a file on the database server host",
	},
	{
		keyword: "DBLINK", prefix: true,
		construct: "dblink",
		reach:     "opens a connection to another database server",
	},
	{
		keyword:   "POSTGRES_FDW",
		construct: "postgres_fdw",
		reach:     "federates data from another database server",
	},
	{
		keyword:   "FILE_FDW",
		construct: "file_fdw",
		reach:     "exposes files on the database server host as tables",
	},
	{
		keyword:   "PG_READ_FILE",
		construct: "pg_read_file",
		reach:     "reads a file on the database server host",
	},
	{
		keyword:   "PG_READ_BINARY_FILE",
		construct: "pg_read_binary_file",
		reach:     "reads a file on the database server host",
	},
	{
		keyword:   "PG_LS_DIR",
		construct: "pg_ls_dir",
		reach:     "lists a directory on the database server host",
	},
	{
		keyword:   "LO_IMPORT",
		construct: "lo_import",
		reach:     "reads a file on the database server host",
	},
	{
		keyword:   "LO_EXPORT",
		construct: "lo_export",
		reach:     "writes a file on the database server host",
	},
}

// CheckPlanStatementsSandboxable refuses pre-planned statements that could
// escape the dev database during rehearsal. It is the gate in front of every
// replay: a plan file is an untrusted input, and a "verification" that can
// mutate the real target or the host filesystem is worse than no verification
// because it reports success while the effect landed elsewhere.
//
// Statements are scanned under both string-escape interpretations (with and
// without backslash escapes), so a payload cannot hide a keyword inside text
// that only one dialect's lexer would treat as a string literal.
func CheckPlanStatementsSandboxable(statements []string) error {
	for i, statement := range statements {
		for _, backslashEscapes := range []bool{false, true} {
			if err := checkStatementSandboxable(statement, i+1, backslashEscapes); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkStatementSandboxable(statement string, index int, backslashEscapes bool) error {
	tokens := significantTokens(statement, backslashEscapes)
	if len(tokens) == 0 {
		return nil
	}
	for _, rule := range escapeRules {
		if rule.leading != "" && !hasLeadingKeyword(tokens, rule.leading) {
			continue
		}
		if !ruleMatches(rule, tokens) {
			continue
		}
		return &PlanEscapeError{StatementIndex: index, Construct: rule.construct, Reach: rule.reach}
	}
	return nil
}

// hasLeadingKeyword reports whether keyword starts a statement anywhere in the
// token stream: at the beginning, or directly after a semicolon. The second
// case matters because the statement splitter and this scanner can disagree
// about where a string literal ends, so a payload may still carry an embedded
// statement inside what the splitter handed over as one statement.
func hasLeadingKeyword(tokens []lexer.Token, keyword string) bool {
	atStatementStart := true
	for _, token := range tokens {
		if token.Type == lexer.TokenSemicolon {
			atStatementStart = true
			continue
		}
		if atStatementStart && token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, keyword) {
			return true
		}
		atStatementStart = false
	}
	return false
}

func ruleMatches(rule escapeRule, tokens []lexer.Token) bool {
	if len(rule.stringArgAfter) > 0 {
		return hasStringArgumentAfter(tokens, rule.stringArgAfter)
	}
	for _, token := range tokens {
		if token.Type != lexer.TokenIdentifier {
			continue
		}
		value := strings.ToUpper(token.Value)
		if value == rule.keyword || (rule.prefix && strings.HasPrefix(value, rule.keyword)) {
			return true
		}
	}
	return false
}

// hasStringArgumentAfter reports whether a string literal directly follows one
// of the given bare keywords, which is how a file path is passed to COPY.
// `COPY t FROM STDIN` has no string argument and does not match.
func hasStringArgumentAfter(tokens []lexer.Token, keywords []string) bool {
	for i, token := range tokens {
		if token.Type != lexer.TokenIdentifier || i+1 >= len(tokens) {
			continue
		}
		if tokens[i+1].Type != lexer.TokenString {
			continue
		}
		value := strings.ToUpper(token.Value)
		if slices.Contains(keywords, value) {
			return true
		}
	}
	return false
}

// significantTokens tokenizes a statement, dropping whitespace and comments so
// rules see the executable token sequence. Quoted identifiers keep their
// quotes (backticks, brackets) or arrive as strings, so they never match a
// bare-keyword rule.
func significantTokens(statement string, backslashEscapes bool) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, lexer.Options{
		StandardStrings:  true,
		BackslashEscapes: backslashEscapes,
	})
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		if token.Type == lexer.TokenEOF {
			return tokens
		}
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		tokens = append(tokens, token)
	}
}
