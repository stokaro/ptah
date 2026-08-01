package atlasschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/lexer"
)

// PlanEscapeError reports that a pre-planned statement matched a known
// dev-database escape construct, so replaying it would not stay inside the dev
// database: the "verification" could modify the real target (or the host
// filesystem) and could even pass while the effect landed somewhere the
// comparison never looks.
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
			"Replaying it would not stay inside the dev database — it could modify the real target or the host "+
			"filesystem while the plan is still being verified, so the plan is refused before anything runs. "+
			"Review the statement and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, e.Construct, e.Reach)
}

// IsPlanEscape reports whether err wraps a dev-database escape refusal.
func IsPlanEscape(err error) bool {
	var target *PlanEscapeError
	return errors.As(err, &target)
}

// maxPlanGuardNesting bounds how deep the scanner follows code carried inside
// string literals (routine bodies inside routine bodies).
const maxPlanGuardNesting = 4

// tokenMatcher reports whether a statement's significant tokens match one
// escape construct.
type tokenMatcher func(tokens []lexer.Token) bool

// escapeRule is one known escape construct: how to recognize it, and what it
// can reach.
type escapeRule struct {
	construct string
	reach     string
	match     tokenMatcher
}

// escapeRules enumerates the escape constructs this guard knows about.
//
// This list is a best-effort deny-list of known constructs, NOT a sandbox and
// NOT exhaustive. SQL dialects offer many ways to address something other than
// the connected database — server-side language extensions, foreign-data
// wrappers, storage-engine options, loadable modules, and engine-specific
// pragmas and functions — and new ones arrive with new engine versions. A
// determined author of a plan file can very likely find a construct this list
// does not name.
//
// The security consequence is stated plainly in the docs and repeated here: a
// --dev-url must be a database you are willing to have a foreign plan file
// execute arbitrary SQL against. The only path with no such exposure is the
// ephemeral SQLite dev database Ptah creates itself for SQLite targets, which
// is a throwaway file in a private temp directory.
//
// The set is dialect-generic on purpose: a plan file is an untrusted document
// and the reader does not know which engine authored it, so every family's
// escape hatches are checked regardless of the target dialect.
var escapeRules = []escapeRule{
	{
		construct: "ATTACH",
		reach:     "attaches another SQLite database file to the session, so the statement can write to databases other than the dev database",
		match:     statementStartsWith("ATTACH"),
	},
	{
		construct: "DETACH",
		reach:     "manipulates the session's attached SQLite databases rather than the dev database schema",
		match:     statementStartsWith("DETACH"),
	},
	{
		construct: "VACUUM INTO",
		reach:     "writes a copy of the database to an arbitrary path on the host filesystem",
		match:     all(statementStartsWith("VACUUM"), containsKeyword("INTO")),
	},
	{
		construct: "PRAGMA temp_store_directory",
		reach:     "redirects SQLite temporary storage to an arbitrary directory on the host filesystem",
		match:     statementStartsWith("PRAGMA", "TEMP_STORE_DIRECTORY"),
	},
	{
		construct: "DO",
		reach:     "executes an anonymous server-side code block, which can call out to anything the database server can reach",
		match:     statementStartsWith("DO"),
	},
	{
		construct: "LOAD DATA INFILE",
		reach:     "reads a file from the database server or the client host",
		match:     all(statementStartsWith("LOAD"), containsKeyword("INFILE")),
	},
	{
		construct: "SELECT ... INTO OUTFILE",
		reach:     "writes a file on the database server host",
		match:     keywordAfterKeyword("INTO", "OUTFILE"),
	},
	{
		construct: "SELECT ... INTO DUMPFILE",
		reach:     "writes a file on the database server host",
		match:     keywordAfterKeyword("INTO", "DUMPFILE"),
	},
	{
		construct: "LOAD_FILE",
		reach:     "reads a file on the database server host",
		match:     calledFunction("LOAD_FILE"),
	},
	{
		construct: "ENGINE=FEDERATED",
		reach:     "stores the table on another database server reached over the network",
		match:     keywordAfterKeyword("ENGINE", "FEDERATED"),
	},
	{
		construct: "CREATE SERVER",
		reach:     "defines a connection to another database server",
		match:     statementStartsWith("CREATE", "SERVER"),
	},
	{
		construct: "INSTALL PLUGIN",
		reach:     "loads a server-side plugin, which runs native code on the database server host",
		match:     statementStartsWith("INSTALL", "PLUGIN"),
	},
	{
		construct: "INSTALL COMPONENT",
		reach:     "loads a server-side component, which runs native code on the database server host",
		match:     statementStartsWith("INSTALL", "COMPONENT"),
	},
	{
		construct: "DATA DIRECTORY",
		reach:     "places table data at an arbitrary path on the database server host",
		match:     adjacentKeywords("DATA", "DIRECTORY"),
	},
	{
		construct: "INDEX DIRECTORY",
		reach:     "places index data at an arbitrary path on the database server host",
		match:     adjacentKeywords("INDEX", "DIRECTORY"),
	},
	{
		construct: "COPY ... PROGRAM",
		reach:     "executes a shell command on the database server host",
		match:     all(statementStartsWith("COPY"), containsKeyword("PROGRAM")),
	},
	{
		construct: "COPY with a file path",
		reach:     "reads or writes a file on the database server host",
		match:     all(statementStartsWith("COPY"), stringArgumentAfter("FROM", "TO")),
	},
	{
		construct: "dblink",
		reach:     "opens a connection to another database server",
		match:     calledFunctionPrefix("DBLINK"),
	},
	{
		construct: "postgres_fdw",
		reach:     "federates data from another database server",
		match:     containsKeyword("POSTGRES_FDW"),
	},
	{
		construct: "file_fdw",
		reach:     "exposes files on the database server host as tables",
		match:     containsKeyword("FILE_FDW"),
	},
	{
		construct: "pg_read_file",
		reach:     "reads a file on the database server host",
		match:     calledFunction("PG_READ_FILE"),
	},
	{
		construct: "pg_read_binary_file",
		reach:     "reads a file on the database server host",
		match:     calledFunction("PG_READ_BINARY_FILE"),
	},
	{
		construct: "pg_ls_dir",
		reach:     "lists a directory on the database server host",
		match:     calledFunction("PG_LS_DIR"),
	},
	{
		construct: "lo_import",
		reach:     "reads a file on the database server host",
		match:     calledFunction("LO_IMPORT"),
	},
	{
		construct: "lo_export",
		reach:     "writes a file on the database server host",
		match:     calledFunction("LO_EXPORT"),
	},
}

// CheckPlanStatementsSandboxable refuses pre-planned statements that match a
// known dev-database escape construct. It is the gate in front of every
// replay: a plan file is an untrusted input, and a "verification" that can
// mutate the real target or the host filesystem is worse than no verification
// because it reports success while the effect landed elsewhere.
//
// It is a best-effort deny-list, not a sandbox — see [escapeRules] for what
// that does and does not buy the caller.
//
// dialect selects the same lexer behavior [SplitApplyStatements] uses, so the
// scanner sees the statements the executor would run. Statements are
// additionally scanned under both string-escape interpretations (with and
// without backslash escapes), so a payload cannot hide a keyword inside text
// that only one dialect's lexer would treat as a string literal, and code
// carried inside routine bodies is scanned as code rather than as data.
func CheckPlanStatementsSandboxable(statements []string, dialect string) error {
	for i, statement := range statements {
		for _, backslashEscapes := range []bool{false, true} {
			if err := checkStatementSandboxable(statement, i+1, backslashEscapes, dialect, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkStatementSandboxable(statement string, index int, backslashEscapes bool, dialect string, depth int) error {
	tokens := significantTokens(statement, backslashEscapes, dialect)
	if len(tokens) == 0 {
		return nil
	}
	for _, rule := range escapeRules {
		if !rule.match(tokens) {
			continue
		}
		return &PlanEscapeError{StatementIndex: index, Construct: rule.construct, Reach: rule.reach}
	}
	if depth >= maxPlanGuardNesting {
		return nil
	}
	// A dollar-quoted body and a routine definition's body are code, not data:
	// the lexer hands them over as a single string token, so nothing inside
	// would otherwise be scanned. Once inside such a body, deeper string
	// literals are code as well — that is how dynamic SQL (EXECUTE '...')
	// nests another routine definition.
	for _, nested := range codeBearingStrings(tokens, depth > 0) {
		if err := checkStatementSandboxable(nested, index, backslashEscapes, dialect, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// codeBearingStrings returns the string-literal contents of a statement that
// carry executable code: dollar-quoted strings anywhere (PostgreSQL routine
// bodies and quoted blocks), plus every string literal of a routine definition
// or of a statement already known to be code, whose bodies may be ordinary
// quoted strings. Ordinary top-level statements keep their string literals
// treated as data, so a column default containing the words
// "COPY ... FROM PROGRAM" stays harmless.
func codeBearingStrings(tokens []lexer.Token, codeContext bool) []string {
	routine := codeContext || isRoutineDefinition(tokens)
	var nested []string
	for _, token := range tokens {
		if token.Type != lexer.TokenString {
			continue
		}
		if inner, ok := dollarQuotedBody(token.Value); ok {
			nested = append(nested, inner)
			continue
		}
		if routine {
			nested = append(nested, unquoteStringLiteral(token.Value))
		}
	}
	return nested
}

// isRoutineDefinition reports whether the statement defines a server-side
// routine, whose body is code even when it arrives as an ordinary string.
func isRoutineDefinition(tokens []lexer.Token) bool {
	if !statementStartsWith("CREATE")(tokens) {
		return false
	}
	return containsKeyword("FUNCTION")(tokens) || containsKeyword("PROCEDURE")(tokens)
}

// dollarQuotedBody extracts the body of a PostgreSQL dollar-quoted string
// ($$body$$ or $tag$body$tag$).
func dollarQuotedBody(value string) (string, bool) {
	if !strings.HasPrefix(value, "$") {
		return "", false
	}
	end := strings.Index(value[1:], "$")
	if end < 0 {
		return "", false
	}
	tag := value[:end+2]
	body := strings.TrimPrefix(value, tag)
	return strings.TrimSuffix(body, tag), true
}

func unquoteStringLiteral(value string) string {
	if len(value) < 2 {
		return value
	}
	quote := value[0]
	if quote != '\'' && quote != '"' {
		return value
	}
	return strings.TrimSuffix(value[1:], string(quote))
}

func all(matchers ...tokenMatcher) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		for _, matcher := range matchers {
			if !matcher(tokens) {
				return false
			}
		}
		return true
	}
}

// statementStartsWith matches a keyword sequence at the start of a statement:
// at the beginning of the token stream, or directly after a semicolon. The
// second case matters because the splitter and this scanner can disagree about
// where a string literal ends, so a payload may still carry an embedded
// statement inside what the splitter handed over as one statement.
func statementStartsWith(keywords ...string) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		atStatementStart := true
		for i, token := range tokens {
			if token.Type == lexer.TokenSemicolon {
				atStatementStart = true
				continue
			}
			if atStatementStart && matchesKeywordSequence(tokens[i:], keywords) {
				return true
			}
			atStatementStart = false
		}
		return false
	}
}

func matchesKeywordSequence(tokens []lexer.Token, keywords []string) bool {
	if len(tokens) < len(keywords) {
		return false
	}
	for i, keyword := range keywords {
		if !isKeyword(tokens[i], keyword) {
			return false
		}
	}
	return true
}

func containsKeyword(keyword string) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		for _, token := range tokens {
			if isKeyword(token, keyword) {
				return true
			}
		}
		return false
	}
}

// adjacentKeywords matches two keywords next to each other anywhere in the
// statement, which is how MySQL spells its DATA DIRECTORY / INDEX DIRECTORY
// table options.
func adjacentKeywords(first, second string) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		for i := range tokens[:max(len(tokens)-1, 0)] {
			if isKeyword(tokens[i], first) && isKeyword(tokens[i+1], second) {
				return true
			}
		}
		return false
	}
}

// keywordAfterKeyword matches a keyword whose preceding significant token —
// ignoring operators such as `=` — is another keyword. It anchors constructs
// like INTO OUTFILE and ENGINE=FEDERATED, so a column merely named `outfile`
// does not match.
func keywordAfterKeyword(previous, keyword string) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		lastIdentifier := -1
		for i, token := range tokens {
			if token.Type != lexer.TokenIdentifier {
				continue
			}
			if isKeyword(token, keyword) && lastIdentifier >= 0 && isKeyword(tokens[lastIdentifier], previous) {
				return true
			}
			lastIdentifier = i
		}
		return false
	}
}

// nameIntroducingKeywords precede an object *name* in DDL. An identifier in
// that position is being declared or referenced, never called, so a table
// named `dblink_events` — whose name is followed by `(` in a CREATE TABLE — is
// not a dblink call site.
var nameIntroducingKeywords = []string{
	"TABLE", "INDEX", "VIEW", "SEQUENCE", "TRIGGER", "FUNCTION", "PROCEDURE",
	"TYPE", "DOMAIN", "EXTENSION", "SERVER", "SCHEMA", "DATABASE", "CONSTRAINT",
	"REFERENCES", "INTO", "EXISTS", "ONLY", "ON", "KEY", "COLUMN", "RENAME", "TO", "AS",
}

// calledFunction matches an identifier in call position: immediately followed
// by an opening parenthesis, and not sitting where DDL introduces an object
// name. This is what separates `SELECT dblink_exec(...)` from a table called
// `dblink_events (id integer)` or an index named `lo_import`.
func calledFunction(name string) tokenMatcher {
	return callPositionMatcher(name, false)
}

// calledFunctionPrefix is calledFunction for a family of function names that
// share a prefix, such as dblink, dblink_exec, and dblink_connect.
func calledFunctionPrefix(prefix string) tokenMatcher {
	return callPositionMatcher(prefix, true)
}

func callPositionMatcher(name string, prefix bool) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		for i, token := range tokens {
			if token.Type != lexer.TokenIdentifier || i+1 >= len(tokens) {
				continue
			}
			if !tokens[i+1].MatchOperatorValue("(") {
				continue
			}
			value := strings.ToUpper(token.Value)
			matched := value == name || (prefix && strings.HasPrefix(value, name))
			if !matched {
				continue
			}
			// A qualified call (pg_catalog.pg_read_file(...)) is preceded by
			// `.`, which is not a name-introducing keyword, so it still fires.
			if i > 0 && tokens[i-1].Type == lexer.TokenIdentifier &&
				slices.Contains(nameIntroducingKeywords, strings.ToUpper(tokens[i-1].Value)) {
				continue
			}
			return true
		}
		return false
	}
}

// stringArgumentAfter reports whether a string literal directly follows one of
// the given keywords, which is how a file path is passed to COPY.
// `COPY t FROM STDIN` has no string argument and does not match.
func stringArgumentAfter(keywords ...string) tokenMatcher {
	return func(tokens []lexer.Token) bool {
		for i, token := range tokens {
			if token.Type != lexer.TokenIdentifier || i+1 >= len(tokens) {
				continue
			}
			if tokens[i+1].Type != lexer.TokenString {
				continue
			}
			if slices.Contains(keywords, strings.ToUpper(token.Value)) {
				return true
			}
		}
		return false
	}
}

// isKeyword reports whether a token is the given bare SQL keyword. Quoted
// identifiers never match: the lexer emits double-quoted ones as strings and
// keeps the quotes on backticked and bracketed ones, and a quoted keyword is
// an ordinary identifier to the database anyway.
func isKeyword(token lexer.Token, keyword string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, keyword)
}

// significantTokens tokenizes a statement, dropping whitespace and comments so
// rules see the executable token sequence. The lexer options mirror
// [SplitApplyStatements] for the same dialect, so the scanner and the executor
// agree on what is a string, a comment, and a quoted identifier.
func significantTokens(statement string, backslashEscapes bool, dialect string) []lexer.Token {
	normalized := platform.NormalizeDialect(dialect)
	lexr := lexer.NewLexerWithOptions(statement, lexer.Options{
		StandardStrings:     true,
		BackslashEscapes:    backslashEscapes,
		BracketIdentifiers:  normalized == platform.SQLServer,
		DisableHashComments: normalized == platform.SQLServer,
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
