package atlasschema

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/lexer"
)

// PlanEscapeError reports that a pre-planned statement matched a known
// dev-database escape construct.
//
// This is a lint result, not a containment verdict: see [escapeRules] for why
// the deny-list cannot be a boundary, and [CheckPlanStatementsSandboxable] for
// which dev databases get real enforcement instead.
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
		"pre-planned migration was refused before it reached the dev database: statement %d uses %s, which %s. "+
			"A dev database executes plan SQL for real, so the plan is refused before anything runs. "+
			"Review the statement and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, e.Construct, e.Reach)
}

// IsPlanEscape reports whether err wraps a dev-database escape refusal.
func IsPlanEscape(err error) bool {
	var target *PlanEscapeError
	return errors.As(err, &target)
}

// PlanScanDepthError reports that a plan statement nested code inside string
// literals more deeply than the scanner follows. The plan is refused rather
// than accepted unscanned: the point of the scan is that unreviewed SQL does
// not reach a dev database, and a document burying code this deep is not
// something a legitimate planner emits.
type PlanScanDepthError struct {
	StatementIndex int
}

func (e *PlanScanDepthError) Error() string {
	return fmt.Sprintf(
		"pre-planned migration was refused before it reached the dev database: statement %d nests executable code "+
			"inside string literals more than %d levels deep, which the plan scanner does not follow, so the "+
			"statement cannot be checked; review it and run it deliberately outside `schema apply --plan`",
		e.StatementIndex, maxPlanGuardNesting)
}

// maxPlanGuardNesting bounds how deep the scanner follows code carried inside
// string literals (routine bodies inside routine bodies). Exceeding it is an
// error, not a pass.
const maxPlanGuardNesting = 4

// scanContext is one statement's token stream plus whether it came from inside
// a routine body, where statements also start after BEGIN/THEN/ELSE/LOOP/DO
// rather than only after a semicolon.
type scanContext struct {
	tokens []lexer.Token
	inBody bool
}

// tokenMatcher reports whether a statement matches one escape construct.
type tokenMatcher func(scanContext) bool

// escapeRule is one known escape construct: how to recognize it, and what it
// can reach.
type escapeRule struct {
	construct string
	reach     string
	match     tokenMatcher
}

// escapeRules enumerates the escape constructs this lint knows about.
//
// This is a BEST-EFFORT LINT over statement text, not a containment boundary,
// and it cannot become one. String concatenation alone defeats any scanner:
// `EXECUTE 'ATT' || 'ACH ...'` and `format('COPY t FROM PROGRAM %L', x)` build
// the dangerous statement at run time, so no lexical rule can see it. Beyond
// that, every dialect keeps adding ways to address something other than the
// connected database.
//
// The security model is therefore split by who chose the database:
//
//   - SQLite dev databases get REAL engine-level enforcement, applied by
//     dbschema.DatabaseConnection.WithUntrustedSQLSession, which is how every
//     rehearsal takes its session. That is what makes the ephemeral dev
//     database safe, not this list.
//   - Operator-supplied --dev-url databases get this lint, and nothing more.
//     The operator chose that database; the docs state plainly that it must be
//     one they are willing to have a foreign plan file execute arbitrary SQL
//     against.
//
// Coverage is deliberately partial and dialect-specific: SQLite, PostgreSQL,
// MySQL/MariaDB, SQL Server, and ClickHouse constructs are represented. It
// catches honest mistakes and known tricks. It does not stop an author who is
// trying to get past it.
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
		match:     pragmaAssignment("TEMP_STORE_DIRECTORY"),
	},
	{
		construct: "PRAGMA data_store_directory",
		reach:     "redirects SQLite database storage to an arbitrary directory on the host filesystem",
		match:     pragmaAssignment("DATA_STORE_DIRECTORY"),
	},
	{
		construct: "load_extension",
		reach:     "loads a native extension module from the host filesystem",
		match:     calledFunction("LOAD_EXTENSION"),
	},
	{
		construct: "LOAD DATA INFILE",
		reach:     "reads a file from the database server or the client host",
		match:     all(statementStartsWith("LOAD"), containsKeyword("INFILE")),
	},
	{
		construct: "SELECT ... INTO OUTFILE",
		reach:     "writes a file on the database server host",
		match:     keywordSequenceWithStringArgument("INTO", "OUTFILE"),
	},
	{
		construct: "SELECT ... INTO DUMPFILE",
		reach:     "writes a file on the database server host",
		match:     keywordSequenceWithStringArgument("INTO", "DUMPFILE"),
	},
	{
		construct: "LOAD_FILE",
		reach:     "reads a file on the database server host",
		match:     calledFunction("LOAD_FILE"),
	},
	{
		construct: "ENGINE=FEDERATED",
		reach:     "stores the table on another database server reached over the network",
		match:     engineAssignment("FEDERATED"),
	},
	{
		construct: "ClickHouse remote table engine",
		reach:     "stores or reads the table over the network or from the host filesystem",
		match: anyMatch(
			engineAssignment("URL"), engineAssignment("FILE"), engineAssignment("S3"),
			engineAssignment("HDFS"), engineAssignment("MYSQL"), engineAssignment("POSTGRESQL"),
		),
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
		match:     tableOptionPath("DATA", "DIRECTORY"),
	},
	{
		construct: "INDEX DIRECTORY",
		reach:     "places index data at an arbitrary path on the database server host",
		match:     tableOptionPath("INDEX", "DIRECTORY"),
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
		construct: "BULK INSERT",
		reach:     "reads a file on the database server host",
		match:     statementStartsWith("BULK", "INSERT"),
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
	{
		construct: "xp_cmdshell",
		reach:     "executes a shell command on the database server host",
		match:     procedureOrFunction("XP_CMDSHELL"),
	},
	{
		construct: "xp_dirtree",
		reach:     "lists directories on the database server host",
		match:     procedureOrFunction("XP_DIRTREE"),
	},
	{
		construct: "sp_addlinkedserver",
		reach:     "defines a connection to another database server",
		match:     procedureOrFunction("SP_ADDLINKEDSERVER"),
	},
	{
		construct: "OPENROWSET",
		reach:     "reads from another data source or a file on the database server host",
		match:     calledFunction("OPENROWSET"),
	},
	{
		construct: "OPENDATASOURCE",
		reach:     "reads from another data source reached over the network",
		match:     calledFunction("OPENDATASOURCE"),
	},
}

// CheckPlanStatementsSandboxable lints pre-planned statements for known
// dev-database escape constructs and refuses the plan when one matches. It
// runs in front of every dev-database replay.
//
// It is a best-effort lint, NOT a containment boundary — see [escapeRules].
// Real enforcement exists only on the ephemeral SQLite dev database Ptah
// creates itself (see dbschema.DatabaseConnection.WithUntrustedSQLSession); an operator-supplied
// --dev-url executes plan SQL for real and must be a database the operator is
// willing to expose to a foreign plan file.
//
// dialect selects the same lexer behavior [SplitApplyStatements] uses, so the
// scanner sees the statements the executor would run. Statements are
// additionally scanned under both string-escape interpretations, and code
// carried in routine bodies or handed to a dynamic executor is scanned as
// code rather than as data.
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
	ctx := scanContext{tokens: tokens, inBody: depth > 0}
	for _, rule := range escapeRules {
		if !rule.match(ctx) {
			continue
		}
		return &PlanEscapeError{StatementIndex: index, Construct: rule.construct, Reach: rule.reach}
	}
	nested := codeBearingStrings(ctx)
	if len(nested) == 0 {
		return nil
	}
	if depth >= maxPlanGuardNesting {
		return &PlanScanDepthError{StatementIndex: index}
	}
	for _, inner := range nested {
		if err := checkStatementSandboxable(inner, index, backslashEscapes, dialect, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// codeBearingStrings returns the string literals of a statement that carry
// executable code. A string is code only where SQL actually executes it:
//
//   - a dollar-quoted string, which in PostgreSQL is how a routine body or a
//     quoted block is written;
//   - the body of a CREATE FUNCTION/PROCEDURE, i.e. the string right after AS;
//   - an argument of a dynamic executor (EXECUTE, EXEC, PREPARE,
//     sp_executesql, PERFORM), including through `(` and `||`.
//
// Every other string literal stays data. That distinction is what keeps
// ordinary PL/pgSQL working: `RAISE EXCEPTION 'Do not delete rows'` and
// `INSERT INTO docs (body) VALUES ('ATTACH the receipt here')` are a message
// and a value, not statements, and must not be scanned as SQL.
func codeBearingStrings(ctx scanContext) []string {
	routineBody := isRoutineDefinition(ctx)
	var nested []string
	for i, token := range ctx.tokens {
		if token.Type != lexer.TokenString {
			continue
		}
		if inner, ok := dollarQuotedBody(token.Value); ok {
			nested = append(nested, inner)
			continue
		}
		if routineBody && followsKeyword(ctx.tokens, i, "AS") {
			nested = append(nested, unquoteStringLiteral(token.Value))
			continue
		}
		if followsDynamicExecutor(ctx.tokens, i) {
			nested = append(nested, unquoteStringLiteral(token.Value))
		}
	}
	return nested
}

// dynamicExecutorKeywords introduce a string that the database will execute.
//
// DO is here rather than in the rules: an anonymous block is not itself an
// escape, it is the standard PostgreSQL idiom for idempotent DDL
// (`DO $$ BEGIN IF NOT EXISTS (...) THEN CREATE TYPE ...; END IF; END $$`),
// and a foreign Atlas-authored PostgreSQL plan is full of them. Refusing the
// block wholesale broke exactly the interop this reader exists for, while
// scanning its body — which the dollar-quoted and single-quoted forms both go
// through — still catches what the block would actually do.
var dynamicExecutorKeywords = []string{"EXECUTE", "EXEC", "PREPARE", "SP_EXECUTESQL", "PERFORM", "DO"}

// followsDynamicExecutor reports whether the string at index i is an argument
// of a dynamic executor, walking back over `(` and `||` so that
// `EXECUTE ('DROP ' || 'TABLE t')` is still recognized as code.
//
// The lexer emits `||` as two separate `|` operator tokens, so the walk-back
// has to accept the single-character form; matching only "||" made this whole
// branch dead code and let concatenated executor arguments through unscanned.
func followsDynamicExecutor(tokens []lexer.Token, i int) bool {
	for j := i - 1; j >= 0; j-- {
		token := tokens[j]
		switch {
		case token.Type == lexer.TokenString:
			continue
		case token.MatchOperatorValue("(") || token.MatchOperatorValue("||") ||
			token.MatchOperatorValue("|"):
			continue
		case token.Type == lexer.TokenIdentifier:
			return slices.Contains(dynamicExecutorKeywords, strings.ToUpper(token.Value))
		default:
			return false
		}
	}
	return false
}

// followsKeyword reports whether the token at index i is directly preceded by
// the given bare keyword.
func followsKeyword(tokens []lexer.Token, i int, keyword string) bool {
	return i > 0 && isKeyword(tokens[i-1], keyword)
}

// isRoutineDefinition reports whether the statement defines a server-side
// routine, whose body is code even when it arrives as an ordinary string.
func isRoutineDefinition(ctx scanContext) bool {
	if !statementStartsWith("CREATE")(ctx) {
		return false
	}
	return containsKeyword("FUNCTION")(ctx) || containsKeyword("PROCEDURE")(ctx)
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

// unquoteStringLiteral returns the text of a quoted string literal, collapsing
// the doubled quotes and backslash escapes SQL uses to embed the delimiter, so
// nested code round-trips as the database would see it.
func unquoteStringLiteral(value string) string {
	if len(value) < 2 {
		return value
	}
	quote := value[0]
	if quote != '\'' && quote != '"' {
		return value
	}
	inner := strings.TrimSuffix(value[1:], string(quote))
	var out strings.Builder
	for i := 0; i < len(inner); i++ {
		switch {
		case inner[i] == quote && i+1 < len(inner) && inner[i+1] == quote:
			out.WriteByte(quote)
			i++
		case inner[i] == '\\' && i+1 < len(inner):
			out.WriteByte(inner[i+1])
			i++
		default:
			out.WriteByte(inner[i])
		}
	}
	return out.String()
}

func all(matchers ...tokenMatcher) tokenMatcher {
	return func(ctx scanContext) bool {
		for _, matcher := range matchers {
			if !matcher(ctx) {
				return false
			}
		}
		return true
	}
}

func anyMatch(matchers ...tokenMatcher) tokenMatcher {
	return func(ctx scanContext) bool {
		for _, matcher := range matchers {
			if matcher(ctx) {
				return true
			}
		}
		return false
	}
}

// statementBoundaryKeywords open a new statement inside a routine body, where
// there is no leading semicolon before the first one.
var statementBoundaryKeywords = []string{"BEGIN", "THEN", "ELSE", "LOOP", "DO"}

// statementStartsWith matches a keyword sequence at the start of a statement:
// at the beginning of the token stream, after a semicolon, or — inside a
// routine body — after BEGIN/THEN/ELSE/LOOP/DO. The semicolon case matters
// because the splitter and this scanner can disagree about where a string
// literal ends; the body case matters because the first statement of a body
// has no separator in front of it at all.
func statementStartsWith(keywords ...string) tokenMatcher {
	return func(ctx scanContext) bool {
		atStatementStart := true
		for i, token := range ctx.tokens {
			if token.Type == lexer.TokenSemicolon {
				atStatementStart = true
				continue
			}
			if atStatementStart && matchesKeywordSequence(ctx.tokens[i:], keywords) {
				return true
			}
			atStatementStart = ctx.inBody && token.Type == lexer.TokenIdentifier &&
				slices.Contains(statementBoundaryKeywords, strings.ToUpper(token.Value))
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
	return func(ctx scanContext) bool {
		for _, token := range ctx.tokens {
			if isKeyword(token, keyword) {
				return true
			}
		}
		return false
	}
}

// keywordSequenceWithStringArgument matches adjacent keywords followed by a
// string literal. `SELECT ... INTO OUTFILE '/tmp/x'` matches; the SQL Server
// table form `SELECT * INTO outfile FROM src` and `INSERT INTO outfile ...` do
// not, because neither is followed by a path literal.
func keywordSequenceWithStringArgument(keywords ...string) tokenMatcher {
	return func(ctx scanContext) bool {
		for i := range ctx.tokens {
			if !matchesKeywordSequence(ctx.tokens[i:], keywords) {
				continue
			}
			next := i + len(keywords)
			if next < len(ctx.tokens) && ctx.tokens[next].Type == lexer.TokenString {
				return true
			}
		}
		return false
	}
}

// tableOptionPath matches a MySQL table option spelled as two adjacent
// keywords assigned a path, such as `DATA DIRECTORY = '/var/lib/x'`. A column
// named `directory` or an index named `directory` never carries a path
// literal, so neither matches.
func tableOptionPath(first, second string) tokenMatcher {
	return func(ctx scanContext) bool {
		for i := range ctx.tokens {
			if !matchesKeywordSequence(ctx.tokens[i:], []string{first, second}) {
				continue
			}
			next := i + 2
			if next < len(ctx.tokens) && ctx.tokens[next].MatchOperatorValue("=") {
				next++
			}
			if next < len(ctx.tokens) && ctx.tokens[next].Type == lexer.TokenString {
				return true
			}
		}
		return false
	}
}

// engineAssignment matches `ENGINE = <name>`, the MySQL and ClickHouse storage
// engine selector, including the ClickHouse parameterized form
// `ENGINE = MySQL(...)`. Only an `=` may sit between the two, so a column list
// such as `(engine, federated)` does not match.
func engineAssignment(name string) tokenMatcher {
	return func(ctx scanContext) bool {
		for i, token := range ctx.tokens {
			if !isKeyword(token, "ENGINE") {
				continue
			}
			next := i + 1
			if next < len(ctx.tokens) && ctx.tokens[next].MatchOperatorValue("=") {
				next++
			}
			if next < len(ctx.tokens) && isKeyword(ctx.tokens[next], name) {
				return true
			}
		}
		return false
	}
}

// pragmaAssignment matches `PRAGMA <name> = ...`, including the schema-
// qualified `PRAGMA main.<name> = ...` spelling.
func pragmaAssignment(name string) tokenMatcher {
	return func(ctx scanContext) bool {
		for i, token := range ctx.tokens {
			if !isKeyword(token, "PRAGMA") {
				continue
			}
			for j := i + 1; j < len(ctx.tokens) && j <= i+3; j++ {
				if ctx.tokens[j].MatchOperatorValue(".") {
					continue
				}
				if isKeyword(ctx.tokens[j], name) {
					return true
				}
				if ctx.tokens[j].Type != lexer.TokenIdentifier {
					break
				}
			}
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
// name. This separates `SELECT dblink_exec(...)` from a table called
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
	return func(ctx scanContext) bool {
		for i, token := range ctx.tokens {
			if i+1 >= len(ctx.tokens) || !ctx.tokens[i+1].MatchOperatorValue("(") {
				continue
			}
			// Quoting a name does not change which function runs, so call
			// position accepts the quoted spellings too — "pg_read_file",
			// `load_file`, [xp_cmdshell]. isKeyword stays strict, so a table
			// named "attach" remains ordinary DDL.
			value, ok := callableName(token)
			if !ok {
				continue
			}
			matched := value == name || (prefix && strings.HasPrefix(value, name))
			if !matched {
				continue
			}
			if i > 0 && ctx.tokens[i-1].Type == lexer.TokenIdentifier &&
				slices.Contains(nameIntroducingKeywords, strings.ToUpper(ctx.tokens[i-1].Value)) {
				continue
			}
			return true
		}
		return false
	}
}

// procedureOrFunction matches a name used as a called function or as the
// target of EXEC/EXECUTE/CALL, which is how SQL Server invokes xp_cmdshell
// and friends.
func procedureOrFunction(name string) tokenMatcher {
	return func(ctx scanContext) bool {
		if calledFunction(name)(ctx) {
			return true
		}
		for i, token := range ctx.tokens {
			value, ok := callableName(token)
			if !ok || value != name || i == 0 {
				continue
			}
			previous := ctx.tokens[i-1]
			if previous.Type != lexer.TokenIdentifier {
				continue
			}
			switch strings.ToUpper(previous.Value) {
			case "EXEC", "EXECUTE", "CALL":
				return true
			}
		}
		return false
	}
}

// callableName returns the upper-cased name of a token that can name a
// callable, unwrapping the quoted spellings. Double-quoted names arrive as
// strings from the lexer; backticked and bracketed ones keep their quotes.
func callableName(token lexer.Token) (string, bool) {
	value := token.Value
	switch token.Type {
	case lexer.TokenIdentifier:
		value = strings.Trim(value, "`[]")
	case lexer.TokenString:
		if !strings.HasPrefix(value, `"`) {
			return "", false
		}
		value = strings.Trim(value, `"`)
	default:
		return "", false
	}
	if value == "" {
		return "", false
	}
	return strings.ToUpper(value), true
}

// stringArgumentAfter reports whether a string literal directly follows one of
// the given keywords, which is how a file path is passed to COPY.
// `COPY t FROM STDIN` has no string argument and does not match.
func stringArgumentAfter(keywords ...string) tokenMatcher {
	return func(ctx scanContext) bool {
		for i, token := range ctx.tokens {
			if token.Type != lexer.TokenIdentifier || i+1 >= len(ctx.tokens) {
				continue
			}
			if ctx.tokens[i+1].Type != lexer.TokenString {
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
