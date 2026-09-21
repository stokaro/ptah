// Package sqlreach recognizes SQL constructs that reach outside the database
// the statement was sent to: another database file or server, a file on the
// host, a shell.
//
// It exists because more than one caller has to refuse the same set and a
// second list agrees with the first only until one of them is extended. A plan
// file is replayed against a dev database; a release assertion is evaluated
// against a live one. Both promise the statement touches nothing else, and
// both read that promise off the text, so both read it here.
//
// What the scan is, and is not, is stated at [escapeRules]: a best-effort lint
// over statement text, never a containment boundary.
package sqlreach

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// MaxNesting bounds how deep the scanner follows code carried inside string
// literals (routine bodies inside routine bodies). Exceeding it is an error,
// not a pass.
//
// It is exported because a refusal that names the limit has to name the same
// number the scanner stopped at.
const MaxNesting = 4

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
	// dialects restricts the rule to the engines whose grammar gives the
	// construct its meaning. Empty means every engine, which is right for a
	// spelling no other dialect can mistake for something ordinary -- there is
	// no `xp_cmdshell` that means anything else.
	//
	// It exists for the ones that can be mistaken: `url(...)` is a remote read
	// on ClickHouse and an ordinary user function name anywhere else, and a
	// rule that fired on every dialect would refuse a PostgreSQL assertion for
	// a grammar PostgreSQL does not have.
	dialects []string
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
		// The same reach without a table to attach it to. ClickHouse takes
		// these in the FROM clause of an ordinary SELECT, so a statement that
		// declares nothing and begins with SELECT still fetches a URL the
		// author named, from inside the server's network.
		construct: "ClickHouse remote table function",
		reach:     "reads over the network or from the host filesystem inside a query",
		match:     calledFunctionAnyOf(clickHouseRemoteTableFunctions()...),
		dialects:  []string{platform.ClickHouse},
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
		// adminpack, which ships with PostgreSQL and is installed on plenty of
		// servers. Each of these fits in one scalar SELECT and changes a file
		// on the host, which no transaction restores.
		construct: "adminpack file function",
		reach:     "writes, renames or removes a file on the database server host",
		match: anyMatch(
			calledFunction("PG_FILE_WRITE"), calledFunction("PG_FILE_RENAME"),
			calledFunction("PG_FILE_UNLINK"), calledFunction("PG_FILE_SYNC"),
			calledFunction("PG_LOGDIR_LS"),
		),
	},
	{
		// Every predefined `pg_ls_` function lists a directory on the server
		// host -- pg_ls_dir, pg_ls_logdir, pg_ls_waldir, pg_ls_tmpdir,
		// pg_ls_archive_statusdir, pg_ls_replslotdir -- so the prefix is the
		// rule rather than a list that goes stale on the next release.
		construct: "pg_ls_ directory listing",
		reach:     "lists a directory on the database server host",
		match:     calledFunctionPrefix("PG_LS_"),
	},
	{
		// Oracle reaches the network through built-in packages rather than
		// through a table or a function name of its own, and a package name is
		// matched wherever it appears: the call is qualified, so the callable
		// token is the procedure and the package sits in front of a dot.
		construct: "Oracle network package",
		reach:     "sends a request from the database server, which no transaction retracts",
		match: anyMatch(
			containsKeyword("UTL_HTTP"), containsKeyword("UTL_TCP"),
			containsKeyword("UTL_SMTP"), containsKeyword("UTL_MAIL"),
			containsKeyword("UTL_INADDR"), containsKeyword("UTL_URL"),
			containsKeyword("DBMS_LDAP"), containsKeyword("HTTPURITYPE"),
			containsKeyword("DBMS_NETWORK_ACL_ADMIN"), containsKeyword("UTL_FILE"),
		),
		dialects: []string{platform.Oracle},
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
		// Not reach outside the database but control over the server it runs
		// on: terminating a backend, reloading the configuration, promoting a
		// standby, switching the write-ahead log. A read-only transaction
		// permits every one of them and undoes none.
		construct: "PostgreSQL server control function",
		reach: "controls the server or its replication state rather than reading from it, " +
			"and no transaction undoes that",
		match: calledFunctionAnyOf(append(
			PostgresControlFunctions(), PostgresReplicationFunctions()...,
		)...),
		dialects: []string{
			platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		},
	},
	{
		construct: "OPENQUERY",
		reach:     "runs a query on a linked server, which is another database server",
		match:     calledFunction("OPENQUERY"),
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
// Finding names one construct that reaches outside the connected database,
// and what it can touch.
type Finding struct {
	// Construct is the SQL construct that reaches out.
	Construct string
	// Reach says what it can touch, in the words a refusal prints.
	Reach string
}

// TooDeepError reports that a statement nested executable code inside string
// literals deeper than the scanner follows.
//
// It is an error rather than a pass because the point of the scan is that
// unreviewed SQL does not run: a document burying code this deep is not
// something a legitimate producer emits, and reporting it as clean would say
// the scan looked when it stopped.
type TooDeepError struct {
	// Depth is how many levels the scanner follows before refusing.
	Depth int
}

func (e *TooDeepError) Error() string {
	return fmt.Sprintf(
		"nests executable code inside string literals more than %d levels deep, which the scanner does not follow",
		e.Depth)
}

// Scan reports the first construct in statement that reaches outside the
// connected database.
//
// It is a BEST-EFFORT LINT over statement text and cannot be a containment
// boundary -- see [escapeRules], which says why and what does enforce. A
// caller that needs enforcement uses an engine that provides it; a caller that
// needs a refusal a person can read uses this.
//
// dialect selects the lexer behavior, and every statement is scanned under
// both string-escape interpretations, because the mode the server is actually
// in is session state: MySQL under NO_BACKSLASH_ESCAPES closes a string where
// the backslash reading continues it, so a scan that assumed one reading sees
// a construct the server does not, or misses one it does.
//
// The boolean is false with no finding. An error means the statement could not
// be scanned to the end, which is not the same as clean.
func Scan(statement, dialect string) (Finding, bool, error) {
	for _, backslashEscapes := range []bool{false, true} {
		finding, found, err := scanStatement(statement, backslashEscapes, dialect, 0)
		if err != nil || found {
			return finding, found, err
		}
	}
	return Finding{}, false, nil
}

func scanStatement(statement string, backslashEscapes bool, dialect string, depth int) (Finding, bool, error) {
	tokens := SignificantTokens(statement, backslashEscapes, dialect)
	if len(tokens) == 0 {
		return Finding{}, false, nil
	}
	ctx := scanContext{tokens: tokens, inBody: depth > 0}
	normalized := platform.NormalizeDialect(dialect)
	for _, rule := range escapeRules {
		if len(rule.dialects) > 0 && !slices.Contains(rule.dialects, normalized) {
			continue
		}
		if !rule.match(ctx) {
			continue
		}
		return Finding{Construct: rule.construct, Reach: rule.reach}, true, nil
	}
	nested := codeBearingStrings(ctx)
	if len(nested) == 0 {
		return Finding{}, false, nil
	}
	if depth >= MaxNesting {
		return Finding{}, false, &TooDeepError{Depth: MaxNesting}
	}
	for _, inner := range nested {
		finding, found, err := scanStatement(inner, backslashEscapes, dialect, depth+1)
		if err != nil || found {
			return finding, found, err
		}
	}
	return Finding{}, false, nil
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
	return i > 0 && IsKeyword(tokens[i-1], keyword)
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
		if !IsKeyword(tokens[i], keyword) {
			return false
		}
	}
	return true
}

func containsKeyword(keyword string) tokenMatcher {
	return func(ctx scanContext) bool {
		for _, token := range ctx.tokens {
			if IsKeyword(token, keyword) {
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
			if !IsKeyword(token, "ENGINE") {
				continue
			}
			next := i + 1
			if next < len(ctx.tokens) && ctx.tokens[next].MatchOperatorValue("=") {
				next++
			}
			if next < len(ctx.tokens) && IsKeyword(ctx.tokens[next], name) {
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
			if !IsKeyword(token, "PRAGMA") {
				continue
			}
			for j := i + 1; j < len(ctx.tokens) && j <= i+3; j++ {
				if ctx.tokens[j].MatchOperatorValue(".") {
					continue
				}
				if IsKeyword(ctx.tokens[j], name) {
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
// calledFunctionAnyOf matches a call to any of the named functions.
func calledFunctionAnyOf(names ...string) tokenMatcher {
	matchers := make([]tokenMatcher, 0, len(names))
	for _, name := range names {
		matchers = append(matchers, calledFunction(name))
	}
	return anyMatch(matchers...)
}

// PostgresControlFunctions enumerates the PostgreSQL functions that act on the
// server rather than reading from it.
//
// It is exported because more than one caller refuses the same set:
// internal/devclean refuses them in a statement it is about to run against a
// dev database, and the check-assertion validator refuses them in a predicate
// it is about to send to a live one. The question differs and the names do
// not, so a second list would agree until one of them was extended.
//
// Advisory-lock functions are deliberately absent, and that is the one
// exclusion. A session advisory lock is released when the session ends, which
// an isolated query session guarantees by discarding the physical connection,
// so nothing outlives the statement for a caller to refuse. Replication state
// is [PostgresReplicationFunctions], separate so a caller can say which it
// refused.
func PostgresControlFunctions() []string {
	return []string{
		"PG_CANCEL_BACKEND", "PG_TERMINATE_BACKEND",
		"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE",
		"PG_PROMOTE", "PG_CREATE_RESTORE_POINT",
		"PG_SWITCH_WAL", "PG_WAL_REPLAY_PAUSE", "PG_WAL_REPLAY_RESUME",
		"PG_NOTIFY", "SET_CONFIG",
		"PG_STAT_RESET", "PG_STAT_RESET_SHARED", "PG_STAT_RESET_SLRU",
		"PG_STAT_RESET_SINGLE_TABLE_COUNTERS", "PG_STAT_RESET_SINGLE_FUNCTION_COUNTERS",
		"PG_STAT_RESET_REPLICATION_SLOT", "PG_STAT_RESET_SUBSCRIPTION_STATS",
	}
}

// PostgresReplicationFunctions enumerates the functions that change
// replication state.
//
// Separate from [PostgresControlFunctions] because a caller may want to say
// which it refused -- internal/devclean names the two classes differently in
// its refusal -- while a caller that only asks "does this reach past the
// statement" takes both.
//
// A slot outlives everything a session can undo: it is created by a SELECT, it
// survives the rollback and the discarded connection, and it retains
// write-ahead log until an operator removes it.
func PostgresReplicationFunctions() []string {
	return []string{
		"PG_COPY_LOGICAL_REPLICATION_SLOT", "PG_COPY_PHYSICAL_REPLICATION_SLOT",
		"PG_CREATE_LOGICAL_REPLICATION_SLOT", "PG_CREATE_PHYSICAL_REPLICATION_SLOT",
		"PG_DROP_REPLICATION_SLOT",
		"PG_REPLICATION_ORIGIN_ADVANCE", "PG_REPLICATION_ORIGIN_CREATE",
		"PG_REPLICATION_ORIGIN_DROP",
		"PG_REPLICATION_ORIGIN_SESSION_RESET", "PG_REPLICATION_ORIGIN_SESSION_SETUP",
		"PG_REPLICATION_ORIGIN_XACT_RESET", "PG_REPLICATION_ORIGIN_XACT_SETUP",
	}
}

// clickHouseRemoteTableFunctions enumerates the table functions that read
// something other than this server's own storage, each with its `Cluster`
// variant, which ClickHouse spells by suffix and which reaches the same place
// from every node.
//
// Enumeration rather than a prefix: `file` and `url` are short enough that a
// prefix match would catch `fileSize` and `urlHash`, which read nothing. The
// list is the one a reader can check against ClickHouse's own table-function
// index, and a name missing from it is a gap to add rather than a rule to
// loosen.
func clickHouseRemoteTableFunctions() []string {
	bases := []string{
		"URL", "REMOTE", "REMOTESECURE", "CLUSTER", "CLUSTERALLREPLICAS",
		"S3", "GCS", "HDFS", "AZUREBLOBSTORAGE", "COSN", "OSS",
		"FILE", "INPUT", "EXECUTABLE", "EXECUTABLEPOOL",
		"MYSQL", "POSTGRESQL", "MONGODB", "REDIS", "SQLITE", "ODBC", "JDBC",
		"ICEBERG", "ICEBERGS3", "ICEBERGAZURE", "ICEBERGHDFS", "ICEBERGLOCAL",
		"DELTALAKE", "DELTALAKES3", "DELTALAKEAZURE", "HUDI", "FUZZJSON",
	}
	names := make([]string, 0, len(bases)*2)
	for _, base := range bases {
		names = append(names, base, base+"CLUSTER")
	}
	return names
}

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

// IsKeyword reports whether a token is the given bare SQL keyword. Quoted
// identifiers never match: the lexer emits double-quoted ones as strings and
// keeps the quotes on backticked and bracketed ones, and a quoted keyword is
// an ordinary identifier to the database anyway.
// IsKeyword reports whether a token is the given bare keyword. It stays
// strict about the token type on purpose: a quoted identifier spelled like a
// keyword is a name, and a string literal carrying one is data.
func IsKeyword(token lexer.Token, keyword string) bool {
	return token.Type == lexer.TokenIdentifier && strings.EqualFold(token.Value, keyword)
}

// DialectUsesBackslashEscapes reports the string-escape interpretation a
// server applies by default, so a scan or a rewrite reads the token stream the
// engine will parse. MySQL, MariaDB and ClickHouse honor backslash escapes.
//
// A default is all it can be: MySQL reads the rule from sql_mode, which is
// session state, which is why [Scan] reads a statement under both
// interpretations rather than trusting this one.
func DialectUsesBackslashEscapes(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB, platform.ClickHouse:
		return true
	default:
		return false
	}
}

// SignificantTokens tokenizes a statement, dropping whitespace and comments so
// rules see the executable token sequence. The lexer options mirror
// [SplitApplyStatements] for the same dialect, so the scanner and the executor
// agree on what is a string, a comment, and a quoted identifier.
func SignificantTokens(statement string, backslashEscapes bool, dialect string) []lexer.Token {
	// The dialect's own options, with the escape mode overridden. Building a
	// set by hand here meant the scanner read a grammar no server has: MySQL
	// starts a line comment at `--` only when whitespace follows, so
	// `SELECT 1--1 INTO OUTFILE '/tmp/p'` is one live statement there and was
	// dropped as a comment by a lexer that did not know the rule. Escape mode
	// is the one field that varies, because it is session state; every other
	// rule the dialect has is fixed and belongs to dialectlexer.
	options := dialectlexer.Options(dialect)
	options.BackslashEscapes = backslashEscapes
	lexr := lexer.NewLexerWithOptions(statement, options)
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
