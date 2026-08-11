package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dialectlexer"
	"go.5x5.cz/ptah/internal/lexer"
	"go.5x5.cz/ptah/internal/ptahdirective"
)

// checkDirective is the +ptah directive keyword that introduces a pre-migration
// assertion check:
//
//	-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
const checkDirective = "check"

// OnFail selects what a failing check does. Only abort is supported today: the
// migration is aborted before any body statement runs, leaving nothing applied
// on the transactional path.
type OnFail string

// OnFailAbort aborts the migration when the assertion is not satisfied. It is
// the default and currently the only supported behavior.
const OnFailAbort OnFail = "abort"

// Check is a pre-migration assertion parsed from a `-- +ptah check` directive.
// Assert is a SQL predicate that must evaluate to a single truthy scalar before
// the migration body runs; Name labels the check in error output; OnFail selects
// the failure behavior.
type Check struct {
	Name   string
	Assert string
	OnFail OnFail
}

type checkGroupMode uint8

const (
	checkGroupAll checkGroupMode = iota
	checkGroupOneOf
)

type checkGroup struct {
	name   string
	checks []Check
	mode   checkGroupMode
}

// ParseChecks extracts ordered `-- +ptah check` assertion directives from
// migration SQL, in file order. Unlike ParseFileDirectives — which merges every
// directive into one file-scoped map (later keys win) — checks are an ordered
// list: multiple checks per migration run in the order written.
//
// The scan reuses the lexer-driven, line-anchored approach of
// ParseFileDirectives (through [ptahdirective.Bodies]), so a
// `-- +ptah check` sequence inside a string literal, a block comment, or a
// trailing comment is never mistaken for a check. Each check's arguments are
// parsed with a quote-aware tokenizer so an assert predicate can contain spaces
// and '=' inside a double-quoted value. A malformed check line is a hard error,
// so a bad directive fails the migration cleanly rather than being silently
// skipped. dialect selects the target engine's string, identifier, comment, and
// statement-boundary rules; pass an empty string only when no target dialect is
// available.
func ParseChecks(source, dialect string) ([]Check, error) {
	var checks []Check
	for body := range ptahdirective.Bodies(source, checkLexerOptions(dialect)) {
		args, ok := strings.CutPrefix(strings.TrimSpace(body), checkDirective)
		if !ok || (args != "" && args[0] != ' ' && args[0] != '\t') {
			continue // a +ptah directive, but not a check
		}
		check, err := parseCheckArgs(strings.TrimSpace(args), dialect)
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func checkLexerOptions(dialect string) lexer.Options {
	return dialectlexer.Options(dialect)
}

// isCheckDirectiveBody reports whether a +ptah directive body (the text after
// "+ptah ") is a check directive. ParseFileDirectives uses this to leave check
// lines out of its merged directive map.
func isCheckDirectiveBody(body string) bool {
	after, ok := strings.CutPrefix(strings.TrimSpace(body), checkDirective)
	return ok && (after == "" || after[0] == ' ' || after[0] == '\t')
}

func parseCheckArgs(args, dialect string) (Check, error) {
	tokens, err := tokenizeCheckArgs(args)
	if err != nil {
		return Check{}, err
	}
	check := Check{OnFail: OnFailAbort}
	seen := make(map[string]bool, len(tokens))
	for _, token := range tokens {
		key, rawValue, found := strings.Cut(token, "=")
		if !found || key == "" {
			return Check{}, fmt.Errorf("malformed +ptah check argument %q (want key=value)", token)
		}
		if seen[key] {
			return Check{}, fmt.Errorf("duplicate +ptah check key %q", key)
		}
		seen[key] = true
		value, err := unquoteCheckValue(rawValue)
		if err != nil {
			return Check{}, err
		}
		switch key {
		case "name":
			check.Name = value
		case "assert":
			check.Assert = value
		case "on_fail":
			check.OnFail = OnFail(value)
		default:
			return Check{}, fmt.Errorf("unknown +ptah check key %q (want name, assert, on_fail)", key)
		}
	}
	if strings.TrimSpace(check.Assert) == "" {
		return Check{}, fmt.Errorf("+ptah check requires a non-empty assert predicate")
	}
	if check.OnFail != OnFailAbort {
		return Check{}, fmt.Errorf("unsupported +ptah check on_fail=%q (only abort is supported)", check.OnFail)
	}
	if statements := splitSQLStatementsForDialect(check.Assert, dialect); len(statements) > 1 {
		return Check{}, fmt.Errorf("+ptah check assert must be a single statement, got %d", len(statements))
	}
	// Drop any trailing statement terminator(s) and whitespace so drivers that
	// reject a trailing ';' on a prepared query (MySQL) accept the assert.
	check.Assert = strings.TrimRight(strings.TrimSpace(check.Assert), "; \t")
	return check, nil
}

// CheckFailedError reports a pre-migration assertion check that was not
// satisfied, or whose assertion query could not run. It names the migration
// version and the check so the operator can see exactly which precondition
// blocked the migration.
type CheckFailedError struct {
	Version int64
	Name    string
	Assert  string
	// Err is set when the assertion query itself failed to execute (as opposed
	// to running and returning a falsy result).
	Err error
}

func (e *CheckFailedError) Error() string {
	label := e.Name
	if label == "" {
		label = "(unnamed)"
	}
	if e.Err != nil {
		return fmt.Sprintf("pre-migration check %s for migration %d could not run: %v (assert: %s)",
			label, e.Version, e.Err, e.Assert)
	}
	return fmt.Sprintf("pre-migration check %s for migration %d was not satisfied (assert: %s)",
		label, e.Version, e.Assert)
}

func (e *CheckFailedError) Unwrap() error {
	return e.Err
}

// CheckGroupFailedError reports an Atlas oneof check file in which no assertion
// returned a truthy result, including an empty group. Individual assertion
// execution errors and invalid result shapes remain [CheckFailedError] values
// because they identify the exact SQL statement that made the group unsafe to
// accept.
type CheckGroupFailedError struct {
	Version    int64
	Name       string
	Assertions int
}

func (e *CheckGroupFailedError) Error() string {
	return fmt.Sprintf(
		"pre-migration check group %s for migration %d was not satisfied: none of %d assertions passed",
		e.Name,
		e.Version,
		e.Assertions,
	)
}

// runCheckGroups executes pre-migration check groups in archive order. Normal
// groups require every assertion to pass; Atlas files marked `atlas:assert
// oneof` require at least one. Query errors and invalid result shapes always
// fail closed, including inside oneof groups.
func runCheckGroups(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	serverVersion string,
	version int64,
	groups []checkGroup,
) error {
	if len(groups) == 0 {
		return nil
	}

	return conn.WithIsolatedQuerySession(ctx, checkTransactionOptions(dialect), func(queryer dbschema.IsolatedQueryer) error {
		for _, group := range groups {
			if err := runCheckGroup(ctx, queryer, dialect, serverVersion, version, group); err != nil {
				return err
			}
		}
		return nil
	})
}

func runCheckGroup(
	ctx context.Context,
	queryer dbschema.IsolatedQueryer,
	dialect string,
	serverVersion string,
	version int64,
	group checkGroup,
) error {
	onePassed := false
	for _, check := range group.checks {
		result, err := runCheckAssertion(ctx, queryer, dialect, serverVersion, check.Assert)
		if err != nil {
			return &CheckFailedError{Version: version, Name: check.Name, Assert: check.Assert, Err: err}
		}
		if assertionPassed(result) {
			onePassed = true
			continue
		}
		if group.mode == checkGroupAll {
			return &CheckFailedError{Version: version, Name: check.Name, Assert: check.Assert}
		}
	}
	if group.mode == checkGroupOneOf {
		if onePassed {
			return nil
		}
		return &CheckGroupFailedError{
			Version:    version,
			Name:       group.name,
			Assertions: len(group.checks),
		}
	}
	return nil
}

func atlasCheckFileMode(source, dialect string) checkGroupMode {
	lexr := lexer.NewLexerWithOptions(source, checkLexerOptions(dialect))
	for {
		tok := lexr.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return checkGroupAll
		case lexer.TokenWhitespace:
			continue
		case lexer.TokenComment:
			if strings.EqualFold(strings.TrimSpace(tok.Value), "-- atlas:assert oneof") {
				return checkGroupOneOf
			}
		default:
			return checkGroupAll
		}
	}
}

func checkTransactionOptions(dialect string) *sql.TxOptions {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
		platform.MySQL, platform.MariaDB:
		return &sql.TxOptions{ReadOnly: true}
	default:
		return new(sql.TxOptions)
	}
}

func runCheckAssertion(
	ctx context.Context,
	queryer dbschema.IsolatedQueryer,
	dialect string,
	serverVersion string,
	assertion string,
) (any, error) {
	if err := validateCheckAssertionStatically(assertion, dialect, serverVersion); err != nil {
		return nil, err
	}

	rows, err := queryer.QueryContext(ctx, assertion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) != 1 {
		return nil, fmt.Errorf("check assertion must return exactly one column, got %d", len(columns))
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("check assertion must return exactly one row, got 0")
	}

	var result any
	if err := rows.Scan(&result); err != nil {
		return nil, err
	}
	if rows.Next() {
		return nil, fmt.Errorf("check assertion must return exactly one row, got more than 1")
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// validateCheckAssertionStatically proves an assertion is well-formed from its
// text alone: a single read-only SELECT that does not advance a SQL Server
// sequence. It needs a dialect and a server version string, never a query, so
// it is the whole of what can be decided about a check without a database.
//
// Both callers go through here so there is exactly one implementation of "is
// this assertion well-formed": [runCheckAssertion] on the evaluation path, and
// [validateCheckGroups] for checks a dry run defers.
func validateCheckAssertionStatically(assertion, dialect, serverVersion string) error {
	if err := validateCheckAssertion(assertion, dialect, serverVersion); err != nil {
		return err
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer &&
		containsIdentifierSequence(assertion, dialect, "NEXT", "VALUE", "FOR") {
		return fmt.Errorf("check assertion must not advance a SQL Server sequence with NEXT VALUE FOR")
	}
	return nil
}

// validateCheckGroups statically validates every assertion in groups without
// touching the database, reporting the first offender wrapped exactly as the
// evaluation path would report it. A dry run that defers a check still runs
// this, so an assertion that is malformed or write-shaped is reported whatever
// state the database is in — that verdict never depended on state.
func validateCheckGroups(groups []checkGroup, dialect, serverVersion string, version int64) error {
	for _, group := range groups {
		for _, check := range group.checks {
			if err := validateCheckAssertionStatically(check.Assert, dialect, serverVersion); err != nil {
				return &CheckFailedError{
					Version: version,
					Name:    check.Name,
					Assert:  check.Assert,
					Err:     err,
				}
			}
		}
	}
	return nil
}

func validateCheckAssertion(assertion, dialect, serverVersion string) error {
	effectiveSQL, err := effectiveCheckSQL(assertion, dialect, serverVersion)
	if err != nil {
		return err
	}
	statements := sqlutil.SplitSQLStatementsForDialect(effectiveSQL, dialect)
	if len(statements) != 1 {
		return fmt.Errorf("check assertion must be one read-only SELECT statement, got %d statements", len(statements))
	}
	tok, found := firstCheckToken(statements[0], dialect)
	if !found || !tok.MatchIdentifierValue("SELECT") {
		return fmt.Errorf("check assertion must be a read-only SELECT statement")
	}
	return nil
}

func effectiveCheckSQL(source, dialect, serverVersion string) (string, error) {
	lexr := lexer.NewLexerWithOptions(source, checkLexerOptions(dialect))
	var effective strings.Builder
	for {
		tok := lexr.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return effective.String(), nil
		case lexer.TokenComment:
			effective.WriteByte(' ')
			continue
		case lexer.TokenUnknown:
			comment, ok := parseExecutableComment(tok.Value)
			if !ok {
				return "", fmt.Errorf("check assertion contains an unrecognized SQL token")
			}
			applies, err := executableCommentApplies(comment, serverVersion)
			if err != nil {
				return "", err
			}
			if !applies {
				effective.WriteByte(' ')
				continue
			}
			expanded, err := effectiveCheckSQL(comment.body, dialect, serverVersion)
			if err != nil {
				return "", err
			}
			effective.WriteByte(' ')
			effective.WriteString(expanded)
			effective.WriteByte(' ')
		default:
			effective.WriteString(tok.Value)
		}
	}
}

type executableComment struct {
	body        string
	guard       int
	guardDigits int
}

func parseExecutableComment(source string) (executableComment, bool) {
	prefixLength := 0
	switch {
	case strings.HasPrefix(source, "/*!"):
		prefixLength = len("/*!")
	case len(source) >= 4 && strings.EqualFold(source[:4], "/*M!"):
		prefixLength = len("/*M!")
	default:
		return executableComment{}, false
	}
	rawBody := source[prefixLength:]
	comment := executableComment{body: rawBody}
	for len(comment.body) > 0 && comment.body[0] >= '0' && comment.body[0] <= '9' {
		comment.guard = comment.guard*10 + int(comment.body[0]-'0')
		comment.body = comment.body[1:]
		comment.guardDigits++
	}
	if comment.guardDigits < 5 {
		comment.body = rawBody
		comment.guard = 0
		comment.guardDigits = 0
	}
	comment.body = strings.TrimSuffix(comment.body, "*/")
	return comment, true
}

func executableCommentApplies(comment executableComment, serverVersion string) (bool, error) {
	if comment.guardDigits == 0 {
		return true, nil
	}
	server, ok := mysqlExecutableCommentServerVersion(serverVersion)
	if !ok {
		return false, fmt.Errorf("check assertion cannot evaluate executable-comment guard against server version %q", serverVersion)
	}
	return server >= comment.guard, nil
}

func mysqlExecutableCommentServerVersion(version string) (int, bool) {
	if strings.Contains(strings.ToLower(version), "mariadb") {
		version = strings.TrimPrefix(version, "5.5.5-")
	}
	parts := [3]int{}
	part := 0
	found := false
	for i := 0; i < len(version) && part < len(parts); i++ {
		ch := version[i]
		switch {
		case ch >= '0' && ch <= '9':
			found = true
			parts[part] = parts[part]*10 + int(ch-'0')
		case found && ch == '.':
			part++
		case found:
			return parts[0]*10000 + parts[1]*100 + parts[2], true
		}
	}
	if !found {
		return 0, false
	}
	return parts[0]*10000 + parts[1]*100 + parts[2], true
}

func firstCheckToken(source, dialect string) (lexer.Token, bool) {
	lexr := lexer.NewLexerWithOptions(source, checkLexerOptions(dialect))
	for {
		tok := lexr.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return lexer.Token{}, false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			return tok, true
		}
	}
}

func containsIdentifierSequence(source, dialect string, sequence ...string) bool {
	if len(sequence) == 0 {
		return false
	}
	lexr := lexer.NewLexerWithOptions(source, checkLexerOptions(dialect))
	matched := 0
	for {
		tok := lexr.NextToken()
		switch tok.Type {
		case lexer.TokenEOF:
			return false
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenIdentifier:
			if matched < len(sequence) && strings.EqualFold(tok.Value, sequence[matched]) {
				matched++
				if matched == len(sequence) {
					return true
				}
				continue
			}
			if strings.EqualFold(tok.Value, sequence[0]) {
				matched = 1
			} else {
				matched = 0
			}
		default:
			matched = 0
		}
	}
}

// assertionPassed interprets a check's scalar result as a truthy pass. Booleans
// use their value; numbers pass when non-zero; string/byte results accept the
// common truthy spellings (t/true/1/y/yes) case-insensitively and otherwise
// parse as a number. A NULL or unrecognized result fails the check, keeping the
// safe default (a check that cannot be shown to hold blocks the migration).
func assertionPassed(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	case []byte:
		return scalarStringTruthy(string(v))
	case string:
		return scalarStringTruthy(v)
	default:
		return false
	}
}

func scalarStringTruthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true", "1", "y", "yes":
		return true
	case "f", "false", "0", "n", "no", "":
		return false
	}
	// Fall back to a numeric interpretation so a driver that returns a count as
	// text still works.
	if f, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil {
		return f != 0
	}
	return false
}

// tokenizeCheckArgs splits a check directive's arguments on unquoted whitespace,
// keeping double-quoted spans (which may contain spaces and '=') together. A
// doubled "" inside a quoted span is an escaped double quote. Quotes are
// retained in the emitted tokens; unquoteCheckValue strips and unescapes them.
func tokenizeCheckArgs(s string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inQuote := false
	started := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			if inQuote && i+1 < len(s) && s[i+1] == '"' {
				cur.WriteString(`""`)
				i++
				started = true
				continue
			}
			inQuote = !inQuote
			cur.WriteByte(c)
			started = true
		case (c == ' ' || c == '\t') && !inQuote:
			if started {
				tokens = append(tokens, cur.String())
				cur.Reset()
				started = false
			}
		default:
			cur.WriteByte(c)
			started = true
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quote in +ptah check directive")
	}
	if started {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}

// unquoteCheckValue strips a surrounding pair of double quotes from a check
// argument value and collapses doubled "" escapes. An unquoted value is
// returned as-is, but a value that mixes a quote with unquoted text is an error.
func unquoteCheckValue(raw string) (string, error) {
	if raw == "" || raw[0] != '"' {
		if strings.Contains(raw, `"`) {
			return "", fmt.Errorf("malformed +ptah check value %q (unbalanced quote)", raw)
		}
		return raw, nil
	}
	if len(raw) < 2 || raw[len(raw)-1] != '"' {
		return "", fmt.Errorf("unterminated quote in +ptah check value %q", raw)
	}
	return strings.ReplaceAll(raw[1:len(raw)-1], `""`, `"`), nil
}
