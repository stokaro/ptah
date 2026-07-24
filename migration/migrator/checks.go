package migrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/lexer"
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

// ParseChecks extracts ordered `-- +ptah check` assertion directives from
// migration SQL, in file order. Unlike ParseFileDirectives — which merges every
// directive into one file-scoped map (later keys win) — checks are an ordered
// list: multiple checks per migration run in the order written.
//
// The scan reuses the lexer-driven, line-anchored approach of
// ParseFileDirectives (shared lexer walk and commentStartsLine), so a
// `-- +ptah check` sequence inside a string literal, a block comment, or a
// trailing comment is never mistaken for a check. Each check's arguments are
// parsed with a quote-aware tokenizer so an assert predicate can contain spaces
// and '=' inside a double-quoted value. A malformed check line is a hard error,
// so a bad directive fails the migration cleanly rather than being silently
// skipped.
func ParseChecks(sql string) ([]Check, error) {
	var checks []Check
	lexr := lexer.NewLexer(sql)
	for {
		tok := lexr.NextToken()
		if tok.Type == lexer.TokenEOF {
			break
		}
		if tok.Type != lexer.TokenComment {
			continue
		}
		body, ok := strings.CutPrefix(tok.Value, "--")
		if !ok {
			continue // block comment: not a directive carrier
		}
		if !commentStartsLine(sql, tok.Start) {
			continue // trailing comment: not a directive
		}
		body, ok = strings.CutPrefix(strings.TrimSpace(body), directivePrefix)
		if !ok || (body != "" && body[0] != ' ' && body[0] != '\t') {
			continue // not a +ptah directive
		}
		args, ok := strings.CutPrefix(strings.TrimSpace(body), checkDirective)
		if !ok || (args != "" && args[0] != ' ' && args[0] != '\t') {
			continue // a +ptah directive, but not a check
		}
		check, err := parseCheckArgs(strings.TrimSpace(args))
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}
	return checks, nil
}

// isCheckDirectiveBody reports whether a +ptah directive body (the text after
// "+ptah ") is a check directive. ParseFileDirectives uses this to leave check
// lines out of its merged directive map.
func isCheckDirectiveBody(body string) bool {
	after, ok := strings.CutPrefix(strings.TrimSpace(body), checkDirective)
	return ok && (after == "" || after[0] == ' ' || after[0] == '\t')
}

func parseCheckArgs(args string) (Check, error) {
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
	if statements := SplitSQLStatements(check.Assert); len(statements) > 1 {
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

// runChecks executes each check's assert predicate against conn in file order
// and returns a *CheckFailedError on the first query error or falsy result.
// Checks are read-only assertions on the pre-migration state; on the
// transactional apply paths conn is the migration's transaction connection, so
// a failure rolls back with nothing applied.
func runChecks(ctx context.Context, conn *dbschema.DatabaseConnection, version int64, checks []Check) error {
	for _, check := range checks {
		var result any
		if err := conn.QueryRowContext(ctx, check.Assert).Scan(&result); err != nil {
			return &CheckFailedError{Version: version, Name: check.Name, Assert: check.Assert, Err: err}
		}
		if !assertionPassed(result) {
			return &CheckFailedError{Version: version, Name: check.Name, Assert: check.Assert}
		}
	}
	return nil
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
	case int64:
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
