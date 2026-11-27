package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/internal/ptahdirective"
	"ptah.run/internal/sqlreach"
	"ptah.run/migration/migrationfile"
)

// OnFail selects what a failing check does. Only abort is supported today: the
// migration is aborted before any body statement runs, leaving nothing applied
// on the transactional path.
type OnFail string

// OnFailAbort aborts the migration when the assertion is not satisfied. It is
// the default and currently the only supported behavior.
const OnFailAbort OnFail = "abort"

// CheckPhase selects when a check's predicate is evaluated, relative to the
// migration body it is written in.
type CheckPhase string

const (
	// CheckPhaseBefore evaluates the predicate before any body statement runs,
	// and is what a directive that names no phase selects. The migration is a
	// precondition's subject: it states what the body needs.
	CheckPhaseBefore CheckPhase = "before"
	// CheckPhaseAfter evaluates the predicate once the body has run and its
	// revision is recorded, so a migration can state what it produced. The body
	// is committed by then, which is why a failure reports the migration as
	// applied rather than rolling anything back -- see
	// [PostMigrationCheckFailedError].
	CheckPhaseAfter CheckPhase = "after"
)

// CheckKind names what a check asserts, for a check a run may waive with the
// others of its kind. The zero value is an ordinary check, which only
// [Migrator.WithSkipChecks] waives.
type CheckKind string

// CheckKindRoundedValues is a check that no row holds a value the column type
// change of its migration would round or cut: a decimal given a lower scale
// or put under an integer, or a temporal column given a lower
// fractional-second precision, which most servers convert without an error.
// `ptah migrations generate` writes one before each such change, and
// [Migrator.WithValueRounding] waives them.
const CheckKindRoundedValues CheckKind = "rounded-values"

// CheckKindTimeZoneLoss is a check that a column whose type removes a time
// zone or offset holds no non-NULL values. The check runs before the conversion,
// including in a generated rollback. [Migrator.WithValueRounding] waives it
// only when the caller explicitly permits this information loss.
const CheckKindTimeZoneLoss CheckKind = "time-zone-loss"

// guardsValueLoss is shared by parsing, execution and diagnostics: every kind
// recognized here must run before its conversion and use the same waiver.
func (k CheckKind) guardsValueLoss() bool {
	return k == CheckKindRoundedValues || k == CheckKindTimeZoneLoss
}

// Check is an assertion parsed from a `-- +ptah check` directive. Assert is a
// SQL predicate that must evaluate to a single truthy scalar; Name labels the
// check in error output; OnFail selects the failure behavior; Phase selects
// whether the predicate is evaluated before or after the migration body, and
// is [CheckPhaseBefore] when the directive names none; Kind is the class a
// run may waive the check with, and empty for an ordinary check.
type Check struct {
	Name   string
	Assert string
	OnFail OnFail
	Phase  CheckPhase
	Kind   CheckKind
}

// Directive is the `-- +ptah check` line [ParseChecks] reads back as c, on
// one line with no line break: name and assert are always quoted, each double
// quote inside doubled, and on_fail, phase and kind are written only where
// they differ from what a directive that omits them selects. A Name or Assert
// that holds a line break cannot be written as one line, and is refused.
func (c Check) Directive() (string, error) {
	if strings.ContainsAny(c.Name+c.Assert, "\r\n") {
		return "", errors.New("a +ptah check directive is one line, and its name or assert holds a line break")
	}
	var line strings.Builder
	line.WriteString("-- +ptah check")
	if c.Kind != "" {
		line.WriteString(" kind=" + string(c.Kind))
	}
	line.WriteString(" name=" + quoteCheckValue(c.Name))
	if c.Phase != "" && c.Phase != CheckPhaseBefore {
		line.WriteString(" phase=" + string(c.Phase))
	}
	if c.OnFail != "" && c.OnFail != OnFailAbort {
		line.WriteString(" on_fail=" + string(c.OnFail))
	}
	line.WriteString(" assert=" + quoteCheckValue(c.Assert))
	return line.String(), nil
}

// quoteCheckValue writes value as a quoted check argument, which
// [unquoteCheckValue] reads back as value.
func quoteCheckValue(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
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
	phase  CheckPhase
	// roundingRequest is how the run asks to waive the checks of kind
	// [CheckKindRoundedValues], which a failed one names; see
	// [ValueRounding.Request].
	roundingRequest string
}

// ParseChecks extracts ordered `-- +ptah check` assertion directives from
// migration SQL, in file order. Unlike [migrationfile.ParseDirectives] — which merges every
// directive into one file-scoped map (later keys win) — checks are an ordered
// list: multiple checks per migration run in the order written.
//
// The scan reuses the lexer-driven, line-anchored approach of
// [migrationfile.ParseDirectives] (through [ptahdirective.Bodies]), so a
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
		args, ok := migrationfile.CheckDirectiveArgs(body)
		if !ok {
			continue // a +ptah directive, but not a check
		}
		check, err := parseCheckArgs(args, dialect)
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

func parseCheckArgs(args, dialect string) (Check, error) {
	tokens, err := tokenizeCheckArgs(args)
	if err != nil {
		return Check{}, err
	}
	check := Check{OnFail: OnFailAbort, Phase: CheckPhaseBefore}
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
		if err := check.set(key, value); err != nil {
			return Check{}, err
		}
	}
	if strings.TrimSpace(check.Assert) == "" {
		return Check{}, fmt.Errorf("+ptah check requires a non-empty assert predicate")
	}
	if check.OnFail != OnFailAbort {
		return Check{}, fmt.Errorf("unsupported +ptah check on_fail=%q (only abort is supported)", check.OnFail)
	}
	if check.Phase != CheckPhaseBefore && check.Phase != CheckPhaseAfter {
		return Check{}, fmt.Errorf("unsupported +ptah check phase=%q (want before or after)", check.Phase)
	}
	// After the body the values are already rounded, so the assertion would
	// hold on every table and refuse nothing.
	if check.Kind.guardsValueLoss() && check.Phase == CheckPhaseAfter {
		return Check{}, fmt.Errorf("+ptah check kind=%s reads the rows before the body and takes no phase=%s",
			check.Kind, CheckPhaseAfter)
	}
	if statements := sqlutil.SplitStatementsForDialect(dialect, check.Assert); len(statements) > 1 {
		return Check{}, fmt.Errorf("+ptah check assert must be a single statement, got %d", len(statements))
	}
	// Drop any trailing statement terminator(s) and whitespace so drivers that
	// reject a trailing ';' on a prepared query (MySQL) accept the assert.
	check.Assert = strings.TrimRight(strings.TrimSpace(check.Assert), "; \t")
	return check, nil
}

// set gives c the value of one directive key. A kind is checked here, where
// the directive names it: an empty Kind is an ordinary check, which a directive
// writes by leaving the key out, never as kind="".
func (c *Check) set(key, value string) error {
	switch key {
	case "name":
		c.Name = value
	case "assert":
		c.Assert = value
	case "on_fail":
		c.OnFail = OnFail(value)
	case "phase":
		c.Phase = CheckPhase(value)
	case "kind":
		if !CheckKind(value).guardsValueLoss() {
			return fmt.Errorf("unsupported +ptah check kind=%q (want %s or %s)", value, CheckKindRoundedValues, CheckKindTimeZoneLoss)
		}
		c.Kind = CheckKind(value)
	default:
		return fmt.Errorf("unknown +ptah check key %q (want name, assert, on_fail, phase, kind)", key)
	}
	return nil
}

// CheckFailedError reports a pre-migration assertion check that was not
// satisfied, or whose assertion query could not run. It names the migration
// version and the check so the operator can see exactly which precondition
// blocked the migration.
type CheckFailedError struct {
	Version int64
	Name    string
	Assert  string
	// Phase is the phase the check was evaluated in, and is what the message
	// names. The zero value is [CheckPhaseBefore], which is the phase a
	// directive that names none selects.
	Phase CheckPhase
	// Err is set when the assertion query itself failed to execute (as opposed
	// to running and returning a falsy result).
	Err error
	// Kind is the kind the directive names, and empty for an ordinary check.
	// A check of kind [CheckKindRoundedValues] that ran and did not hold says
	// what a row holds and how the run asks to waive it.
	Kind CheckKind
	// versionLabel names Version as its file name spells it, for an Atlas
	// file whose version is not a number; empty otherwise.
	versionLabel string
	// request is how the run asks to waive a check of Kind; see
	// [ValueRounding.Request].
	request string
}

func (e *CheckFailedError) Error() string {
	label := e.Name
	if label == "" {
		label = "(unnamed)"
	}
	kind := "pre-migration check"
	if e.Phase == CheckPhaseAfter {
		kind = "post-migration check"
	}
	if e.Err != nil {
		how := ""
		if e.Kind.guardsValueLoss() && e.request != "" {
			how = "; ask for the change with " + e.request + " to apply the migration without the check"
		}
		return fmt.Sprintf("%s %s for migration %d could not run: %v%s (assert: %s)",
			kind, label, versionText{number: e.Version, label: e.versionLabel}, e.Err, how, e.Assert)
	}
	if e.Kind == CheckKindTimeZoneLoss {
		how := "keep a zoned type or explicitly convert the values in a reviewed migration"
		if e.request != "" {
			how += ", or ask for the change with " + e.request
		}
		return fmt.Sprintf("%s %s for migration %d was not satisfied: the migration removes a time zone or offset "+
			"while the column holds a non-NULL value; %s (assert: %s)",
			kind, label, versionText{number: e.Version, label: e.versionLabel}, how, e.Assert)
	}
	if e.Kind == CheckKindRoundedValues {
		how := "store values the new type keeps"
		if e.request != "" {
			how += ", or ask for the change with " + e.request
		}
		return fmt.Sprintf("%s %s for migration %d was not satisfied: the migration converts a column to a type that "+
			"keeps fewer digits after the point while the table holds a value with more digits, which the server rounds "+
			"or cuts without an error; %s (assert: %s)",
			kind, label, versionText{number: e.Version, label: e.versionLabel}, how, e.Assert)
	}
	return fmt.Sprintf("%s %s for migration %d was not satisfied (assert: %s)",
		kind, label, versionText{number: e.Version, label: e.versionLabel}, e.Assert)
}

func (e *CheckFailedError) Unwrap() error {
	return e.Err
}

// PostMigrationCheckFailedError reports a migration whose body applied and
// whose post-migration check did not hold.
//
// It is a type of its own because the outcome it names has no equivalent on
// the precondition path: the body is committed and the revision says applied,
// so nothing is rolled back and nothing is re-run. An operator reading
// "migration 20 failed" would look for a migration to retry; what there is to
// do here is decide what to do about a database that took the change and did
// not reach the state the change was for.
//
// Unwrap yields the check failure underneath, so a caller that wants to know
// which assertion it was asks [CheckFailedError] through errors.As.
type PostMigrationCheckFailedError struct {
	Version int64
	// Direction is the half that ran. It decides what the message says the
	// database now holds, which is the opposite thing in each direction: an
	// applied migration after an up, and a removed revision after a down.
	// Naming only one of them would tell half the operators the reverse of
	// their own state.
	Direction MigrationDirection
	Err       error
	// versionLabel names Version as its file name spells it, for an Atlas
	// file whose version is not a number; empty otherwise.
	versionLabel string
}

func (e *PostMigrationCheckFailedError) Error() string {
	if e.Direction == MigrationDirectionDown {
		return fmt.Sprintf(
			"rollback of migration %d completed and its post-migration check did not hold: %v "+
				"(the rollback ran, its revision is gone, and nothing was re-applied)",
			versionText{number: e.Version, label: e.versionLabel},
			e.Err,
		)
	}
	return fmt.Sprintf(
		"migration %d applied and its post-migration check did not hold: %v "+
			"(the migration is recorded as applied and nothing was rolled back)",
		versionText{number: e.Version, label: e.versionLabel},
		e.Err,
	)
}

func (e *PostMigrationCheckFailedError) Unwrap() error {
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
	// versionLabel names Version as its file name spells it, for an Atlas
	// file whose version is not a number; empty otherwise.
	versionLabel string
}

func (e *CheckGroupFailedError) Error() string {
	return fmt.Sprintf(
		"pre-migration check group %s for migration %d was not satisfied: none of %d assertions passed",
		e.Name,
		versionText{number: e.Version, label: e.versionLabel},
		e.Assertions,
	)
}

// runCheckGroups executes pre-migration check groups in archive order. Normal
// groups require every assertion to pass; Atlas files marked `atlas:assert
// oneof` require at least one. Query errors and invalid result shapes always
// fail closed, including inside oneof groups.
//
// rule proves each assertion read-only before it runs; see
// [Migrator.checkAssertionRule].
func runCheckGroups(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	rule assertionRule,
	version versionText,
	groups []checkGroup,
) error {
	if len(groups) == 0 {
		return nil
	}
	dialect := conn.Info().Dialect
	return conn.WithIsolatedQuerySession(ctx, checkTransactionOptions(dialect), func(queryer dbschema.IsolatedQueryer) error {
		for _, group := range groups {
			if err := runCheckGroup(ctx, queryer, rule, version, group); err != nil {
				return err
			}
		}
		return nil
	})
}

func runCheckGroup(
	ctx context.Context,
	queryer dbschema.IsolatedQueryer,
	rule assertionRule,
	version versionText,
	group checkGroup,
) error {
	onePassed := false
	for _, check := range group.checks {
		result, err := runCheckAssertion(ctx, queryer, rule, check.Assert)
		if err != nil {
			return &CheckFailedError{
				Version: version.number, Name: check.Name, Assert: check.Assert, Phase: check.Phase, Err: err,
				Kind: check.Kind, versionLabel: version.label, request: group.roundingRequest,
			}
		}
		if assertionPassed(result) {
			onePassed = true
			continue
		}
		if group.mode == checkGroupAll {
			return &CheckFailedError{
				Version: version.number, Name: check.Name, Assert: check.Assert, Phase: check.Phase,
				Kind: check.Kind, versionLabel: version.label, request: group.roundingRequest,
			}
		}
	}
	if group.mode == checkGroupOneOf {
		if onePassed {
			return nil
		}
		return &CheckGroupFailedError{
			Version:      version.number,
			versionLabel: version.label,
			Name:         group.name,
			Assertions:   len(group.checks),
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

// checkTransactionOptions asks for a read-only session where the engine's
// server enforces one, so a statement that gets past the static rules is
// still refused by something other than Ptah (see
// internal/migratorext.Services.ReadOnlyChecks).
func checkTransactionOptions(dialect string) *sql.TxOptions {
	if builtin.Lookup(dialect).ReadOnlyChecks {
		return &sql.TxOptions{ReadOnly: true}
	}
	return new(sql.TxOptions)
}

// runCheckAssertion runs one assertion once rule proves its text read-only.
func runCheckAssertion(
	ctx context.Context,
	queryer dbschema.IsolatedQueryer,
	rule assertionRule,
	assertion string,
) (any, error) {
	if err := rule(assertion); err != nil {
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
// text alone: a single read-only SELECT that reaches nothing outside the
// database it is sent to and advances no sequence. It needs a dialect and a
// server version string, never a query, so it is the whole of what can be
// decided about a check without a database.
//
// Beginning with SELECT is not by itself enough, and the two rules below the
// shape check are the constructs where it is not. What they have in common is
// an effect the transaction around the statement does not own, so no isolation
// level and no read-only session takes it back.
//
// The first is reach. A SELECT can name another database server, a file on the
// host or a shell -- dblink, postgres_fdw, INTO OUTFILE, LOAD_FILE,
// pg_read_file, xp_cmdshell -- and what those touch is outside every
// transaction. [sqlreach.Scan] is the one place that set is written down,
// shared with the plan guard that refuses the same constructs before a dev
// database replay, because two lists agree only until one of them is extended.
// It reads a statement under both string-escape interpretations, which is what
// closes the MySQL sql_mode gap: under NO_BACKSLASH_ESCAPES the server ends a
// string where the backslash reading continues it, so `SELECT 'x\' INTO
// OUTFILE '/tmp/p'` is a SELECT with a live clause on one reading and an
// opaque literal on the other.
//
// The second is a sequence, which is a side effect inside the database rather
// than reach outside it: SQL Server's NEXT VALUE FOR and Oracle's NEXTVAL
// advance a counter no rollback restores.
//
// Every rule reads the effective SQL rather than the text as written, for the
// reason the effective form exists: a MySQL-family executable comment carries
// SQL the server runs and the lexer reports as one opaque token, so
// `SELECT 'x' /*! INTO OUTFILE '/tmp/x' */` hides the clause from a scan of the
// original. Expanding first is what makes one rule cover both spellings.
//
// And every rule runs over every effective form, because the expansion itself
// depends on the escape mode the server is in: see [effectiveCheckSQLForms].
//
// Both callers go through here so there is exactly one implementation of "is
// this assertion well-formed": [runCheckAssertion] on the evaluation path, and
// [validateCheckGroups] for checks a dry run defers.
func validateCheckAssertionStatically(assertion, dialect, serverVersion string) error {
	forms, err := effectiveCheckSQLForms(assertion, dialect, serverVersion)
	if err != nil {
		return err
	}
	for _, effective := range forms {
		if err := validateEffectiveCheckAssertion(effective, dialect); err != nil {
			return err
		}
	}
	return nil
}

// validateEffectiveCheckAssertion applies every rule to one effective form.
func validateEffectiveCheckAssertion(effective, dialect string) error {
	if err := validateCheckAssertion(effective, dialect); err != nil {
		return err
	}
	finding, reaches, err := sqlreach.Scan(effective, dialect)
	if err != nil {
		return fmt.Errorf("check assertion cannot be proved read-only: it %w", err)
	}
	if reaches {
		return fmt.Errorf(
			"check assertion must not use %s, which %s",
			finding.Construct, finding.Reach,
		)
	}
	if checks := builtin.Lookup(dialect).Checks; checks != nil {
		if err := checks.Validate(effective); err != nil {
			return err
		}
	}
	return nil
}

// validateCheckGroups statically validates every assertion in groups without
// touching the database, reporting the first offender wrapped exactly as the
// evaluation path would report it. A dry run that defers a check still runs
// this, so an assertion that is malformed or write-shaped is reported whatever
// state the database is in — that verdict never depended on state.
func validateCheckGroups(groups []checkGroup, rule assertionRule, version versionText) error {
	for _, group := range groups {
		for _, check := range group.checks {
			if err := rule(check.Assert); err != nil {
				return &CheckFailedError{
					Version:      version.number,
					versionLabel: version.label,
					Name:         check.Name,
					Assert:       check.Assert,
					Phase:        check.Phase,
					Kind:         check.Kind,
					Err:          err,
					request:      group.roundingRequest,
				}
			}
		}
	}
	return nil
}

func validateCheckAssertion(effectiveSQL, dialect string) error {
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

// effectiveCheckSQLForms returns the effective SQL of an assertion under every
// string-escape interpretation the server might apply.
//
// One form is not enough, because the two decisions compose. The expansion
// tokenizes to find the executable comments, and which characters end a string
// decides whether a comment is a comment at all: with backslash escapes on,
// `SELECT 'x\' /*! INTO OUTFILE '/tmp/p' */` opens a string at `'x` that runs
// through the comment, so nothing is expanded; with them off the string ends
// at the second quote and the comment is real SQL the server runs. A scan of
// one expansion therefore reads a statement the server may never see.
//
// Duplicates are dropped, so a dialect with no executable comments and no
// backslash ambiguity still produces one form and every rule runs once.
func effectiveCheckSQLForms(source, dialect, serverVersion string) ([]string, error) {
	forms := make([]string, 0, 2)
	for _, backslashEscapes := range []bool{false, true} {
		options := checkLexerOptions(dialect)
		options.BackslashEscapes = backslashEscapes
		form, err := effectiveCheckSQLWithOptions(source, options, dialect, serverVersion)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(forms, form) {
			forms = append(forms, form)
		}
	}
	return forms, nil
}

func effectiveCheckSQLWithOptions(
	source string,
	options lexer.Options,
	dialect, serverVersion string,
) (string, error) {
	lexr := lexer.NewLexerWithOptions(source, options)
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
			expanded, err := effectiveCheckSQLWithOptions(comment.body, options, dialect, serverVersion)
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
	return migratorext.ContainsIdentifierSequence(source, checkLexerOptions(dialect), sequence...)
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
