// Package crdbttl owns what Ptah does about CockroachDB row-level TTL: which
// declared parameters it manages, how they are rendered, how they are read back
// out of the catalog, and what it refuses before a server is touched.
//
// CockroachDB expresses row expiry as table storage parameters — the same
// `WITH (...)` position PostgreSQL uses for fillfactor — and the parameters are
// not interchangeable. Four facts drive everything here, and each was measured
// against live CockroachDB CCL v25.4.14 and v26.2.5 rather than read off a
// manual:
//
//   - Only `ttl_expire_after` and `ttl_expiration_expression` may ENABLE a TTL.
//     Every other ttl_* parameter, and `ttl` itself, is answered
//     `ERROR: "ttl_expire_after" and/or "ttl_expiration_expression" must be set`
//     when it arrives alone. So a declaration that sets a knob and no enabler is
//     refused here rather than at the server.
//   - The server REWRITES two of the values on the way in.
//     `ttl_expire_after = '72 hours'` reads back as `'72:00:00':::INTERVAL` and
//     `'5 minutes'` as `'00:05:00'`; `ttl_row_stats_poll_interval = '600s'` reads
//     back as `'10m0s'` and `'1500ms'` as `'1s'`. Neither is compared as text.
//     Each spelling denotes a VALUE, so each is compared through the value:
//     ttl_expire_after by [go.5x5.cz/ptah/internal/crdbinterval]
//     (stokaro/ptah#1605), ttl_row_stats_poll_interval by
//     [go.5x5.cz/ptah/internal/crdbduration] (stokaro/ptah#1721). The two need
//     different readers because the rewrites differ in kind — one is
//     PostgreSQL's interval normalization, the other truncation to whole
//     seconds in Go's duration spelling. The duration has one edge the interval
//     does not: a value below one second truncates to zero and is then stored
//     NOWHERE AT ALL, so `'100ms'` would leave a table reporting the parameter
//     missing on every inspection — that one is refused before SQL rather than
//     compared, along with the two other values the server will not keep as
//     written, by [rowStatsPollIntervalProblems].
//   - Everything else round-trips VERBATIM. `ttl_expiration_expression` keeps its
//     whitespace, case, parentheses, casts and quoted identifiers exactly; the
//     cron string, the four integer knobs and the three boolean knobs come back
//     as written. That is the surface this package manages.
//   - `ttl` is derived. It reads back as `'on'` whenever a TTL is configured and
//     cannot be set by itself, so it is never declared and never compared — but
//     `RESET (ttl)` is how the whole configuration is removed, which is the one
//     place the name is emitted.
//
// PostgreSQL 18.4 answers `ERROR: unrecognized parameter
// "ttl_expiration_expression"` and YugabyteDB 2026.1 answers `WARNING: storage
// parameter ttl_expiration_expression is unsupported, ignoring` before its own
// error. The warning is why the dialect gate lives here and not in the server's
// hands: an engine that ignores a storage parameter would accept a data
// -lifecycle policy and not apply it.
package crdbttl

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/crdbduration"
	"go.5x5.cz/ptah/internal/crdbinterval"
)

// The policy type is [ast.RowTTLSpec], defined in core/ast and used unchanged
// by the schema model and the live description too. One struct rather than one
// per layer is deliberate: the parameters are a closed, measured set, and three
// parallel copies with conversions between them is three places for a field to
// go missing silently -- which for a data-lifecycle policy means a table
// quietly keeping rows it was declared to delete. core/ast has no Ptah imports
// of its own, so nothing is coupled by naming it here.

// Equal reports whether two specs describe the same policy.
//
// The comparison is exact on every value, and deliberately so. Each modeled
// parameter was measured to round-trip verbatim, so two spellings that differ
// are two policies that differ as far as the catalog is concerned — treating
// them as equal is how a tool reports convergence while a table's data
// -lifecycle policy differs, which is the failure stokaro/ptah#1027 names.
func Equal(a, b *ast.RowTTLSpec) bool {
	if a.IsZero() || b.IsZero() {
		return a.IsZero() && b.IsZero()
	}
	return a.ExpirationExpression == b.ExpirationExpression &&
		equalInterval(a.ExpireAfter, b.ExpireAfter) &&
		equalPollInterval(a.RowStatsPollInterval, b.RowStatsPollInterval) &&
		a.JobCron == b.JobCron &&
		equalPtr(a.SelectBatchSize, b.SelectBatchSize) &&
		equalPtr(a.DeleteBatchSize, b.DeleteBatchSize) &&
		equalPtr(a.SelectRateLimit, b.SelectRateLimit) &&
		equalPtr(a.DeleteRateLimit, b.DeleteRateLimit) &&
		equalPtr(a.Pause, b.Pause) &&
		equalPtr(a.LabelMetrics, b.LabelMetrics) &&
		equalPtr(a.DisableChangefeedReplication, b.DisableChangefeedReplication)
}

func equalPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Option is one rendered storage parameter, in the order [Options] fixes.
type Option struct {
	// Name is the parameter as CockroachDB spells it, such as
	// `ttl_expiration_expression`.
	Name string
	// Value is the rendered right-hand side, already quoted where the
	// parameter takes a string.
	Value string
}

// Options renders a spec as the storage parameters a `WITH (...)` or
// `SET (...)` clause carries.
//
// The order is fixed rather than derived from a map, and it is the order this
// function lists: a plan whose statement text changed between runs over the
// same two states would be re-approved by a human every time and would break
// every fingerprint the migration layer takes of it. The enabler comes first
// because it is the parameter the others modify.
//
// It returns nil for a nil or empty spec, which is a table with no TTL rather
// than a table with an empty one.
func Options(s *ast.RowTTLSpec) []Option {
	if s.IsZero() {
		return nil
	}
	options := make([]Option, 0, 10)
	if s.ExpirationExpression != "" {
		options = append(options, Option{ExpirationExpressionParameter, quote(s.ExpirationExpression)})
	}
	if s.ExpireAfter != "" {
		options = append(options, Option{ExpireAfterParameter, quote(s.ExpireAfter)})
	}
	if s.RowStatsPollInterval != "" {
		options = append(options, Option{RowStatsPollIntervalParameter, quote(s.RowStatsPollInterval)})
	}
	if s.JobCron != "" {
		options = append(options, Option{JobCronParameter, quote(s.JobCron)})
	}
	for _, knob := range []struct {
		name  string
		value *int64
	}{
		{SelectBatchSizeParameter, s.SelectBatchSize},
		{DeleteBatchSizeParameter, s.DeleteBatchSize},
		{SelectRateLimitParameter, s.SelectRateLimit},
		{DeleteRateLimitParameter, s.DeleteRateLimit},
	} {
		if knob.value != nil {
			options = append(options, Option{knob.name, strconv.FormatInt(*knob.value, 10)})
		}
	}
	for _, knob := range []struct {
		name  string
		value *bool
	}{
		{PauseParameter, s.Pause},
		{LabelMetricsParameter, s.LabelMetrics},
		{DisableChangefeedReplicationParameter, s.DisableChangefeedReplication},
	} {
		if knob.value != nil {
			options = append(options, Option{knob.name, strconv.FormatBool(*knob.value)})
		}
	}
	return options
}

// quote renders a string value as CockroachDB stores it, doubling an embedded
// single quote. A TTL expression is arbitrary SQL an author wrote, so it may
// legitimately contain one -- `expires_at + INTERVAL '1 day'` is the shape the
// engine's own documentation uses.
func quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// The parameter names, spelled exactly as CockroachDB spells them. They are
// exported because the reader, the renderer and the planner all name them, and
// a second spelling of any of these is a silent no-op rather than an error.
const (
	// MarkerParameter is `ttl`, the derived marker. It is never declared and
	// never compared; `RESET (ttl)` is the only statement that names it, and
	// it is how a whole configuration is removed.
	MarkerParameter = "ttl"

	ExpirationExpressionParameter         = "ttl_expiration_expression"
	JobCronParameter                      = "ttl_job_cron"
	SelectBatchSizeParameter              = "ttl_select_batch_size"
	DeleteBatchSizeParameter              = "ttl_delete_batch_size"
	SelectRateLimitParameter              = "ttl_select_rate_limit"
	DeleteRateLimitParameter              = "ttl_delete_rate_limit"
	PauseParameter                        = "ttl_pause"
	LabelMetricsParameter                 = "ttl_label_metrics"
	DisableChangefeedReplicationParameter = "ttl_disable_changefeed_replication"

	// ExpireAfterParameter and RowStatsPollIntervalParameter are the two whose
	// values the server rewrites, so both are compared through the value they
	// denote rather than as text -- see [equalInterval] and [equalPollInterval].
	ExpireAfterParameter          = "ttl_expire_after"
	RowStatsPollIntervalParameter = "ttl_row_stats_poll_interval"
)

// refusedParameters are the ttl_* parameters a declaration may not carry, each
// mapped to the reason a diagnostic prints.
//
// It held three entries once. `ttl_expire_after` and
// `ttl_row_stats_poll_interval` were both refused because the server REWRITES
// the value it stores, so a declaration compared as text reads as missing on
// every inspection and the plan re-issues it forever -- the same rule
// internal/clickhouserbac applies to a rewritten privilege, with a value
// canonicalization in place of a name expansion. Both are managed now, each
// compared through the value its spelling denotes rather than through the text:
// see [go.5x5.cz/ptah/internal/crdbinterval] (stokaro/ptah#1605) and
// [go.5x5.cz/ptah/internal/crdbduration] (stokaro/ptah#1721), which carry the
// measured tables.
//
// What remains is refused for an unrelated reason. `ttl` is not a knob at all:
// it is derived from the other parameters, and the server answers
// `"ttl_expire_after" and/or "ttl_expiration_expression" must be set` when it
// arrives on its own. There is nothing to canonicalize and nothing to compare.
var refusedParameters = map[string]string{
	MarkerParameter: "it is derived from the other parameters and is refused by the server when it arrives " +
		"alone; declare " + ExpirationExpressionParameter + " to turn a TTL on, and remove that to turn it off",
}

// ValidateDeclared refuses declared TTL configuration Ptah cannot manage, and
// returns nil when nothing declares one.
//
// It is the entry point a declaration reaches before a server does — the
// renderer and the schema comparison both call it — so the refusal arrives
// instead of a mutation, never after one.
//
// caps decides the dialect gate. A declaration reaching a target without
// [capability.RowLevelTTL] is refused HERE rather than at the server, because
// YugabyteDB answers `WARNING: storage parameter ttl_expiration_expression is
// unsupported, ignoring` — an engine that ignores a storage parameter would
// accept a row-expiry policy and never apply it, and the operator would be told
// the migration succeeded.
func ValidateDeclared(dialect string, caps capability.Capabilities, tables []Declared) error {
	declaring := slices.DeleteFunc(slices.Clone(tables), func(t Declared) bool { return t.TTL.IsZero() })
	if len(declaring) == 0 {
		return nil
	}
	slices.SortFunc(declaring, func(a, b Declared) int { return cmp.Compare(a.Table, b.Table) })

	if !caps.Has(capability.RowLevelTTL) {
		return unsupportedDialect(dialect, declaring)
	}

	var problems []error
	for _, table := range declaring {
		problems = append(problems, tableProblems(table)...)
	}
	return errors.Join(problems...)
}

// Declared pairs a table name with the TTL it declares, which is all this
// package needs to validate one. It exists so that callers holding a
// goschema.Table and callers holding an ast.CreateTableNode can both reach the
// same rules without this package importing either.
type Declared struct {
	// Table is the table's name, for the diagnostic.
	Table string
	// TTL is what it declares, nil for a table declaring none.
	TTL *ast.RowTTLSpec
}

// unsupportedDialect is the refusal for a target that has no row-level TTL.
//
// It names the first table in sorted order rather than every one, because the
// answer is the same for all of them and the operator's next action is to
// remove the declaration or change the target.
func unsupportedDialect(dialect string, declaring []Declared) error {
	return fmt.Errorf(
		"table %q declares row-level TTL, which %s does not support: it is a CockroachDB table storage "+
			"parameter, and PostgreSQL answers `unrecognized parameter %q` while YugabyteDB warns that it "+
			"is ignoring the parameter — a policy an engine ignores is worse than one it refuses, so Ptah "+
			"refuses it here",
		declaring[0].Table, platform.NormalizeDialect(dialect), ExpirationExpressionParameter)
}

// tableProblems reports everything wrong with one table's declaration.
func tableProblems(table Declared) []error {
	var problems []error
	if table.TTL.ExpirationExpression == "" && table.TTL.ExpireAfter == "" {
		problems = append(problems, fmt.Errorf(
			"table %q declares row-level TTL settings but neither %s nor %s: CockroachDB refuses every "+
				"other ttl_* parameter when no expiry is configured, answering `\"ttl_expire_after\" "+
				"and/or \"ttl_expiration_expression\" must be set`",
			table.Table, ExpirationExpressionParameter, ExpireAfterParameter))
	}
	problems = append(problems, expireAfterProblems(table)...)
	problems = append(problems, rowStatsPollIntervalProblems(table)...)
	problems = append(problems, knobProblems(table)...)
	return problems
}

// knobProblems reports the declared integer knobs whose values cannot converge.
//
// The bound is 1, and the reason is two different measurements rather than one.
// A NEGATIVE value is refused by the server:
//
//	ttl_select_batch_size = -1   ERROR: "ttl_select_batch_size" must be at least 0
//
// ZERO is accepted, and pg_class.reloptions then reports the parameter NOWHERE
// -- `WITH (ttl_expiration_expression='expires_at', ttl_select_batch_size=0)`
// stores `{ttl='on',ttl_expiration_expression='expires_at'}`. So zero is the
// silent shape: the statement succeeds, the operator believes a batch size was
// set, and every later inspection reports the parameter missing while the plan
// re-issues it forever.
//
// Refusing both here means the declaration fails before the statement runs,
// rather than after part of a migration already applied.
func knobProblems(table Declared) []error {
	var problems []error
	for _, knob := range []struct {
		name  string
		value *int64
	}{
		{SelectBatchSizeParameter, table.TTL.SelectBatchSize},
		{DeleteBatchSizeParameter, table.TTL.DeleteBatchSize},
		{SelectRateLimitParameter, table.TTL.SelectRateLimit},
		{DeleteRateLimitParameter, table.TTL.DeleteRateLimit},
	} {
		if knob.value == nil || *knob.value >= 1 {
			continue
		}
		problems = append(problems, fmt.Errorf(
			"table %q declares %s = %d: CockroachDB refuses a negative value (`must be at least 0`) and "+
				"stores nothing at all for zero, so neither can ever read back as declared — omit the "+
				"parameter to leave the engine's default in place",
			table.Table, knob.name, *knob.value))
	}
	return problems
}

// RefusalFor returns why a ttl_* parameter is not declarable, and false for one
// this package models.
//
// It is exported for the declaration parsers, which meet the parameter as an
// attribute name and have to answer for it before any Spec exists.
func RefusalFor(parameter string) (string, bool) {
	reason, refused := refusedParameters[strings.ToLower(strings.TrimSpace(parameter))]
	return reason, refused
}

// DroppedParameters names the parameters the target carries and the declaration
// does not, in the order [ManagedParameters] lists them.
//
// It exists because `SET` replaces only what it names. Measured on v26.2.5, a
// table carrying ttl_job_cron and ttl_select_batch_size, given
// `SET (ttl_job_cron = '@hourly')`, keeps its batch size — so a declaration
// that stopped naming a parameter would leave it in place forever unless the
// plan resets it by name. `RESET` takes several names at once, which is why
// this returns a list rather than one name at a time.
//
// It is only meaningful when the declaration still asks for a policy. Removing
// the policy entirely is `RESET (ttl)`, and the caller decides that before
// asking this.
func DroppedParameters(desired, current *ast.RowTTLSpec) []string {
	if current.IsZero() {
		return nil
	}
	var dropped []string
	for _, parameter := range ManagedParameters() {
		if declares(current, parameter) && !declares(desired, parameter) {
			dropped = append(dropped, parameter)
		}
	}
	return dropped
}

// declares reports whether a spec carries a value for one parameter.
func declares(spec *ast.RowTTLSpec, parameter string) bool {
	if spec == nil {
		return false
	}
	switch parameter {
	case ExpirationExpressionParameter:
		return spec.ExpirationExpression != ""
	case ExpireAfterParameter:
		return spec.ExpireAfter != ""
	case JobCronParameter:
		return spec.JobCron != ""
	case SelectBatchSizeParameter:
		return spec.SelectBatchSize != nil
	case DeleteBatchSizeParameter:
		return spec.DeleteBatchSize != nil
	case SelectRateLimitParameter:
		return spec.SelectRateLimit != nil
	case DeleteRateLimitParameter:
		return spec.DeleteRateLimit != nil
	case PauseParameter:
		return spec.Pause != nil
	case LabelMetricsParameter:
		return spec.LabelMetrics != nil
	case DisableChangefeedReplicationParameter:
		return spec.DisableChangefeedReplication != nil
	default:
		return false
	}
}

// DeclaredIn collects the TTL declarations of a schema's tables, which is the
// shape [ValidateDeclared] takes. It is here so every caller builds the list the
// same way and none of them has to know which field carries the policy.
func DeclaredIn(tables []TableTTL) []Declared {
	declared := make([]Declared, 0, len(tables))
	for _, table := range tables {
		declared = append(declared, Declared{Table: table.Name, TTL: table.RowTTL})
	}
	return declared
}

// TableTTL is the pair a caller holding any table representation can produce.
type TableTTL struct {
	// Name is the table's name, for the diagnostic.
	Name string
	// RowTTL is what it declares, nil for a table declaring none.
	RowTTL *ast.RowTTLSpec
}

// equalInterval compares two ttl_expire_after values by the interval they
// denote rather than by their text.
//
// It is the one field here compared that way, and it has to be: the server
// rewrites the interval it stores, so `72 hours` reads back as `72:00:00` and a
// text comparison would find a difference on every run. See
// [go.5x5.cz/ptah/internal/crdbinterval] for the measured table.
//
// A value neither side can parse falls back to text equality. That case does
// not arise through a declaration -- [ValidateDeclared] refuses an unreadable
// one before it reaches here -- and the catalog's own spelling always parses,
// so the fallback covers a hand-built spec rather than a real state.
func equalInterval(a, b string) bool {
	if a == b {
		return true
	}
	if a == "" || b == "" {
		return false
	}
	equal, err := crdbinterval.Equal(a, b)
	if err != nil {
		return false
	}
	return equal
}

// equalPollInterval compares two ttl_row_stats_poll_interval values by the
// duration they denote rather than by their text.
//
// It is the second field here compared that way, and for the same reason as
// [equalInterval]: the server rewrites what it stores, so `600s` reads back as
// `10m0s` and a text comparison would find a difference on every run. The
// rewrite is a different one -- truncation to whole seconds and Go's duration
// spelling rather than PostgreSQL's interval normalization -- which is why it
// goes through [go.5x5.cz/ptah/internal/crdbduration] and not crdbinterval.
//
// A value neither side can read falls back to text equality, on the same
// reasoning: [ValidateDeclared] refuses an unreadable declaration before it
// reaches here, and the catalog's own spelling always reads, so the fallback
// covers a hand-built spec rather than a real state.
func equalPollInterval(a, b string) bool {
	if a == b {
		return true
	}
	if a == "" || b == "" {
		return false
	}
	left, err := crdbduration.Canonical(a)
	if err != nil {
		return false
	}
	right, err := crdbduration.Canonical(b)
	if err != nil {
		return false
	}
	return left == right
}

// rowStatsPollIntervalProblems refuses a declared duration the server would not
// keep as written.
//
// Three shapes reach it, each measured on v26.2.5. A value below one second is
// truncated to zero and the parameter is then stored NOWHERE AT ALL, so the
// statement succeeds and every later inspection reports the parameter missing
// -- the same silent shape [knobProblems] refuses for the integer knobs. A
// negative value is refused by the server. A value past the largest duration it
// holds WRAPS in the server and is answered `must be at least 0`, which names
// the sign of a number the operator never wrote.
//
// Refusing all three here turns each into a message naming what would have
// happened, before any statement runs.
func rowStatsPollIntervalProblems(table Declared) []error {
	if table.TTL.RowStatsPollInterval == "" {
		return nil
	}
	if _, err := crdbduration.Canonical(table.TTL.RowStatsPollInterval); err != nil {
		return []error{fmt.Errorf("table %q declares %s: %w", table.Table, RowStatsPollIntervalParameter, err)}
	}
	return nil
}

// expireAfterProblems refuses a declared interval Ptah cannot read.
//
// The refusal is what keeps an unreadable spelling from becoming a silent
// non-convergence: Ptah would send it, the server would store its own
// normalization of it, and every later comparison would find a difference that
// no plan could remove. Refusing here turns that into a message naming the
// forms this surface reads.
func expireAfterProblems(table Declared) []error {
	if table.TTL.ExpireAfter == "" {
		return nil
	}
	if _, err := crdbinterval.Parse(table.TTL.ExpireAfter); err != nil {
		return []error{fmt.Errorf("table %q declares %s: %w", table.Table, ExpireAfterParameter, err)}
	}
	return nil
}
