// Package ttlsql holds the CockroachDB row-level TTL rules that comparison,
// planning, rendering and reversal share: value equivalence, the storage
// parameter spelling a statement carries, and the parameters a change resets.
package ttlsql

import (
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/internal/crdbduration"
	"ptah.run/internal/crdbinterval"
)

// Equivalent reports whether two policies are the same policy on the server.
//
// Text parameters, counts and flags compare exactly: each was measured to read
// back verbatim, so two spellings that differ are two stored values. Treating
// them as equal would report convergence while the policy differs, which is the
// failure stokaro/ptah#1027 names. ttl_expire_after and
// ttl_row_stats_poll_interval are the exception, because the server rewrites
// both: `72 hours` reads back as `72:00:00` and `600s` as `10m0s`. Each is
// compared through the value its spelling denotes (stokaro/ptah#1605,
// stokaro/ptah#1721).
func Equivalent(a, b crdbschema.Policy) bool {
	left, right := a.Clone(), b.Clone()
	if !sameInterval(left.ExpireAfter, right.ExpireAfter) || !samePollInterval(left.RowStatsPollInterval, right.RowStatsPollInterval) {
		return false
	}
	left.ExpireAfter, right.ExpireAfter = "", ""
	left.RowStatsPollInterval, right.RowStatsPollInterval = "", ""
	return left.Same(right)
}

// sameInterval compares two ttl_expire_after values by the interval they
// denote. A spelling neither side can read falls back to text equality: a
// declaration is refused before it gets here, and the catalog's own spelling
// always reads, so the fallback covers a hand-built value.
func sameInterval(a, b string) bool {
	if a == b {
		return true
	}
	if a == "" || b == "" {
		return false
	}
	equal, err := crdbinterval.Equal(a, b)
	return err == nil && equal
}

// samePollInterval compares two ttl_row_stats_poll_interval values by the
// duration they denote, through the server's truncation to whole seconds.
func samePollInterval(a, b string) bool {
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
	return err == nil && left == right
}

// Options renders the policy as the `name = value` pairs a `WITH (...)` or
// `SET (...)` clause carries, in statement order. A text value is quoted as
// CockroachDB stores it, doubling an embedded single quote: an expiration
// expression is author SQL, and `expires_at + INTERVAL '1 day'` is the shape
// the engine's own documentation uses.
func Options(p crdbschema.Policy) []string {
	parameters := p.Parameters()
	if len(parameters) == 0 {
		return nil
	}
	options := make([]string, 0, len(parameters))
	for _, parameter := range parameters {
		value := parameter.Value
		if parameter.Kind == crdbschema.TextValue {
			value = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		}
		options = append(options, parameter.Name+" = "+value)
	}
	return options
}

// Dropped names the parameters current sets and desired does not, in
// statement order.
//
// `SET` replaces only what it names. Measured on v26.2.5, a table carrying
// ttl_job_cron and ttl_select_batch_size, given `SET (ttl_job_cron =
// '@hourly')`, keeps its batch size, so a declaration that stopped naming a
// parameter would leave it in place forever unless the plan resets it by name.
// Removing the whole policy is `RESET (ttl)`, which callers decide first.
func Dropped(desired, current crdbschema.Policy) []string {
	var dropped []string
	for _, name := range crdbschema.ManagedParameters() {
		if current.Sets(name) && !desired.Sets(name) {
			dropped = append(dropped, name)
		}
	}
	return dropped
}

// ValidateChange checks the same transition before planning, rendering and
// reversal: valid operands that differ on the server. A change whose operands
// are equivalent would plan statements that change nothing.
func ValidateChange(change *crdbdiff.RowTTL) error {
	if err := crdbdiff.Validate(change); err != nil {
		return err
	}
	if change.Before != nil && change.After != nil && Equivalent(change.Before.Policy, change.After.Policy) {
		return fmt.Errorf("%w: CockroachDB row-level TTL operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// Statements lowers a validated change to its ALTER TABLE statements for the
// already-quoted table name: `RESET (ttl)` when the policy goes, and otherwise
// a `RESET` of what the new policy stops naming before a `SET` of everything it
// names. RESET comes first so the text is fixed; either order converges.
// Measured, `RESET (ttl)` also succeeds on a table without a TTL, so the
// removal is idempotent.
func Statements(table string, change *crdbdiff.RowTTL) []string {
	if change.After == nil {
		return []string{fmt.Sprintf("ALTER TABLE %s RESET (%s);", table, crdbschema.MarkerParameter)}
	}
	var statements []string
	if change.Before != nil {
		if dropped := Dropped(change.After.Policy, change.Before.Policy); len(dropped) > 0 {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s RESET (%s);", table, strings.Join(dropped, ", ")))
		}
	}
	return append(statements, fmt.Sprintf("ALTER TABLE %s SET (%s);", table, strings.Join(Options(change.After.Policy), ", ")))
}
