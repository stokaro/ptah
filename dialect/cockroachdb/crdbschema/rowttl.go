// Package crdbschema owns CockroachDB row-level TTL and its desired and
// observed representations. It contains no provider selection, database
// access, or DDL.
//
// CockroachDB expresses row expiry as table storage parameters, the same
// `WITH (...)` position PostgreSQL uses for fillfactor. The parameter names
// are the vocabulary of both representations: what a declaration writes, what
// a statement carries, and what pg_class.reloptions reports back. Measured on
// CockroachDB CCL v25.4.14 and v26.2.5:
//
//   - Only ttl_expire_after and ttl_expiration_expression enable a TTL. The
//     server refuses every other ttl_ parameter without one.
//   - The server rewrites two values: ttl_expire_after is stored as its
//     normalized interval (`72 hours` reads back as `72:00:00`), and
//     ttl_row_stats_poll_interval as a Go duration (`600s` reads back as
//     `10m0s`). The comparison owner reads both through the value they denote.
//   - Every other parameter reads back verbatim.
//   - ttl is derived. It reads back as on whenever a TTL is configured and
//     cannot be set alone, so neither representation carries it.
//
// A table without a TTL has no value of this kind. Whether that absence is
// known is source coverage, never a zero value: a desired source that can
// declare the policy records complete knowledge, and the reader records it
// for every table it returns.
package crdbschema

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
	"ptah.run/internal/crdbduration"
	"ptah.run/internal/crdbinterval"
)

// RowTTLKind identifies the row-level TTL attached to a common table.
const RowTTLKind schemaext.Kind = "ptah.run/cockroachdb/row-ttl"

// Policy holds the managed storage parameters of one row-level TTL. An empty
// string, a nil count, or a false flag leaves the parameter at the engine's
// default. A false flag is not a separate state: the server stores no flag set
// to false, and `SET (ttl_pause = false)` erases a stored one exactly as
// `RESET (ttl_pause)` does.
type Policy struct {
	// ExpirationExpression is the SQL expression whose value is when a row
	// expires. Its text is kept verbatim, including whitespace and case.
	ExpirationExpression string
	// ExpireAfter is the interval after a row is written at which it expires.
	ExpireAfter string
	// RowStatsPollInterval is how often the job reports row statistics.
	RowStatsPollInterval string
	// JobCron is the cron schedule of the deletion job.
	JobCron string
	// SelectBatchSize is the number of rows the job selects per batch.
	SelectBatchSize *int64
	// DeleteBatchSize is the number of rows the job deletes per batch.
	DeleteBatchSize *int64
	// SelectRateLimit is the number of rows the job selects per second.
	SelectRateLimit *int64
	// DeleteRateLimit is the number of rows the job deletes per second.
	DeleteRateLimit *int64
	// Pause pauses the deletion job without removing the policy.
	Pause bool
	// LabelMetrics labels the job's metrics with the table name.
	LabelMetrics bool
	// DisableChangefeedReplication omits the job's deletes from changefeeds.
	DisableChangefeedReplication bool
}

// Clone returns a policy that shares no counts with p.
func (p Policy) Clone() Policy {
	for _, count := range p.counts() {
		if *count.value != nil {
			*count.value = new(**count.value)
		}
	}
	return p
}

// Same reports structural equality: the same text, counts and flags. It does
// not read an interval or a duration as the value it denotes.
func (p Policy) Same(other Policy) bool {
	if p.ExpirationExpression != other.ExpirationExpression || p.ExpireAfter != other.ExpireAfter ||
		p.RowStatsPollInterval != other.RowStatsPollInterval || p.JobCron != other.JobCron ||
		p.Pause != other.Pause || p.LabelMetrics != other.LabelMetrics ||
		p.DisableChangefeedReplication != other.DisableChangefeedReplication {
		return false
	}
	theirs := other.counts()
	for i, count := range p.counts() {
		mine, their := *count.value, *theirs[i].value
		if (mine == nil) != (their == nil) || (mine != nil && *mine != *their) {
			return false
		}
	}
	return true
}

// IsZero reports a policy that sets no parameter.
func (p Policy) IsZero() bool { return p.Same(Policy{}) }

// DesiredRowTTL is the policy a declaration asks for. Its presence on a table
// requests exactly these parameters; a parameter it leaves out is reset to the
// engine's default. Removing the policy is the absence of this value under
// complete source coverage.
type DesiredRowTTL struct {
	Policy Policy
}

// ObservedRowTTL is the policy a table read found in pg_class.reloptions, in
// the spelling the server stores. Its absence on a table the read covered is
// an observed absence.
type ObservedRowTTL struct {
	Policy Policy
}

// Kind returns the owned row-level TTL identity.
func (*DesiredRowTTL) Kind() schemaext.Kind { return RowTTLKind }

// Kind returns the owned row-level TTL identity.
func (*ObservedRowTTL) Kind() schemaext.Kind { return RowTTLKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRowTTL) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredRowTTL)(nil)
	}
	return &DesiredRowTTL{Policy: v.Policy.Clone()}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRowTTL) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedRowTTL)(nil)
	}
	return &ObservedRowTTL{Policy: v.Policy.Clone()}
}

// Equal compares declarations structurally, without reading intervals.
func (v *DesiredRowTTL) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredRowTTL)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Policy.Same(w.Policy)
}

// Equal compares observations structurally, without reading intervals.
func (v *ObservedRowTTL) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedRowTTL)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Policy.Same(w.Policy)
}

// Desired captures the observed policy as an exact declaration, keeping the
// stored spelling of every value. A nil receiver remains nil.
func (v *ObservedRowTTL) Desired() *DesiredRowTTL {
	if v == nil {
		return nil
	}
	return &DesiredRowTTL{Policy: v.Policy.Clone()}
}

// Observed projects a declaration as the policy a CREATE or SET would leave,
// keeping the declared spelling. It does not predict the server's rewrite of
// an interval; comparison reads both spellings as values. Nil and invalid
// declarations are refused with schemaext.ErrInvalidValue.
func (v *DesiredRowTTL) Observed() (*ObservedRowTTL, error) {
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return &ObservedRowTTL{Policy: v.Policy.Clone()}, nil
}

// ValidateDesired refuses a declaration the server would refuse or would not
// keep as written. Nil is invalid. Errors are schemaext.InvalidModelError
// values wrapping schemaext.ErrInvalidValue. The checks, each measured on
// CockroachDB v26.2.5:
//
//   - one of ttl_expiration_expression and ttl_expire_after is required,
//     because the server refuses every other ttl_ parameter without one;
//   - a count below one is refused: the server refuses a negative value and
//     stores nothing at all for zero, so it could never read back as declared;
//   - ttl_expire_after must be an interval this owner can read, because the
//     comparison reads it as a value and the server stores its own spelling;
//   - ttl_row_stats_poll_interval must be a duration the server keeps: below
//     one second it stores nothing, and past the largest duration it wraps;
//   - text must be valid UTF-8 without NUL bytes, and a set text parameter
//     must not be blank.
func ValidateDesired(v *DesiredRowTTL) error {
	if v == nil {
		return modelValidation(schemaext.Desired, fmt.Errorf("%w: nil CockroachDB row-level TTL declaration", schemaext.ErrInvalidValue))
	}
	if err := validateText(v.Policy); err != nil {
		return modelValidation(schemaext.Desired, err)
	}
	return modelValidation(schemaext.Desired, validateDeclared(v.Policy))
}

// ValidateObserved checks that an observation is representable and enabled.
// It does not refuse a value the server stored in a spelling this owner cannot
// read; comparison falls back to text for that value. Nil is invalid.
func ValidateObserved(v *ObservedRowTTL) error {
	if v == nil {
		return modelValidation(schemaext.Observed, fmt.Errorf("%w: nil CockroachDB row-level TTL observation", schemaext.ErrInvalidValue))
	}
	if err := validateText(v.Policy); err != nil {
		return modelValidation(schemaext.Observed, err)
	}
	if v.Policy.ExpirationExpression == "" && v.Policy.ExpireAfter == "" {
		return modelValidation(schemaext.Observed, fmt.Errorf("%w: an observed row-level TTL names neither %s nor %s",
			schemaext.ErrInvalidValue, ExpirationExpressionParameter, ExpireAfterParameter))
	}
	return nil
}

func validateText(p Policy) error {
	for _, text := range p.texts() {
		value := *text.value
		if !utf8.ValidString(value) {
			return fmt.Errorf("%w: %s is not valid UTF-8", schemaext.ErrInvalidValue, text.name)
		}
		if strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("%w: %s contains a NUL byte", schemaext.ErrInvalidValue, text.name)
		}
		if value != "" && strings.TrimSpace(value) == "" {
			return fmt.Errorf("%w: %s cannot contain only whitespace; omit it to leave the engine's default",
				schemaext.ErrInvalidValue, text.name)
		}
	}
	return nil
}

func validateDeclared(p Policy) error {
	if p.ExpirationExpression == "" && p.ExpireAfter == "" {
		return fmt.Errorf("%w: row-level TTL settings name neither %s nor %s: CockroachDB refuses every other "+
			"ttl_* parameter when no expiry is configured, answering `\"ttl_expire_after\" and/or "+
			"\"ttl_expiration_expression\" must be set`", schemaext.ErrInvalidValue, ExpirationExpressionParameter, ExpireAfterParameter)
	}
	if p.ExpireAfter != "" {
		if _, err := crdbinterval.Parse(p.ExpireAfter); err != nil {
			return fmt.Errorf("%w: %s: %w", schemaext.ErrInvalidValue, ExpireAfterParameter, err)
		}
	}
	if p.RowStatsPollInterval != "" {
		if _, err := crdbduration.Canonical(p.RowStatsPollInterval); err != nil {
			return fmt.Errorf("%w: %s: %w", schemaext.ErrInvalidValue, RowStatsPollIntervalParameter, err)
		}
	}
	for _, count := range p.counts() {
		if value := *count.value; value != nil && *value < 1 {
			return fmt.Errorf("%w: %s = %d: CockroachDB refuses a negative value (`must be at least 0`) and "+
				"stores nothing at all for zero, so neither can ever read back as declared; omit the "+
				"parameter to leave the engine's default in place", schemaext.ErrInvalidValue, count.name, *value)
		}
	}
	return nil
}

func modelValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: RowTTLKind, Representation: representation, Message: err.Error()}
}
