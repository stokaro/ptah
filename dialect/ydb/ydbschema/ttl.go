package ydbschema

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbttl"
)

// TTLKind identifies the TTL of a YDB table: the setting that makes YDB delete
// a row once an interval has passed since the time one of its columns holds,
// `WITH (TTL = Interval("P30D") ON created_at)`.
//
// A tier that moves rows to an external data source belongs to a column
// table's own declaration, not to this value.
const TTLKind schemaext.Kind = "ptah.run/ydb/ttl"

// TTL is one TTL setting: the column, the interval as an ISO 8601 duration of
// weeks, days, hours, minutes and seconds, and, for an integer column, the
// unit it counts since the Unix epoch. Unit is empty for a date column and is
// spelled as YQL writes it after AS, such as SECONDS.
type TTL struct {
	Column   string
	Interval string
	Unit     string
}

// DesiredTTL is the TTL a declaration asks for. Removing the TTL is the
// absence of this value under complete source coverage.
type DesiredTTL struct {
	Policy TTL
}

// ObservedTTL is the TTL a table read found, its interval written as YDB shows
// it. RunIntervalSeconds is how often YDB runs the deletion when the SDK or the
// CLI set it, and zero for the server's default; YQL cannot write it, and
// `SET (TTL = ...)` resets it, measured on 25.1.4.7 and 26.2.1.14.
type ObservedTTL struct {
	Policy             TTL
	RunIntervalSeconds uint64
}

// Kind returns the owned TTL identity.
func (*DesiredTTL) Kind() schemaext.Kind { return TTLKind }

// Kind returns the owned TTL identity.
func (*ObservedTTL) Kind() schemaext.Kind { return TTLKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredTTL) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredTTL)(nil)
	}
	return &DesiredTTL{Policy: v.Policy}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedTTL) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedTTL)(nil)
	}
	return &ObservedTTL{Policy: v.Policy, RunIntervalSeconds: v.RunIntervalSeconds}
}

// Equal compares declarations structurally, without reading intervals.
func (v *DesiredTTL) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredTTL)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Policy == w.Policy
}

// Equal compares observations structurally, without reading intervals.
func (v *ObservedTTL) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedTTL)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures the observed TTL as an exact declaration. The run interval
// is not part of a declaration. A nil receiver remains nil.
func (v *ObservedTTL) Desired() *DesiredTTL {
	if v == nil {
		return nil
	}
	return &DesiredTTL{Policy: v.Policy}
}

// Observed projects a declaration as the TTL a CREATE or SET would leave: the
// interval as YDB shows the seconds it keeps of it, and no run interval. Nil
// and invalid declarations are refused with schemaext.ErrInvalidValue.
func (v *DesiredTTL) Observed() (*ObservedTTL, error) {
	if err := ValidateDesiredTTL(v); err != nil {
		return nil, err
	}
	seconds, err := ydbttl.IntervalSeconds(v.Policy.Interval)
	if err != nil {
		return nil, ttlValidation(schemaext.Desired, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err))
	}
	policy := v.Policy
	policy.Interval = ydbttl.FormatInterval(seconds)
	return &ObservedTTL{Policy: policy}, nil
}

// EquivalentTTL reports whether two TTL settings delete the same rows on the
// same schedule: the same column under columnKey, the target's identifier
// rule, the same unit, and an interval of the same whole seconds, since YDB
// keeps nothing else (`PT720H` reads back as `P30D`).
func EquivalentTTL(a, b TTL, columnKey func(string) string) bool {
	if columnKey(a.Column) != columnKey(b.Column) || a.Unit != b.Unit {
		return false
	}
	left, leftErr := ydbttl.IntervalSeconds(a.Interval)
	right, rightErr := ydbttl.IntervalSeconds(b.Interval)
	return leftErr == nil && rightErr == nil && left == right
}

// TTLColumnRefusal says why the table's columns cannot carry policy, or
// returns the empty string when they can: the column is not declared, or its
// YDB type is one YDB reads no TTL from with that unit.
func TTLColumnRefusal(policy TTL, columnTypes map[string]string) string {
	ydbType, declared := columnTypes[policy.Column]
	if !declared {
		return fmt.Sprintf("it reads column %q, which the table does not declare (`Cannot enable TTL on unknown column`)", policy.Column)
	}
	return ydbttl.ColumnRefusal(policy.Column, ydbType, policy.Unit)
}

// ValidateDesiredTTL refuses a declaration YDB would refuse or keep as
// something else: a missing column, an interval ydbttl.IntervalSeconds refuses,
// and a unit that is not one of SECONDS, MILLISECONDS, MICROSECONDS and
// NANOSECONDS in capitals. Nil is invalid. Errors are
// schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
func ValidateDesiredTTL(v *DesiredTTL) error {
	if v == nil {
		return ttlValidation(schemaext.Desired, fmt.Errorf("%w: nil YDB TTL declaration", schemaext.ErrInvalidValue))
	}
	return ttlValidation(schemaext.Desired, validateTTL(v.Policy))
}

// ValidateObservedTTL checks an observation as ValidateDesiredTTL checks a
// declaration. Nil is invalid.
func ValidateObservedTTL(v *ObservedTTL) error {
	if v == nil {
		return ttlValidation(schemaext.Observed, fmt.Errorf("%w: nil YDB TTL observation", schemaext.ErrInvalidValue))
	}
	return ttlValidation(schemaext.Observed, validateTTL(v.Policy))
}

func validateTTL(p TTL) error {
	switch {
	case strings.TrimSpace(p.Column) == "":
		return fmt.Errorf("%w: a TTL needs the column its interval is measured from", schemaext.ErrInvalidValue)
	case !utf8.ValidString(p.Column) || strings.ContainsRune(p.Column, '\x00'):
		return fmt.Errorf("%w: the TTL column is not valid UTF-8 without NUL", schemaext.ErrInvalidValue)
	}
	if _, err := ydbttl.IntervalSeconds(p.Interval); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	unit, err := ydbttl.Unit(p.Unit)
	if err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	if unit != p.Unit {
		return fmt.Errorf("%w: the TTL unit %q is written %q", schemaext.ErrInvalidValue, p.Unit, unit)
	}
	return nil
}

func ttlValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: TTLKind, Representation: representation, Message: err.Error()}
}
