// Package spannerschema owns Spanner's row deletion policy and its desired and
// observed representations. It contains no provider selection, database
// access, or DDL.
//
// Spanner deletes a row once an interval has passed since a timestamp column,
// declared as a table clause: `TTL INTERVAL '30 days' ON created_at` through
// PGAdapter. Measured against the Cloud Spanner emulator behind PGAdapter:
//
//   - The server rewrites the interval into a mixed-radix rendering of the same
//     number of days: `30 days` reads back as `4 WEEKS 2 DAYS`, `60 days` as
//     `2 MONTHS`. The comparison reads both spellings as the hours they denote,
//     at 30 days to a month, 7 to a week and 24 hours to a day, which is the
//     server's own arithmetic.
//   - The interval must be a whole number of days.
//   - The column is a timestamp column. The clause has no spelling for an
//     integer column counting a unit since the Unix epoch.
//
// A table without a policy has no value of this kind. Whether that absence is
// known is source coverage, never a zero value.
package spannerschema

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
	"ptah.run/internal/spannerttl"
)

// RowDeletionKind identifies the row deletion policy attached to a common
// table.
const RowDeletionKind schemaext.Kind = "ptah.run/spanner/row-deletion-policy"

// Policy is one row deletion policy: the column a row's age is measured from
// and the interval after which the row is deleted, in Spanner's spelling.
type Policy struct {
	// Column is the timestamp column the interval is measured from.
	Column string
	// Interval is the interval literal without quotes, such as `30 days`.
	Interval string
}

// DesiredRowDeletion is the policy a declaration asks for. Removing the policy
// is the absence of this value under complete source coverage.
type DesiredRowDeletion struct {
	Policy Policy
}

// ObservedRowDeletion is the policy a table read found, in the spelling the
// server stores. Its absence on a table the read covered is an observed
// absence.
type ObservedRowDeletion struct {
	Policy Policy
}

// Kind returns the owned row deletion identity.
func (*DesiredRowDeletion) Kind() schemaext.Kind { return RowDeletionKind }

// Kind returns the owned row deletion identity.
func (*ObservedRowDeletion) Kind() schemaext.Kind { return RowDeletionKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRowDeletion) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredRowDeletion)(nil)
	}
	return &DesiredRowDeletion{Policy: v.Policy}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRowDeletion) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedRowDeletion)(nil)
	}
	return &ObservedRowDeletion{Policy: v.Policy}
}

// Equal compares declarations structurally, without reading intervals.
func (v *DesiredRowDeletion) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredRowDeletion)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Policy == w.Policy
}

// Equal compares observations structurally, without reading intervals.
func (v *ObservedRowDeletion) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedRowDeletion)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Policy == w.Policy
}

// Desired captures the observed policy as an exact declaration, keeping the
// stored spelling of the interval. A nil receiver remains nil.
func (v *ObservedRowDeletion) Desired() *DesiredRowDeletion {
	if v == nil {
		return nil
	}
	return &DesiredRowDeletion{Policy: v.Policy}
}

// Observed projects a declaration as the policy a CREATE or ADD would leave,
// keeping the declared spelling; comparison reads both spellings as values.
// Nil and invalid declarations are refused with schemaext.ErrInvalidValue.
func (v *DesiredRowDeletion) Observed() (*ObservedRowDeletion, error) {
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	return &ObservedRowDeletion{Policy: v.Policy}, nil
}

// Equivalent reports whether an observed policy is the declared one: the same
// column under columnKey, the target's identifier rule, and the same number of
// hours.
func Equivalent(desired, observed Policy, columnKey func(string) string) bool {
	return columnKey(desired.Column) == columnKey(observed.Column) && spannerttl.EqualIntervals(desired.Interval, observed.Interval)
}

// ValidateDesired refuses a declaration the server would refuse: a missing or
// blank column, and an interval that is negative, is not a whole number of
// days, or is in a spelling this owner does not read. Nil is invalid. Errors are
// schemaext.InvalidModelError values wrapping schemaext.ErrInvalidValue.
func ValidateDesired(v *DesiredRowDeletion) error {
	if v == nil {
		return modelValidation(schemaext.Desired, fmt.Errorf("%w: nil Spanner row deletion policy declaration", schemaext.ErrInvalidValue))
	}
	if err := validateText(v.Policy); err != nil {
		return modelValidation(schemaext.Desired, err)
	}
	if err := spannerttl.ValidateInterval(v.Policy.Interval); err != nil {
		return modelValidation(schemaext.Desired, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err))
	}
	return nil
}

// ValidateObserved checks that an observation names a column and an interval.
// It does not refuse an interval in a spelling this owner cannot read;
// comparison falls back to text for it. Nil is invalid.
func ValidateObserved(v *ObservedRowDeletion) error {
	if v == nil {
		return modelValidation(schemaext.Observed, fmt.Errorf("%w: nil Spanner row deletion policy observation", schemaext.ErrInvalidValue))
	}
	return modelValidation(schemaext.Observed, validateText(v.Policy))
}

func validateText(p Policy) error {
	for _, field := range []struct{ name, value string }{{"column", p.Column}, {"interval", p.Interval}} {
		switch {
		case strings.TrimSpace(field.value) == "":
			return fmt.Errorf("%w: a row deletion policy needs its %s", schemaext.ErrInvalidValue, field.name)
		case !utf8.ValidString(field.value):
			return fmt.Errorf("%w: the %s is not valid UTF-8", schemaext.ErrInvalidValue, field.name)
		case strings.ContainsRune(field.value, '\x00'):
			return fmt.Errorf("%w: the %s contains a NUL byte", schemaext.ErrInvalidValue, field.name)
		case strings.ContainsRune(field.value, '\''):
			return fmt.Errorf("%w: the %s contains a quote, which the clause cannot carry", schemaext.ErrInvalidValue, field.name)
		}
	}
	return nil
}

func modelValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: RowDeletionKind, Representation: representation, Message: err.Error()}
}
