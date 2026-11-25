package descriptor

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Rounding states how a plan asks a table whether a column type change that
// keeps fewer digits after the point would change a value the column holds: a
// decimal given a lower scale, a decimal under an integer, or a temporal column
// given a lower fractional-second precision. Most servers convert such a column
// without an error, storing each value rounded or cut to the new digits, so the
// plan reads the rows first and refuses the change while a value would change.
//
// Each template is a condition on one row of the table, true where converting
// the value to the declared type changes it and false or NULL where it does
// not. The condition converts with the server's own CAST, which rounds or cuts
// as the server's ALTER does, so the count holds for any step the new type
// keeps. A CAST that overflows raises the error the ALTER would raise. A
// temporal condition compares the value and its conversion in the declared
// type's own domain ({domain}), so a change that also drops a time zone or an
// offset compares the time of day the new column keeps, not two instants. The
// zero value states no condition: the change is planned without reading the
// rows, as it is for an engine that refuses the conversion itself.
type Rounding struct {
	// Decimal is the condition for a decimal under a decimal or an integer,
	// such as `{column} <> CAST({column} AS {type})`.
	Decimal string `json:"decimal,omitempty"`
	// Temporal is the condition for a temporal type under one of the same
	// kind, such as `CAST({column} AS {domain}) IS DISTINCT FROM CAST({column}
	// AS {type})`.
	Temporal string `json:"temporal,omitempty"`
	// CastTypes maps a type name, in lowercase, to the spelling the server's
	// CAST takes for the type, where it does not take the name a statement
	// declares: MySQL casts to SIGNED rather than to INT. {args} in the
	// spelling stands for what the declared type's parentheses hold, with
	// them; a spelling without it drops them. Whatever follows the name or
	// its parentheses is dropped with a mapped name, and a name not listed is
	// written as declared.
	CastTypes map[string]string `json:"cast_types,omitempty"`
	// Domains maps a declared type name, in lowercase, to the type {domain}
	// stands for: the type a temporal value and its conversion are compared
	// in. A name not listed compares in the declared type with its
	// parentheses taken out, `timestamp` for `timestamp(0)` and `timestamp
	// with time zone` for `timestamp(0) with time zone`. A spelling replaces
	// what follows the name too, so `interval` stands for `interval hour`.
	Domains map[string]string `json:"domains,omitempty"`
}

// RoundingColumn and the placeholders after it are what the Rounding
// templates hold, replaced by a plan that reads the rows.
const (
	// RoundingColumn is the column a Rounding condition reads, quoted.
	RoundingColumn = "{column}"
	// RoundingType is the type the plan converts the column to, as the
	// server's CAST takes it.
	RoundingType = "{type}"
	// RoundingDomain is the type a temporal condition compares the value and
	// its conversion in; see [Rounding.Domains].
	RoundingDomain = "{domain}"
	// RoundingArguments is what a declared type's parentheses hold, with
	// them, in a [Rounding.CastTypes] spelling.
	RoundingArguments = "{args}"
)

func (d Dialect) validateRounding() error {
	rounding := d.Rounding
	conditions := []struct{ kind, condition string }{
		{"a decimal", rounding.Decimal},
		{"a temporal type", rounding.Temporal},
	}
	for _, stated := range conditions {
		if stated.condition == "" {
			continue
		}
		for _, placeholder := range []string{RoundingColumn, RoundingType} {
			if !strings.Contains(stated.condition, placeholder) {
				return fmt.Errorf("%w: dialect %q: the rounding condition %q for %s has no %s",
					ErrInvalidDescriptor, d.Name, stated.condition, stated.kind, placeholder)
			}
		}
	}
	for _, spellings := range []struct {
		what string
		of   map[string]string
	}{{"cast type", rounding.CastTypes}, {"domain", rounding.Domains}} {
		for _, name := range slices.Sorted(maps.Keys(spellings.of)) {
			spelling := spellings.of[name]
			if name == "" || name != strings.ToLower(name) || strings.TrimSpace(spelling) == "" {
				return fmt.Errorf("%w: dialect %q: the rounding %s %q -> %q needs a lowercase name and a spelling",
					ErrInvalidDescriptor, d.Name, spellings.what, name, spelling)
			}
		}
	}
	return nil
}
