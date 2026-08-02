package migrator

import (
	"fmt"
	"slices"
	"strings"
)

// ExecOrder controls how the migrator handles unapplied migrations whose
// version is below the current high-water mark.
type ExecOrder string

const (
	// ExecOrderLinear rejects out-of-order pending migrations.
	ExecOrderLinear ExecOrder = "linear"
	// ExecOrderLinearSkip leaves out-of-order pending migrations unapplied.
	ExecOrderLinearSkip ExecOrder = "linear-skip"
	// ExecOrderNonLinear applies every pending migration in version order.
	ExecOrderNonLinear ExecOrder = "non-linear"
)

// ParseExecOrder parses a CLI/API exec-order value.
func ParseExecOrder(value string) (ExecOrder, error) {
	switch ExecOrder(strings.ToLower(strings.TrimSpace(value))) {
	case "", ExecOrderLinear:
		return ExecOrderLinear, nil
	case ExecOrderLinearSkip:
		return ExecOrderLinearSkip, nil
	case ExecOrderNonLinear:
		return ExecOrderNonLinear, nil
	default:
		return "", fmt.Errorf("invalid exec-order %q: expected linear, linear-skip, or non-linear", value)
	}
}

func normalizeExecOrder(value ExecOrder) ExecOrder {
	switch value {
	case "", ExecOrderLinear:
		return ExecOrderLinear
	case ExecOrderLinearSkip, ExecOrderNonLinear:
		return value
	default:
		return ExecOrderLinear
	}
}

// outOfOrderExempt drops versions whose position below the high-water mark is
// known not to mean "authored earlier".
//
// The linear guard reads "version below the current one" as evidence that a
// migration was authored before what is already applied. That inference holds
// only while the version is a chronology. It is not one for a Flyway directory
// read through ?format=: there the int64 is a projection of Atlas CE's sum
// order, in which a surviving baseline is emitted FIRST regardless of its own
// version, so the baseline is deliberately placed below every survivor. Reading
// that placement as out-of-order refuses a directory Atlas CE applies.
//
// The exemption is a list of specific versions supplied by the caller that
// computed the projection, never a rule this package infers. Everything else
// stays guarded, which is what keeps an ordinary migration inserted below the
// high-water mark refused — Atlas CE refuses that too.
func outOfOrderExempt(versions, exempt []int64) []int64 {
	if len(exempt) == 0 {
		return versions
	}
	out := make([]int64, 0, len(versions))
	for _, version := range versions {
		if !slices.Contains(exempt, version) {
			out = append(out, version)
		}
	}
	return out
}

// OutOfOrderError reports pending migrations that are below the current
// high-water mark while linear execution is required.
type OutOfOrderError struct {
	CurrentVersion int64
	Versions       []int64
}

func (e *OutOfOrderError) Error() string {
	return fmt.Sprintf(
		"out-of-order pending migrations below current version %d: %v (use --exec-order=non-linear to apply or --exec-order=linear-skip to ignore)",
		e.CurrentVersion,
		e.Versions,
	)
}

// NewOutOfOrderError builds the typed error returned for linear execution when
// lower-version pending migrations are present.
func NewOutOfOrderError(currentVersion int64, versions []int64) *OutOfOrderError {
	return &OutOfOrderError{
		CurrentVersion: currentVersion,
		Versions:       slices.Clone(versions),
	}
}
