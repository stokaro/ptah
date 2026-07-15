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
