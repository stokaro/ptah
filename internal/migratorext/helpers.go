package migratorext

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// WidenVersion runs widen unless dataType, the version column's type as the
// catalog spells it, is already bigint.
func WidenVersion(ctx context.Context, q Queryer, dataType, widen string) error {
	if strings.ToLower(dataType) == "bigint" {
		return nil
	}
	if _, err := q.ExecContext(ctx, widen); err != nil {
		return fmt.Errorf("failed to widen version column from %s to BIGINT: %w", strings.ToLower(dataType), err)
	}
	return nil
}

// MillisLiteral spells duration as whole milliseconds with an ms suffix,
// rounded up, so a timeout is never shorter than asked.
func MillisLiteral(duration time.Duration) string {
	return strconv.FormatInt(Millis(duration), 10) + "ms"
}

// Millis is duration in whole milliseconds, rounded up.
func Millis(duration time.Duration) int64 {
	return ceilUnits(duration, time.Millisecond)
}

// Seconds is duration in whole seconds, rounded up.
func Seconds(duration time.Duration) int64 {
	return ceilUnits(duration, time.Second)
}

func ceilUnits(duration, unit time.Duration) int64 {
	return int64((duration-1)/unit) + 1
}

// Reversed returns a reversed copy of values: restore statements run in the
// order opposite to the setup they undo.
func Reversed(values []string) []string {
	reversed := slices.Clone(values)
	slices.Reverse(reversed)
	return reversed
}
