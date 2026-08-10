// Package testsummary formats deterministic summaries for the integration
// runner without exposing its mutable state.
package testsummary

import (
	"fmt"
	"time"
)

// Format summarizes executed, skipped, and failed tests.
func Format(start, end time.Time, passed, failed, skipped int) string {
	executed := passed + failed
	successRate := 0.0
	if executed > 0 {
		successRate = float64(passed) / float64(executed) * 100
	}
	return fmt.Sprintf(
		"Executed %d tests in %v. %d passed, %d failed, %d skipped (%.1f%% success rate)",
		executed,
		end.Sub(start).Round(time.Millisecond),
		passed,
		failed,
		skipped,
		successRate,
	)
}
