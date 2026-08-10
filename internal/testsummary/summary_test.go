package testsummary_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/testsummary"
)

func TestFormatExcludesSkippedTestsFromSuccessRate(t *testing.T) {
	c := qt.New(t)
	start := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	got := testsummary.Format(start, start.Add(time.Second), 1, 0, 2)
	c.Assert(got, qt.Equals, "Executed 1 tests in 1s. 1 passed, 0 failed, 2 skipped (100.0% success rate)")
}

func TestFormatReportsZeroExecutedTests(t *testing.T) {
	c := qt.New(t)
	start := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	got := testsummary.Format(start, start.Add(500*time.Millisecond), 0, 0, 3)
	c.Assert(got, qt.Equals, "Executed 0 tests in 500ms. 0 passed, 0 failed, 3 skipped (0.0% success rate)")
}
