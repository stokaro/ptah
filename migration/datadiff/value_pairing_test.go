package datadiff_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/datadiff"
)

// A declaration carries text and a driver decides for itself what a timestamp
// column scans into. These drive Compute, which is where the two meet: a row
// that agrees plans nothing, and one that does not plans an update.

func timestampDiff(c *qt.C, desired, live any) *datadiff.DataDiff {
	c.Helper()
	diff, err := datadiff.Compute("", "windows", []string{"code"},
		[]datadiff.Row{{"code": "one", "seen": desired}},
		[]datadiff.Row{{"code": "one", "seen": live}},
	)
	c.Assert(err, qt.IsNil)
	return diff
}

func TestCompute_TimeAndTextPairing_HappyPath(t *testing.T) {
	moment := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("RFC 3339 text and the moment it names are one value", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "2026-01-02T03:04:05Z", moment).Updates, qt.HasLen, 0)
	})

	t.Run("a space-separated spelling pairs too", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "2026-01-02 03:04:05", moment).Updates, qt.HasLen, 0)
	})

	t.Run("a date pairs with midnight", func(t *testing.T) {
		c := qt.New(t)
		midnight := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
		c.Assert(timestampDiff(c, "2026-01-02", midnight).Updates, qt.HasLen, 0)
	})

	t.Run("the same instant in another zone pairs", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "2026-01-02T04:04:05+01:00", moment).Updates, qt.HasLen, 0)
	})
}

func TestCompute_TimeAndTextPairing_FailurePath(t *testing.T) {
	moment := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("another moment is an update", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "2026-03-04T05:06:07Z", moment).Updates, qt.HasLen, 1)
	})

	t.Run("text that names no moment is an update", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "whenever", moment).Updates, qt.HasLen, 1)
	})

	t.Run("two texts stay two texts", func(t *testing.T) {
		// Both sides are text, so the column is text: two spellings of one
		// instant are two values there, and folding them would report a row
		// the author never wrote as converged.
		c := qt.New(t)
		diff := timestampDiff(c, "2026-01-02T03:04:05Z", "2026-01-02T04:04:05+01:00")
		c.Assert(diff.Updates, qt.HasLen, 1)
	})

	t.Run("a NULL live value is an update", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(timestampDiff(c, "2026-01-02T03:04:05Z", nil).Updates, qt.HasLen, 1)
	})
}
