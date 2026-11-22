package rollbacklimit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/rollbacklimit"
)

func TestTarget(t *testing.T) {
	tests := []struct {
		name    string
		applied []int64
		limit   uint64
		want    int64
	}{
		{name: "the last one", applied: []int64{1, 2, 3}, limit: 1, want: 2},
		{name: "the last two", applied: []int64{1, 2, 3}, limit: 2, want: 1},
		{name: "the whole history", applied: []int64{1, 2, 3}, limit: 3, want: 0},
		{name: "more than the history", applied: []int64{1, 2, 3}, limit: 9, want: 0},
		{name: "a zero limit selects nothing", applied: []int64{1, 2, 3}, limit: 0, want: 3},
		{name: "nothing applied", limit: 1, want: 0},
		{name: "versions out of order", applied: []int64{30, 10, 20}, limit: 1, want: 20},
		{name: "versions with gaps", applied: []int64{20240101000001, 20240301000000}, limit: 1, want: 20240101000001},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rollbacklimit.Target(test.applied, test.limit), qt.Equals, test.want)
		})
	}
}

// TestTarget_LeavesTheHistoryAlone passes an unsorted history and reads it
// back unchanged.
func TestTarget_LeavesTheHistoryAlone(t *testing.T) {
	c := qt.New(t)
	applied := []int64{3, 1, 2}

	rollbacklimit.Target(applied, 1)

	c.Assert(applied, qt.DeepEquals, []int64{3, 1, 2})
}
