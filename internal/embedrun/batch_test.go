package embedrun_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedrun"
)

// inputs builds rows whose text is the given size.
func inputs(sizes ...int) []embedrun.BatchRow {
	rows := make([]embedrun.BatchRow, 0, len(sizes))
	for index, size := range sizes {
		rows = append(rows, embedrun.BatchRow{
			Key:   []string{string(rune('a' + index))},
			Input: strings.Repeat("x", size),
		})
	}
	return rows
}

// shape reports how many rows each batch holds.
func shape(batches []embedrun.Batch) []int {
	counts := make([]int, 0, len(batches))
	for _, batch := range batches {
		counts = append(counts, len(batch.Rows))
	}
	return counts
}

// TestAssemble_RespectsEveryBoundAtOnce is why the bounds are a struct rather
// than a number.
//
// They come from four places -- the provider's inputs, the provider's bytes,
// this process's memory, and how responsive the scan is to cancellation -- and
// a batch sized by one of them alone fails against the others.
func TestAssemble_RespectsEveryBoundAtOnce(t *testing.T) {
	tests := []struct {
		name   string
		rows   []embedrun.BatchRow
		bounds embedrun.BatchBounds
		want   []int
	}{
		{
			name: "the input count", rows: inputs(1, 1, 1, 1, 1),
			bounds: embedrun.BatchBounds{MaxInputs: 2}, want: []int{2, 2, 1},
		},
		{
			name: "the provider's byte bound", rows: inputs(40, 40, 40),
			bounds: embedrun.BatchBounds{MaxInputs: 10, MaxBytes: 100}, want: []int{2, 1},
		},
		{
			name: "this process's memory", rows: inputs(40, 40, 40),
			bounds: embedrun.BatchBounds{MaxInputs: 10, MaxLocalBytes: 50}, want: []int{1, 1, 1},
		},
		{
			name: "the row bound keeps a scan cancellable", rows: inputs(1, 1, 1, 1),
			bounds: embedrun.BatchBounds{MaxInputs: 10, MaxRows: 3}, want: []int{3, 1},
		},
		{
			name: "the smallest bound wins", rows: inputs(10, 10, 10, 10),
			bounds: embedrun.BatchBounds{MaxInputs: 3, MaxRows: 2, MaxBytes: 100}, want: []int{2, 2},
		},
		{
			name: "no batching means one input a request", rows: inputs(1, 1, 1),
			bounds: embedrun.BatchBounds{}, want: []int{1, 1, 1},
		},
		{
			name: "nothing to do", rows: nil,
			bounds: embedrun.BatchBounds{MaxInputs: 10}, want: make([]int, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			batches, err := embedrun.Assemble(test.rows, test.bounds)

			c.Assert(err, qt.IsNil)
			c.Assert(shape(batches), qt.DeepEquals, test.want)
		})
	}
}

// TestAssemble_ASkippedRowTravelsAndCostsNothing keeps two facts together.
//
// It costs no bytes because it is never sent to the provider; it travels
// because verification has to tell a row nobody embedded from a row the
// specification declined, and dropping it here is exactly the silent omission
// the epic forbids.
func TestAssemble_ASkippedRowTravelsAndCostsNothing(t *testing.T) {
	c := qt.New(t)
	rows := inputs(40, 40, 40)
	rows[1].Skipped = true
	rows[1].SkipReason = "the canonical input is empty"

	batches, err := embedrun.Assemble(rows, embedrun.BatchBounds{MaxInputs: 10, MaxBytes: 100})

	c.Assert(err, qt.IsNil)
	// All three fit: the skipped row contributes no bytes to the bound.
	c.Assert(shape(batches), qt.DeepEquals, []int{3})
	c.Assert(batches[0].Bytes, qt.Equals, 80)
	c.Assert(batches[0].Rows[1].Skipped, qt.IsTrue)
	c.Assert(batches[0].Rows[1].SkipReason, qt.Equals, "the canonical input is empty")
}

// TestAssemble_RefusesARowNoBatchCouldHold names the fix rather than failing
// later.
//
// A single input over the byte bound would otherwise sit alone in a batch that
// still exceeds it, and the provider would refuse the request after the scan,
// the canonicalization and the hash -- with its own error, about its own limit,
// naming none of them.
func TestAssemble_RefusesARowNoBatchCouldHold(t *testing.T) {
	tests := []struct {
		name   string
		bounds embedrun.BatchBounds
	}{
		{name: "over the provider's bound", bounds: embedrun.BatchBounds{MaxInputs: 10, MaxBytes: 50}},
		{name: "over this process's bound", bounds: embedrun.BatchBounds{MaxInputs: 10, MaxLocalBytes: 50}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := embedrun.Assemble(inputs(10, 200), test.bounds)

			c.Assert(err, qt.ErrorMatches,
				`.*is 200 bytes and no batch may exceed 50.*truncation policy.*`)
		})
	}
}

// TestAssemble_AnOversizedSkipIsNotRefused is the control for the row above: a
// skipped row is never sent, so its size cannot break a request.
func TestAssemble_AnOversizedSkipIsNotRefused(t *testing.T) {
	c := qt.New(t)
	rows := inputs(10, 200)
	rows[1].Skipped = true

	batches, err := embedrun.Assemble(rows, embedrun.BatchBounds{MaxInputs: 10, MaxBytes: 50})

	c.Assert(err, qt.IsNil)
	c.Assert(shape(batches), qt.DeepEquals, []int{2})
}
