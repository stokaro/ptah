package embedreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedreport"
)

// TestBoundaryText_AnAbsentWatermarkSaysWhyItIsAbsent is stokaro/ptah#2646
// finding 2.
//
// Every empty watermark rendered as "the selected consistency mode records no
// boundary". That is true of `immutable` and `dual_write` and false of an
// outbox run between `prepare` and its first `catchup` — the state every outbox
// run passes through, including the one the quick start walks a reader through.
// The operator was told something untrue about their own specification, and
// pointed away from the reason the same output stated a few lines down.
func TestBoundaryText_AnAbsentWatermarkSaysWhyItIsAbsent(t *testing.T) {
	tests := []struct {
		name string
		mode embedcatchup.Mode
		want string
	}{
		{
			name: "an outbox before its first catch-up",
			mode: embedcatchup.ModeOutbox,
			want: "none yet, because catch-up has not run",
		},
		{
			name: "a mode that records no boundary",
			mode: embedcatchup.ModeImmutable,
			want: "none, because the selected consistency mode records no boundary",
		},
		{
			name: "the other one",
			mode: embedcatchup.ModeDualWrite,
			want: "none, because the selected consistency mode records no boundary",
		},
		{
			// A caller with no specification claims no reason. Inventing one is
			// the defect rather than a lesser version of it.
			name: "a caller that does not have the mode",
			mode: "",
			want: "none recorded",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embedreport.BoundaryText("", test.mode), qt.Equals, test.want)
		})
	}
}

// TestBoundaryText_APresentWatermarkIsItself is the control.
//
// A renderer that answered a sentence for every input would satisfy every row
// above while losing the number the whole field is for.
func TestBoundaryText_APresentWatermarkIsItself(t *testing.T) {
	tests := []struct {
		name string
		mode embedcatchup.Mode
	}{
		{name: "outbox", mode: embedcatchup.ModeOutbox},
		{name: "immutable", mode: embedcatchup.ModeImmutable},
		{name: "unstated", mode: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(embedreport.BoundaryText("4288", test.mode), qt.Equals, "4288")
		})
	}
}
