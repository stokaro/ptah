package datadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/datadiff"
)

// TestCompute_ADeclaredNumberMeetsTheTextADriverReturned pairs a declared
// number or boolean with the text a numeric column hands back.
//
// Oracle's driver scans NUMBER(10) as "30" and NUMBER(1) as "1". Compared by
// type, a declared 30 or true never pairs with its own column, and every
// reconciliation plans the same UPDATE again.
func TestCompute_ADeclaredNumberMeetsTheTextADriverReturned(t *testing.T) {
	tests := []struct {
		name    string
		desired any
		live    any
	}{
		{name: "true meets the 1 a NUMBER(1) column holds", desired: true, live: "1"},
		{name: "false meets 0", desired: false, live: "0"},
		{name: "an integer meets its decimal text", desired: int64(30), live: "30"},
		{name: "a negative integer meets its text", desired: int64(-7), live: "-7"},
		{name: "a float meets its shortest text", desired: 12.5, live: "12.5"},
		{name: "text a driver returned as bytes", desired: int64(30), live: []byte("30")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := datadiff.Compute("", "flags", []string{"code"},
				[]datadiff.Row{{"code": "one", "v": tt.desired}},
				[]datadiff.Row{{"code": "one", "v": tt.live}},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.Updates, qt.HasLen, 0)
		})
	}
}

// TestCompute_ADeclaredNumberDiffersFromOtherText is the control for the test
// above: the pair is equal only when the text is exactly what the renderer
// writes for the value, so a comparison that pairs every number with every
// text still reports these.
func TestCompute_ADeclaredNumberDiffersFromOtherText(t *testing.T) {
	tests := []struct {
		name    string
		desired any
		live    any
	}{
		{name: "true against 0", desired: true, live: "0"},
		{name: "true against the word true", desired: true, live: "true"},
		{name: "an integer against zero-padded text", desired: int64(30), live: "030"},
		{name: "an integer against another integer's text", desired: int64(30), live: "31"},
		{name: "a float against a longer spelling", desired: float64(1), live: "1.0"},
		{name: "an integer against a NULL", desired: int64(30), live: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := datadiff.Compute("", "flags", []string{"code"},
				[]datadiff.Row{{"code": "one", "v": tt.desired}},
				[]datadiff.Row{{"code": "one", "v": tt.live}},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.Updates, qt.HasLen, 1)
		})
	}
}
