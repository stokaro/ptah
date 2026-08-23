package timescale_test

import (
	"bytes"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/timescale"
)

// TestReportUndescribed_NamesWhatTheDescriptionLeavesOut pins both notes and
// the cases that must produce none.
//
// The two omissions are different facts and get different sentences: a
// hypertable IS in the document and is incomplete, a continuous aggregate is
// not in the document at all. A note that merged them would be wrong about one
// of them whichever way it was written (stokaro/ptah#1026).
func TestReportUndescribed_NamesWhatTheDescriptionLeavesOut(t *testing.T) {
	tests := []struct {
		name   string
		schema *types.DBSchema
		// wantLines is how many notes the row expects, asserted before the
		// substrings: qt.Contains with an empty string matches anything, so a
		// row expecting silence needs a count rather than an absent substring.
		wantLines int
		want      []string
		notWant   []string
	}{
		{
			name:      "an ordinary PostgreSQL description",
			schema:    &types.DBSchema{Tables: []types.DBTable{{Name: "users"}}},
			wantLines: 0,
		},
		{
			name: "one hypertable",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time", Dimensions: 1}},
			},
			wantLines: 1,
			want: []string{
				"1 hypertable is described as ordinary tables",
				"conditions (on time)",
			},
		},
		{
			name: "two hypertables are named in order",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "metrics"}, {Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "metrics", PrimaryDimension: "ts"},
					{Name: "conditions", PrimaryDimension: "time"},
				},
			},
			wantLines: 1,
			want:      []string{"2 hypertables are", "conditions (on time), metrics (on ts)"},
		},
		{
			name: "a hypertable the selection removed is not named",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Name: "users"}},
				Hypertables: []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time"}},
			},
			wantLines: 0,
		},
		{
			name: "a hypertable in a named schema",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Schema: "app", Name: "conditions"}},
				Hypertables: []types.DBHypertable{{Schema: "app", Name: "conditions", PrimaryDimension: "time"}},
			},
			wantLines: 1,
			want:      []string{"app.conditions (on time)"},
		},
		{
			name: "one continuous aggregate",
			schema: &types.DBSchema{
				ContinuousAggregates: []types.DBContinuousAggregate{{Name: "conditions_hourly"}},
			},
			wantLines: 1,
			want: []string{
				"1 continuous aggregate is not in this description at all",
				"conditions_hourly",
			},
		},
		{
			name: "both, as two notes",
			schema: &types.DBSchema{
				Tables:               []types.DBTable{{Name: "conditions"}},
				Hypertables:          []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time"}},
				ContinuousAggregates: []types.DBContinuousAggregate{{Name: "conditions_hourly"}},
			},
			wantLines: 2,
			want:      []string{"1 hypertable is", "1 continuous aggregate is not"},
		},
		{
			name: "a hypertable whose dimension the catalog did not report",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{{Name: "conditions"}},
			},
			wantLines: 1,
			want:      []string{"conditions."},
			notWant:   []string{"(on )"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			timescale.ReportUndescribed(&out, test.schema)

			c.Assert(noteLines(out.String()), qt.HasLen, test.wantLines)
			for _, want := range test.want {
				c.Assert(out.String(), qt.Contains, want)
			}
			for _, notWant := range test.notWant {
				c.Assert(out.String(), qt.Not(qt.Contains), notWant)
			}
		})
	}
}

// TestReportUndescribed_DropsTheNoteWithNowhereToWriteIt pins the two shapes the
// inspect surfaces pass when they have no diagnostics stream. A diagnostic that
// panics fails a read that succeeded.
func TestReportUndescribed_DropsTheNoteWithNowhereToWriteIt(t *testing.T) {
	c := qt.New(t)
	schema := &types.DBSchema{
		Tables:      []types.DBTable{{Name: "conditions"}},
		Hypertables: []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time"}},
	}

	timescale.ReportUndescribed(nil, schema)
	var out bytes.Buffer
	timescale.ReportUndescribed(&out, nil)

	c.Assert(out.String(), qt.Equals, "")
}

// noteLines counts the notes written, so a row can assert silence by a count
// rather than by the absence of a substring -- qt.Contains with an empty string
// matches anything, which is how a row expecting nothing passes for free.
func noteLines(output string) []string {
	trimmed := strings.TrimSuffix(output, "\n")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}
