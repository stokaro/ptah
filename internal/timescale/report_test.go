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
// One object earns a note: a hypertable IS in the document and is incomplete
// when it has a dimension no declaration can carry. A continuous aggregate has
// a declaration of its own and earns none, and the row asserting its silence is
// what would catch the note coming back after the block was added
// (stokaro/ptah#1026).
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
			// A declaration carries this one, so the description is complete
			// and there is nothing to report.
			name: "one hypertable on one dimension",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time", Dimensions: 1}},
			},
			wantLines: 0,
		},
		{
			name: "one hypertable on two dimensions",
			schema: &types.DBSchema{
				Tables:      []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{{Name: "conditions", PrimaryDimension: "time", Dimensions: 2}},
			},
			wantLines: 1,
			want: []string{
				"1 hypertable is described with the first partitioning dimension only",
				"conditions (on time and 1 more dimension)",
			},
		},
		{
			name: "two hypertables are named in order",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "metrics"}, {Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "metrics", PrimaryDimension: "ts", Dimensions: 2},
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 2},
				},
			},
			wantLines: 1,
			want: []string{
				"2 hypertables are",
				"conditions (on time and 1 more dimension), metrics (on ts and 1 more dimension)",
			},
		},
		{
			name: "a hypertable the selection removed is not named",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "users"}},
				Hypertables: []types.DBHypertable{
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 2},
				},
			},
			wantLines: 0,
		},
		{
			name: "a hypertable in a named schema",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Schema: "app", Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Schema: "app", Name: "conditions", PrimaryDimension: "time", Dimensions: 2},
				},
			},
			wantLines: 1,
			want:      []string{"app.conditions (on time and 1 more dimension)"},
		},
		{
			// The aggregate is described now, by a block of its own, so there
			// is nothing to warn about. A note here would send an operator
			// looking for what is missing from a document that has it.
			name: "one continuous aggregate",
			schema: &types.DBSchema{
				ContinuousAggregates: []types.DBContinuousAggregate{{Name: "conditions_hourly"}},
			},
			wantLines: 0,
		},
		{
			name: "an aggregate over an incompletely described hypertable",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 2},
				},
				ContinuousAggregates: []types.DBContinuousAggregate{{Name: "conditions_hourly"}},
			},
			wantLines: 1,
			want:      []string{"1 hypertable is"},
			notWant:   []string{"conditions_hourly"},
		},
		{
			// A hypertable partitioned on two columns loses BOTH on replay, and
			// a note naming one would say less than the truth about what the
			// description drops -- the same failure this note exists to
			// prevent, one level down.
			name: "a hypertable with a second dimension",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 2},
				},
			},
			wantLines: 1,
			want:      []string{"conditions (on time and 1 more dimension)"},
		},
		{
			name: "a hypertable with three dimensions",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 3},
				},
			},
			wantLines: 1,
			want:      []string{"conditions (on time and 2 more dimensions)"},
		},
		{
			// The control on the threshold: one dimension is a complete
			// description, so the note that names an incomplete one must not
			// fire for it.
			name: "the ordinary single dimension is not named",
			schema: &types.DBSchema{
				Tables: []types.DBTable{{Name: "conditions"}},
				Hypertables: []types.DBHypertable{
					{Name: "conditions", PrimaryDimension: "time", Dimensions: 1},
				},
			},
			wantLines: 0,
			notWant:   []string{"conditions"},
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
