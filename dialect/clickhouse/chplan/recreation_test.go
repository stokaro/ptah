package chplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
)

func recreationChange(edit func(*chschema.DesiredTable)) *chdiff.Table {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a, u", PrimaryKey: "a", Settings: "index_granularity = 8192"}
	after := before.Desired()
	edit(after)
	return &chdiff.Table{Before: before, After: after}
}

func TestRecreationReason_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		edit func(*chschema.DesiredTable)
		want string
	}{
		{"engine", func(v *chschema.DesiredTable) { v.Engine.Value = "ReplacingMergeTree" }, "ClickHouse has no ALTER TABLE that changes a table's ENGINE"},
		{"partition", func(v *chschema.DesiredTable) { v.PartitionBy.Value = "b" }, "ClickHouse has no ALTER TABLE that changes a table's PARTITION BY"},
		{"primary key", func(v *chschema.DesiredTable) { v.PrimaryKey.Value = "a, u" }, "ClickHouse has no ALTER TABLE that changes a table's PRIMARY KEY"},
		{"reordered key", func(v *chschema.DesiredTable) { v.OrderBy.Value = "u, a" }, "MODIFY ORDER BY can only drop the last expressions of a sorting key and add expressions over columns the same ALTER TABLE adds, and u is already in the key a, u"},
		{"fixed setting", func(v *chschema.DesiredTable) { v.Settings.Value = "index_granularity = 4096" }, "ClickHouse fixes table setting index_granularity when the table is created"},
		{"existing column joins the key", func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, b" }, `column "b" is already in the table, and ClickHouse adds to the sorting key only expressions over columns the same ALTER TABLE adds`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, required := chplan.RecreationReason(recreationChange(test.edit), []string{"a", "b", "u"}, []string{"a", "b", "u"})

			c.Assert(required, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestRecreationReason_InPlaceChange(t *testing.T) {
	tests := []struct {
		name     string
		edit     func(*chschema.DesiredTable)
		existing []string
	}{
		{"TTL", func(v *chschema.DesiredTable) { v.TTL.Value = "toDateTime(a) + INTERVAL 1 DAY" }, []string{"a", "u"}},
		{"a key expression over a column the same ALTER TABLE adds", func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" }, []string{"a", "u"}},
		{"a trailing key expression dropped", func(v *chschema.DesiredTable) { v.OrderBy.Value = "a" }, []string{"a", "u"}},
		{"a sampling key outside the primary key", func(v *chschema.DesiredTable) { v.SampleBy.Value = "u" }, []string{"a", "u"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, required := chplan.RecreationReason(recreationChange(test.edit), test.existing, []string{"a", "u", "k"})

			c.Assert(required, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestRecreationReason_InvalidChange(t *testing.T) {
	c := qt.New(t)

	got, required := chplan.RecreationReason(&chdiff.Table{}, nil, nil)

	c.Assert(required, qt.IsFalse)
	c.Assert(got, qt.Equals, "")
}

// The storage owner accounts for a table created again, from the declaration it
// is created from, and says what the plan costs.
func TestStorageParentAccountsForARebuild(t *testing.T) {
	c := qt.New(t)
	request := requestFixture()
	request.Changes = nil
	request.Tables[0].Action = featureplan.RebuildTable

	result, err := (chplan.Service{}).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Parents, qt.HasLen, 1)
	c.Assert(result.Parents[0].Action, qt.Equals, featureplan.RebuildTable)
	c.Assert(result.Parents[0].Strategy, qt.Contains, "the steps are not atomic, and rows written to the table while they run are lost")
}
