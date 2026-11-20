package chreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chreverse"
	"ptah.run/dialect/clickhouse/chschema"
)

func storageRequest(before *chschema.ObservedTable, after *chschema.DesiredTable) schemaext.ReversalRequest {
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).Table("events")
	return schemaext.ReversalRequest{Target: "clickhouse", Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &chdiff.Table{Before: before, After: after}}}}
}

// The reverse restores every captured clause in place and names what each one
// cannot recover; a sorting key that gained an expression has nothing to
// recover.
func TestStorageReversalRestoresEveryClause(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a", TTL: "t + toIntervalDay(1)", Settings: "a = 1"}
	after := before.Desired()
	after.OrderBy.Value, after.TTL.Value, after.Settings.Value = "a, k", "", "a = 2"
	request := storageRequest(before, after)
	got, err := (chreverse.Service{}).ReverseChanges(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 1)
	projected, err := after.Observed()
	c.Assert(err, qt.IsNil)
	c.Assert(got[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: request.Changes[0].Subject, Value: &chdiff.Table{Before: projected, After: before.Desired()}})
	c.Assert(got[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.TableKind, Value: projected}})
	c.Assert(got[0].Strategy, qt.Equals, "restore the captured storage clauses in place")
	c.Assert(got[0].Limitations, qt.DeepEquals, []string{
		"Restoring TTL rules cannot recover rows or values already expired or aggregated, or undo data movement caused by TTL processing.",
		"Restoring table settings cannot undo merges, moves, or expirations done under the planned settings.",
	})
}

// An engine that merges rows by its sorting key combines rows that differed
// only in a dropped expression, and restoring the key does not split them.
func TestStorageReversalNamesRowsAMergingEngineCombined(t *testing.T) {
	for _, test := range []struct {
		name, engine string
		want         []string
	}{
		{name: "merging engine", engine: "ReplacingMergeTree(version)", want: []string{"Restoring the sorting key cannot separate rows the engine merged while the key lacked the dropped expressions."}},
		{name: "plain engine", engine: "MergeTree"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := &chschema.ObservedTable{Engine: test.engine, OrderBy: "a, u", PrimaryKey: "a"}
			after := before.Desired()
			after.OrderBy.Value = "a"
			got, err := (chreverse.Service{}).ReverseChanges(t.Context(), storageRequest(before, after))
			c.Assert(err, qt.IsNil)
			c.Assert(got[0].Limitations, qt.DeepEquals, test.want)
		})
	}
}

// A clause ClickHouse has no ALTER TABLE for reverses to a change of the same
// clause back, which creates the table again, so the reversal says so instead
// of refusing: the planner of the reverse direction decides whether it may.
func TestStorageReversalOfAClauseClickHouseCannotChangeCreatesTheTableAgain(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a"}
	after := before.Desired()
	after.PartitionBy.Value = "a"

	got, err := (chreverse.Service{}).ReverseChanges(t.Context(), storageRequest(before, after))

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 1)
	c.Assert(got[0].Strategy, qt.Equals, "restore the captured storage clauses by creating the table again and copying its rows")
	c.Assert(got[0].Limitations, qt.DeepEquals, []string{
		"Restoring the definition creates the table again, which is not atomic: rows written to the table while it runs are lost.",
	})
}

// A change the table cannot take for another reason is still refused.
func TestStorageReversalRefusesAnInvalidChange(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a"}

	got, err := (chreverse.Service{}).ReverseChanges(t.Context(), storageRequest(before, before.Desired()))

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.IsNil)
}
