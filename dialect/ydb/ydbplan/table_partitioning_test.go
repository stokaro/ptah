package ydbplan_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

func settingsPlanningRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: slices.Concat(ydbschema.TablePartitioningCodecs(), []schemaext.Codec{ydbdiff.TablePartitioningCodec(), ydbast.TablePartitioningCodec()}),
		Planning: []engine.Planning{{
			Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.TablePartitioningKind}, ParentKinds: []schemaext.Kind{ydbschema.TablePartitioningKind},
			OperationKinds: []schemaext.Kind{ydbast.AlterTablePartitioningKind}, Service: ydbplan.TablePartitioningService{},
		}},
	}))
}

// settingsTable is table items under action, declaring declared and read
// holding held, either nil for none.
func settingsTable(action featureplan.ParentAction, declared, held *ydbschema.TablePartitioning) featureplan.Table {
	declaration := schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "I", Name: "items"},
		Fields: []schemamodel.Field{{StructName: "I", Name: "id", Type: "BIGINT", Primary: true}}}
	if declared != nil {
		declaration.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: *declared}))
	}
	observation := schemacapture.TableObservation{Table: catalog.Table{Name: "items"}}
	if held != nil {
		observation.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.ObservedTablePartitioning{TablePartitioning: *held}))
	}
	return featureplan.Table{Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("items"), Action: action,
		Desired: declaration, Current: observation}
}

func settingsRequest(declared, held *ydbschema.TablePartitioning) featureplan.Request {
	table := settingsTable("", declared, held)
	change := &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: *declared}}
	if held != nil {
		change.Before = &ydbschema.ObservedTablePartitioning{TablePartitioning: *held}
	}
	return featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Tables: []featureplan.Table{table}, Changes: []schemaext.ChangeRecord{{Subject: table.Subject, Value: change}}}
}

// TestTablePartitioningPlanFeatures_PlansTheChange pins the receipt: one
// owned operation that writes the change in place, outside a transaction,
// carrying a copy of the operands.
func TestTablePartitioningPlanFeatures_PlansTheChange(t *testing.T) {
	c := qt.New(t)
	request := settingsRequest(&ydbschema.TablePartitioning{MinPartitions: 4}, &ydbschema.TablePartitioning{ByLoad: new(true)})

	result, err := settingsPlanningRuntime().PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
	c.Assert(steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(steps[0].Payload.Role, qt.Equals, ast.AlterExtension)
	op := steps[0].Payload.Payload.(*ydbast.AlterTablePartitioning)
	op.Change.After.MinPartitions = 9
	c.Assert(request.Changes[0].Value.(*ydbdiff.TablePartitioning).After.MinPartitions, qt.Equals, uint64(4))
}

// TestTablePartitioningPlanFeatures_RefusesWithoutOutput refuses a change the
// target cannot make in place, each with no receipt or contribution: a key
// either side needs, a side YDB could not hold, a starting layout the table
// was not created with, and a change of a table the plan does not keep.
func TestTablePartitioningPlanFeatures_RefusesWithoutOutput(t *testing.T) {
	tests := []struct {
		name     string
		declared ydbschema.TablePartitioning
		held     *ydbschema.TablePartitioning
		caps     capability.Capabilities
		action   featureplan.ParentAction
		want     string
	}{
		{name: "a key the declaration needs", declared: ydbschema.TablePartitioning{MinPartitions: 4},
			caps: capability.YDB262().With(capability.PartitioningOptions, false), want: `changing the partitioning of table "items", which requires`},
		{name: "a key the table needs", declared: ydbschema.TablePartitioning{MinPartitions: 4}, held: &ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:1"},
			caps: capability.YDB262().With(capability.ReadReplicas, false), want: `changing the read replicas of table "items", which requires`},
		{name: "a size YDB refuses", declared: ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 64},
			caps: capability.YDB262(), want: `table "items": auto_partitioning_partition_size_mb is set while`},
		{name: "a starting layout", declared: ydbschema.TablePartitioning{UniformPartitions: 4},
			caps: capability.YDB262(), want: `table "items": it declares UNIFORM_PARTITIONS = 4, which YDB takes only when it creates a table`},
		{name: "a table the plan rebuilds", declared: ydbschema.TablePartitioning{MinPartitions: 4},
			caps: capability.YDB262(), action: featureplan.RebuildTable, want: "requires a table that survives the plan in place"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := settingsRequest(&test.declared, test.held)
			request.Capabilities = test.caps
			request.Tables[0].Action = test.action
			request.ParentKinds = []schemaext.Kind{ydbschema.TablePartitioningKind}

			result, err := ydbplan.TablePartitioningService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Feature, qt.Equals, "YDB table partitioning planning")
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestTablePartitioningPlanFeatures_AccountsForEachParentAction pins the
// parent receipts, and a rebuild holding the settings it writes to the
// target.
func TestTablePartitioningPlanFeatures_AccountsForEachParentAction(t *testing.T) {
	tests := []struct {
		action featureplan.ParentAction
		want   string
	}{
		{action: featureplan.CreateTable, want: "write the declared settings into the CREATE TABLE"},
		{action: featureplan.DropTable, want: "remove the settings with the table"},
		{action: featureplan.AlterTable, want: "retain the settings unless a planned change in this plan changes them"},
		{action: featureplan.RebuildTable, want: "write every setting into the rebuilt table: the declared ones, and the held value of every other one"},
	}
	for _, test := range tests {
		t.Run(string(test.action), func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				Tables:      []featureplan.Table{settingsTable(test.action, &ydbschema.TablePartitioning{MinPartitions: 2}, nil)},
				ParentKinds: []schemaext.Kind{ydbschema.TablePartitioningKind}}

			result, err := ydbplan.TablePartitioningService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Parents[0].Strategy, qt.Equals, test.want)
		})
	}
	c := qt.New(t)
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262().With(capability.KeyBloomFilter, false),
		Tables:      []featureplan.Table{settingsTable(featureplan.RebuildTable, &ydbschema.TablePartitioning{MinPartitions: 2}, nil)},
		ParentKinds: []schemaext.Kind{ydbschema.TablePartitioningKind}}
	result, err := ydbplan.TablePartitioningService{}.PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, `rebuilding table "items" with its key bloom filter, which requires .*`)
}

// TestRebuiltTablePartitioning names every setting a rebuilt table is created
// with: each the declaration names, the held value of every other one, and
// the declaration's starting layout.
func TestRebuiltTablePartitioning(t *testing.T) {
	tests := []struct {
		name           string
		declared, held *ydbschema.TablePartitioning
		want           *ydbschema.TablePartitioning
	}{
		{name: "nothing declared over the defaults", want: &ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048,
			ByLoad: new(false), MinPartitions: 1, ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(false)}},
		{name: "a layout over held settings", declared: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}},
			held: &ydbschema.TablePartitioning{BySize: new(false), MaxPartitions: 9, KeyBloomFilter: new(true)},
			want: &ydbschema.TablePartitioning{BySize: new(false), ByLoad: new(false), MinPartitions: 2, MaxPartitions: 9,
				ReadReplicas: "PER_AZ:0", KeyBloomFilter: new(true), PartitionAtKeys: [][]string{{"10"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := settingsTable(featureplan.RebuildTable, test.declared, test.held)

			got, err := ydbplan.RebuiltTablePartitioning(table.Desired, table.Current)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestTablePartitioningRebuildReason names a starting layout the table was not
// created with, and nothing for a change made in place, a side YDB could not
// hold, or no change.
func TestTablePartitioningRebuildReason(t *testing.T) {
	change := func(declared ydbschema.TablePartitioning) *ydbdiff.TablePartitioning {
		return &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: declared}}
	}
	c := qt.New(t)
	c.Assert(ydbplan.TablePartitioningRebuildReason(change(ydbschema.TablePartitioning{UniformPartitions: 4})), qt.Matches, `it declares UNIFORM_PARTITIONS = 4, .*`)
	c.Assert(ydbplan.TablePartitioningRebuildReason(change(ydbschema.TablePartitioning{UniformPartitions: 4, MinPartitions: 2})), qt.Equals, "")
	c.Assert(ydbplan.TablePartitioningRebuildReason(change(ydbschema.TablePartitioning{BySize: new(false), PartitionSizeMB: 4, UniformPartitions: 4})), qt.Equals, "")
	c.Assert(ydbplan.TablePartitioningRebuildReason(nil), qt.Equals, "")
}
