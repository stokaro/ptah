package chplan_test

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
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

const commonOwner = "example.org/common"

var builder = objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))

// storageFixture is a table with columns a, b and u, sorted by (a, u) with the
// primary key a, whose declaration changes the storage to after.
type storageFixture struct {
	after   func(*chschema.DesiredTable)
	columns []*ast.ColumnNode
	dropped []string
}

func (f storageFixture) request() featureplan.Request {
	subject := builder.Table("events")
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a, u", PrimaryKey: "a", Settings: "index_granularity = 8192"}
	after := before.Desired()
	f.after(after)
	runtime := must.Must(builtin.New())
	models := runtime.Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	coverage := must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})}}, nil))
	table := featureplan.Table{Subject: subject, Action: featureplan.AlterTable}
	table.Current.Table = catalog.Table{Name: "events", Facets: must.Must(schemaext.NewFacets(before)), Columns: []catalog.Column{
		{Name: "a", DataType: "Int32"}, {Name: "b", DataType: "Int32"}, {Name: "u", DataType: "UInt64"},
	}}
	table.Current.FeatureCoverage = coverage
	table.Desired.Table = schemamodel.Table{Name: "events", StructName: "Event", Facets: must.Must(schemaext.NewFacets(after))}
	for _, name := range slices.DeleteFunc([]string{"a", "b", "u"}, func(name string) bool { return slices.Contains(f.dropped, name) }) {
		table.Desired.Fields = append(table.Desired.Fields, schemamodel.Field{StructName: "Event", Name: name, Type: "Int32"})
	}
	request := featureplan.Request{
		Target: "clickhouse", Identifiers: identifier.ForDialect("clickhouse"), Tables: []featureplan.Table{table}, ParentKinds: []schemaext.Kind{chschema.TableKind},
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &chdiff.Table{Before: before, After: after}}},
	}
	for _, column := range f.columns {
		request.Tables[0].Desired.Fields = append(request.Tables[0].Desired.Fields, schemamodel.Field{StructName: "Event", Name: column.Name, Type: column.Type})
		request.CommonSteps = append(request.CommonSteps, featureplan.CommonStep{
			ID: plangraph.StepID{Owner: commonOwner, Name: "add-" + column.Name}, Parent: subject, AddedColumn: column,
			Effects: []plangraph.Effect{{Subject: subject, Action: plangraph.Read}, {Subject: builder.Column("events", column.Name), Action: plangraph.Create}},
		})
	}
	for _, name := range f.dropped {
		request.CommonSteps = append(request.CommonSteps, featureplan.CommonStep{
			ID: plangraph.StepID{Owner: commonOwner, Name: "drop-" + name}, Parent: subject,
			Effects: []plangraph.Effect{{Subject: subject, Action: plangraph.Read}, {Subject: builder.Column("events", name), Action: plangraph.Drop}},
		})
	}
	return request
}

// commonContribution is the host's side of the request, its steps in order.
func commonContribution(request featureplan.Request) plangraph.Contribution[featureplan.Operation] {
	common := plangraph.Contribution[featureplan.Operation]{Owner: commonOwner}
	for _, step := range request.CommonSteps {
		common.Steps = append(common.Steps, plangraph.Step[featureplan.Operation]{ID: step.ID, Effects: step.Effects})
	}
	for i := 1; i < len(request.CommonSteps); i++ {
		common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: request.CommonSteps[i-1].ID, After: request.CommonSteps[i].ID})
	}
	return common
}

func stepNames(steps []plangraph.Step[featureplan.Operation]) []string {
	names := make([]string, len(steps))
	for i, step := range steps {
		names[i] = step.ID.Name
	}
	return names
}

// A sorting key that gains a column takes over every column addition of the
// table, so the columns and the key change in one ALTER TABLE, and a column the
// new key does not use keeps its default.
func TestStoragePlanAddsKeyColumnsWithTheNewSortingKey(t *testing.T) {
	c := qt.New(t)
	request := storageFixture{
		after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" },
		columns: []*ast.ColumnNode{
			{Name: "k", Type: "Int32", Comment: "key"},
			{Name: "note", Type: "String", Nullable: true, Default: &ast.DefaultValue{Value: "n", ValueSet: true}},
		},
	}.request()
	result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	step := result.Contributions[0].Steps[0]
	c.Assert(result.Rewrites, qt.DeepEquals, []plangraph.Rewrite{{Sources: []plangraph.StepID{request.CommonSteps[0].ID, request.CommonSteps[1].ID}, Replacement: step.ID}})
	op := step.Payload.Payload.(*chast.AlterStorage)
	c.Assert(op.Columns, qt.DeepEquals, []chast.Column{
		{Name: "k", Type: "Int32", Comment: "key"},
		{Name: "note", Type: "Nullable(String)", Value: "DEFAULT", Expression: "'n'"},
	})
	c.Assert(result.Changes[0].Strategy, qt.Contains, "added in the ALTER TABLE that changes the sorting key")
	plan, err := plangraph.ScheduleRewritten(t.Context(), commonContribution(request), result.Rewrites, result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(stepNames(plan.Steps), qt.DeepEquals, []string{step.ID.Name})
}

// A key expression removed from the end of the sorting key is removed before
// the column it used is dropped.
func TestStoragePlanDropsATrailingKeyColumnAfterTheKey(t *testing.T) {
	c := qt.New(t)
	request := storageFixture{after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a" }, dropped: []string{"u"}}.request()
	result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Rewrites, qt.HasLen, 0)
	plan, err := plangraph.ScheduleRewritten(t.Context(), commonContribution(request), result.Rewrites, result.Contributions...)
	c.Assert(err, qt.IsNil)
	c.Assert(stepNames(plan.Steps), qt.DeepEquals, []string{"table-storage/0", "drop-u"})
}

func TestStoragePlanRefusesWhatClickHouseCannotChangeInPlace(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture storageFixture
		code    schemavalidation.Code
		want    string
	}{
		{name: "engine", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.Engine.Value = "ReplacingMergeTree" }},
			code: schemavalidation.UnsupportedFeature, want: "no ALTER TABLE that changes a table's ENGINE"},
		{name: "partition", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.PartitionBy.Value = "b" }},
			code: schemavalidation.UnsupportedFeature, want: "no ALTER TABLE that changes a table's PARTITION BY"},
		{name: "primary key", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.PrimaryKey.Value = "a, u" }},
			code: schemavalidation.UnsupportedFeature, want: "no ALTER TABLE that changes a table's PRIMARY KEY"},
		{name: "existing column joins the key", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, b" }},
			code: schemavalidation.UnsupportedFeature, want: `column "b" is already in the table`},
		{name: "key reordered", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "u, a" }},
			code: schemavalidation.UnsupportedFeature, want: "u is already in the key"},
		{name: "primary key no longer a prefix", fixture: storageFixture{
			after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "k, a, u" }, columns: []*ast.ColumnNode{{Name: "k", Type: "Int32"}},
		}, code: schemavalidation.UnsupportedFeature, want: "a prefix of the sorting key"},
		{name: "nullable key column", fixture: storageFixture{
			after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" }, columns: []*ast.ColumnNode{{Name: "k", Type: "Int32", Nullable: true}},
		}, code: schemavalidation.UnsupportedFeature, want: "must be NOT NULL"},
		{name: "key column with a default", fixture: storageFixture{
			after:   func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" },
			columns: []*ast.ColumnNode{{Name: "k", Type: "Int32", Default: &ast.DefaultValue{Value: "1", ValueSet: true}}},
		}, code: schemavalidation.UnsupportedFeature, want: "refuses a DEFAULT value"},
		{name: "computed key column", fixture: storageFixture{
			after:   func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" },
			columns: []*ast.ColumnNode{{Name: "k", Type: "Int32", GeneratedExpression: "a + 1", GeneratedKind: "ALIAS"}},
		}, code: schemavalidation.UnsupportedFeature, want: "refuses a ALIAS value"},
		{name: "sampling key outside the primary key", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.SampleBy.Value = "u" }},
			code: schemavalidation.UnsupportedFeature, want: "sampling key u to be an expression of the primary key a"},
		{name: "fixed setting", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.Settings.Value = "index_granularity = 4096" }},
			code: schemavalidation.UnsupportedFeature, want: "fixes table setting index_granularity"},
		{name: "repeated setting", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.Settings.Value = "a = 1, a = 2" }},
			code: schemavalidation.InvalidSchema, want: "names a twice"},
		{name: "key column dropped", fixture: storageFixture{after: func(v *chschema.DesiredTable) { v.TTL.Value = "toDateTime(a)" }, dropped: []string{"u"}},
			code: schemavalidation.UnsupportedFeature, want: `column "u" is used by the table's storage clauses after the change`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := test.fixture.request()
			result, err := (chplan.Service{}).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, test.code)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Rewrites, qt.HasLen, 0)
		})
	}
}

// A non-MergeTree engine has no MODIFY SETTING, MODIFY ORDER BY or MODIFY
// SAMPLE BY: measured on 26.9, `ALTER TABLE ... MODIFY SETTING` on a Log table
// answers `Alter of type 'MODIFY_SETTING' is not supported by storage Log`.
func TestStoragePlanRefusesChangesOutsideMergeTree(t *testing.T) {
	c := qt.New(t)
	request := storageFixture{after: func(v *chschema.DesiredTable) { v.Settings.Value = "flush_interval_milliseconds = 10" }}.request()
	change := request.Changes[0].Value.(*chdiff.Table)
	change.Before.Engine, change.After.Engine.Value = "Log", "Log"
	request.Tables[0].Current.Table.Facets = must.Must(schemaext.NewFacets(change.Before))
	request.Tables[0].Desired.Table.Facets = must.Must(schemaext.NewFacets(change.After))
	result, err := (chplan.Service{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "changing a table's SETTINGS in place requires a MergeTree engine")
}

// A column addition that does not carry its definition cannot be taken over,
// and the plan says which column it lacks.
func TestStoragePlanRefusesAnAdditionWithoutItsDefinition(t *testing.T) {
	c := qt.New(t)
	request := storageFixture{after: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a, u, k" }, columns: []*ast.ColumnNode{{Name: "k", Type: "Int32"}}}.request()
	request.CommonSteps[0].AddedColumn = nil
	result, err := (chplan.Service{}).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.InvalidSchema)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, `adds column "k" without its definition`)
}
