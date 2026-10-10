package ydbplan_test

import (
	"context"
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
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
)

// familyPlanningRuntime selects only this owner's column family service, so
// the engine's reply validation runs without the bundled runtime.
func familyPlanningRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: ydbschema.Owner, Targets: []engine.Target{{Name: "ydb"}},
		Codecs: slices.Concat(ydbschema.ColumnFamiliesCodecs(), []schemaext.Codec{ydbdiff.ColumnFamiliesCodec(), ydbast.ColumnFamiliesCodec()}),
		Planning: []engine.Planning{{
			Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.ColumnFamiliesKind}, ParentKinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind},
			OperationKinds: []schemaext.Kind{ydbast.AlterColumnFamiliesKind}, Service: ydbplan.ColumnFamiliesService{},
		}},
	}))
}

// familyDeclaration is table docs with a key column id and the columns body
// and blob.
func familyDeclaration() schemacapture.TableDeclaration {
	return schemacapture.TableDeclaration{
		Table: schemamodel.Table{StructName: "D", Name: "docs"},
		Fields: []schemamodel.Field{
			{StructName: "D", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "D", Name: "body", Type: "TEXT", Nullable: true},
			{StructName: "D", Name: "blob", Type: "BYTEA", Nullable: true},
		},
	}
}

// familyObservation is docs as a read finds it: the declared columns and
// extra, which the declaration does not name, so the plan drops it.
func familyObservation(held []ydbschema.ColumnFamily) schemacapture.TableObservation {
	observation := schemacapture.TableObservation{Table: catalog.Table{Name: "docs", Columns: []catalog.Column{
		{Name: "id", DataType: "Int64"}, {Name: "body", DataType: "Utf8"}, {Name: "blob", DataType: "String"}, {Name: "extra", DataType: "Utf8"},
	}}}
	observation.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnFamilies{Families: held}))
	return observation
}

func familyRequestFixture(before, after []ydbschema.ColumnFamily) featureplan.Request {
	semantics := identifier.ForDialect("ydb")
	builder := objectidentity.NewBuilder(semantics)
	docs, archive := builder.Table("docs"), builder.Table("archive")
	dropped := featureplan.Table{Subject: archive, Action: featureplan.DropTable}
	dropped.Current.Table = catalog.Table{Name: "archive"}
	change := &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{Families: after}}
	if before != nil {
		change.Before = &ydbschema.ObservedColumnFamilies{Families: before}
	}
	return featureplan.Request{
		Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(),
		Tables:  []featureplan.Table{{Subject: docs, Desired: familyDeclaration(), Current: familyObservation(before)}, dropped},
		Changes: []schemaext.ChangeRecord{{Subject: docs, Value: change}},
	}
}

// TestColumnFamiliesPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable
// pins the receipts: one owned operation for the change, carrying copied
// operands without the column the plan drops, and a parent receipt for the
// families a dropped table takes with it.
func TestColumnFamiliesPlanFeatures_PlansTheChangeAndAccountsForTheRemovedTable(t *testing.T) {
	c := qt.New(t)
	request := familyRequestFixture(
		[]ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Columns: []string{"body", "extra"}}},
		[]ydbschema.ColumnFamily{{Name: "default", Compression: "off"}, {Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"blob", "body"}}},
	)

	result, err := familyPlanningRuntime().PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Kind, qt.Equals, ydbdiff.ColumnFamiliesKind)
	c.Assert(result.Changes[0].Strategy, qt.Equals, "add families, set their settings and move columns in one ALTER TABLE after column additions")
	c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{
		Subject: request.Tables[1].Subject, Kind: ydbschema.ColumnFamiliesKind, Action: featureplan.DropTable, Strategy: "remove the column families with the table",
	}})
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
	c.Assert(steps[0].Impact.Impact, qt.Equals, schemaext.Additive)
	c.Assert(steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(steps[0].Payload.Role, qt.Equals, ast.AlterExtension)
	c.Assert(steps[0].Payload.Parent, qt.DeepEquals, request.Tables[0].Subject)
	op := steps[0].Payload.Payload.(*ydbast.AlterColumnFamilies)
	c.Assert(op.Actions(), qt.DeepEquals, []string{"ALTER FAMILY `cold` SET COMPRESSION 'lz4'", "ALTER COLUMN `blob` SET FAMILY `cold`"})
	c.Assert(op.Change.Before.Families[1].Columns, qt.DeepEquals, []string{"body"})
	op.Change.After.Families[1].Columns[0] = "mutated"
	c.Assert(request.Changes[0].Value.(*ydbdiff.ColumnFamilies).After.Families[1].Columns, qt.DeepEquals, []string{"blob", "body"})
}

// TestColumnFamiliesPlanFeatures_ADroppedColumnMovesNowhere pins a change that
// only moves the column the plan drops out of its family: the column goes with
// its DROP COLUMN, so the change is accounted for with no statement.
func TestColumnFamiliesPlanFeatures_ADroppedColumnMovesNowhere(t *testing.T) {
	c := qt.New(t)
	request := familyRequestFixture(
		[]ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body", "extra"}}},
		[]ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}},
	)

	result, err := familyPlanningRuntime().PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.HasLen, 0)
	c.Assert(result.Changes[0].Strategy, qt.Equals, "nothing to change once the columns the plan drops leave their families")
}

// TestColumnFamiliesPlanFeatures_RefusesWithoutOutput pins the completed
// refusals: a change the target cannot make, one YDB would refuse, one that
// would lose a setting, and one without a table to make it on, each with no
// receipt, contribution or prefix.
func TestColumnFamiliesPlanFeatures_RefusesWithoutOutput(t *testing.T) {
	cold := []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}
	tests := []struct {
		name          string
		before, after []ydbschema.ColumnFamily
		caps          capability.Capabilities
		action        featureplan.ParentAction
		want          string
	}{
		{name: "a target without column families", after: cold, caps: capability.YDB262().With(capability.ColumnFamilies, false),
			want: `changing the column families of table "docs", which requires target capability column_families`},
		{name: "a cache mode without its key", after: []ydbschema.ColumnFamily{{Name: "warm", CacheMode: "in_memory"}},
			caps: capability.YDB262().With(capability.ColumnFamilyCacheMode, false), want: "requires target capability column_family_cache_mode"},
		{name: "a column the table does not declare", after: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"gone"}}}, caps: capability.YDB262(),
			want: `column family "cold" names column "gone", which the table does not declare`},
		{name: "a key column", after: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"id"}}}, caps: capability.YDB262(),
			want: `column family "cold" names key column "id"`},
		{name: "a keep_in_memory no statement writes", after: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			before: []ydbschema.ColumnFamily{{Name: "default", Compression: "off"}}, caps: capability.YDB262(),
			want: "keeps its columns in memory (keep_in_memory) on one side only"},
		{name: "a table the plan drops", after: cold, caps: capability.YDB262(), action: featureplan.DropTable,
			want: "a column family change requires a table that survives the plan in place"},
		{name: "a table the plan rebuilds", after: cold, caps: capability.YDB262(), action: featureplan.RebuildTable,
			want: "a column family change requires a table that survives the plan in place"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := familyRequestFixture(test.before, test.after)
			request.Capabilities = test.caps
			request.Tables[0].Action = test.action
			request.ParentKinds = []schemaext.Kind{ydbschema.ColumnFamiliesKind}

			result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Feature, qt.Equals, "YDB column family planning")
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

// TestColumnFamiliesPlanFeatures_RefusesAChangeItCannotRead pins the changes
// that are not this owner's to plan: a value of another kind, one without the
// families the table ends up holding, and one on a table the request does not
// carry.
func TestColumnFamiliesPlanFeatures_RefusesAChangeItCannotRead(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.ChangeValue
		tables int
		want   string
	}{
		{name: "another kind", change: &ydbdiff.TTL{}, tables: 2, want: "expected a YDB column family change"},
		{name: "no after", change: &ydbdiff.ColumnFamilies{}, tables: 2, want: "requires the families the table ends up holding"},
		{name: "no table", change: &ydbdiff.ColumnFamilies{After: &ydbschema.DesiredColumnFamilies{}}, tables: 0,
			want: "a column family change requires captured parent state"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := familyRequestFixture(nil, nil)
			request.Changes[0].Value = test.change
			request.Tables = request.Tables[:test.tables]

			result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// familyParent is docs under a parent action, declared with declared families
// under the given source coverage and read holding held.
func familyParent(action featureplan.ParentAction, declared, held []ydbschema.ColumnFamily, coverage schemaext.Coverage) featureplan.Table {
	declaration := familyDeclaration()
	declaration.FeatureCoverage = coverage
	if declared != nil {
		declaration.Table.Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: declared}))
	}
	return featureplan.Table{Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("docs"), Action: action,
		Desired: declaration, Current: familyObservation(held)}
}

var familiesComplete = must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))

// TestColumnFamiliesPlanFeatures_AccountsForEachParentAction pins the parent
// receipts: a created table writes the declaration, a rebuilt one what the
// table holds once the declaration is applied, and a surviving one keeps its
// families.
func TestColumnFamiliesPlanFeatures_AccountsForEachParentAction(t *testing.T) {
	cold := []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}}
	tests := []struct {
		name   string
		action featureplan.ParentAction
		want   string
	}{
		{name: "a created table", action: featureplan.CreateTable, want: "write the declared column families into the CREATE TABLE"},
		{name: "a rebuilt table", action: featureplan.RebuildTable,
			want: "write the families the table holds once the declaration is applied into the rebuilt table"},
		{name: "a surviving table", action: featureplan.AlterTable, want: "retain the column families unless a planned change in this plan changes them"},
		{name: "a dropped table", action: featureplan.DropTable, want: "remove the column families with the table"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				Tables: []featureplan.Table{familyParent(test.action, cold, cold, familiesComplete)}, ParentKinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind}}

			result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Parents[0].Strategy, qt.Equals, test.want)
		})
	}
}

// TestColumnFamiliesPlanFeatures_RefusesARebuiltTableTheTargetCannotHold pins
// that a rebuild whose families the new CREATE TABLE cannot write is refused
// before any statement rather than when the rebuild is rendered: a family the
// target has no key for, and a keep_in_memory the table holds, which no CREATE
// TABLE writes and which the rebuilt table would lose.
func TestColumnFamiliesPlanFeatures_RefusesARebuiltTableTheTargetCannotHold(t *testing.T) {
	tests := []struct {
		name     string
		declared []ydbschema.ColumnFamily
		held     []ydbschema.ColumnFamily
		caps     capability.Capabilities
		want     string
	}{
		{name: "a target without column families", declared: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}},
			caps: capability.YDB262().With(capability.ColumnFamilies, false),
			want: `rebuilding table "docs" with column families, which requires target capability column_families`},
		{name: "a held keep_in_memory", held: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			caps: capability.YDB262(), want: `rebuilding table "docs": column family "default" keeps its columns in memory`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: test.caps,
				Tables:      []featureplan.Table{familyParent(featureplan.RebuildTable, test.declared, test.held, familiesComplete)},
				ParentKinds: []schemaext.Kind{ydbschema.ColumnFamiliesKind}}

			result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.ValidateOutcome(request), qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, test.want)
			c.Assert(result.Parents, qt.HasLen, 0)
		})
	}
}

// TestRebuiltFamilies pins what a rebuild writes. Under a source that can
// declare families it is what the table holds once the declaration is applied:
// every held family stays, an unstated setting keeps the held value, and each
// column sits where the declaration puts it, which is the default family for
// a column it places in none. Under a source that cannot, the table keeps its
// families, columns included, except the column the document leaves out,
// which the rebuilt table does not have.
func TestRebuiltFamilies(t *testing.T) {
	held := []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold", Data: "hdd", Columns: []string{"body", "extra"}}}
	tests := []struct {
		name     string
		declared []ydbschema.ColumnFamily
		coverage schemaext.Coverage
		want     []ydbschema.ColumnFamily
	}{
		{name: "a declaration", declared: []ydbschema.ColumnFamily{{Name: "cold", Compression: "lz4", Columns: []string{"blob"}}}, coverage: familiesComplete,
			want: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"blob"}}, {Name: "default", Compression: "lz4"}}},
		{name: "no family declared", coverage: familiesComplete,
			want: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd"}, {Name: "default", Compression: "lz4"}}},
		{name: "a source that cannot declare families",
			want: []ydbschema.ColumnFamily{{Name: "default", Compression: "lz4"}, {Name: "cold", Data: "hdd", Columns: []string{"body"}}}},
		{name: "families adopted from the table", declared: held,
			want: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"body"}}, {Name: "default", Compression: "lz4"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := familyParent(featureplan.RebuildTable, test.declared, held, test.coverage)

			got, err := ydbplan.RebuiltFamilies(table.Desired, table.Current)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestFamiliesKnown reads a table's own claim over the kind's.
func TestFamiliesKnown(t *testing.T) {
	docs := objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("docs")
	unread := []schemaext.SubjectCoverage{{Kind: ydbschema.ColumnFamiliesKind, Subject: docs,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unread setting"}}}
	read := []schemaext.SubjectCoverage{{Kind: ydbschema.ColumnFamiliesKind, Subject: docs, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}
	uninspected := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
		want     bool
	}{
		{name: "no coverage", coverage: schemaext.Coverage{}},
		{name: "a complete kind", coverage: familiesComplete, want: true},
		{name: "an uninspected kind", coverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, uninspected, nil))},
		{name: "a read table under an uninspected kind", coverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, uninspected, read)), want: true},
		{name: "an unread table under a complete kind",
			coverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, unread))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbplan.FamiliesKnown(test.coverage), qt.Equals, test.want)
		})
	}
}

// TestColumnFamiliesPlanFeatures_FailurePath pins the request errors.
func TestColumnFamiliesPlanFeatures_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*featureplan.Request)
		wantErr error
	}{
		{name: "another target", mutate: func(r *featureplan.Request) { r.Target = "postgres" }, wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "another parent kind", mutate: func(r *featureplan.Request) { r.ParentKinds = []schemaext.Kind{ydbschema.TTLKind} }, wantErr: schemaext.ErrInvalidValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := familyRequestFixture(nil, []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"body"}}})
			test.mutate(&request)

			result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
	c := qt.New(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := ydbplan.ColumnFamiliesService{}.PlanFeatures(canceled, familyRequestFixture(nil, nil))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Complete, qt.IsFalse)
}
