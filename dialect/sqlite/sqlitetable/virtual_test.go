package sqlitetable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
)

var docsTable = objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "docs", Normalized: "docs"}}

func declaredVirtual(module, arguments string) *sqlitetable.DesiredVirtual {
	return &sqlitetable.DesiredVirtual{Virtual: sqlitetable.Virtual{Module: module, Arguments: arguments}}
}

func observedVirtual(module, arguments string) *sqlitetable.ObservedVirtual {
	return &sqlitetable.ObservedVirtual{Virtual: sqlitetable.Virtual{Module: module, Arguments: arguments}}
}

func virtualCoverage(representation schemaext.Representation) schemaext.Coverage {
	return must.Must(sqlitetable.VirtualCoverage(representation, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

// TestVirtualCodecs_RoundTrip writes the module and the arguments as written,
// leaves empty arguments out, and reads back exactly what it wrote.
func TestVirtualCodecs_RoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		declared *sqlitetable.DesiredVirtual
		want     string
	}{
		{name: "with arguments", declared: declaredVirtual("fts5", `"col,two", tokenize = 'porter'`),
			want: `{"module":"fts5","arguments":"\"col,two\", tokenize = 'porter'"}`},
		{name: "without arguments", declared: declaredVirtual("dbstat", ""), want: `{"module":"dbstat"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := sqlitetable.VirtualCodecs()[0]

			data, err := codec.Encode(test.declared)
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, test.want)
			decoded, err := codec.Decode(data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, schemaext.Value(test.declared))
		})
	}
}

// TestVirtualCodecs_RefusesASpellingTheEncoderNeverWrites refuses a
// declaration with no module and empty arguments written out.
func TestVirtualCodecs_RefusesASpellingTheEncoderNeverWrites(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "no module", data: `{"arguments":"body"}`},
		{name: "an empty module", data: `{"module":""}`},
		{name: "empty arguments", data: `{"arguments":"","module":"fts5"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := sqlitetable.VirtualCodecs()[0].Decode([]byte(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestVirtual_SameDeclaration folds the module over ASCII letters only, the
// way SQLite resolves it, and compares the arguments as written.
func TestVirtual_SameDeclaration(t *testing.T) {
	tests := []struct {
		name  string
		other sqlitetable.Virtual
		want  bool
	}{
		{name: "the same text", other: sqlitetable.Virtual{Module: "fts5", Arguments: "title, body"}, want: true},
		{name: "the module in another ASCII case", other: sqlitetable.Virtual{Module: "FTS5", Arguments: "title, body"}, want: true},
		{name: "other arguments", other: sqlitetable.Virtual{Module: "fts5", Arguments: "title,  body"}},
		{name: "another module", other: sqlitetable.Virtual{Module: "fts4", Arguments: "title, body"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			same := sqlitetable.Virtual{Module: "fts5", Arguments: "title, body"}.SameDeclaration(test.other)

			c.Assert(same, qt.Equals, test.want)
		})
	}
}

// TestVirtual_SameDeclarationFoldsNoOtherLetter keeps a module that differs
// in a letter outside ASCII distinct, as SQLite does.
func TestVirtual_SameDeclarationFoldsNoOtherLetter(t *testing.T) {
	c := qt.New(t)

	c.Assert(sqlitetable.Virtual{Module: "éx"}.SameDeclaration(sqlitetable.Virtual{Module: "Éx"}), qt.IsFalse)
}

// TestVirtual_DefinesTheTable answers the neutral contract in both
// representations.
func TestVirtual_DefinesTheTable(t *testing.T) {
	c := qt.New(t)

	for _, facets := range []schemaext.Facets{
		must.Must(schemaext.NewFacets(declaredVirtual("fts5", "body"))),
		must.Must(schemaext.NewFacets(observedVirtual("fts5", "body"))),
	} {
		kind, defined := schemaext.DefiningKind(facets)
		c.Assert(kind, qt.Equals, sqlitetable.VirtualKind)
		c.Assert(defined, qt.IsTrue)
	}
}

// TestVirtualOf reads either representation, and nothing from an ordinary
// table.
func TestVirtualOf(t *testing.T) {
	tests := []struct {
		name        string
		facets      schemaext.Facets
		want        sqlitetable.Virtual
		wantVirtual bool
	}{
		{name: "a declaration", facets: must.Must(schemaext.NewFacets(declaredVirtual("fts5", "body"))),
			want: sqlitetable.Virtual{Module: "fts5", Arguments: "body"}, wantVirtual: true},
		{name: "an observation", facets: must.Must(schemaext.NewFacets(observedVirtual("rtree", "id, x0, x1"))),
			want: sqlitetable.Virtual{Module: "rtree", Arguments: "id, x0, x1"}, wantVirtual: true},
		{name: "an ordinary table", facets: must.Must(schemaext.NewFacets(&sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true}}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, virtual, err := sqlitetable.VirtualOf(test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(declared, qt.DeepEquals, test.want)
			c.Assert(virtual, qt.Equals, test.wantVirtual)
		})
	}
}

// TestVirtualDeclaration_FailurePath refuses a virtual table that also
// states STRICT or WITHOUT ROWID, which belong to CREATE TABLE.
func TestVirtualDeclaration_FailurePath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(schemaext.NewFacets(declaredVirtual("fts5", "body"), &sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true}}))

	declared, err := sqlitetable.VirtualDeclaration(facets)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(declared, qt.IsNil)
}

// TestVirtualCompareService_CompareFacets reports a change for a table both
// sides hold whose declarations differ, or that is virtual on one side and
// ordinary on the other where that side's coverage describes virtual tables.
// A side that makes no claim says nothing about an absent declaration.
func TestVirtualCompareService_CompareFacets(t *testing.T) {
	tests := []struct {
		name            string
		desired         schemaext.Value
		desiredCoverage schemaext.Coverage
		current         schemaext.Value
		currentCoverage schemaext.Coverage
		wantChange      *sqlitetable.VirtualChange
	}{
		{name: "the same declaration", desired: declaredVirtual("fts5", "title, body"), current: observedVirtual("FTS5", "title, body")},
		{name: "other arguments", desired: declaredVirtual("fts5", "title"), current: observedVirtual("fts5", "title, body"),
			wantChange: &sqlitetable.VirtualChange{Before: observedVirtual("fts5", "title, body"), After: declaredVirtual("fts5", "title")}},
		{name: "a declared virtual table over a read ordinary one", desired: declaredVirtual("fts5", "body"),
			currentCoverage: virtualCoverage(schemaext.Observed), wantChange: &sqlitetable.VirtualChange{After: declaredVirtual("fts5", "body")}},
		{name: "a declared virtual table over a side that did not look", desired: declaredVirtual("fts5", "body")},
		{name: "a declared ordinary table over a virtual one", desiredCoverage: virtualCoverage(schemaext.Desired),
			current: observedVirtual("fts5", "body"), wantChange: &sqlitetable.VirtualChange{Before: observedVirtual("fts5", "body")}},
		{name: "a source with no virtual table syntax", current: observedVirtual("fts5", "body")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.FacetComparisonRequest{
				Target: platform.SQLite, Kinds: []schemaext.Kind{sqlitetable.VirtualKind},
				Desired: docsState(test.desired, test.desiredCoverage),
				Current: docsState(test.current, test.currentCoverage),
				Owners:  []schemaext.ParentState{{Subject: docsTable, Desired: true, Current: true}},
			}

			result, err := sqlitetable.VirtualCompareService{}.CompareFacets(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(changeValues(result.Changes), qt.DeepEquals, wantChanges(test.wantChange))
		})
	}
}

// docsState is one side's facets on the docs table: the value, when the side
// holds one, and the side's coverage.
func docsState(value schemaext.Value, coverage schemaext.Coverage) schemaext.FacetState {
	state := schemaext.FacetState{Coverage: coverage}
	if value != nil {
		state.Records = []schemaext.FacetRecord{{Subject: docsTable, Values: must.Must(schemaext.NewFacets(value))}}
	}
	return state
}

func changeValues(changes []schemaext.FacetChange) []schemaext.ChangeValue {
	values := make([]schemaext.ChangeValue, 0, len(changes))
	for _, change := range changes {
		values = append(values, change.Change.Value)
	}
	return values
}

func wantChanges(change *sqlitetable.VirtualChange) []schemaext.ChangeValue {
	if change == nil {
		return make([]schemaext.ChangeValue, 0)
	}
	return []schemaext.ChangeValue{change}
}

// TestVirtualPlanService_RefusesEveryChange names what SQLite cannot do for
// each shape of change.
func TestVirtualPlanService_RefusesEveryChange(t *testing.T) {
	tests := []struct {
		name   string
		change *sqlitetable.VirtualChange
		want   string
	}{
		{name: "a changed declaration", change: &sqlitetable.VirtualChange{Before: observedVirtual("fts5", "a"), After: declaredVirtual("fts5", "b")},
			want: `virtual table .* is declared with another module or other arguments; SQLite has no ALTER VIRTUAL TABLE, .*`},
		{name: "a kind collision", change: &sqlitetable.VirtualChange{After: declaredVirtual("fts5", "b")},
			want: `table .* is a virtual table on one side and an ordinary table on the other; .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := sqlitetable.VirtualPlanService{}.PlanFeatures(c.Context(), featureplan.Request{Target: platform.SQLite,
				Changes: []schemaext.ChangeRecord{{Subject: docsTable, Value: test.change}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, test.want)
		})
	}
}

// TestVirtualPlanService_AccountsForTheTableActions gives a receipt for
// creation, a surviving table and removal, and refuses a rebuild.
func TestVirtualPlanService_AccountsForTheTableActions(t *testing.T) {
	c := qt.New(t)
	request := featureplan.Request{Target: platform.SQLite, ParentKinds: []schemaext.Kind{sqlitetable.VirtualKind}}
	for _, action := range []featureplan.ParentAction{featureplan.CreateTable, featureplan.AlterTable, featureplan.DropTable} {
		request.Tables = append(request.Tables, featureplan.Table{Subject: docsTable, Action: action})
	}

	result, err := sqlitetable.VirtualPlanService{}.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 3)
	c.Assert(result.Diagnostics, qt.HasLen, 0)

	request.Tables = []featureplan.Table{{Subject: docsTable, Action: featureplan.RebuildTable}}
	refused, err := sqlitetable.VirtualPlanService{}.PlanFeatures(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Diagnostics, qt.HasLen, 1)
	c.Assert(refused.Diagnostics[0].Problem.Message, qt.Equals, "SQLite virtual tables are not rebuilt: their rows belong to the module, not to a table")
}

// TestVirtualReportService_CountsEachTable counts one virtual table per value.
func TestVirtualReportService_CountsEachTable(t *testing.T) {
	c := qt.New(t)

	reports, err := sqlitetable.VirtualReportService{}.ReportValues(c.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed, Values: []schemaext.Value{observedVirtual("fts5", "body")},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{{Kind: sqlitetable.VirtualKind,
		Counts: []schemaext.MetricCount{{Name: "sqlite_virtual_tables", Value: 1}}}})
}
