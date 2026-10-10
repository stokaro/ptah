package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

const definingKind schemaext.Kind = "example.org/defining"

// definingWidget is a table facet value that says whether it defines what its
// table is.
type definingWidget struct{ defines bool }

func (*definingWidget) Kind() schemaext.Kind     { return definingKind }
func (v *definingWidget) Clone() schemaext.Value { return &definingWidget{defines: v.defines} }
func (v *definingWidget) DefinesTable() bool     { return v.defines }
func (v *definingWidget) Equal(other schemaext.Value) bool {
	w, ok := other.(*definingWidget)
	return ok && v.defines == w.defines
}

// TestDefiningKind names the kind of the value that defines its table, and
// nothing for a value that does not implement the contract or answers false.
func TestDefiningKind(t *testing.T) {
	tests := []struct {
		name        string
		facets      schemaext.Facets
		wantKind    schemaext.Kind
		wantDefined bool
	}{
		{name: "a value that defines the table", facets: must.Must(schemaext.NewFacets(&definingWidget{defines: true},
			&widget{ID: widgetKind})), wantKind: definingKind, wantDefined: true},
		{name: "a value that answers false", facets: must.Must(schemaext.NewFacets(&definingWidget{}))},
		{name: "a value without the contract", facets: must.Must(schemaext.NewFacets(&widget{ID: widgetKind}))},
		{name: "no value"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kind, defined := schemaext.DefiningKind(test.facets)

			c.Assert(kind, qt.Equals, test.wantKind)
			c.Assert(defined, qt.Equals, test.wantDefined)
		})
	}
}

// TestPlansDefinedTableRemoval removes a live table a value defines only
// where the desired coverage of the defining kind says the source describes
// such tables, Complete or Absent. Every other answer keeps the table, a kind
// the source never enrolled included.
func TestPlansDefinedTableRemoval(t *testing.T) {
	registry := must.Must(schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired))))
	table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "docs", Normalized: "docs"}}
	claim := func(knowledge schemaext.Knowledge) schemaext.Coverage {
		return must.Must(schemaext.NewCoverage(schemaext.Desired,
			[]schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
			[]schemaext.SubjectCoverage{{Kind: widgetKind, Subject: table, Knowledge: knowledge}}))
	}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
		want     bool
	}{
		{name: "complete", coverage: claim(schemaext.Knowledge{State: schemaext.Complete}), want: true},
		{name: "absent", coverage: claim(schemaext.Knowledge{State: schemaext.Absent}), want: true},
		{name: "defaulted", coverage: claim(schemaext.Knowledge{State: schemaext.Defaulted})},
		{name: "uninspected", coverage: claim(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"})},
		{name: "unrepresentable", coverage: claim(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "no syntax"})},
		{name: "never enrolled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemaext.PlansDefinedTableRemoval(test.coverage, widgetKind, table), qt.Equals, test.want)
		})
	}
}
