package schemapreparation_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

type labels struct{ names []string }

func (*labels) Kind() schemaext.Kind { return "example.org/labels" }
func (v *labels) Clone() schemaext.Value {
	return &labels{names: slices.Clone(v.names)}
}
func (v *labels) Equal(other schemaext.Value) bool {
	w, ok := other.(*labels)
	return ok && slices.Equal(slices.Sorted(slices.Values(v.names)), slices.Sorted(slices.Values(w.names)))
}

func labelFacets(names ...string) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&labels{names: names}))
}

func TestAcceptUsesOwnerEqualityAndRetainsOriginalRepresentations(t *testing.T) {
	c := qt.New(t)
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", Facets: labelFacets("a", "b")},
			Fields: []schemamodel.Field{{Name: "id", Facets: labelFacets("a", "b")}},
		},
		Current:          schemacapture.TableObservation{Table: catalog.Table{Name: "events", Columns: []catalog.Column{{Name: "id", Facets: labelFacets("a", "b")}}}},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}}
	reply := request.Clone()
	reply.Tables[0].Desired.Table.Facets = labelFacets("b", "a")
	reply.Tables[0].Desired.Fields[0].Facets = labelFacets("b", "a")
	reply.Tables[0].Current.Table.Columns[0].Facets = labelFacets("b", "a")
	capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
	c.Assert(err, qt.IsNil)
	source, _, err := schemaext.FacetAs[*labels](capture.Source[0].Desired.Table.Facets, "example.org/labels")
	c.Assert(err, qt.IsNil)
	prepared, _, err := schemaext.FacetAs[*labels](capture.Prepared[0].Desired.Table.Facets, "example.org/labels")
	c.Assert(err, qt.IsNil)
	c.Assert(source.names, qt.DeepEquals, []string{"a", "b"})
	c.Assert(prepared.names, qt.DeepEquals, []string{"b", "a"})
}

func TestAcceptRejectsFeatureChanges(t *testing.T) {
	tests := []struct {
		name   string
		facets schemaext.Facets
	}{
		{name: "dropped", facets: schemaext.Facets{}},
		{name: "changed", facets: labelFacets("changed")},
		{name: "scope", facets: must.Must(labelFacets("a", "b").WithTargetScope("example.org/labels", "postgres"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
				Table: schemamodel.Table{Name: "events", Facets: labelFacets("a", "b")},
			}}}}
			reply := request.Clone()
			reply.Tables[0].Desired.Table.Facets = test.facets
			capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}
