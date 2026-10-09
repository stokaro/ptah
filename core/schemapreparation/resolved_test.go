package schemapreparation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

func TestAcceptKeepsResolvedFacetsSeparateFromSource(t *testing.T) {
	c := qt.New(t)
	source := must.Must(labelFacets("declared").WithTargetScope("example.org/labels", "custom"))
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "events", Facets: source}},
	}}}
	reply := request.Clone()
	reply.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: labelFacets("resolved")}}
	capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
	c.Assert(err, qt.IsNil)
	c.Assert(capture.Source[0].ResolvedFacets, qt.HasLen, 0)
	c.Assert(capture.Prepared[0].Desired.Table.Facets, qt.DeepEquals, source)
	resolved, found, err := schemaext.FacetAs[*labels](capture.Prepared[0].ResolvedFacets[0].Values, "example.org/labels")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(resolved.names, qt.DeepEquals, []string{"resolved"})
	resolved.names[0] = "mutated"
	retained, _, err := schemaext.FacetAs[*labels](capture.Prepared[0].ResolvedFacets[0].Values, "example.org/labels")
	c.Assert(err, qt.IsNil)
	c.Assert(retained.names, qt.DeepEquals, []string{"resolved"})
}

func TestAcceptRefusesInventedResolvedFacets(t *testing.T) {
	for _, test := range []struct {
		name           string
		source, output schemaext.Facets
		input          []schemaext.FacetRecord
	}{
		{name: "undeclared model", output: labelFacets("resolved")},
		{name: "new output scope", source: labelFacets("declared"), output: must.Must(labelFacets("resolved").WithTargetScope("example.org/labels", "custom"))},
		{name: "already resolved input", source: labelFacets("declared"), input: []schemaext.FacetRecord{{Values: labelFacets("resolved")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemapreparation.Request{Tables: []schemapreparation.Table{{
				Desired:        schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "events", Facets: test.source}},
				ResolvedFacets: test.input,
			}}}
			reply := request.Clone()
			reply.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: test.output}}
			capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}
