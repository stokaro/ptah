package schemapreparation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

func indexPreparationRequest() (schemapreparation.Request, objectidentity.ID) {
	semantics := identifier.ForDialect("clickhouse")
	builder := objectidentity.NewBuilder(semantics)
	source := must.Must(labelFacets("declared").WithTargetScope("example.org/labels", "clickhouse"))
	return schemapreparation.Request{Target: "clickhouse", Identifiers: semantics, Tables: []schemapreparation.Table{{
		Subject: builder.TableParts("tenant", "events.with.dot"),
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "events.with.dot", Schema: "tenant"},
			Indexes: []schemamodel.Index{{Name: "by.id", Fields: []string{"id"}, Facets: source}}},
	}}}, builder.IndexParts("tenant", "events.with.dot", "by.id")
}

func TestAcceptRetainsDeclaredIndexAndIndependentResolvedRecords(t *testing.T) {
	c := qt.New(t)
	request, subject := indexPreparationRequest()
	reply := request.Clone()
	reply.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: subject, Values: labelFacets("resolved")}}
	capture := must.Must(schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables}))
	c.Assert(capture.Source[0].ResolvedFacets, qt.HasLen, 0)
	c.Assert(capture.Prepared[0].Desired.Indexes[0], qt.DeepEquals, request.Tables[0].Desired.Indexes[0])
	c.Assert(capture.Prepared[0].ResolvedFacets[0].Values.Equal(labelFacets("resolved")), qt.IsTrue)
	reply.Tables[0].ResolvedFacets[0].Subject.Name.Source = "changed"
	c.Assert(capture.Prepared[0].ResolvedFacets[0].Subject, qt.Equals, subject)
	cloned := capture.Clone()
	cloned.Prepared[0].ResolvedFacets[0].Values = labelFacets("changed")
	c.Assert(capture.Prepared[0].ResolvedFacets[0].Values.Equal(labelFacets("resolved")), qt.IsTrue)
}

func TestAcceptRefusesUnrelatedAndDuplicateIndexResolution(t *testing.T) {
	request, subject := indexPreparationRequest()
	missing := subject
	missing.Name.Source, missing.Name.Normalized = "missing", "missing"
	wrongSpelling := subject
	wrongSpelling.Name.Source = "another index"
	resolved := schemaext.FacetRecord{Subject: subject, Values: labelFacets("resolved")}
	for _, test := range []struct {
		name    string
		records []schemaext.FacetRecord
	}{
		{"undeclared owner", []schemaext.FacetRecord{{Subject: missing, Values: resolved.Values}}},
		{"changed identity spelling", []schemaext.FacetRecord{{Subject: wrongSpelling, Values: resolved.Values}}},
		{"wrong owner kind", []schemaext.FacetRecord{{Subject: request.Tables[0].Subject, Values: resolved.Values}}},
		{"duplicate", []schemaext.FacetRecord{resolved, resolved}},
		{"empty values", []schemaext.FacetRecord{{Subject: subject}}},
		{"new scope", []schemaext.FacetRecord{{Subject: subject, Values: must.Must(resolved.Values.WithTargetScope("example.org/labels", "clickhouse"))}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reply := request.Clone()
			reply.Tables[0].ResolvedFacets = test.records
			capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}

func TestAcceptRefusesRewritingTheAuthoredIndex(t *testing.T) {
	c := qt.New(t)
	request, subject := indexPreparationRequest()
	reply := request.Clone()
	reply.Tables[0].ResolvedFacets = []schemaext.FacetRecord{{Subject: subject, Values: labelFacets("resolved")}}
	reply.Tables[0].Desired.Indexes[0].Facets = labelFacets("resolved")
	capture, err := schemapreparation.Accept(request, schemapreparation.Result{Complete: true, Tables: reply.Tables})
	c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
	c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
	c.Assert(request.Tables[0].Desired.Indexes[0].Facets.TargetScope("example.org/labels"), qt.DeepEquals, []string{"clickhouse"})
}
