package chprepare_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chschema"
)

func indexCreationRequest() schemaprojection.TableCreationRequest {
	base := indexRequest()
	declaration := base.Tables[0].Desired
	declaration.Table.Engine = "Memory"
	return schemaprojection.TableCreationRequest{Target: "clickhouse", Identifiers: base.Identifiers,
		Tables: []schemaprojection.TableCreationInput{{Subject: base.Tables[0].Subject, Declaration: declaration}}}
}

func TestIndexCreationResolvesIntentAndKeepsAuthoredSettings(t *testing.T) {
	for _, test := range []struct {
		name    string
		desired *chschema.DesiredIndex
		want    *chschema.ObservedIndex
	}{
		{"omitted", &chschema.DesiredIndex{}, &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"defaults", &chschema.DesiredIndex{IndexType: chschema.Setting{State: chschema.Default}, Granularity: chschema.GranularitySetting{State: chschema.Default}},
			&chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"explicit", (&chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}).Desired(),
			&chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexCreationRequest()
			request.Tables[0].Declaration.Indexes[0].Facets = must.Must(schemaext.NewFacets(test.desired))
			before := request.Clone()
			result, err := (chprepare.Service{}).ProjectTableCreations(t.Context(), request)
			c.Assert(err, qt.IsNil)
			result, err = schemaprojection.AcceptTableCreations(request, result)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Tables[0].Facets, qt.HasLen, 2)
			c.Assert(result.Tables[0].Facets[1].Subject, qt.Equals, objectidentity.NewBuilder(request.Identifiers).IndexParts("tenant", "events", "by.id"))
			value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](result.Tables[0].Facets[1].Values, chschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(value, qt.DeepEquals, test.want.Desired())
			c.Assert(request, qt.DeepEquals, before)
		})
	}
}

func TestIndexCreationRemainsIndependentOfTableFacetExclusion(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	table := must.Must(schemaext.NewFacets(&chschema.DesiredTable{}))
	table = must.Must(table.WithTargetScope(chschema.TableKind, "other"))
	request.Tables[0].Declaration.Table.Facets = must.Must(table.ForTarget(must.Must(schemaext.NewTargetSelection("clickhouse"))))
	result, err := (chprepare.Service{}).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	result, err = schemaprojection.AcceptTableCreations(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Facets, qt.HasLen, 1)
	c.Assert(result.Tables[0].Facets[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{chschema.IndexKind})
	c.Assert(result.Tables[0].ColumnPrimaryKeysPrepared, qt.IsFalse)
}

func TestIndexCreationDoesNotRestoreExcludedIndexSettings(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	index := must.Must(request.Tables[0].Declaration.Indexes[0].Facets.WithTargetScope(chschema.IndexKind, "other"))
	request.Tables[0].Declaration.Indexes[0].Facets = must.Must(index.ForTarget(must.Must(schemaext.NewTargetSelection("clickhouse"))))
	result, err := (chprepare.Service{}).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	result, err = schemaprojection.AcceptTableCreations(request, result)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables[0].Facets, qt.HasLen, 1)
	c.Assert(result.Tables[0].Facets[0].Values.Kinds(), qt.DeepEquals, []schemaext.Kind{chschema.TableKind})
}

func TestIndexCreationRefusesCompetingTypeWithoutPartialTable(t *testing.T) {
	c := qt.New(t)
	request := indexCreationRequest()
	request.Tables[0].Declaration.Indexes[0].Type = "set(100)"
	result, err := (chprepare.Service{}).ProjectTableCreations(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}
