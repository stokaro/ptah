package chprepare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

func indexCoverage(state schemaext.KnowledgeState, claims ...schemaext.SubjectCoverage) schemaext.Coverage {
	var owned []schemaext.OwnedCodec
	for _, codec := range chschema.IndexCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	models := make(map[schemaext.Representation]schemaext.CodecIdentity)
	for _, model := range must.Must(schemaext.NewRegistry(owned...)).Definitions() {
		models[model.Representation] = model
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{
		Model: models[schemaext.Observed], Knowledge: schemaext.Knowledge{State: state, Reason: "captured index enumeration"},
	}}, claims))
}

func indexRequest() schemapreparation.Request {
	semantics := identifier.ForDialect("clickhouse")
	return schemapreparation.Request{Target: "clickhouse", Identifiers: semantics, Tables: []schemapreparation.Table{{
		Subject: objectidentity.NewBuilder(semantics).TableParts("tenant", "events"),
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "events", Schema: "tenant"},
			Indexes: []schemamodel.Index{{Name: "by.id", Fields: []string{"id"},
				Facets: must.Must(schemaext.NewFacets(&chschema.DesiredIndex{}))}}},
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "events", Schema: "tenant"},
			Indexes: []catalog.Index{{Name: "by.id", TableName: "events", Schema: "tenant", Columns: []string{"id"},
				Facets: must.Must(schemaext.NewFacets(&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 64}))}},
			FeatureCoverage: indexCoverage(schemaext.Complete)},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}}
}

func TestIndexPreparationUsesOnlyEstablishedCurrentState(t *testing.T) {
	base := indexRequest()
	subject := objectidentity.NewBuilder(base.Identifiers).IndexParts("tenant", "events", "by.id")
	retained := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: 64}
	defaults := &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}
	for _, test := range []struct {
		name      string
		current   schemacapture.TableObservation
		existence schemaext.KnowledgeState
		want      *chschema.ObservedIndex
	}{
		{"complete captured index", base.Tables[0].Current, schemaext.Complete, retained},
		{"partial enumeration with captured index", schemacapture.TableObservation{Table: base.Tables[0].Current.Table,
			Indexes: base.Tables[0].Current.Indexes, FeatureCoverage: indexCoverage(schemaext.Uninspected)}, schemaext.Complete, retained},
		{"new index on existing table", schemacapture.TableObservation{Table: base.Tables[0].Current.Table,
			FeatureCoverage: indexCoverage(schemaext.Complete)}, schemaext.Complete, defaults},
		{"explicitly absent index", schemacapture.TableObservation{Table: base.Tables[0].Current.Table,
			FeatureCoverage: indexCoverage(schemaext.Uninspected, schemaext.SubjectCoverage{Kind: chschema.IndexKind, Subject: subject,
				Knowledge: schemaext.Knowledge{State: schemaext.Absent}})}, schemaext.Complete, defaults},
		{"new parent", schemacapture.TableObservation{}, schemaext.Absent, defaults},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := base.Clone()
			request.Tables[0].Current = test.current
			request.Tables[0].CurrentKnowledge.State = test.existence
			result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Tables[0].ResolvedFacets, qt.HasLen, 1)
			c.Assert(result.Tables[0].ResolvedFacets[0].Subject.Key(), qt.Equals, subject.Key())
			value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](result.Tables[0].ResolvedFacets[0].Values, chschema.IndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(value, qt.DeepEquals, test.want.Desired())
			c.Assert(result.Tables[0].Desired, qt.DeepEquals, request.Tables[0].Desired)
			c.Assert(result.Tables[0].Current, qt.DeepEquals, request.Tables[0].Current)
			c.Assert(request.Tables[0].ResolvedFacets, qt.HasLen, 0)
			_, err = schemapreparation.Accept(request, result)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestIndexPreparationRefusesUnknownEvidenceWithoutPartialOutput(t *testing.T) {
	base := indexRequest()
	subject := objectidentity.NewBuilder(base.Identifiers).IndexParts("tenant", "events", "by.id")
	limited := func(state schemaext.KnowledgeState) schemacapture.TableObservation {
		current := base.Tables[0].Current.Clone()
		current.FeatureCoverage = indexCoverage(schemaext.Complete, schemaext.SubjectCoverage{Kind: chschema.IndexKind, Subject: subject,
			Knowledge: schemaext.Knowledge{State: state, Reason: "settings were not available"}})
		return current
	}
	for _, test := range []struct {
		name      string
		current   schemacapture.TableObservation
		existence schemaext.KnowledgeState
		want      error
	}{
		{"unknown parent", schemacapture.TableObservation{}, schemaext.Uninspected, chresolve.ErrUnknownCurrent},
		{"unknown index enumeration", schemacapture.TableObservation{Table: base.Tables[0].Current.Table}, schemaext.Complete, chresolve.ErrUnknownCurrent},
		{"subject not inspected", limited(schemaext.Uninspected), schemaext.Complete, chresolve.ErrUnknownCurrent},
		{"subject not representable", limited(schemaext.Unrepresentable), schemaext.Complete, chresolve.ErrUnknownCurrent},
		{"present index marked absent", limited(schemaext.Absent), schemaext.Complete, schemaext.ErrInvalidValue},
		{"index on absent table", base.Tables[0].Current, schemaext.Absent, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := base.Clone()
			request.Tables[0].Current = test.current
			request.Tables[0].CurrentKnowledge.State = test.existence
			result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
			c.Assert(request.Tables[0].ResolvedFacets, qt.HasLen, 0)
		})
	}
}

func TestIndexPreparationRefusesCompetingSourceSettings(t *testing.T) {
	for _, test := range []struct {
		name        string
		indexType   string
		granularity int
	}{
		{name: "undecoded type", indexType: "minmax"},
		{name: "undecoded granularity", granularity: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := indexRequest()
			request.Tables[0].Desired.Indexes[0].Type = test.indexType
			request.Tables[0].Desired.Indexes[0].Granularity = test.granularity
			result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
		})
	}
}

func TestIndexPreparationUsesStructuralObservedOwner(t *testing.T) {
	c := qt.New(t)
	request := indexRequest()
	table := &request.Tables[0]
	table.Desired.Table.Schema = "tenant.archive"
	table.Desired.Table.Name = "events.2026"
	table.Subject = objectidentity.NewBuilder(request.Identifiers).TableParts("tenant.archive", "events.2026")
	table.Current.Table.Schema = "tenant.archive"
	table.Current.Table.Name = "events.2026"
	table.Current.Indexes[0].Schema = "tenant.archive"
	table.Current.Indexes[0].TableName = "events.2026"
	result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
	c.Assert(err, qt.IsNil)
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](result.Tables[0].ResolvedFacets[0].Values, chschema.IndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, (&chschema.ObservedIndex{IndexType: "set(100)", Granularity: 64}).Desired())
}
