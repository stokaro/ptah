package goschematodb_test

import (
	"math"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

func indexCreationRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse", Creations: chprepare.Service{}}},
		Codecs:      append(chschema.Codecs(), chschema.IndexCodecs()...),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.TableKind, chschema.IndexKind}, Service: chconvert.Service{}}},
		Properties:  []engine.PropertySource{{Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{}}},
	}))
}

func TestDocumentProjectionDecodesIndexPropertiesBeforeCreation(t *testing.T) {
	c := qt.New(t)
	source := indexCreationDocument()
	source.Indexes[0].Facets = schemaext.Facets{}
	source.Indexes[0].Overrides = map[string]map[string]string{"clickhouse": {"type.state": "default", "granularity.state": "default"}}
	source.Indexes[1].Facets = schemaext.Facets{}
	source.Indexes[1].Type = "set(100)"
	source.Indexes[1].Overrides = map[string]map[string]string{"clickhouse": {"granularity": "18446744073709551615"}}
	projected, err := goschematodb.ToDBSchema(t.Context(), source, "clickhouse", indexCreationRuntime())
	c.Assert(err, qt.IsNil)
	for i, want := range []*chschema.ObservedIndex{
		{IndexType: "minmax", Granularity: 1},
		{IndexType: "set(100)", Granularity: math.MaxUint64},
	} {
		value, found, err := schemaext.FacetAs[*chschema.ObservedIndex](projected.Indexes[i].Facets, chschema.IndexKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(value, qt.DeepEquals, want)
		c.Assert(projected.Indexes[i].Method, qt.Equals, "")
		c.Assert(source.Indexes[i].Facets.IsZero(), qt.IsTrue)
	}
	c.Assert(source.Indexes[1].Type, qt.Equals, "set(100)")
	c.Assert(source.Indexes[1].Overrides["clickhouse"]["granularity"], qt.Equals, "18446744073709551615")
}

func indexCreationDocument() *schemamodel.Database {
	defaulted := must.Must(schemaext.NewFacets(&chschema.DesiredIndex{}))
	explicit := must.Must(schemaext.NewFacets((&chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}).Desired()))
	explicit = must.Must(explicit.WithTargetScope(chschema.IndexKind, "clickhouse"))
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "events.2026", Schema: "tenant.archive", StructName: "Archive", Engine: "Memory"},
			{Name: "events", Schema: "tenant.archive", StructName: "Current", Engine: "Memory"},
		},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "Archive", Type: "UInt64"}, {Name: "id", StructName: "Current", Type: "UInt64"},
		},
		Indexes: []schemamodel.Index{
			{Name: "by.id", TableName: schemamodel.QualifyTableName("tenant.archive", "events.2026"), Fields: []string{"id"}, Facets: defaulted},
			{Name: "by.id", StructName: "Current", Fields: []string{"id"}, Facets: explicit},
		},
	}
}

func TestDocumentProjectionKeepsIndexOwnersAndSettingsAcrossConversion(t *testing.T) {
	c := qt.New(t)
	runtime := indexCreationRuntime()
	source := indexCreationDocument()
	before := indexCreationDocument()
	projected, err := goschematodb.ToDBSchema(t.Context(), source, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(projected.Indexes, qt.HasLen, 2)
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	for i, test := range []struct {
		table string
		want  *chschema.ObservedIndex
	}{
		{"events.2026", &chschema.ObservedIndex{IndexType: "minmax", Granularity: 1}},
		{"events", &chschema.ObservedIndex{IndexType: "set(100)", Granularity: math.MaxUint64}},
	} {
		value, found, err := schemaext.FacetAs[*chschema.ObservedIndex](projected.Indexes[i].Facets, chschema.IndexKind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(value, qt.DeepEquals, test.want)
		c.Assert(projected.Indexes[i].TableName, qt.Equals, test.table)
		c.Assert(projected.Indexes[i].Schema, qt.Equals, "tenant.archive")
		c.Assert(projected.FeatureCoverage.Lookup(chschema.IndexKind, builder.IndexParts("tenant.archive", test.table, "by.id")).State, qt.Equals, schemaext.Complete)
	}
	c.Assert(projected.FeatureCoverage.Lookup(chschema.IndexKind, builder.IndexParts("tenant.archive", "absent", "by.id")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(projected.Indexes[1].Facets.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
	restored, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), projected, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Indexes, qt.HasLen, 2)
	again, err := goschematodb.ToDBSchema(t.Context(), restored, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(again.Indexes, qt.DeepEquals, projected.Indexes)
	c.Assert(again.FeatureCoverage, qt.DeepEquals, projected.FeatureCoverage)
	c.Assert(source, qt.DeepEquals, before)
}

func TestDocumentProjectionPreservesIndexKnowledgeLimits(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			runtime := indexCreationRuntime()
			source := indexCreationDocument()
			models := runtime.Codecs().Definitions()
			position := slices.IndexFunc(models, func(model schemaext.CodecIdentity) bool {
				return model.Kind == chschema.IndexKind && model.Representation == schemaext.Desired
			})
			c.Assert(position >= 0, qt.IsTrue)
			subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).IndexParts("tenant.archive", "events.2026", "by.id")
			limit := schemaext.Knowledge{State: state, Reason: "source did not describe all index settings"}
			source.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired,
				[]schemaext.KindCoverage{{Model: models[position], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}},
				[]schemaext.SubjectCoverage{{Kind: chschema.IndexKind, Subject: subject, Knowledge: limit}},
			))
			projected, err := goschematodb.ToDBSchema(t.Context(), source, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(projected.FeatureCoverage.Lookup(chschema.IndexKind, subject), qt.DeepEquals, limit)
			restored, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), projected, "clickhouse", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(restored.FeatureCoverage.Lookup(chschema.IndexKind, subject), qt.DeepEquals, limit)
			c.Assert(source.FeatureCoverage.Lookup(chschema.IndexKind, subject), qt.DeepEquals, limit)
		})
	}
}
