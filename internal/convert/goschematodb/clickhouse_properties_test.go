package goschematodb_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

func propertyDocument(properties map[string]string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Event", Overrides: map[string]map[string]string{"clickhouse": properties}}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "UInt64", Primary: true}},
	}
}

func TestDocumentProjectionResolvesEmptyAndDefaultKeys(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]string
		primary    string
		key        bool
	}{
		{"inherited", map[string]string{"order_by": "id"}, "id", true},
		{"empty", map[string]string{"order_by": "id", "primary_key": ""}, "", false},
		{"default", map[string]string{"order_by": "id", "primary_key.state": "default"}, "id", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := propertyDocument(test.properties)
			projected, err := goschematodb.ToDBSchema(t.Context(), source, "clickhouse", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			value, found, err := schemaext.FacetAs[*chschema.ObservedTable](projected.Tables[0].Facets, chschema.TableKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(value.Engine, qt.Equals, "MergeTree")
			c.Assert(value.PrimaryKey, qt.Equals, test.primary)
			c.Assert(projected.Tables[0].Columns[0].IsPrimaryKey, qt.Equals, test.key)
			subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts("", "events")
			c.Assert(projected.FeatureCoverage.Lookup(chschema.TableKind, subject).State, qt.Equals, schemaext.Complete)
			c.Assert(source.Tables[0].Overrides["clickhouse"], qt.DeepEquals, test.properties)
			c.Assert(source.Tables[0].Facets.IsZero(), qt.IsTrue)
		})
	}
}

func TestDocumentProjectionPreservesExplicitKnowledgeLimits(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	source := propertyDocument(map[string]string{"order_by": "id"})
	models := runtime.Codecs().Definitions()
	index := slices.IndexFunc(models, func(model schemaext.CodecIdentity) bool {
		return model.Kind == chschema.TableKind && model.Representation == schemaext.Desired
	})
	c.Assert(index >= 0, qt.IsTrue)
	source.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: models[index], Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "source omitted a storage clause"}}}, nil))

	projected, err := goschematodb.ToDBSchema(t.Context(), source, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts("", "events")
	c.Assert(projected.FeatureCoverage.Lookup(chschema.TableKind, subject), qt.DeepEquals, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "source omitted a storage clause"})
}

// A current document predicts creation defaults even when it carries no platform
// properties. Adding explicit intent must compare against that predicted state.
func TestCompareDocumentsRetainsDefaultsFromCommonCurrentTable(t *testing.T) {
	c := qt.New(t)
	current := propertyDocument(nil)
	desired := propertyDocument(map[string]string{"settings": "index_granularity = 4096"})
	diff, err := schemadiff.CompareSchemas(t.Context(), desired, current, "clickhouse", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
}
