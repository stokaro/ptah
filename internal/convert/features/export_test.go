package features_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

func TestNamedChangefeeds_GoExportRoundTrip(t *testing.T) {
	c := qt.New(t)
	feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ydbtopic.ConsumerSpec{{Name: "audit", SupportedCodecs: []string{"raw", "gzip"}}}}
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("shop", "items", feed))
	c.Assert(err, qt.IsNil)
	database := &schemamodel.Database{FeatureObjects: objects, Tables: []schemamodel.Table{{Name: "items", Schema: "shop", StructName: "Item"}}, Fields: []schemamodel.Field{{StructName: "Item", Name: "id", FieldName: "ID", Type: "BIGINT", Primary: true}}}
	files, err := goschematogo.Render(c.Context(), database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), "schema.go", string(files[0].Data))
	c.Assert(err, qt.IsNil)
	feeds, err := ydbschema.DesiredChangefeeds(parsed.FeatureObjects, "shop", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(feeds, qt.DeepEquals, []ydbschema.ChangefeedSpec{feed})
}

func TestFeatureRendering_RefusesFacetsAtEveryCommonScope(t *testing.T) {
	c := qt.New(t)
	database := facetedSchema(c)
	for i := range database.FacetSlots() {
		isolated := facetedSchema(c)
		slots := isolated.FacetSlots()
		selected := *slots[i]
		for _, slot := range slots {
			*slot = schemaext.Facets{}
		}
		*slots[i] = selected

		_, err := builtin.GetOrderedCreateStatementsWithCapabilities(isolated, "postgres", capability.Postgres18())
		c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
		files, err := goschematogo.Render(c.Context(), isolated, goschematogo.Options{SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(files, qt.IsNil)
	}
}

func TestFeatureRendering_RefusesOrphanAndWrongParent(t *testing.T) {
	c := qt.New(t)
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("", "missing", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}))
	c.Assert(err, qt.IsNil)
	database := &schemamodel.Database{FeatureObjects: objects}
	_, err = builtin.GetOrderedCreateStatementsWithCapabilities(database, "ydb", capability.YDB262())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	_, err = goschematogo.Render(c.Context(), database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	table := ast.NewCreateTable("items").AddColumn(ast.NewColumn("id", "Int64").SetPrimary())
	table.OwnedObjects = objects
	_, err = builtin.RenderSQLWithCapabilities("ydb", capability.YDB262(), table)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
}

func TestGoExport_RefusesDisabledChangefeed(t *testing.T) {
	c := qt.New(t)
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("", "items", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true}))
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), &schemamodel.Database{FeatureObjects: objects, Tables: []schemamodel.Table{{Name: "items"}}}, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(files, qt.IsNil)
}

func TestExportRefusesToRecreateRetainedReplicationState(t *testing.T) {
	c := qt.New(t)
	value := &ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "stream", Mode: "UPDATES", Format: "JSON"},
		RetainedReplication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ydbschema.ChangefeedRef("", "items", "stream"), Value: value})
	c.Assert(err, qt.IsNil)
	database := &schemamodel.Database{FeatureObjects: objects,
		Tables: []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true}}}
	// Complete-schema validation checks the same creation contract as export.
	c.Assert(builtin.ValidateSchemaWithCapabilities(database, "ydb", capability.YDB262()), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	files, err := goschematogo.Render(c.Context(), database, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "(?s).*cannot preserve the retained replication binding.*")
	c.Assert(files, qt.IsNil)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, "ydb", capability.YDB262())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.IsNil)
}

func TestHCLExport_ReportsOpaqueFeatureLoss(t *testing.T) {
	c := qt.New(t)
	value := &facetValue{Number: 7, Representation: schemaext.Desired}
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(facetKind), "public", "sample")
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value})
	c.Assert(err, qt.IsNil)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(&schemamodel.Database{FeatureObjects: objects, Facets: facets})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 2)
	c.Assert(result.Diagnostics[0].Message, qt.Contains, "is not represented in HCL")
	c.Assert(result.Diagnostics[0].Path, qt.Contains, `features["example.org/facet"]`)
	c.Assert(result.Diagnostics[1].Message, qt.Equals, "feature facet example.org/facet is not represented in HCL")
}
