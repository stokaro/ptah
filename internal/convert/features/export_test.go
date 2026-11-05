package features_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

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
		files, err := goschematogo.Render(c.Context(), isolated, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())})
		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(files, qt.IsNil)
	}
}

func TestHCLExport_ReportsOpaqueFeatureLoss(t *testing.T) {
	c := qt.New(t)
	value := &facetValue{Number: 7, Representation: schemaext.Desired}
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(facetKind), "public", "sample")
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value})
	c.Assert(err, qt.IsNil)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	result, err := atlashclrender.Render(&schemamodel.Database{FeatureObjects: objects, Facets: facets}, atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 2)
	c.Assert(result.Diagnostics[0].Message, qt.Contains, "is not represented in HCL")
	c.Assert(result.Diagnostics[0].Path, qt.Contains, `features["example.org/facet"]`)
	c.Assert(result.Diagnostics[1].Message, qt.Equals, "feature facet example.org/facet is not represented in HCL")
}
