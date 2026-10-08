package features_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematogo"
)

func scopedRenderFacets(target string) schemaext.Facets {
	facets := must.Must(schemaext.NewFacets(&facetValue{Number: 7, Representation: schemaext.Desired}))
	return must.Must(facets.WithTargetScope(facetKind, target))
}

func TestSchemaRenderingPreservesExcludedTableFacetBindings(t *testing.T) {
	foreign := scopedRenderFacets("mysql")
	excluded := must.Must(foreign.ForTarget(must.Must(schemaext.NewTargetSelection("postgres"))))
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
	}{
		{name: "source", facets: foreign},
		{name: "already projected", facets: excluded},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "items", StructName: "Item", Facets: test.facets}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Item", Type: "BIGINT"}},
			}
			c.Assert(builtin.ValidateSchema(database, "postgres"), qt.IsNil)
			statements, err := builtin.GetOrderedCreateStatements(database, "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, `CREATE TABLE "items"`)
			runtime := must.Must(builtin.New())
			result, err := runtime.RenderSchema(t.Context(), renderer.SchemaRequest{Target: "postgresql", Schema: database})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Statements, qt.DeepEquals, statements)
			c.Assert(database.Tables[0].Facets, qt.DeepEquals, test.facets)
		})
	}
}

func facetASTFixture() ([]ast.Node, map[string]*schemaext.Facets) {
	table := ast.NewCreateTable("items").AddColumn(ast.NewColumn("id", "BIGINT")).
		AddConstraint(ast.NewUniqueConstraint("items_id", "id"))
	index := ast.NewIndex("items_lookup", "items", "id")
	return []ast.Node{table, index}, map[string]*schemaext.Facets{
		"table": &table.Facets, "column": &table.Columns[0].Facets,
		"constraint": &table.Constraints[0].Facets, "index": &index.Facets,
	}
}

func TestASTRenderingScopesEveryFacetPlacement(t *testing.T) {
	for _, name := range []string{"table", "column", "constraint", "index"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			nodes, slots := facetASTFixture()
			want, err := builtin.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			facets := scopedRenderFacets("mysql")
			*slots[name] = facets
			got, err := builtin.RenderSQL("postgresql", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, want)
			c.Assert(*slots[name], qt.DeepEquals, facets)
		})
	}
}

func TestASTRenderingRefusesIncludedUnknownFacets(t *testing.T) {
	for _, name := range []string{"table", "column", "constraint", "index"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			nodes, slots := facetASTFixture()
			*slots[name] = scopedRenderFacets("postgresql")
			sql, err := builtin.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestASTRenderingRefusesToRestoreExcludedFacetsForAnotherTarget(t *testing.T) {
	for _, name := range []string{"table", "column", "constraint", "index"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			nodes, slots := facetASTFixture()
			facets := scopedRenderFacets("postgresql")
			*slots[name] = must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("mysql"))))
			sql, err := builtin.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*was excluded from this captured declaration.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

func TestGoExportRefusesUnrepresentableExcludedFacetBindings(t *testing.T) {
	c := qt.New(t)
	facets := scopedRenderFacets("mysql")
	excluded := must.Must(facets.ForTarget(must.Must(schemaext.NewTargetSelection("postgres"))))
	database := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Item", Facets: excluded}}}
	files, err := goschematogo.Render(c.Context(), database, goschematogo.Options{SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*has an excluded facet "example.org/facet".*`)
	c.Assert(files, qt.IsNil)
	c.Assert(database.Tables[0].Facets, qt.DeepEquals, excluded)
}
