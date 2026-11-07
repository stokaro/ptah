package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// namespaceSemantics are the identifier rules of an engine whose default
// schema is main.
func namespaceSemantics() identifier.Semantics {
	return identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact,
		IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceSchema}
}

// A read that lists its namespaces decides which declared schemas a plan
// creates, as a PostgreSQL read does.
func TestDeclaredNamespaces_HappyPath(t *testing.T) {
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "main"}, {Name: "kept"}, {Name: "app", Comment: "Application"}}}
	for _, row := range []struct {
		name string
		held []catalog.Schema
		want []schemamodel.Schema
	}{
		{"a read that holds none of them", nil, []schemamodel.Schema{{Name: "app", Comment: "Application"}, {Name: "kept"}}},
		{"a read that holds one", []catalog.Schema{{Name: "main"}, {Name: "kept"}}, []schemamodel.Schema{{Name: "app", Comment: "Application"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{Schemas: row.held}
			cov := compare.CoverageOf(desired, current)
			diff := &difftypes.SchemaDiff{}

			compare.DeclaredNamespaces(diff, desired, current.Schemas, cov, namespaceSemantics())

			c.Assert(diff.SchemasAdded, qt.DeepEquals, row.want)
			c.Assert(cov.UndecidedAdditions(), qt.HasLen, 0)
		})
	}
}

// A read that left the namespaces uninspected says nothing of them, so a
// declared schema is withheld and reported, not created.
func TestDeclaredNamespacesOfAnUninspectedRead(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}}
	current := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Object{Kind: coverage.Schema, Reason: "the read left namespaces uninspected"})}
	cov := compare.CoverageOf(desired, current)
	diff := &difftypes.SchemaDiff{}

	compare.DeclaredNamespaces(diff, desired, current.Schemas, cov, namespaceSemantics())

	c.Assert(diff.SchemasAdded, qt.HasLen, 0)
	c.Assert(cov.UndecidedAdditions(), qt.DeepEquals, []coverage.Object{{Kind: coverage.Schema, Name: "app", Reason: "the read left namespaces uninspected"}})
}
