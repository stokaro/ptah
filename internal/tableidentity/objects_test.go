package tableidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableidentity"
)

func TestCommonTableBindingPreservesOwnerQualifiedIdentity(t *testing.T) {
	for _, row := range []struct {
		name     string
		database objectidentity.Part
		path     objectidentity.Parts
	}{
		{"database", objectidentity.Part{Source: "tenant", Normalized: "tenant"}, objectidentity.Parts{}},
		{"path", objectidentity.Part{}, objectidentity.NewParts(objectidentity.Part{Source: "dir", Normalized: "dir"})},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			ref := objectidentity.ID{Kind: "example.org/owned", Database: row.database, Namespace: row.path,
				Parent: objectidentity.Part{Source: "Mixed", Normalized: "Mixed"}, Name: objectidentity.Part{Source: "child", Normalized: "child"}}
			bound := tableidentity.BindRef(ref, "postgres", identifier.ForDialect("postgres"))
			c.Assert(bound, qt.Equals, ref)
			ref.Database, ref.Namespace = objectidentity.Part{}, objectidentity.Parts{}
			bound = tableidentity.BindRef(ref, "postgres", identifier.ForDialect("postgres"))
			c.Assert(bound.Schema.Normalized, qt.Equals, "public")
		})
	}
}
