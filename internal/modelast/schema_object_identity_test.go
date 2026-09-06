package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

func TestFromSchemaObjects_PreservesReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		modelast.FromDomain(schemamodel.Domain{Name: "tenant.data", BaseType: "TEXT"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
	c.Assert(
		modelast.FromDomain(schemamodel.Domain{Schema: "tenant", Name: "data", BaseType: "TEXT"}).Name,
		qt.Equals,
		"tenant.data",
	)
	c.Assert(
		modelast.FromCompositeType(schemamodel.CompositeType{Name: "tenant.data"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
	c.Assert(
		modelast.FromRange(schemamodel.Range{Name: "tenant.data", Subtype: "integer"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
}
