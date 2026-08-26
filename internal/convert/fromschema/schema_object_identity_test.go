package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

func TestFromSchemaObjects_PreservesReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		fromschema.FromDomain(schemamodel.Domain{Name: "tenant.data", BaseType: "TEXT"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
	c.Assert(
		fromschema.FromDomain(schemamodel.Domain{Schema: "tenant", Name: "data", BaseType: "TEXT"}).Name,
		qt.Equals,
		"tenant.data",
	)
	c.Assert(
		fromschema.FromCompositeType(schemamodel.CompositeType{Name: "tenant.data"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
	c.Assert(
		fromschema.FromRange(schemamodel.Range{Name: "tenant.data", Subtype: "integer"}).Name,
		qt.Equals,
		`"tenant.data"`,
	)
}
