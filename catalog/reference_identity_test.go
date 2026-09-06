package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

func TestQualifiedSchemaObjectNamesPreserveReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert(catalog.Domain{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.Domain{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(catalog.CompositeType{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.CompositeType{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(catalog.Range{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.Range{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(catalog.Sequence{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.Sequence{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(catalog.View{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.View{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(catalog.MaterializedView{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(catalog.MaterializedView{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
}
