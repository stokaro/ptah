package types_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

func TestQualifiedSchemaObjectNamesPreserveReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	c.Assert(types.DBDomain{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBDomain{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(types.DBComposite{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBComposite{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(types.DBRange{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBRange{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(types.DBSequence{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBSequence{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(types.DBView{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBView{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(types.DBMatView{Name: "tenant.data"}.QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(types.DBMatView{Schema: "tenant", Name: "data"}.QualifiedName(), qt.Equals, "tenant.data")
}
