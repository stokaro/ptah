package goschema

// White-box testing required: these package-local resolvers are shared by every
// composite-schema conflict validator, and their edge cases cannot be isolated
// through Merge without coupling tests to unrelated reconciliation behavior.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

type definitionWithPrivateState struct {
	Name  string
	state int
}

func TestEquivalentDefinitions_HandlesUnexportedFields(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		equivalentDefinitions(
			definitionWithPrivateState{Name: "users", state: 1},
			definitionWithPrivateState{Name: "users", state: 1},
		),
		qt.IsTrue,
	)
	c.Assert(
		equivalentDefinitions(
			definitionWithPrivateState{Name: "users", state: 1},
			definitionWithPrivateState{Name: "users", state: 2},
		),
		qt.IsFalse,
	)
}

func TestTableScopeResolver_PreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	resolver := newTableScopeResolver([]Table{
		{StructName: "Literal", Name: "tenant.data"},
		{StructName: "Qualified", Schema: "tenant", Name: "data"},
	})

	c.Assert(resolver.resolve("Literal", `"tenant.data"`), qt.Equals, `"tenant.data"`)
	c.Assert(resolver.resolve("Literal", "tenant.data"), qt.Equals, "tenant.data")
	c.Assert(resolver.resolve("Qualified", `"tenant.data"`), qt.Equals, `"tenant.data"`)
	c.Assert(resolver.resolve("Qualified", "tenant.data"), qt.Equals, "tenant.data")
}
