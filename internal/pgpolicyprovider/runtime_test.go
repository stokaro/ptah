package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicyprovider"
)

// newRuntime selects the owner beside a provider of each of its targets and
// of spanner, which speaks the PostgreSQL dialect without row security.
func newRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	targets := []engine.Target{{Name: "spanner"}}
	for _, name := range pgpolicyprovider.Targets() {
		targets = append(targets, engine.Target{Name: name})
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/targets", Targets: targets}, pgpolicyprovider.Provider())
	c.Assert(err, qt.IsNil)
	return runtime
}

var postgres = identifier.ForDialect("postgres")

func desiredPolicy(c *qt.C, table, name string, policy pgpolicy.DesiredPolicy) schemaext.Object {
	c.Helper()
	return must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("app", table, name), policy))
}

func observedPolicy(c *qt.C, table, name string, policy pgpolicy.ObservedPolicy) schemaext.Object {
	c.Helper()
	return must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("app", table, name), policy))
}

// tableRef is the identity of the table a policy reference names.
func tableRef(table string) objectidentity.ID {
	return pgpolicy.Table(pgpolicy.PolicyRef("app", table, "unused"))
}

func surviving(table string) schemaext.ParentState {
	return schemaext.ParentState{Subject: tableRef(table), Desired: true, Current: true}
}

func objects(c *qt.C, values ...schemaext.Object) schemaext.Objects {
	c.Helper()
	return must.Must(schemaext.NewObjects(values...))
}

func complete(c *qt.C, representation schemaext.Representation) schemaext.Coverage {
	c.Helper()
	return must.Must(pgpolicy.CompleteCoverage(representation))
}

// uninspected enrolls the policy model as a source that could not describe it.
func uninspected(c *qt.C, representation schemaext.Representation) schemaext.Coverage {
	c.Helper()
	return must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, representation,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
}

// comparison is one object comparison on postgres with both sides complete
// unless a test replaces a coverage.
func comparison(c *qt.C, desired, current []schemaext.Object, parents ...schemaext.ParentState) schemaext.ObjectComparisonRequest {
	c.Helper()
	return schemaext.ObjectComparisonRequest{
		Target: "postgres", Identifiers: postgres,
		Desired: schemaext.ObjectState{Objects: objects(c, desired...), Coverage: complete(c, schemaext.Desired)},
		Current: schemaext.ObjectState{Objects: objects(c, current...), Coverage: complete(c, schemaext.Observed)},
		Parents: parents,
	}
}

func TestProvider_RegistersWithItsTargets(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(engine.Provider{ID: "example.org/targets", Targets: []engine.Target{{Name: "postgres"}}}, pgpolicyprovider.Provider())

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
	c.Assert(pgpolicyprovider.Targets(), qt.DeepEquals, []string{"postgres", "cockroachdb", "yugabytedb"})
}
