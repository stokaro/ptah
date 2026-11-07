package providerhost

// White-box testing required: the engine registration a provider's planning
// declaration produces, the host's choice to hand an engine its tables and
// the frozen planning contract are private adapter state. The subprocess
// tests in integration/providercli cover the transport.

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/featureplan"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func TestTableOwnershipRegistration_HappyPath(t *testing.T) {
	t.Run("a service that owns tables", func(t *testing.T) {
		c := qt.New(t)
		target := yamlIdentityTarget()
		target.engine.Planning = []*protocol.ModelPlanning{{Id: "tables", OwnsTables: true}}
		var registered engine.Provider

		target.registerPlanning(&registered)

		c.Assert(registered.Planning, qt.HasLen, 1)
		c.Assert(registered.Planning[0].OwnsTables, qt.IsTrue)
		c.Assert(registered.Planning[0].ParentKinds, qt.HasLen, 0)
		c.Assert(target.assessesParents(), qt.IsTrue)
	})
	t.Run("a service with a parent kind", func(t *testing.T) {
		c := qt.New(t)
		target := yamlIdentityTarget()
		target.engine.Planning = []*protocol.ModelPlanning{{Id: "settings", ParentKinds: []string{"example.org/fixture/value"}}}
		var registered engine.Provider

		target.registerPlanning(&registered)

		c.Assert(registered.Planning[0].OwnsTables, qt.IsFalse)
		c.Assert(target.assessesParents(), qt.IsTrue)
	})
	t.Run("a service that plans changes alone", func(t *testing.T) {
		c := qt.New(t)
		target := yamlIdentityTarget()
		target.engine.Planning = []*protocol.ModelPlanning{{Id: "changes", ChangeKinds: []string{"example.org/fixture/change"}}}

		c.Assert(target.assessesParents(), qt.IsFalse)
	})
}

// Table ownership is part of a service's frozen planning contract, and a
// service that does not own tables keeps the contract it had without the
// field, so other providers' model identities do not move.
func TestTableOwnershipContract_HappyPath(t *testing.T) {
	c := qt.New(t)
	plain := &protocol.ModelPlanning{Id: "plan", ParentKinds: []string{"parent"}}
	owner := &protocol.ModelPlanning{Id: "plan", ParentKinds: []string{"parent"}, OwnsTables: true}

	plainJSON, err := json.Marshal(planningContracts([]*protocol.ModelPlanning{plain}))
	c.Assert(err, qt.IsNil)
	ownerJSON, err := json.Marshal(planningContracts([]*protocol.ModelPlanning{owner}))
	c.Assert(err, qt.IsNil)

	c.Assert(string(plainJSON), qt.Equals, `[{"context":[],"id":"plan","changes":[],"parents":["parent"],"operations":[]}]`)
	c.Assert(string(ownerJSON), qt.Equals, `[{"context":[],"id":"plan","changes":[],"parents":["parent"],"operations":[],"owns_tables":true}]`)
}

// ownershipRequest is a planning request of target fixture that tells its
// service it owns tables, with no table to plan.
func ownershipRequest() featureplan.Request {
	return featureplan.Request{Target: "fixture", OwnsTables: true, Identifiers: identifier.Semantics{TableNames: identifier.ComparisonExact,
		ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}}
}

func TestTableOwnershipRequest_HappyPath(t *testing.T) {
	c := qt.New(t)
	service, _ := planningFixture(c)
	service.declaration.OwnsTables = true

	wire, err := service.planningRequest(ownershipRequest())

	c.Assert(err, qt.IsNil)
	c.Assert(wire.GetPlanningId(), qt.Equals, "plan")
	c.Assert(wire.GetParentKinds(), qt.HasLen, 0)
}

// A request that tells a service it owns tables is refused when the
// service's declaration does not say so: the SDK reads ownership from the
// declaration, so the wire could not carry the claim.
func TestTableOwnershipRequest_FailurePath(t *testing.T) {
	c := qt.New(t)
	service, _ := planningFixture(c)

	wire, err := service.planningRequest(ownershipRequest())

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*planning service plan does not own tables`)
	c.Assert(wire, qt.IsNil)
}
