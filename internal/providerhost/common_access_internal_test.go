package providerhost

// White-box testing required: these tests exercise the common-wire adapter independently of a fixture
// engine's object vocabulary. The public inspection path validates the wire
// before this conversion runs.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
)

func TestCommonAccessPreservesOwnersAndIndependentMemberships(t *testing.T) {
	c := qt.New(t)
	wire := &protocol.ObservedCommonObjects{
		Memberships: []*protocol.RoleMembership{{Group: "system", Member: "outside"}},
		ObjectOwners: []*protocol.ObjectOwner{
			{Object: ref("table", "app", "unknown"), Owner: "outside"},
			{Object: ref("table", "app", "disabled"), Owner: "disabled", OwnerCanLogin: new(false)},
			{Object: ref("table", "app", "enabled"), Owner: "enabled", OwnerCanLogin: new(true)},
		},
	}
	result := &catalog.Database{}
	c.Assert((reader{}).commonObjects(result, wire), qt.IsNil)
	c.Assert(result.RoleMemberships, qt.DeepEquals, []catalog.RoleMembership{{Role: "system", Member: "outside"}})
	c.Assert(result.ObjectOwners, qt.DeepEquals, []catalog.ObjectOwner{
		{Kind: "table", Schema: "app", Name: "unknown", Owner: "outside"},
		{Kind: "table", Schema: "app", Name: "disabled", Owner: "disabled", OwnerCanLogin: new(false)},
		{Kind: "table", Schema: "app", Name: "enabled", Owner: "enabled", OwnerCanLogin: new(true)},
	})
	*wire.ObjectOwners[2].OwnerCanLogin = false
	c.Assert(*result.ObjectOwners[2].OwnerCanLogin, qt.IsTrue)
}

func TestNativeGrantPreservesNamingBindingAndAccessFootprint(t *testing.T) {
	c := qt.New(t)
	description := scopeDescription()
	description.Engines[0].ScopeSelections[1].GrantObjectType = "RESOURCE POOL"
	description.Engines[0].CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_GRANT}}
	description.Features = []string{provider.CommonObjectsFeature}
	description.Engines[0].Features = []string{provider.CommonObjectsFeature}
	selections, err := newScopeSelections(description)
	c.Assert(err, qt.IsNil)
	target := &target{host: &Host{selections: selections, description: description}, engine: description.Engines[0]}
	node := ast.NewGrantPrivilege("app", "RESOURCE POOL", "fast.pool", []string{"SELECT"})
	node.DatabasePath = "/local"
	change, carried, err := target.commonObjectChange(node)
	c.Assert(err, qt.IsNil)
	c.Assert(carried, qt.IsTrue)
	grant := change.GetCommonObject().GetGrant()
	c.Assert(grant.GetTarget(), qt.CmpEquals(protocmp.Transform()), &protocol.Reference{Engine: "example", Kind: "resource pool", Database: "/local", Name: "fast.pool"})
	metadata, err := commonPlanningMetadata(featureplan.Request{Identifiers: selections.selected["example.org/pool"].rules}, []ast.Node{node}, selections)
	c.Assert(err, qt.IsNil)
	c.Assert(metadata, qt.HasLen, 1)
	c.Assert(metadata[0].Effects, qt.HasLen, 2)
	c.Assert(metadata[0].Effects[0].Action, qt.Equals, plangraph.Read)
	c.Assert(metadata[0].Effects[0].Subject.Kind, qt.Equals, objectidentity.Kind("example.org/pool"))
	c.Assert(metadata[0].Effects[0].Subject.Name.Source, qt.Equals, "fast.pool")
	c.Assert(metadata[0].Effects[1].Action, qt.Equals, plangraph.Alter)
	c.Assert(metadata[0].Effects[1].Subject.Kind, qt.Equals, objectidentity.Kind("object_access"))
	c.Assert(metadata[0].Effects[1].Subject.Namespace, qt.DeepEquals, objectidentity.NewParts(scopePart("example.org/pool")))
}

func TestNativeDropAccountsForTheRevokedAccessResource(t *testing.T) {
	c := qt.New(t)
	selections, err := newScopeSelections(scopeDescription())
	c.Assert(err, qt.IsNil)
	node := ast.NewRevokePrivilege("app", "TOPIC", "main.jobs", []string{"SELECT"})
	metadata, err := commonPlanningMetadata(featureplan.Request{Identifiers: selections.selected["example.org/queue"].rules}, []ast.Node{node}, selections)
	c.Assert(err, qt.IsNil)
	common := plangraph.Step[string]{ID: plangraph.StepID{Owner: "ptah.run/host", Name: "revoke"}, Payload: "revoke", Effects: metadata[0].Effects}
	drop := plangraph.Step[string]{ID: plangraph.StepID{Owner: "example.org/provider", Name: "drop"}, Payload: "drop", Effects: []plangraph.Effect{
		{Subject: common.Effects[0].Subject, Action: plangraph.Drop}, common.Effects[1],
	}}
	host := plangraph.Contribution[string]{Owner: "ptah.run/host", Steps: []plangraph.Step[string]{common}}
	owned := plangraph.Contribution[string]{Owner: "example.org/provider", Steps: []plangraph.Step[string]{drop}}
	rewrite := []plangraph.Rewrite{{Sources: []plangraph.StepID{common.ID}, Replacement: drop.ID}}
	result, err := plangraph.ScheduleRewritten(c.Context(), host, rewrite, owned)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Steps, qt.HasLen, 1)
	c.Assert(result.Steps[0].Payload, qt.Equals, "drop")
	owned.Steps[0].Effects = owned.Steps[0].Effects[:1]
	_, err = plangraph.ScheduleRewritten(c.Context(), host, rewrite, owned)
	c.Assert(err, qt.ErrorIs, plangraph.ErrInvalid)
}

func TestCommonAccessRetainsIndependentCoverage(t *testing.T) {
	c := qt.New(t)
	wire := &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{
		{Kind: protocol.CommonObjectSupport_ROLE, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
		{Kind: protocol.CommonObjectSupport_ROLE_MEMBERSHIP, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED}},
		{Kind: protocol.CommonObjectSupport_OBJECT_OWNER, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED}},
	}}
	result := &catalog.Database{}
	c.Assert((reader{}).commonObjects(result, wire), qt.IsNil)
	c.Assert(result.NotDescribed.Describes(coverage.Role), qt.IsTrue)
	c.Assert(result.NotDescribed.Describes(coverage.RoleMembership), qt.IsFalse)
	c.Assert(result.NotDescribed.Describes(coverage.ObjectOwner), qt.IsFalse)
}

func TestCommonAccessRequiresExplicitCoverage(t *testing.T) {
	for _, test := range []struct {
		name  string
		wire  *protocol.ObservedCommonObjects
		known bool
	}{
		{name: "missing common observations"},
		{name: "missing independent coverage", wire: &protocol.ObservedCommonObjects{}},
		{name: "explicit empty catalog", known: true, wire: &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{
			{Kind: protocol.CommonObjectSupport_ROLE_MEMBERSHIP, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
			{Kind: protocol.CommonObjectSupport_OBJECT_OWNER, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &catalog.Database{}
			c.Assert((reader{}).commonObjects(result, test.wire), qt.IsNil)
			c.Assert(result.NotDescribed.Describes(coverage.ObjectOwner), qt.Equals, test.known)
			c.Assert(result.NotDescribed.Describes(coverage.RoleMembership), qt.Equals, test.known)
		})
	}
}

func TestCommonOwnerRefusesLostCoordinates(t *testing.T) {
	for _, test := range []struct {
		name   string
		object *protocol.Reference
	}{
		{name: "foreign database", object: &protocol.Reference{Kind: "table", Name: "items", Database: "/foreign"}},
		{name: "catalog", object: &protocol.Reference{Kind: "table", Name: "items", Catalog: "other"}},
		{name: "child path", object: &protocol.Reference{Kind: "column", Name: "items", Path: []string{"parent"}}},
		{name: "overload", object: &protocol.Reference{Kind: "function", Name: "items", Overload: []string{"text"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			r := reader{session: &session{remote: &protocol.Session{Database: "/local"}}}
			wire := &protocol.ObservedCommonObjects{ObjectOwners: []*protocol.ObjectOwner{{Object: test.object, Owner: "app"}}}
			result := &catalog.Database{}
			err := r.commonObjects(result, wire)
			c.Assert(err, qt.ErrorMatches, ".*object ownership.*")
			c.Assert(result.ObjectOwners, qt.HasLen, 0)
		})
	}
}

func TestCommonOwnerAcceptsExactSessionCoordinates(t *testing.T) {
	for _, database := range []string{"", "/local"} {
		t.Run(database, func(t *testing.T) {
			c := qt.New(t)
			r := reader{session: &session{remote: &protocol.Session{Database: "/local"}}}
			wire := &protocol.ObservedCommonObjects{ObjectOwners: []*protocol.ObjectOwner{{Object: &protocol.Reference{Kind: "table", Name: "items", Database: database}, Owner: "app"}}}
			result := &catalog.Database{}
			c.Assert(r.commonObjects(result, wire), qt.IsNil)
			c.Assert(result.ObjectOwners, qt.HasLen, 1)
		})
	}
}
