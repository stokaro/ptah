package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func accessInspection() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	d, request, schema := commonInspection()
	d.Features = append(d.Features, provider.CommonAccessFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.CommonAccessFeature)
	engine.Identifiers = commonIdentifiers().Rules
	for _, kind := range []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_OBJECT_OWNER, protocol.CommonObjectSupport_ROLE_MEMBERSHIP} {
		engine.CommonObjects.Kinds = append(engine.CommonObjects.Kinds, kind)
		schema.CommonObjects.Coverage = append(schema.CommonObjects.Coverage, &protocol.CommonObjectCoverage{Kind: kind, Knowledge: complete()})
	}
	schema.CommonObjects.Roles[1].MemberOf = nil
	schema.CommonObjects.Memberships = []*protocol.RoleMembership{{Group: "readers", Member: "app"}, {Group: "readers", Member: "root"}}
	schema.CommonObjects.ObjectOwners = []*protocol.ObjectOwner{{Object: commonRef("table", "app", "users"), Owner: "root"},
		{Object: commonRef("column", "app", "id", "users"), Owner: "app", OwnerCanLogin: new(true)}}
	return d, request, schema
}

func TestCommonAccessPreservesUnknownLoginAndIndependentMemberships(t *testing.T) {
	c := qt.New(t)
	d, request, schema := accessInspection()
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	_, err := provider.InspectionValues(d, request, schema)
	c.Assert(err, qt.IsNil)
	c.Assert(schema.CommonObjects.ObjectOwners[0].OwnerCanLogin, qt.IsNil)
	c.Assert(*schema.CommonObjects.ObjectOwners[1].OwnerCanLogin, qt.IsTrue)
	schema.CommonObjects.ObjectOwners = append(schema.CommonObjects.ObjectOwners, &protocol.ObjectOwner{Object: commonRef("database", "", ""), Owner: "root"})
	schema.CommonObjects.ObjectOwners[0].OwnerCanLogin = new(false)
	// Observations preserve self-membership when the server allows it. A
	// declaration's policy must not erase facts from a read.
	schema.CommonObjects.Memberships = append(schema.CommonObjects.Memberships, &protocol.RoleMembership{Group: "root", Member: "root"})
	_, err = provider.InspectionValues(d, request, schema)
	c.Assert(err, qt.IsNil)
	legacy := provider.NegotiatedDescription(d, []string{provider.CommonObjectsFeature})
	c.Assert(provider.ValidateDescription(legacy, nil), qt.IsNil)
	c.Assert(legacy.Engines[0].CommonObjects.Kinds, qt.DeepEquals, allCommonKinds())
	_, err = provider.InspectionValues(legacy, request, schema)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

func TestCommonAccessRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ObservedCommonObjects)
	}{
		{"legacy membership mixed", func(o *protocol.ObservedCommonObjects) { o.Roles[1].MemberOf = []string{"readers"} }},
		{"repeated membership", func(o *protocol.ObservedCommonObjects) {
			o.Memberships = append(o.Memberships, proto.CloneOf(o.Memberships[0]))
		}},
		{"empty member", func(o *protocol.ObservedCommonObjects) { o.Memberships[0].Member = " " }},
		{"unknown membership coverage", func(o *protocol.ObservedCommonObjects) {
			o.Coverage[len(o.Coverage)-1].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "hidden memberships"}
		}},
		{"unknown ownership coverage", func(o *protocol.ObservedCommonObjects) {
			o.Coverage[len(o.Coverage)-2].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "hidden owners"}
		}},
		{"missing owner", func(o *protocol.ObservedCommonObjects) { o.ObjectOwners[0].Owner = "" }},
		{"foreign engine", func(o *protocol.ObservedCommonObjects) { o.ObjectOwners[0].Object.Engine = "other" }},
		{"conflicting folded identity", func(o *protocol.ObservedCommonObjects) {
			owner := proto.CloneOf(o.ObjectOwners[0])
			owner.Object.Name, owner.Owner = "USERS", "other"
			o.ObjectOwners = append(o.ObjectOwners, owner)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, schema := accessInspection()
			test.mutate(schema.CommonObjects)
			_, err := provider.InspectionValues(d, request, schema)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestCommonAccessNegotiationRequiresCommonObjects(t *testing.T) {
	c := qt.New(t)
	d, _, _ := accessInspection()
	negotiated := provider.NegotiatedDescription(d, []string{provider.CommonAccessFeature})
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	c.Assert(negotiated.Engines[0].CommonObjects, qt.IsNil)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.CommonAccessFeature)
}
