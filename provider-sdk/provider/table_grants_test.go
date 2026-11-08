package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// grantsExchange is planningExchange on an engine that carries GRANT common
// objects, with table grants negotiated: the altered table, held as "a.b"."C",
// carries the one grant its read found on it.
func grantsExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	d.Features = append(d.Features, provider.CommonObjectsFeature, provider.TableGrantsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.CommonObjectsFeature, provider.TableGrantsFeature)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT}}
	request.TargetFeatures = append(request.TargetFeatures, provider.TableGrantsFeature)
	request.Tables[0].CurrentGrants = &protocol.PlanningTableGrants{Complete: true, Grants: []*protocol.ObservedGrant{heldTableGrant("reader", "SELECT")}}
	return d, request, result
}

// heldTableGrant is a grant of privilege to role on the table grantsExchange
// holds.
func heldTableGrant(role, privilege string) *protocol.ObservedGrant {
	return &protocol.ObservedGrant{Role: role, Privilege: privilege, Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "C"}}
}

func TestTableGrants_HappyPath(t *testing.T) {
	t.Run("the grants a read found on the held table", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		column := heldTableGrant("writer", "UPDATE")
		column.Columns, column.WithGrantOption, column.GrantedBy = []string{"id"}, true, "admin"
		implicit := heldTableGrant("owner", "SELECT")
		implicit.Implicit, implicit.Target.Engine = true, "fixture"
		request.Tables[0].CurrentGrants.Grants = append(request.Tables[0].CurrentGrants.Grants, column, implicit)

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	})
	t.Run("a held table without grants", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		request.Tables[0].CurrentGrants.Grants = nil

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	})
	t.Run("a read that did not describe grants", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		request.Tables[0].CurrentGrants = &protocol.PlanningTableGrants{}

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	})
	t.Run("a host that does not list the feature sends none", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		unlisted := slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.TableGrantsFeature })
		request.TargetFeatures = withoutFeature(request.TargetFeatures, provider.TableGrantsFeature)
		request.Tables[0].CurrentGrants = nil

		negotiated := provider.NegotiatedDescription(d, unlisted)

		c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.TableGrantsFeature)
		c.Assert(provider.ValidatePlannedFeatures(negotiated, request, result), qt.IsNil)
	})
}

// The grants travel in a planning request alone: a table preparation of an
// engine that negotiated them sends a held table without grants, which its
// validation reads with the planning table rules, and is valid.
func TestTableGrantsStayInPlanning_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := preparationExchange()
	d.Features = append(d.Features, provider.CommonObjectsFeature, provider.TableGrantsFeature)
	engine := d.Engines[0]
	engine.Features = append(engine.Features, provider.CommonObjectsFeature, provider.TableGrantsFeature)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_GRANT}}
	request.TargetFeatures = append(request.TargetFeatures, provider.TableGrantsFeature)

	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	c.Assert(provider.ValidatePreparedTables(d, request, result), qt.IsNil)
}

// A host that lists the feature and not the common objects that carry GRANT
// reads no grant to send, so the negotiation takes the engine's feature away
// with its common objects, and the description stays valid.
func TestTableGrantsNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _, _ := grantsExchange()
	listed := withoutFeature(provider.TwoWayFeatures(), provider.CommonObjectsFeature)

	negotiated := provider.NegotiatedDescription(d, listed)

	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.TableGrantsFeature)
	c.Assert(negotiated.Features, qt.Contains, provider.TableGrantsFeature)
	c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
}

// Each grant names the held table and is a grant at all: a role, a
// privilege, a column list without repeats, and no entry repeated.
func TestTableGrantsContent_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name  string
		grant *protocol.ObservedGrant
		want  string
	}{
		{"a grant without a role", heldTableGrant(" ", "SELECT"), `.*a grant on table C needs a role and a privilege`},
		{"a grant without a privilege", heldTableGrant("reader", ""), `.*a grant on table C needs a role and a privilege`},
		{"a grant on another table", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT", Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "D"}},
			`.*a grant listed for table C names another object`},
		{"a grant on the table in another schema", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT", Target: &protocol.Reference{Kind: "table", Name: "C"}},
			`.*a grant listed for table C names another object`},
		{"a grant on a view of the name", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT", Target: &protocol.Reference{Kind: "view", Schema: "a.b", Name: "C"}},
			`.*a grant listed for table C names another object`},
		{"a grant on a path below the table", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT",
			Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "C", Path: []string{"C"}}}, `.*a grant listed for table C names another object`},
		{"a grant on a table of another engine", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT",
			Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "C", Engine: "other"}}, `.*a grant listed for table C names another object`},
		{"a grant without a target", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT"}, `.*a grant listed for table C names another object`},
		{"a repeated column", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT", Columns: []string{"id", "id"},
			Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "C"}}, `.*an empty or repeated column`},
		{"a repeated grant", &protocol.ObservedGrant{Role: "reader", Privilege: "SELECT", GrantedBy: "admin", WithGrantOption: true,
			Target: &protocol.Reference{Kind: "table", Schema: "a.b", Name: "C"}}, `.*a repeated grant on table C`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := grantsExchange()
			request.Tables[0].CurrentGrants.Grants = append(request.Tables[0].CurrentGrants.Grants, test.grant)

			err := provider.ValidatePlannedFeatures(d, request, result)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// The grants travel exactly when the table's current state does and both
// sides list the feature, and say nothing when the read did not describe
// grants.
func TestTableGrantsPresence_FailurePath(t *testing.T) {
	t.Run("grants the host did not negotiate", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		request.TargetFeatures = withoutFeature(request.TargetFeatures, provider.TableGrantsFeature)

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*table grants: the grants of table C were not negotiated`)
	})
	t.Run("a held table without its grants", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		request.Tables[0].CurrentGrants = nil

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*table grants: held table C needs its grants, complete or not`)
	})
	t.Run("grants of a table the request creates", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := grantsExchange()
		table := request.Tables[0]
		table.Action, table.Current = protocol.PlanningTable_CREATE, nil

		c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorMatches, `.*table grants: grants of a table the request does not hold`)
	})
	t.Run("grants a read did not describe", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := grantsExchange()
		request.Tables[0].CurrentGrants.Complete = false

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*table grants: table C lists grants the read did not describe`)
	})
	t.Run("grants on an outlined table", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := grantsExchange()
		d.Features = append(d.Features, provider.TableOutlinesFeature)
		engine := d.Engines[0]
		engine.Features = append(engine.Features, provider.TableOutlinesFeature)
		engine.Planning[0].ParentKinds, engine.Planning[0].Tables = nil, protocol.PlanningTables_PLANNING_TABLES_OUTLINED
		request.TargetFeatures = append(request.TargetFeatures, provider.TableOutlinesFeature)
		request.ParentKinds, request.Changes = nil, nil
		grants := request.Tables[0].CurrentGrants
		request.Tables[0] = provider.TableOutline(request.Tables[0])
		request.Tables[0].CurrentGrants = proto.CloneOf(grants)

		c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorMatches, `.*an outlined table carries more than its subject, action and the names of its states`)
	})
}

// The engine's feature needs the provider's and GRANT common objects, which
// are what a read reports the grants as.
func TestTableGrantsDescription_FailurePath(t *testing.T) {
	t.Run("an engine feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := grantsExchange()
		d.Features = withoutFeature(d.Features, provider.TableGrantsFeature)

		err := provider.ValidateDescription(d, nil)

		c.Assert(err, qt.ErrorMatches, `.*table grants: the engine's feature requires the provider's`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("an engine without GRANT common objects", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := grantsExchange()
		d.Engines[0].CommonObjects.Kinds = []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_ROLE}

		err := provider.ValidateDescription(d, nil)

		c.Assert(err, qt.ErrorMatches, `.*table grants: engine fixture carries no GRANT common objects`)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
}
