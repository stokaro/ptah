package oratypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/feature/oratype"
)

// TestPlanTypes_OrdersBothWindows pins the order a change set runs in: the
// domains it creates, the object types it creates and then the ones it
// replaces, each kind by name, before the tables; and the drops after them,
// domains first. Input order is the reverse of each, so the order is the
// plan's own.
func TestPlanTypes_OrdersBothWindows(t *testing.T) {
	c := qt.New(t)
	number := []oratype.Attribute{{Name: "n", Type: "NUMBER"}}
	request := planning(
		change(oratype.ObjectTypeKind, "gone_t", &oratype.ObjectTypeChange{Before: &oratype.ObservedObjectType{Attributes: number}}),
		change(oratype.DomainKind, "gone_d", &oratype.DomainChange{Before: &oratype.ObservedDomain{BaseType: "NUMBER"}}),
		change(oratype.ObjectTypeKind, "kept_t", &oratype.ObjectTypeChange{Before: &oratype.ObservedObjectType{Attributes: number},
			After: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER(5)"}}}, Differs: []string{oratype.DiffAttributes}}),
		change(oratype.ObjectTypeKind, "b_t", &oratype.ObjectTypeChange{After: &oratype.DesiredObjectType{Attributes: number}}),
		change(oratype.ObjectTypeKind, "a_t", &oratype.ObjectTypeChange{After: &oratype.DesiredObjectType{Attributes: number}}),
		change(oratype.DomainKind, "b_d", &oratype.DomainChange{After: &oratype.DesiredDomain{BaseType: "NUMBER"}}),
		change(oratype.DomainKind, "a_d", &oratype.DomainChange{After: &oratype.DesiredDomain{BaseType: "NUMBER", Comment: "first"}}),
	)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	creation, removal := windowsOf(stepsOf(c, result.Contributions))
	c.Assert(creation, qt.DeepEquals, []planned{
		{Subject: "domain a_d", Payload: &oratype.CreateDomain{Target: domainTarget("a_d"), BaseType: "NUMBER", Comment: "first"},
			Phase: featureplan.PhaseType, Effect: plangraph.Create},
		{Subject: "domain b_d", Payload: &oratype.CreateDomain{Target: domainTarget("b_d"), BaseType: "NUMBER"},
			Phase: featureplan.PhaseType, Effect: plangraph.Create},
		{Subject: "composite type a_t", Payload: &oratype.ReplaceObjectType{Target: objectTypeTarget("a_t"), Attributes: number},
			Phase: featureplan.PhaseType, Effect: plangraph.Create},
		{Subject: "composite type b_t", Payload: &oratype.ReplaceObjectType{Target: objectTypeTarget("b_t"), Attributes: number},
			Phase: featureplan.PhaseType, Effect: plangraph.Create},
		{Subject: "composite type kept_t", Payload: &oratype.ReplaceObjectType{Target: objectTypeTarget("kept_t"),
			Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER(5)"}}, Replaces: true},
			Phase: featureplan.PhaseType, Effect: plangraph.Alter},
	})
	c.Assert(removal, qt.DeepEquals, []planned{
		{Subject: "domain gone_d", Payload: &oratype.Drop{Target: domainTarget("gone_d")}, Phase: featureplan.PhaseType, Effect: plangraph.Drop},
		{Subject: "composite type gone_t", Payload: &oratype.Drop{Target: objectTypeTarget("gone_t")}, Phase: featureplan.PhaseType, Effect: plangraph.Drop},
	})
}

// windowsOf splits steps into the creation window's and the removal window's,
// each in order. A host puts the two windows apart, so their relative order in
// the owner's own schedule says nothing.
func windowsOf(steps []planned) (creation, removal []planned) {
	for _, step := range steps {
		if step.Effect == plangraph.Drop {
			removal = append(removal, step)
			continue
		}
		creation = append(creation, step)
	}
	return creation, removal
}

// TestPlanTypes_KeepsAChangedDomain pins that a surviving domain whose base
// type or NOT NULL changed plans no statement, and says so: Oracle's ALTER
// DOMAIN changes neither.
func TestPlanTypes_KeepsAChangedDomain(t *testing.T) {
	c := qt.New(t)
	request := planning(change(oratype.DomainKind, "d", &oratype.DomainChange{Before: &oratype.ObservedDomain{BaseType: "NUMBER"},
		After: &oratype.DesiredDomain{BaseType: "NUMBER(5)", NotNull: true}, Differs: []string{oratype.DiffBaseType, oratype.DiffNotNull}}))

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.HasLen, 0)
	c.Assert(result.Changes[0].Strategy, qt.Matches, `keep the domain as it is: .*`)
}

// TestPlanTypes_RefusesADomainOracle21CannotCreate pins the refusal of a
// creation the target has no statement for, naming the lexicographically
// first domain whatever the input order, and leaving an object type it can
// create unrefused.
func TestPlanTypes_RefusesADomainOracle21CannotCreate(t *testing.T) {
	c := qt.New(t)
	request := planning(
		change(oratype.ObjectTypeKind, "a_t", &oratype.ObjectTypeChange{After: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}}}),
		change(oratype.DomainKind, "z_d", &oratype.DomainChange{After: &oratype.DesiredDomain{BaseType: "NUMBER"}}),
		change(oratype.DomainKind, "m_d", &oratype.DomainChange{After: &oratype.DesiredDomain{BaseType: "NUMBER"}}),
	)
	request.Capabilities = capability.Oracle21()

	result, err := oratype.PlanService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(*result.Diagnostics[0].Change, qt.Equals, 2)
	c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.UnsupportedFeature)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(oratype.DomainChangeKind))
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, `oracle: CREATE DOMAIN m_d: this target does not create it, .*`)
}

// TestPlanDeclarations_CreatesATypeAnotherNamesFirst pins a whole-schema
// render: domains, then object types, each by name, except that a type
// another one names is created first.
func TestPlanDeclarations_CreatesATypeAnotherNamesFirst(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: planning().Target, Identifiers: planning().Identifiers, Capabilities: capability.Oracle23(),
		Objects: []schemaext.Object{
			declared("a_t", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "inner", Type: "c_t"}}}),
			declared("b_d", &oratype.DesiredDomain{BaseType: "NUMBER"}),
			declared("c_t", &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}}),
		}}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	steps := stepsOf(c, result.Contributions)
	subjects := make([]string, 0, len(steps))
	for _, step := range steps {
		subjects = append(subjects, step.Subject)
	}
	c.Assert(subjects, qt.DeepEquals, []string{"domain b_d", "composite type c_t", "composite type a_t"})
}
