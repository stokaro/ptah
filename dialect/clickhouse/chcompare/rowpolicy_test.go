package chcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// rowPolicySemantics are the rules of a comparison against the connection's
// database `app`.
func rowPolicySemantics() identifier.Semantics {
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "app"
	return semantics
}

func complete() schemaext.Knowledge { return schemaext.Knowledge{State: schemaext.Complete} }

func rowPolicyState(representation schemaext.Representation, knowledge schemaext.Knowledge, objects ...schemaext.Object) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(chschema.RowPolicyCoverage(representation, knowledge, nil))}
}

func declared(database string, policy chschema.DesiredRowPolicy) schemaext.Object {
	return must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef(database, "orders", "tenant"), policy))
}

func observed(policy chschema.ObservedRowPolicy) schemaext.Object {
	return must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("app", "orders", "tenant"), policy))
}

func compareRowPolicies(c *qt.C, desired, current schemaext.ObjectState, parents ...schemaext.ParentState) schemaext.ObjectComparisonResult {
	c.Helper()
	return must.Must(chcompare.RowPolicyService{}.CompareObjects(c.Context(), schemaext.ObjectComparisonRequest{
		Target: "clickhouse", Identifiers: rowPolicySemantics(), Kinds: []schemaext.Kind{chschema.RowPolicyKind},
		Desired: desired, Current: current, Parents: parents,
	}))
}

var tenantPolicy = chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive,
	Roles: chschema.RoleSelection{Names: []string{"alice"}}}

// A declaration that leaves the database to the connection, omits the
// default composition and was spelled differently matches the policy the
// server reports, so an unchanged policy plans nothing.
func TestRowPolicyComparisonMatchesAnUnchangedPolicy(t *testing.T) {
	c := qt.New(t)
	desired := rowPolicyState(schemaext.Desired, complete(), declared("", chschema.DesiredRowPolicy{
		Filter: new("tenant=1"), NormalizedFilter: new("tenant = 1"), Roles: chschema.RoleSelection{Names: []string{"alice"}}}))

	result := compareRowPolicies(c, desired, rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy)))

	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// A created, a changed and a dropped policy each become one change carrying
// both operands and the owner's assessment.
func TestRowPolicyComparisonReportsEachTransition(t *testing.T) {
	restrictive := chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	for _, test := range []struct {
		name          string
		desired       []chschema.DesiredRowPolicy
		current       []chschema.ObservedRowPolicy
		before, after bool
		access        schemaext.Access
	}{
		{"a creation", []chschema.DesiredRowPolicy{restrictive}, nil, false, true, schemaext.AccessUnknown},
		{"a change in place", []chschema.DesiredRowPolicy{restrictive}, []chschema.ObservedRowPolicy{tenantPolicy}, true, true, schemaext.AccessNarrows},
		{"a drop", nil, []chschema.ObservedRowPolicy{tenantPolicy}, true, false, schemaext.AccessUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var desiredObjects, currentObjects []schemaext.Object
			for _, policy := range test.desired {
				desiredObjects = append(desiredObjects, declared("app", policy))
			}
			for _, policy := range test.current {
				currentObjects = append(currentObjects, observed(policy))
			}

			result := compareRowPolicies(c, rowPolicyState(schemaext.Desired, complete(), desiredObjects...),
				rowPolicyState(schemaext.Observed, complete(), currentObjects...))

			c.Assert(result.Changes, qt.HasLen, 1)
			change, ok := result.Changes[0].Value.(*chdiff.RowPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(change.Before != nil, qt.Equals, test.before)
			c.Assert(change.After != nil, qt.Equals, test.after)
			c.Assert(change.Access.Access, qt.Equals, test.access)
			c.Assert(result.Changes[0].Subject.Key(), qt.Equals, chschema.RowPolicyRefWith(rowPolicySemantics(), "app", "orders", "tenant").Key())
		})
	}
}

// A source that does not describe row policies has not asked for one to be
// dropped: the server's policy is adopted into the effective declaration and
// recorded as known, and nothing is planned.
func TestRowPolicyComparisonKeepsAPolicyTheSourceCannotDescribe(t *testing.T) {
	c := qt.New(t)
	uninspected := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source states no row policies"}

	result := compareRowPolicies(c, rowPolicyState(schemaext.Desired, uninspected),
		rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy)))

	c.Assert(result.Changes, qt.HasLen, 0)
	adopted, found, err := result.Desired.Objects.Get(chschema.RowPolicyRef("app", "orders", "tenant"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted.Value.Equal(tenantPolicy.Desired()), qt.IsTrue)
	c.Assert(result.Desired.Coverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("app", "orders", "tenant")).State, qt.Equals, schemaext.Complete)
}

// A declared policy whose presence on the server was not established is
// undecided rather than created, and so is one the source could not describe
// completely.
func TestRowPolicyComparisonLeavesUnestablishedStateUndecided(t *testing.T) {
	limited := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "defined in users.xml"}
	ref := chschema.RowPolicyRef("app", "orders", "tenant")
	declaredState := rowPolicyState(schemaext.Desired, complete(), declared("app", chschema.DesiredRowPolicy{}))
	heldState := rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy))
	for _, test := range []struct {
		name             string
		desired, current schemaext.ObjectState
	}{
		{"an unread namespace", declaredState, rowPolicyState(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "access denied"})},
		{"a policy the read could not describe", declaredState, schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(observed(tenantPolicy))),
			Coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, complete(),
				[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: ref, Knowledge: limited}}))}},
		{"a policy the source could not describe completely", schemaext.ObjectState{Objects: declaredState.Objects,
			Coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, complete(),
				[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: ref, Knowledge: limited}}))}, heldState},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compareRowPolicies(c, test.desired, test.current)

			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Kind, qt.Equals, chschema.RowPolicyKind)
		})
	}
}

// A policy whose table the plan creates or removes belongs to that table's
// transition, so the comparison reports no change for it.
func TestRowPolicyComparisonLeavesPoliciesOfAChangedTableToItsTransition(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.NewBuilder(rowPolicySemantics()).TablePartsVerbatim("app", "orders")

	result := compareRowPolicies(c, rowPolicyState(schemaext.Desired, complete()),
		rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy)),
		schemaext.ParentState{Subject: table, Current: true})

	c.Assert(result.Changes, qt.HasLen, 0)
}

// The service compares row policies on ClickHouse only, and refuses a batch
// it cannot read.
func TestRowPolicyComparison_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		kinds  []schemaext.Kind
		want   error
	}{
		{"another target", "postgres", []schemaext.Kind{chschema.RowPolicyKind}, ptaherr.ErrUnsupportedDialect},
		{"another kind", "clickhouse", []schemaext.Kind{chschema.RefreshKind}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := chcompare.RowPolicyService{}.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: test.target, Identifiers: rowPolicySemantics(), Kinds: test.kinds})

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
