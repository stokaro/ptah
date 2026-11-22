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
	"ptah.run/engine"
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

// heldOrders is the parent of a policy on app.orders, a table both sides
// hold.
func heldOrders() schemaext.ParentState {
	return schemaext.ParentState{Subject: objectidentity.NewBuilder(rowPolicySemantics()).TablePartsVerbatim("app", "orders"), Desired: true, Current: true}
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

	result := compareRowPolicies(c, desired, rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy)), heldOrders())

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
				rowPolicyState(schemaext.Observed, complete(), currentObjects...), heldOrders())

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
		rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy)), heldOrders())

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

			result := compareRowPolicies(c, test.desired, test.current, heldOrders())

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

// rowPolicyRuntime selects the row policy owner on ClickHouse and nothing
// else, registered as engine/builtin registers it, so the engine checks each
// count the owner states against its changes and each change against the
// request.
func rowPolicyRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: chschema.Owner, Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: append(chschema.RowPolicyCodecs(), chdiff.RowPolicyCodecs()...),
		Comparisons: []engine.ObjectComparison{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.RowPolicyKind},
			ChangeKinds: []schemaext.Kind{chdiff.RowPolicyKind}, OutlivesParent: true, Service: chcompare.RowPolicyService{}}},
	}))
}

// declaredNamed and observedNamed are the policy name on app.orders, declared
// and held as tenantPolicy.
func declaredNamed(name string) schemaext.Object {
	return must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("app", "orders", name), *tenantPolicy.Desired()))
}

func observedNamed(name string) schemaext.Object {
	return must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("app", "orders", name), tenantPolicy))
}

// TestRowPolicyComparison_CountsWhatItCompares pins the counts the inventory
// reports for row policies: a policy both sides hold is matched whether or not
// it differs, a creation is added and a drop removed, and a policy the
// comparison keeps, cannot decide or leaves to its table's transition is
// counted on neither side, so a server that agrees counts what the
// declaration does.
func TestRowPolicyComparison_CountsWhatItCompares(t *testing.T) {
	limited := []schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("app", "orders", "tenant"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "defined in users.xml"}}}
	restrictive := chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	orders := objectidentity.NewBuilder(rowPolicySemantics()).TablePartsVerbatim("app", "orders")
	held := []schemaext.ParentState{{Subject: orders, Desired: true, Current: true}}
	tests := []struct {
		name             string
		desired, current schemaext.ObjectState
		parents          []schemaext.ParentState
		want             schemaext.ObjectCount
		changes          int
		undecided        int
	}{
		{name: "a policy that agrees", want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Matched: 1}, parents: held,
			desired: rowPolicyState(schemaext.Desired, complete(), declaredNamed("tenant")),
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))},
		{name: "a policy that differs", changes: 1, want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Matched: 1}, parents: held,
			desired: rowPolicyState(schemaext.Desired, complete(), declared("app", restrictive)),
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))},
		{name: "a creation, a drop and a match", changes: 2, want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Matched: 1, Added: 1, Removed: 1}, parents: held,
			desired: rowPolicyState(schemaext.Desired, complete(), declaredNamed("tenant"), declaredNamed("created")),
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"), observedNamed("dropped"))},
		{name: "a policy the desired source cannot describe", want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind}, parents: held,
			desired: rowPolicyState(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source states no row policies"}),
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))},
		{name: "a declaration the source could not describe completely", undecided: 1, want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind}, parents: held,
			desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(declaredNamed("tenant"))),
				Coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, complete(), limited))},
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))},
		{name: "a declaration on an unread namespace", undecided: 1, want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind}, parents: held,
			desired: rowPolicyState(schemaext.Desired, complete(), declaredNamed("tenant")),
			current: rowPolicyState(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "access denied"})},
		{name: "a held policy the read could not describe", want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind}, parents: held,
			desired: rowPolicyState(schemaext.Desired, complete()),
			current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(observedNamed("tenant"))),
				Coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, complete(), limited))}},
		{name: "a policy of a table the plan drops", want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind},
			desired: rowPolicyState(schemaext.Desired, complete()),
			current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant")),
			parents: []schemaext.ParentState{{Subject: orders, Current: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := rowPolicyRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: "clickhouse", Identifiers: rowPolicySemantics(), Desired: test.desired, Current: test.current, Parents: test.parents})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestRowPolicyComparisonPlansAPolicyItsTableOutlived compares a policy the
// server kept when its table was dropped as a policy of its own, whatever the
// plan does to a table of the name: one the declaration leaves out is
// dropped, and a declared one is altered in place, even where it agrees,
// because a table created under the name would otherwise create a policy the
// server already holds.
func TestRowPolicyComparisonPlansAPolicyItsTableOutlived(t *testing.T) {
	orders := objectidentity.NewBuilder(rowPolicySemantics()).TablePartsVerbatim("app", "orders")
	created := []schemaext.ParentState{{Subject: orders, Desired: true}}
	restrictive := chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	for _, test := range []struct {
		name          string
		parents       []schemaext.ParentState
		desired       []schemaext.Object
		before, after bool
		want          schemaext.ObjectCount
	}{
		{name: "an undeclared policy and no table of the name", before: true,
			want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Removed: 1}},
		{name: "an undeclared policy and a table the plan creates", parents: created, before: true,
			want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Removed: 1}},
		{name: "a declared policy that agrees", parents: created, desired: []schemaext.Object{declaredNamed("tenant")}, before: true, after: true,
			want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Matched: 1}},
		{name: "a declared policy that differs", parents: created, desired: []schemaext.Object{declared("app", restrictive)}, before: true, after: true,
			want: schemaext.ObjectCount{Kind: chdiff.RowPolicyKind, Matched: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := rowPolicyRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: "clickhouse", Identifiers: rowPolicySemantics(), Parents: test.parents,
				Desired: rowPolicyState(schemaext.Desired, complete(), test.desired...),
				Current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("app", "orders", "tenant").Key())
			change, ok := result.Changes[0].Value.(*chdiff.RowPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(change.Before != nil, qt.Equals, test.before)
			c.Assert(change.After != nil, qt.Equals, test.after)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}

// TestRowPolicyComparisonLeavesAnOutlivingPolicyUndecided neither drops nor
// adopts a policy the server kept for a dropped table when the desired source
// does not describe row policies: it has not asked for the drop, and the
// policy cannot be kept as declared beside a table the declaration does not
// hold.
func TestRowPolicyComparisonLeavesAnOutlivingPolicyUndecided(t *testing.T) {
	c := qt.New(t)
	uninspected := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source states no row policies"}

	result, err := rowPolicyRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "clickhouse", Identifiers: rowPolicySemantics(),
		Desired: rowPolicyState(schemaext.Desired, uninspected),
		Current: rowPolicyState(schemaext.Observed, complete(), observedNamed("tenant"))})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Subject.Key(), qt.Equals, chschema.RowPolicyRef("app", "orders", "tenant").Key())
	c.Assert(result.Undecided[0].Reason, qt.Matches, `the database holds this policy for table "orders", which it does not hold, .*`)
}

// TestRowPolicyComparisonReportsAPolicyOnARelationItDoesNotModel reports, and
// so keeps, a policy the read recorded as one it cannot describe below a
// table it does not hold, such as a policy on a view; one on a table both
// sides hold is decided beside its table and reported only when declared.
func TestRowPolicyComparisonReportsAPolicyOnARelationItDoesNotModel(t *testing.T) {
	c := qt.New(t)
	onView := chschema.RowPolicyRef("app", "active_orders", "tenant")
	onTable := chschema.RowPolicyRef("app", "orders", "tenant")
	reason := "the policy is on \"active_orders\", whose engine View the read does not hold as a table; the plan keeps the policy"
	current := schemaext.ObjectState{Coverage: must.Must(chschema.RowPolicyCoverage(schemaext.Observed, complete(), []schemaext.SubjectCoverage{
		{Kind: chschema.RowPolicyKind, Subject: onView, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason}},
		{Kind: chschema.RowPolicyKind, Subject: onTable, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "defined in users.xml"}},
	}))}

	result, err := rowPolicyRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "clickhouse", Identifiers: rowPolicySemantics(), Parents: []schemaext.ParentState{heldOrders()},
		Desired: rowPolicyState(schemaext.Desired, complete()), Current: current})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: chschema.RowPolicyKind, Subject: onView, Reason: reason}})
}

// TestRowPolicyComparison_RefusesAnOutlivingPolicySpelledTwoWays refuses a
// declaration that pairs with a policy the server kept for a dropped table
// under another spelling of its database: the change would name a policy the
// request does not hold.
func TestRowPolicyComparison_RefusesAnOutlivingPolicySpelledTwoWays(t *testing.T) {
	c := qt.New(t)
	orders := objectidentity.NewBuilder(rowPolicySemantics()).TablePartsVerbatim("app", "orders")

	result, err := chcompare.RowPolicyService{}.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "clickhouse", Identifiers: rowPolicySemantics(), Kinds: []schemaext.Kind{chschema.RowPolicyKind},
		Parents: []schemaext.ParentState{{Subject: orders, Desired: true}},
		Desired: rowPolicyState(schemaext.Desired, complete(), declared("", *tenantPolicy.Desired())),
		Current: rowPolicyState(schemaext.Observed, complete(), observed(tenantPolicy))})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*spell one policy two ways`)
	c.Assert(result.Changes, qt.HasLen, 0)
}
