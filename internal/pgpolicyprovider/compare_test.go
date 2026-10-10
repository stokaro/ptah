package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policycompare"
)

var (
	public = pgpolicy.RoleSelector{Keyword: pgpolicy.Public}
	reader = pgpolicy.RoleSelector{Name: "reader"}
	writer = pgpolicy.RoleSelector{Name: "writer"}
	app    = pgpolicy.RoleSelector{Name: "app"}
)

// held is the policy the server holds in most rows: FOR ALL TO PUBLIC, one
// clause, permissive.
func held(using string) pgpolicy.ObservedPolicy {
	return pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{public}, Using: new(using), Composition: pgpolicy.Permissive}
}

// TestComparePolicies_FoldsWhatPostgreSQLFolds pins the declarations that ask
// for the policy the server holds: PostgreSQL's defaults resolved, the role
// list a set, the clause spacing folded, and the server's own spelling of a
// clause and of a role keyword where a probe attached it. None of them is a
// change.
func TestComparePolicies_FoldsWhatPostgreSQLFolds(t *testing.T) {
	tests := []struct {
		name     string
		declared pgpolicy.DesiredPolicy
		observed pgpolicy.ObservedPolicy
	}{
		{name: "an omitted command, role list and composition", declared: pgpolicy.DesiredPolicy{Using: new("tenant_id = 1")},
			observed: held("tenant_id = 1")},
		{name: "the same values written out", observed: held("tenant_id = 1"), declared: pgpolicy.DesiredPolicy{
			Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{public}, Using: new("tenant_id = 1"), Composition: pgpolicy.Permissive}},
		{name: "roles in another order", declared: pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{writer, reader}, Using: new("true")},
			observed: pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{reader, writer}, Using: new("true"), Composition: pgpolicy.Permissive}},
		{name: "a clause the catalog wraps in parentheses", declared: pgpolicy.DesiredPolicy{Using: new("tenant_id = 1")},
			observed: held("(tenant_id = 1)")},
		{name: "a clause in the server's spelling", observed: held("((owner)::text = 'x'::text)"), declared: pgpolicy.DesiredPolicy{
			Using:      new("owner = 'x'"),
			Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{public}, Using: new("((owner)::text = 'x'::text)")}}},
		{name: "a role keyword the server resolved", observed: pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll,
			Roles: []pgpolicy.RoleSelector{app, reader}, Using: new("true"), Composition: pgpolicy.Permissive}, declared: pgpolicy.DesiredPolicy{
			Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}, reader}, Using: new("true"),
			Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{reader, app}, Using: new("true")}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, []schemaext.Object{desiredPolicy(c, "orders", "tenant", test.declared)},
				[]schemaext.Object{observedPolicy(c, "orders", "tenant", test.observed)}, surviving("orders"))

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestComparePolicies_FindsChanges pins each difference a comparison reports,
// with the access assessment it carries. A clause compared as declared text
// is a change even where the server would store the same tree, which is what
// the probe exists to answer, and a role keyword nobody resolved never equals
// the role name the catalog reports.
func TestComparePolicies_FindsChanges(t *testing.T) {
	stored := held("tenant_id = 1")
	resolved := pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{app}, Using: new("true"), Composition: pgpolicy.Permissive}
	rewritten := held("((owner)::text = 'x'::text)")
	selectOnly := pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Using: new("tenant_id = 1")}
	restrictive := pgpolicy.DesiredPolicy{Composition: pgpolicy.Restrictive, Using: new("tenant_id = 1")}
	forReader := pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{reader}, Using: new("tenant_id = 1")}
	forCurrentUser := pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}, Using: new("true")}
	otherClause := pgpolicy.DesiredPolicy{Using: new("tenant_id = 2")}
	asDeclared := pgpolicy.DesiredPolicy{Using: new("owner = 'x'")}
	withCheck := pgpolicy.DesiredPolicy{Using: new("tenant_id = 1"), WithCheck: new("true")}
	commented := pgpolicy.DesiredPolicy{Using: new("tenant_id = 1"), Comment: "tenants"}
	plain := pgpolicy.DesiredPolicy{Using: new("tenant_id = 1")}
	commentedForCurrentUser := pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}}, Using: new("true"), Comment: "mine",
		Normalized: &pgpolicy.NormalizedPolicy{Roles: []pgpolicy.RoleSelector{app}, Using: new("true")}}
	clauseChanged := schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "a USING or WITH CHECK expression changed, and which rows it admits is not established"}
	tests := []struct {
		name     string
		declared []pgpolicy.DesiredPolicy
		observed []pgpolicy.ObservedPolicy
		want     *pgpolicy.PolicyChange
	}{
		{name: "a command", declared: []pgpolicy.DesiredPolicy{selectOnly}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &selectOnly,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "the policy no longer applies to commands it applied to"}}},
		{name: "the composition", declared: []pgpolicy.DesiredPolicy{restrictive}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &restrictive,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "a permissive policy made restrictive can hide rows it admitted"}}},
		{name: "a role instead of PUBLIC", declared: []pgpolicy.DesiredPolicy{forReader}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &forReader,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "the policy no longer applies to roles it applied to"}}},
		{name: "a role keyword nobody resolved", declared: []pgpolicy.DesiredPolicy{forCurrentUser}, observed: []pgpolicy.ObservedPolicy{resolved},
			want: &pgpolicy.PolicyChange{Before: &resolved, After: &forCurrentUser, Access: schemaext.AccessEffect{Access: schemaext.AccessUnknown,
				Reason: "a role keyword the server resolves when the policy is created makes its roles unknown here"}}},
		{name: "a clause", declared: []pgpolicy.DesiredPolicy{otherClause}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &otherClause, Access: clauseChanged}},
		{name: "a clause compared as declared", declared: []pgpolicy.DesiredPolicy{asDeclared}, observed: []pgpolicy.ObservedPolicy{rewritten},
			want: &pgpolicy.PolicyChange{Before: &rewritten, After: &asDeclared, Access: clauseChanged}},
		{name: "a clause added", declared: []pgpolicy.DesiredPolicy{withCheck}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &withCheck, Access: clauseChanged}},
		{name: "the comment alone", declared: []pgpolicy.DesiredPolicy{commented}, observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored, After: &commented, CommentOnly: true,
				Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the policy's comment changes"}}},
		{name: "the comment alone of a policy whose role keyword the server resolved", declared: []pgpolicy.DesiredPolicy{commentedForCurrentUser},
			observed: []pgpolicy.ObservedPolicy{resolved},
			want: &pgpolicy.PolicyChange{Before: &resolved, After: &commentedForCurrentUser, CommentOnly: true,
				Access: schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "only the policy's comment changes"}}},
		{name: "a policy the server lacks", declared: []pgpolicy.DesiredPolicy{plain},
			want: &pgpolicy.PolicyChange{After: &plain,
				Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "a permissive policy can admit rows to the roles it names"}}},
		{name: "a policy nothing declares", observed: []pgpolicy.ObservedPolicy{stored},
			want: &pgpolicy.PolicyChange{Before: &stored,
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "removing a permissive policy can hide the rows it admitted"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(),
				comparison(c, declaredObjects(c, test.declared...), observedObjects(c, test.observed...), surviving("orders")))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: pgpolicy.PolicyRef("app", "orders", "tenant"), Value: test.want}})
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// declaredObjects captures declarations of orders.tenant.
func declaredObjects(c *qt.C, values ...pgpolicy.DesiredPolicy) []schemaext.Object {
	c.Helper()
	var result []schemaext.Object
	for _, value := range values {
		result = append(result, desiredPolicy(c, "orders", "tenant", value))
	}
	return result
}

// observedObjects captures observations of orders.tenant.
func observedObjects(c *qt.C, values ...pgpolicy.ObservedPolicy) []schemaext.Object {
	c.Helper()
	var result []schemaext.Object
	for _, value := range values {
		result = append(result, observedPolicy(c, "orders", "tenant", value))
	}
	return result
}

// TestComparePolicies_KeepsWhatNobodyDescribed pins coverage: an observed
// policy a desired source could not describe is adopted rather than dropped,
// and a declared policy whose presence the read did not establish is
// undecided rather than created.
func TestComparePolicies_KeepsWhatNobodyDescribed(t *testing.T) {
	c := qt.New(t)
	request := comparison(c, nil, []schemaext.Object{observedPolicy(c, "orders", "tenant", held("tenant_id = 1"))}, surviving("orders"))
	request.Desired.Coverage = uninspected(c, schemaext.Desired)

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	adopted, found := must.Must2(result.Desired.Objects.Get(pgpolicy.PolicyRef("app", "orders", "tenant")))
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted.Value, qt.DeepEquals, schemaext.Value(&pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll,
		Roles: []pgpolicy.RoleSelector{public}, Using: new("tenant_id = 1"), Composition: pgpolicy.Permissive}))
	c.Assert(result.Desired.Coverage.Lookup(pgpolicy.PolicyKind, pgpolicy.PolicyRef("app", "orders", "tenant")).State, qt.Equals, schemaext.Complete)
}

// TestComparePolicies_UndecidedWhereNothingWasRead pins the declared policy,
// and the table, whose current state the read did not establish, and the
// declared policy the desired source could not describe in full.
func TestComparePolicies_UndecidedWhereNothingWasRead(t *testing.T) {
	ref := pgpolicy.PolicyRef("app", "orders", "tenant")
	tests := []struct {
		name          string
		desired       schemaext.Coverage
		current       schemaext.Coverage
		wantUndecided []schemaext.UndecidedChange
	}{
		{name: "a read that skipped policies", desired: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
			current: must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil)),
			wantUndecided: []schemaext.UndecidedChange{
				{Kind: pgpolicy.PolicyKind, Subject: ref, Reason: "the current policy or its absence was not established: not read"},
				{Kind: pgpolicy.PolicyKind, Subject: tableRef("orders"), Reason: "the table's policies were not enumerated: not read"},
			}},
		{name: "a declaration the source could not describe in full", desired: must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: pgpolicy.PolicyKind, Subject: ref,
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "a clause it cannot spell"}}})),
			current:       must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
			wantUndecided: []schemaext.UndecidedChange{{Kind: pgpolicy.PolicyKind, Subject: ref, Reason: "the desired source cannot describe the complete policy"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, []schemaext.Object{desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{Using: new("true")})}, nil, surviving("orders"))
			request.Desired.Coverage, request.Current.Coverage = test.desired, test.current

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.DeepEquals, test.wantUndecided)
		})
	}
}

// TestComparePolicies_LeavesATablesLifecycleToTheTable pins that a policy on a
// table the plan creates or drops is not a change of its own: the table's
// creation carries its policies and its removal takes them.
func TestComparePolicies_LeavesATablesLifecycleToTheTable(t *testing.T) {
	tests := []struct {
		name     string
		parent   schemaext.ParentState
		declared []pgpolicy.DesiredPolicy
		observed []pgpolicy.ObservedPolicy
	}{
		{name: "a table the plan creates", parent: schemaext.ParentState{Subject: tableRef("orders"), Desired: true},
			declared: []pgpolicy.DesiredPolicy{{Using: new("true")}}},
		{name: "a table the plan drops", parent: schemaext.ParentState{Subject: tableRef("orders"), Current: true},
			observed: []pgpolicy.ObservedPolicy{held("true")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(),
				comparison(c, declaredObjects(c, test.declared...), observedObjects(c, test.observed...), test.parent))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestComparePolicies_PairsByIdentity pins the identity: a policy is its
// table and its name, so a declaration that left the schema to the default
// pairs with the policy the catalog reports in that schema, and a policy of one
// name on each of two tables is two policies compared apart.
func TestComparePolicies_PairsByIdentity(t *testing.T) {
	c := qt.New(t)
	defaulted := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{Using: new("true")}))
	named := must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("public", "orders", "tenant"), held("true")))
	onInvoices := desiredPolicy(c, "invoices", "tenant", pgpolicy.DesiredPolicy{Using: new("false")})
	heldOnInvoices := observedPolicy(c, "invoices", "tenant", held("true"))
	parents := []schemaext.ParentState{
		{Subject: pgpolicy.Table(pgpolicy.PolicyRef("public", "orders", "tenant")), Desired: true, Current: true}, surviving("invoices"),
	}

	result, err := newRuntime(c).CompareObjects(t.Context(),
		comparison(c, []schemaext.Object{defaulted, onInvoices}, []schemaext.Object{named, heldOnInvoices}, parents...))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Subject, qt.DeepEquals, pgpolicy.PolicyRef("app", "invoices", "tenant"))
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// TestComparePolicies_FailurePath pins that a policy needs its table on its
// own side: the runtime refuses one whose table that side does not hold
// before the owner runs, and the owner refuses it as well when a host calls it
// directly.
func TestComparePolicies_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired []pgpolicy.DesiredPolicy
		current []pgpolicy.ObservedPolicy
		parent  schemaext.ParentState
		want    string
	}{
		{name: "a declared policy on a table nothing declares", desired: []pgpolicy.DesiredPolicy{{Using: new("true")}},
			parent: schemaext.ParentState{Subject: tableRef("orders"), Current: true}, want: `.*is on a table its desired schema does not hold`},
		{name: "an observed policy on a table the read does not hold", current: []pgpolicy.ObservedPolicy{held("true")},
			parent: schemaext.ParentState{Subject: tableRef("orders"), Desired: true}, want: `.*is on a table its observed schema does not hold`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, declaredObjects(c, test.desired...), observedObjects(c, test.current...), test.parent)

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*feature object has no parent on its source side.*`)
			c.Assert(result.Changes, qt.HasLen, 0)

			request.Kinds = []schemaext.Kind{pgpolicy.PolicyKind}
			direct, err := policycompare.PolicyService{}.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(direct.Changes, qt.HasLen, 0)
		})
	}
}

// TestComparePolicies_OnlyOnRowSecurityTargets pins that the owner claims no
// target without row security: spanner speaks the PostgreSQL dialect and the
// runtime refuses a policy there, and the service refuses a target outside the
// family when it is called directly.
func TestComparePolicies_OnlyOnRowSecurityTargets(t *testing.T) {
	c := qt.New(t)
	request := comparison(c, []schemaext.Object{desiredPolicy(c, "orders", "tenant", pgpolicy.DesiredPolicy{Using: new("true")})}, nil, surviving("orders"))
	request.Target = "spanner"
	request.Desired.Coverage = schemaext.Coverage{}
	request.Current.Coverage = schemaext.Coverage{}

	result, err := newRuntime(c).CompareObjects(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*no object comparison for "spanner"/"ptah.run/pgpolicy/policy"`)
	c.Assert(result.Changes, qt.HasLen, 0)

	request.Target = "mysql"
	direct, err := policycompare.PolicyService{}.CompareObjects(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(direct.Changes, qt.HasLen, 0)
}
