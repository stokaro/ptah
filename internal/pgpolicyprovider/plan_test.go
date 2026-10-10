package pgpolicyprovider_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyplan"
)

var (
	permissiveDeclared  = pgpolicy.DesiredPolicy{Roles: []pgpolicy.RoleSelector{reader}, Using: new("tenant_id = 1")}
	restrictiveDeclared = pgpolicy.DesiredPolicy{Composition: pgpolicy.Restrictive, Using: new("tenant_id = 1")}
	permissiveObserved  = pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{reader},
		Using: new("tenant_id = 1"), Composition: pgpolicy.Permissive}
	restrictiveObserved = pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{public},
		Using: new("tenant_id = 1"), Composition: pgpolicy.Restrictive}
)

// policyChange is a change of the policy name on orders, with the access the
// owner assesses for it.
func policyChange(name string, before *pgpolicy.ObservedPolicy, after *pgpolicy.DesiredPolicy, commentOnly bool) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: pgpolicy.PolicyRef("app", "orders", name), Value: &pgpolicy.PolicyChange{Before: before, After: after,
		CommentOnly: commentOnly, Access: pgpolicy.PolicyAccess(before, after, map[bool]pgpolicy.ExpressionFinding{false: pgpolicy.ExpressionsDiffer,
			true: pgpolicy.ExpressionsSame}[commentOnly])}}
}

// switchChange is a change of the orders table's switches.
func switchChange(table string, before pgpolicy.ObservedTableState, after pgpolicy.DesiredTableState) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: tableRef(table), Value: &pgpolicy.TableStateChange{Before: &before, After: &after,
		Access: pgpolicy.TableStateAccess(&before, &after)}}
}

func planning(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "postgres", Identifiers: postgres, Changes: changes}
}

// summary spells each contributed step: its name, its operation, its effects,
// its transaction and its phase.
func summary(contributions []plangraph.Contribution[featureplan.Operation]) []string {
	var lines []string
	for _, contribution := range contributions {
		for _, step := range contribution.Steps {
			var effects []string
			for _, effect := range step.Effects {
				effects = append(effects, fmt.Sprintf("%s %s", effect.Action, effect.Subject))
			}
			lines = append(lines, fmt.Sprintf("%s %T %s %s %s", step.ID.Name, step.Payload.Payload, strings.Join(effects, ", "),
				step.Transaction, step.Payload.Phase))
		}
	}
	return lines
}

// TestPlanPolicies_Steps pins the operation each change becomes: a creation, a
// drop, a replacement in one step that needs a transaction, a comment set
// after a creation or a replacement, a comment alone, and a table's switches.
// Every step asks for the dependent phase, reads its table and the roles its
// policy names, and writes the switches under their own subject.
func TestPlanPolicies_Steps(t *testing.T) {
	commented := restrictiveDeclared
	commented.Comment = "tenants"
	selectOnly := permissiveDeclared
	selectOnly.Command = pgpolicy.CommandSelect
	policy, table := "ptah.run/pgpolicy/policy app.orders.tenant", "table app.orders"
	tests := []struct {
		name     string
		change   schemaext.ChangeRecord
		want     []string
		strategy string
	}{
		{name: "a creation", change: policyChange("tenant", nil, &permissiveDeclared, false), strategy: "create the policy",
			want: []string{"policy/000000 *pgpolicy.PolicyOperation create " + policy + ", read " + table + ", read role reader allowed dependent"}},
		{name: "a creation with a comment", change: policyChange("tenant", nil, &commented, false), strategy: "create the policy",
			want: []string{
				"policy/000000 *pgpolicy.PolicyOperation create " + policy + ", read " + table + " allowed dependent",
				"policy/000000/comment *pgpolicy.PolicyCommentOperation alter " + policy + ", read " + table + " allowed dependent",
			}},
		{name: "a drop", change: policyChange("tenant", &restrictiveObserved, nil, false), strategy: "drop the policy",
			want: []string{"policy/000000 *pgpolicy.PolicyOperation drop " + policy + ", read " + table + " allowed dependent"}},
		{name: "a replacement", change: policyChange("tenant", &permissiveObserved, &selectOnly, false),
			strategy: "drop the policy and create it again in one transaction; PostgreSQL alters neither its command nor its composition in place",
			want:     []string{"policy/000000 *pgpolicy.PolicyOperation alter " + policy + ", read " + table + ", read role reader required dependent"}},
		{name: "a comment alone", change: policyChange("tenant", &restrictiveObserved, &commented, true), strategy: "set the policy's comment",
			want: []string{"policy/000000/comment *pgpolicy.PolicyCommentOperation alter " + policy + ", read " + table + " allowed dependent"}},
		{name: "a table's switches", change: switchChange("orders", pgpolicy.ObservedTableState{}, pgpolicy.DesiredTableState{Enabled: true}),
			strategy: "set the table's row-security switches",
			want:     []string{"table-state/000000 *pgpolicy.TableStateOperation alter ptah.run/pgpolicy/table-state app.orders, read " + table + " allowed dependent"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).PlanFeatures(t.Context(), planning(test.change))

			c.Assert(err, qt.IsNil)
			c.Assert(summary(result.Contributions), qt.DeepEquals, test.want)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Strategy, qt.Equals, test.strategy)
			c.Assert(result.Changes[0].Steps, qt.HasLen, len(test.want))
		})
	}
}

// TestPlanPolicies_SetsTheCommentAfterTheCreation pins that a comment step
// depends on the step that creates its policy.
func TestPlanPolicies_SetsTheCommentAfterTheCreation(t *testing.T) {
	c := qt.New(t)
	commented := permissiveDeclared
	commented.Comment = "tenants"

	result, err := newRuntime(c).PlanFeatures(t.Context(), planning(policyChange("tenant", nil, &commented, false)))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, []plangraph.Dependency{{
		Before: plangraph.StepID{Owner: pgpolicy.Owner, Name: "policy/000000"},
		After:  plangraph.StepID{Owner: pgpolicy.Owner, Name: "policy/000000/comment"},
	}})
}

// TestPlanPolicies_OrdersATableByAccess pins the owner's order on one table:
// what can only narrow access runs first, what can widen it runs last, and a
// change whose effect is unknown runs between. The changes arrive in the
// opposite order, and the steps of another table are not ordered against
// them.
func TestPlanPolicies_OrdersATableByAccess(t *testing.T) {
	c := qt.New(t)
	changed := permissiveDeclared
	changed.Using = new("tenant_id = 2")
	request := planning(
		policyChange("permissive_new", nil, &permissiveDeclared, false),
		policyChange("restrictive_old", &restrictiveObserved, nil, false),
		switchChange("orders", pgpolicy.ObservedTableState{Enabled: true}, pgpolicy.DesiredTableState{}),
		policyChange("changed", &permissiveObserved, &changed, false),
		policyChange("restrictive_new", nil, &restrictiveDeclared, false),
		policyChange("permissive_old", &permissiveObserved, nil, false),
		switchChange("orders", pgpolicy.ObservedTableState{}, pgpolicy.DesiredTableState{Forced: true}),
	)
	request.Changes[6] = schemaext.ChangeRecord{Subject: tableRef("invoices"), Value: request.Changes[6].Value}

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)

	c.Assert(err, qt.IsNil)
	order := make(map[string]int)
	for i, step := range plan.Steps {
		order[step.ID.Name] = i
	}
	narrowing := []string{"policy/000004", "policy/000005"}
	widening := []string{"policy/000000", "policy/000001", "table-state/000002"}
	for _, before := range narrowing {
		c.Assert(order[before] < order["policy/000003"], qt.IsTrue, qt.Commentf("%s before the unknown change", before))
		for _, after := range widening {
			c.Assert(order[before] < order[after], qt.IsTrue, qt.Commentf("%s before %s", before, after))
		}
	}
	for _, after := range widening {
		c.Assert(order["policy/000003"] < order[after], qt.IsTrue, qt.Commentf("the unknown change before %s", after))
	}
	c.Assert(slices.ContainsFunc(result.Contributions[0].Dependencies, func(edge plangraph.Dependency) bool {
		return edge.Before.Name == "table-state/000006" || edge.After.Name == "table-state/000006"
	}), qt.IsFalse)
}

// TestPlanPolicies_AccountsForATablesOperation pins the receipts for a table
// the host drops or alters, one per model: a drop takes the policies and
// switches with the table, and a surviving table keeps them. A rebuild, which
// would create the table without them, is refused.
func TestPlanPolicies_AccountsForATablesOperation(t *testing.T) {
	tests := []struct {
		name  string
		table featureplan.Table
		want  []string
	}{
		{name: "a drop", table: featureplan.Table{Subject: tableRef("orders"), Action: featureplan.DropTable, Current: observedOrders}, want: []string{
			"ptah.run/pgpolicy/policy: DROP TABLE removes the table's policies with it",
			"ptah.run/pgpolicy/table-state: DROP TABLE removes the table's row-security switches with it",
		}},
		{name: "an alteration", table: featureplan.Table{Subject: tableRef("orders"), Action: featureplan.AlterTable,
			Desired: declaredOrders, Current: observedOrders}, want: []string{
			"ptah.run/pgpolicy/policy: keep the table's policies unless a planned change in this plan changes them",
			"ptah.run/pgpolicy/table-state: keep the table's row-security switches unless a planned change in this plan changes them",
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning()
			request.Tables = []featureplan.Table{test.table}

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			var receipts []string
			for _, parent := range result.Parents {
				receipts = append(receipts, fmt.Sprintf("%s: %s", parent.Kind, parent.Strategy))
			}
			c.Assert(receipts, qt.DeepEquals, test.want)
		})
	}
}

// TestPlanPolicies_CreatesACreatedTablesPolicies pins a table the plan
// creates: each of its policies is created in the dependent phase, with its
// comment after it and access unchanged, since no role could read the table
// before. The policy receipt names those steps; the switch receipt names none,
// because the statements after the CREATE TABLE set the switches.
func TestPlanPolicies_CreatesACreatedTablesPolicies(t *testing.T) {
	c := qt.New(t)
	commented := permissiveDeclared
	commented.Comment = "tenants"
	request := planning()
	request.Tables = []featureplan.Table{{Subject: tableRef("orders"), Action: featureplan.CreateTable,
		Desired: schemacapture.TableDeclaration{Table: declaredOrders.Table, OwnedObjects: objects(c,
			desiredPolicy(c, "orders", "tenant", commented), desiredPolicy(c, "orders", "limit", restrictiveDeclared))}}}
	limit, tenant, table := "ptah.run/pgpolicy/policy app.orders.limit", "ptah.run/pgpolicy/policy app.orders.tenant", "table app.orders"

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(summary(result.Contributions), qt.DeepEquals, []string{
		"created/000000/policy/000000 *pgpolicy.PolicyOperation create " + limit + ", read " + table + " allowed dependent",
		"created/000000/policy/000001 *pgpolicy.PolicyOperation create " + tenant + ", read " + table + ", read role reader allowed dependent",
		"created/000000/policy/000001/comment *pgpolicy.PolicyCommentOperation alter " + tenant + ", read " + table + " allowed dependent",
	})
	c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, []plangraph.Dependency{{
		Before: plangraph.StepID{Owner: pgpolicy.Owner, Name: "created/000000/policy/000001"},
		After:  plangraph.StepID{Owner: pgpolicy.Owner, Name: "created/000000/policy/000001/comment"},
	}})
	for _, step := range result.Contributions[0].Steps[:2] {
		c.Assert(step.Payload.Payload.(*pgpolicy.PolicyOperation).Change.Access, qt.Equals, pgpolicy.CreatedTableAccess())
	}
	c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{
		{Subject: tableRef("orders"), Kind: pgpolicy.PolicyKind, Action: featureplan.CreateTable,
			Strategy: "create the table's policies after the objects their expressions may name", Steps: []plangraph.StepID{
				{Owner: pgpolicy.Owner, Name: "created/000000/policy/000000"},
				{Owner: pgpolicy.Owner, Name: "created/000000/policy/000001"},
				{Owner: pgpolicy.Owner, Name: "created/000000/policy/000001/comment"},
			}},
		{Subject: tableRef("orders"), Kind: pgpolicy.TableStateKind, Action: featureplan.CreateTable,
			Strategy: "set the table's declared row-security switches in the statements after its CREATE TABLE"},
	})
}

// TestPlanPolicies_KeepsTheSchemaTheSourceLeftOut pins that a policy on a table
// in the default schema names its table as the source did. Qualified with the
// default the identity was built with, the statements would name another
// table wherever the connection's search_path puts the table elsewhere.
func TestPlanPolicies_KeepsTheSchemaTheSourceLeftOut(t *testing.T) {
	c := qt.New(t)
	commented := permissiveDeclared
	commented.Comment = "tenants"
	change := schemaext.ChangeRecord{Subject: pgpolicy.PolicyRef("", "orders", "tenant"),
		Value: &pgpolicy.PolicyChange{After: &commented, Access: pgpolicy.PolicyAccess(nil, &commented, pgpolicy.ExpressionsSame)}}

	result, err := newRuntime(c).PlanFeatures(t.Context(), planning(change))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions[0].Steps, qt.HasLen, 2)
	c.Assert(result.Contributions[0].Steps[0].Payload.Payload.(*pgpolicy.PolicyOperation).QualifiedTable(), qt.Equals, "orders")
	c.Assert(result.Contributions[0].Steps[1].Payload.Payload.(*pgpolicy.PolicyCommentOperation).QualifiedTable(), qt.Equals, "orders")
}

// TestPlanDeclarations_CreatesEachPolicyLast pins a whole-schema render: each
// declared policy is created in the default phase after every common step,
// since its expressions may name anything the render creates, and its comment
// after it. Each receipt names its policy's steps.
func TestPlanDeclarations_CreatesEachPolicyLast(t *testing.T) {
	c := qt.New(t)
	commented := permissiveDeclared
	commented.Comment = "tenants"
	createTable := plangraph.StepID{Owner: "example.org/common", Name: "create-table"}
	createView := plangraph.StepID{Owner: "example.org/common", Name: "create-view"}
	request := featureplan.DeclarationRequest{Target: "postgres", Identifiers: postgres,
		Objects: []schemaext.Object{desiredPolicy(c, "orders", "tenant", commented), desiredPolicy(c, "orders", "limit", restrictiveDeclared)},
		Tables:  []schemacapture.TableDeclaration{declaredOrders},
		CommonSteps: []featureplan.CommonStep{
			{ID: createTable, Effects: []plangraph.Effect{{Subject: tableRef("orders"), Action: plangraph.Create}}, Transaction: plangraph.TransactionAllowed},
			{ID: createView, Transaction: plangraph.TransactionAllowed},
		}}
	limit, tenant, table := "ptah.run/pgpolicy/policy app.orders.limit", "ptah.run/pgpolicy/policy app.orders.tenant", "table app.orders"
	first, comment, second := plangraph.StepID{Owner: pgpolicy.Owner, Name: "declared/000000"},
		plangraph.StepID{Owner: pgpolicy.Owner, Name: "declared/000000/comment"}, plangraph.StepID{Owner: pgpolicy.Owner, Name: "declared/000001"}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(summary(result.Contributions), qt.DeepEquals, []string{
		"declared/000000 *pgpolicy.PolicyOperation create " + tenant + ", read " + table + ", read role reader allowed ",
		"declared/000000/comment *pgpolicy.PolicyCommentOperation alter " + tenant + ", read " + table + " allowed ",
		"declared/000001 *pgpolicy.PolicyOperation create " + limit + ", read " + table + " allowed ",
	})
	c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, []plangraph.Dependency{
		{Before: createTable, After: first}, {Before: createView, After: first}, {Before: first, After: comment},
		{Before: createTable, After: second}, {Before: createView, After: second},
	})
	c.Assert(result.Declarations, qt.DeepEquals, []featureplan.DeclarationPlan{
		{Subject: pgpolicy.PolicyRef("app", "orders", "tenant"), Strategy: "create the policy after every object the render creates, since its expressions may name any of them",
			Steps: []plangraph.StepID{first, comment}},
		{Subject: pgpolicy.PolicyRef("app", "orders", "limit"), Strategy: "create the policy after every object the render creates, since its expressions may name any of them",
			Steps: []plangraph.StepID{second}},
	})
}

// TestPlanDeclarations_FailurePath pins what the owner refuses when a host asks
// it directly: another target family, and a value that is not a declared
// policy.
func TestPlanDeclarations_FailurePath(t *testing.T) {
	observed := must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef("app", "orders", "tenant"), permissiveObserved))
	tests := []struct {
		name    string
		request featureplan.DeclarationRequest
		want    error
	}{
		{name: "another family", request: featureplan.DeclarationRequest{Target: "mysql"}, want: ptaherr.ErrUnsupportedDialect},
		{name: "an observed policy", request: featureplan.DeclarationRequest{Target: "postgres", Objects: []schemaext.Object{observed}},
			want: schemaext.ErrInvalidValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := policyplan.Service{}.PlanDeclarations(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

// TestPlanPolicies_RefusesARebuild pins the refusal of a table rebuild.
func TestPlanPolicies_RefusesARebuild(t *testing.T) {
	c := qt.New(t)
	request := planning()
	request.Tables = []featureplan.Table{{Subject: tableRef("orders"), Action: featureplan.RebuildTable, Desired: declaredOrders, Current: observedOrders}}

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Equals, `Ptah has no plan for a table's row-security state through parent action "rebuild-table"`)
	c.Assert(result.Contributions, qt.HasLen, 0)
}

// TestPlanPolicies_FailurePath pins what the owner refuses when a host asks it
// directly: another target family, and a parent model it does not own.
func TestPlanPolicies_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request featureplan.Request
		want    error
	}{
		{name: "another family", request: featureplan.Request{Target: "mysql"}, want: ptaherr.ErrUnsupportedDialect},
		{name: "another model", request: featureplan.Request{Target: "postgres", ParentKinds: []schemaext.Kind{"example.org/other"}},
			want: schemaext.ErrInvalidValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := policyplan.Service{}.PlanFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

var (
	declaredOrders = schemacapture.TableDeclaration{Table: schemamodel.Table{Schema: "app", Name: "orders"}}
	observedOrders = schemacapture.TableObservation{Table: catalog.Table{Schema: "app", Name: "orders"}}
)
