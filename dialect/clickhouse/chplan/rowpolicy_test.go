package chplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
)

var plannedPolicy = &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive,
	Roles: chschema.RoleSelection{Names: []string{"alice"}}}

func policyRecord(before *chschema.ObservedRowPolicy, after *chschema.DesiredRowPolicy) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: chschema.RowPolicyRef("app", "orders", "tenant"), Value: chdiff.NewRowPolicy(before, after)}
}

func planRowPolicies(c *qt.C, request featureplan.Request) featureplan.Result {
	c.Helper()
	request.Target, request.Identifiers = "clickhouse", identifier.ForDialect("clickhouse")
	return must.Must(chplan.RowPolicyService{}.PlanFeatures(c.Context(), request))
}

// Each change is one statement outside a transaction that reads the table
// and acts on the policy; the operation carries the change, assessment
// included, and the receipt names the step.
func TestRowPolicyPlanIsOneStatementPerChange(t *testing.T) {
	for _, test := range []struct {
		name   string
		record schemaext.ChangeRecord
		action plangraph.Action
	}{
		{"a creation", policyRecord(nil, &chschema.DesiredRowPolicy{Filter: new("tenant = 1")}), plangraph.Create},
		{"a change in place", policyRecord(plannedPolicy, &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{Names: []string{"alice"}}}), plangraph.Alter},
		{"a drop", policyRecord(plannedPolicy, nil), plangraph.Drop},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := planRowPolicies(c, featureplan.Request{Changes: []schemaext.ChangeRecord{test.record}})

			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 1)
			steps := result.Contributions[0].Steps
			c.Assert(steps, qt.HasLen, 1)
			c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
			c.Assert(steps[0].Transaction, qt.Equals, plangraph.TransactionForbidden)
			c.Assert(steps[0].Payload.Role, qt.Equals, ast.StatementExtension)
			c.Assert(steps[0].Effects, qt.DeepEquals, []plangraph.Effect{
				{Subject: objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TablePartsVerbatim("app", "orders"), Action: plangraph.Read},
				{Subject: test.record.Subject, Action: test.action},
			})
			operation, ok := steps[0].Payload.Payload.(*chast.RowPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(operation.AccessEffect(), qt.DeepEquals, test.record.Value.(*chdiff.RowPolicy).Access)
			c.Assert([]string{operation.Database, operation.Table, operation.Name}, qt.DeepEquals, []string{"app", "orders", "tenant"})
		})
	}
}

// ClickHouse keeps a row policy when its table is dropped, so the owner drops
// a dropped table's captured policies itself and keeps them through an alter
// or a rebuild.
func TestRowPolicyPlanAccountsForTheTable(t *testing.T) {
	table := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TablePartsVerbatim("app", "orders")
	captured := must.Must(schemaext.NewObjects(must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("app", "orders", "tenant"), *plannedPolicy))))
	for _, test := range []struct {
		name     string
		action   featureplan.ParentAction
		objects  schemaext.Objects
		steps    int
		strategy string
	}{
		{"a dropped table with a policy", featureplan.DropTable, captured, 1, "drop the table's row policies, which ClickHouse keeps when the table is dropped"},
		{"a dropped table without one", featureplan.DropTable, schemaext.Objects{}, 0, "no row policy of the table is captured; ClickHouse keeps any it holds when the table is dropped"},
		{"an altered table", featureplan.AlterTable, captured, 0, "keep the table's row policies; ClickHouse resolves a policy's table by name"},
		{"a rebuilt table", featureplan.RebuildTable, captured, 0, "keep the table's row policies; ClickHouse keeps a policy while its table is dropped and created again"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := planRowPolicies(c, featureplan.Request{ParentKinds: []schemaext.Kind{chschema.RowPolicyKind},
				Tables: []featureplan.Table{{Action: test.action, Subject: table, Current: schemacapture.TableObservation{OwnedObjects: test.objects}}}})

			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Parents[0].Strategy, qt.Equals, test.strategy)
			c.Assert(result.Parents[0].Steps, qt.HasLen, test.steps)
			c.Assert(contributedSteps(result), qt.Equals, test.steps)
		})
	}
}

func contributedSteps(result featureplan.Result) int {
	steps := 0
	for _, contribution := range result.Contributions {
		steps += len(contribution.Steps)
	}
	return steps
}

// The planner refuses what it cannot plan with a diagnostic and no prefix,
// and an invalid request with an error.
func TestRowPolicyPlan_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := chschema.RowPolicyRef("app", "", "tenant")

	refused := planRowPolicies(c, featureplan.Request{Changes: []schemaext.ChangeRecord{
		{Subject: database, Value: chdiff.NewRowPolicy(nil, &chschema.DesiredRowPolicy{})}}})
	_, wrongTarget := chplan.RowPolicyService{}.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres"})
	_, wrongParents := chplan.RowPolicyService{}.PlanFeatures(t.Context(), featureplan.Request{Target: "clickhouse",
		ParentKinds: []schemaext.Kind{chschema.TableKind}})

	c.Assert(refused.Diagnostics, qt.HasLen, 1)
	c.Assert(refused.Diagnostics[0].Problem.Message, qt.Matches, `.*database-wide policy \(ON db\.\*\) is not supported.*`)
	c.Assert(refused.Contributions, qt.HasLen, 0)
	c.Assert(wrongTarget, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(wrongParents, qt.ErrorIs, schemaext.ErrInvalidValue)
}
