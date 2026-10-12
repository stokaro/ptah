package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
)

// TestPolicyDefinitionTransitions pins the shared decision of the planner
// and renderer, including the operation codec a saved plan passes through.
func TestPolicyDefinitionTransitions(t *testing.T) {
	reader := []pgpolicy.RoleSelector{{Name: "reader"}}
	base := pgpolicy.DesiredPolicy{Using: new("true")}
	checked := pgpolicy.DesiredPolicy{Using: new("true"), WithCheck: new("true")}
	tests := []struct {
		name        string
		before      pgpolicy.DesiredPolicy
		after       pgpolicy.DesiredPolicy
		transaction plangraph.Transaction
		want        string
	}{
		{name: "roles", before: base, after: pgpolicy.DesiredPolicy{Roles: reader, Using: new("true")},
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO \"reader\"\n    USING (true)\n;\n"},
		{name: "omitted roles mean PUBLIC", before: pgpolicy.DesiredPolicy{Roles: reader, Using: new("true")}, after: base,
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (true)\n;\n"},
		{name: "add USING", before: pgpolicy.DesiredPolicy{}, after: base,
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (true)\n;\n"},
		{name: "change USING", before: base, after: pgpolicy.DesiredPolicy{Using: new("tenant_id = 2")},
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (tenant_id = 2)\n;\n"},
		{name: "add WITH CHECK", before: base, after: checked,
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (true)\n    WITH CHECK (true)\n;\n"},
		{name: "change WITH CHECK", before: checked, after: pgpolicy.DesiredPolicy{Using: new("true"), WithCheck: new("tenant_id = 2")},
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (true)\n    WITH CHECK (tenant_id = 2)\n;\n"},
		{name: "explicit default command and composition", before: base,
			after:       pgpolicy.DesiredPolicy{Command: pgpolicy.CommandAll, Composition: pgpolicy.Permissive, Using: new("false")},
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (false)\n;\n"},
		{name: "restrictive SELECT", before: pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Composition: pgpolicy.Restrictive, Using: new("true")},
			after:       pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Composition: pgpolicy.Restrictive, Using: new("false")},
			transaction: plangraph.TransactionAllowed,
			want:        "ALTER POLICY \"tenant\" ON \"app\".\"orders\" TO PUBLIC\n    USING (false)\n;\n"},
		{name: "remove USING", before: checked, after: pgpolicy.DesiredPolicy{WithCheck: new("true")},
			transaction: plangraph.TransactionRequired,
			want:        "DROP POLICY \"tenant\" ON \"app\".\"orders\";\nCREATE POLICY \"tenant\" ON \"app\".\"orders\"\n    WITH CHECK (true)\n;\n"},
		{name: "remove WITH CHECK", before: checked, after: base,
			transaction: plangraph.TransactionRequired,
			want:        "DROP POLICY \"tenant\" ON \"app\".\"orders\";\nCREATE POLICY \"tenant\" ON \"app\".\"orders\"\n    USING (true)\n;\n"},
		{name: "change command", before: base, after: pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Using: new("true")},
			transaction: plangraph.TransactionRequired,
			want:        "DROP POLICY \"tenant\" ON \"app\".\"orders\";\nCREATE POLICY \"tenant\" ON \"app\".\"orders\" FOR SELECT\n    USING (true)\n;\n"},
		{name: "change composition", before: base, after: pgpolicy.DesiredPolicy{Composition: pgpolicy.Restrictive, Using: new("true")},
			transaction: plangraph.TransactionRequired,
			want:        "DROP POLICY \"tenant\" ON \"app\".\"orders\";\nCREATE POLICY \"tenant\" ON \"app\".\"orders\" AS RESTRICTIVE\n    USING (true)\n;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := must.Must(test.before.Observed())
			result := planPolicyTransition(c, before, &test.after)
			c.Assert(result.Contributions, qt.HasLen, 1)
			steps := result.Contributions[0].Steps
			c.Assert(steps, qt.HasLen, 1)
			c.Assert(steps[0].Transaction, qt.Equals, test.transaction)
			runtime := must.Must(builtin.New())
			data, err := runtime.Codecs().Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{steps[0].Payload.Payload})
			c.Assert(err, qt.IsNil)
			values, err := runtime.Codecs().Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("postgres", &ast.ExtensionStatement{Payload: values[0].(ast.ExtensionPayload)})
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

func planPolicyTransition(c *qt.C, before *pgpolicy.ObservedPolicy, after *pgpolicy.DesiredPolicy) featureplan.Result {
	c.Helper()
	runtime := must.Must(builtin.New())
	result, err := runtime.PlanFeatures(c.Context(), featureplan.Request{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"),
		Changes: []schemaext.ChangeRecord{{Subject: pgpolicy.PolicyRef("app", "orders", "tenant"), Value: &pgpolicy.PolicyChange{
			Before: before, After: after, Access: pgpolicy.PolicyAccess(before, after, pgpolicy.ExpressionsDiffer),
		}}},
	})
	c.Assert(err, qt.IsNil)
	return result
}

func TestPolicyDefinitionCommentSteps(t *testing.T) {
	tests := []struct {
		name        string
		before      string
		after       string
		composition pgpolicy.Composition
		wantSteps   int
	}{
		{name: "retain empty comment", wantSteps: 1},
		{name: "retain comment", before: "tenants", after: "tenants", wantSteps: 1},
		{name: "set comment", after: "tenants", wantSteps: 2},
		{name: "change comment", before: "tenants", after: "readers", wantSteps: 2},
		{name: "clear comment", before: "tenants", wantSteps: 2},
		{name: "replacement restores comment", before: "tenants", after: "tenants", composition: pgpolicy.Restrictive, wantSteps: 2},
		{name: "replacement clears comment", before: "tenants", composition: pgpolicy.Restrictive, wantSteps: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			before := must.Must((&pgpolicy.DesiredPolicy{Using: new("true"), Comment: test.before}).Observed())
			after := &pgpolicy.DesiredPolicy{Using: new("false"), Comment: test.after, Composition: test.composition}
			result := planPolicyTransition(c, before, after)
			c.Assert(result.Contributions[0].Steps, qt.HasLen, test.wantSteps)
			c.Assert(result.Contributions[0].Dependencies, qt.HasLen, test.wantSteps-1)
		})
	}
}
