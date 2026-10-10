package builtin_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// The SQL Server security policy owner is exercised through the built-in
// runtime, so each test passes through the registration that selects the
// owner's service for the target, not only through the service itself.

var (
	policyFunction = mssqlschema.ObjectName{Schema: "rls", Name: "fn_tenant"}
	policyOrders   = mssqlschema.ObjectName{Schema: "app", Name: "orders"}
	policyInvoices = mssqlschema.ObjectName{Schema: "app", Name: "invoices"}
	sqlServerNames = identifier.ForDialect("sqlserver")
)

func policyFilter(table mssqlschema.ObjectName, argument string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: mssqlschema.Filter, Function: policyFunction, Arguments: []string{argument}, Table: table}
}

// desiredPolicy, observedPolicy and policyState build fixtures whose
// construction is not what a test is about.
func desiredPolicy(name string, policy mssqlschema.DesiredSecurityPolicy) schemaext.Object {
	return must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", name), policy))
}

func observedPolicy(name string, policy mssqlschema.ObservedSecurityPolicy) schemaext.Object {
	return must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", name), policy))
}

func policyState(representation schemaext.Representation, state schemaext.KnowledgeState, objects ...schemaext.Object) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(mssqlschema.Coverage(representation, schemaext.Knowledge{State: state, Reason: "the test says so"}, nil))}
}

func comparePolicies(c *qt.C, desired, current schemaext.ObjectState) (schemaext.ObjectComparisonResult, error) {
	c.Helper()
	return must.Must(builtin.New()).CompareObjects(context.Background(), schemaext.ObjectComparisonRequest{
		Target: "sqlserver", Identifiers: sqlServerNames, Kinds: []schemaext.Kind{mssqlschema.SecurityPolicyKind},
		Desired: desired, Current: current,
	})
}

// TestSecurityPolicyComparison pins what a comparison finds for one policy:
// a creation and a drop with their access assessment, nothing for a
// declaration the catalog spells its own way, an undecided pair for an
// argument the server rewrote, and an observed policy kept rather than
// dropped when the description could not express policies.
func TestSecurityPolicyComparison(t *testing.T) {
	tests := []struct {
		name          string
		desired       schemaext.ObjectState
		current       schemaext.ObjectState
		wantAccess    []schemaext.Access
		wantUndecided int
		wantDesired   int
	}{
		{name: "a new policy", wantAccess: []schemaext.Access{schemaext.AccessNarrows}, wantDesired: 1,
			desired: policyState(schemaext.Desired, schemaext.Complete, desiredPolicy("tenancy", mssqlschema.DesiredSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}})),
			current: policyState(schemaext.Observed, schemaext.Complete)},
		{name: "a dropped policy", wantAccess: []schemaext.Access{schemaext.AccessWidens},
			desired: policyState(schemaext.Desired, schemaext.Complete),
			current: policyState(schemaext.Observed, schemaext.Complete, observedPolicy("tenancy", mssqlschema.ObservedSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}))},
		{name: "the catalog's spelling of the declaration", wantDesired: 1,
			desired: policyState(schemaext.Desired, schemaext.Complete, desiredPolicy("tenancy", mssqlschema.DesiredSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}})),
			current: policyState(schemaext.Observed, schemaext.Complete, observedPolicy("tenancy", mssqlschema.ObservedSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}))},
		{name: "an argument the server rewrote", wantUndecided: 1, wantDesired: 1,
			desired: policyState(schemaext.Desired, schemaext.Complete, desiredPolicy("tenancy", mssqlschema.DesiredSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "CAST(tenant AS int) + 0")}})),
			current: policyState(schemaext.Observed, schemaext.Complete, observedPolicy("tenancy", mssqlschema.ObservedSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "CONVERT([int],[tenant])+(0)")}, Enabled: true, SchemaBinding: true}))},
		{name: "a policy the description could not express", wantDesired: 1,
			desired: policyState(schemaext.Desired, schemaext.Uninspected),
			current: policyState(schemaext.Observed, schemaext.Complete, observedPolicy("tenancy", mssqlschema.ObservedSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}))},
		{name: "a declaration on a server whose policies were not read", wantUndecided: 1, wantDesired: 1,
			desired: policyState(schemaext.Desired, schemaext.Complete, desiredPolicy("tenancy", mssqlschema.DesiredSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}})),
			current: policyState(schemaext.Observed, schemaext.Uninspected)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := comparePolicies(c, test.desired, test.current)

			c.Assert(err, qt.IsNil)
			var access []schemaext.Access
			for _, change := range result.Changes {
				access = append(access, change.Value.(*mssqldiff.SecurityPolicy).Access.Access)
			}
			c.Assert(access, qt.DeepEquals, test.wantAccess)
			c.Assert(result.Undecided, qt.HasLen, test.wantUndecided)
			c.Assert(must.Must(result.Desired.Objects.All()), qt.HasLen, test.wantDesired)
		})
	}
}

// TestSecurityPolicyComparison_FailurePath_OneEnabledPolicyPerTable pins the
// refusal of a desired state SQL Server refuses with Msg 33264: two enabled
// policies binding one table, whether both are declared or one is an
// observed policy the description keeps. A disabled second policy is the
// control.
func TestSecurityPolicyComparison_FailurePath_OneEnabledPolicyPerTable(t *testing.T) {
	tests := []struct {
		name    string
		desired schemaext.ObjectState
		current schemaext.ObjectState
	}{
		{name: "two declarations", desired: policyState(schemaext.Desired, schemaext.Complete,
			desiredPolicy("first", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}}),
			desiredPolicy("second", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
				policyFilter(mssqlschema.ObjectName{Schema: "APP", Name: "Orders"}, "tenant_id")}})), current: policyState(schemaext.Observed, schemaext.Complete)},
		{name: "a declaration and a kept observation", desired: schemaext.ObjectState{
			Objects: must.Must(schemaext.NewObjects(desiredPolicy("second", mssqlschema.DesiredSecurityPolicy{
				Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}}))),
			Coverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
				[]schemaext.SubjectCoverage{{Kind: mssqlschema.SecurityPolicyKind, Subject: mssqlschema.SecurityPolicyRef("rls", "first"),
					Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the source cannot write this policy"}}})),
		}, current: policyState(schemaext.Observed, schemaext.Complete, observedPolicy("first", mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := comparePolicies(c, test.desired, test.current)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*enabled security policies \[rls\]\.\[first\] and \[rls\]\.\[second\] both bind table .*Msg 33264.*`)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
	t.Run("a disabled second policy", func(t *testing.T) {
		c := qt.New(t)
		result, err := comparePolicies(c, policyState(schemaext.Desired, schemaext.Complete,
			desiredPolicy("first", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}}),
			desiredPolicy("second", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")},
				Enabled: new(false)})), policyState(schemaext.Observed, schemaext.Complete))
		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 2)
	})
}

// TestSecurityPolicyConversion pins that a declaration converts to the
// observation it predicts, defaults resolved, and back to a declaration that
// names every value.
func TestSecurityPolicyConversion(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	declared := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}}

	observed, err := runtime.ConvertFeatures(context.Background(), schemaext.ConversionRequest{Target: "sqlserver",
		From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declared}})
	c.Assert(err, qt.IsNil)
	back, err := runtime.ConvertFeatures(context.Background(), schemaext.ConversionRequest{Target: "sqlserver",
		From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&mssqlschema.ObservedSecurityPolicy{
		Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}, Enabled: true, SchemaBinding: true}})
	c.Assert(back, qt.DeepEquals, []schemaext.Value{&mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}, Enabled: new(true), SchemaBinding: new(true)}})
}

// TestSecurityPolicyRelations pins the dependencies a policy records: every
// table it binds and every predicate function, complete when each argument is
// a column or a literal, and not when one is an expression that may call
// functions of its own.
func TestSecurityPolicyRelations(t *testing.T) {
	builder := objectidentity.NewBuilder(sqlServerNames)
	function := builder.TableParts("rls", "fn_tenant")
	function.Kind = objectidentity.KindFunction
	tests := []struct {
		name         string
		argument     string
		wantComplete bool
	}{
		{name: "column arguments", argument: "tenant_id", wantComplete: true},
		{name: "an expression argument", argument: "dbo.tenant_of(tenant_id)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := mssqlschema.SecurityPolicyRef("rls", "tenancy")
			snapshot, err := must.Must(builtin.New()).CaptureRelations(context.Background(), schemaext.RelationRequest{
				Target: "sqlserver", Representation: schemaext.Desired, Identifiers: sqlServerNames,
				Kinds: []schemaext.Kind{mssqlschema.SecurityPolicyKind},
				Values: []schemaext.RelationValue{{Subject: schemaext.RelationSubject{Kind: mssqlschema.SecurityPolicyKind, Placement: schemaext.ObjectPlacement, Subject: subject},
					Value: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{
						policyFilter(policyOrders, test.argument), policyFilter(policyInvoices, test.argument)}}}},
				Coverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
			})
			c.Assert(err, qt.IsNil)
			records := snapshot.Records()
			c.Assert(records, qt.HasLen, 1)
			c.Assert(records[0].Complete, qt.Equals, test.wantComplete)
			c.Assert(records[0].Dependencies, qt.DeepEquals, []objectidentity.ID{
				function, builder.TableParts("app", "invoices"), builder.TableParts("app", "orders")})
		})
	}
}

// TestSecurityPolicyReversal pins that a change reverses into the change that
// restores the observed policy, assessed again, and that it says what a
// restore cannot undo.
func TestSecurityPolicyReversal(t *testing.T) {
	c := qt.New(t)
	before := &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}
	change := &mssqldiff.SecurityPolicy{Before: before, Access: mssqldiff.Assess(sqlServerNames, before, nil)}

	reversals, err := must.Must(builtin.New()).ReverseChanges(context.Background(), schemaext.ReversalRequest{Target: "sqlserver", Identifiers: sqlServerNames,
		Changes: []schemaext.ChangeRecord{{Subject: mssqlschema.SecurityPolicyRef("rls", "tenancy"), Value: change}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reversals, qt.HasLen, 1)
	reversed := reversals[0].Change.Value.(*mssqldiff.SecurityPolicy)
	c.Assert(reversed.Before, qt.IsNil)
	c.Assert(reversed.After, qt.DeepEquals, &mssqlschema.DesiredSecurityPolicy{Predicates: before.Predicates, Enabled: new(true), SchemaBinding: new(true)})
	c.Assert(reversed.Access.Access, qt.Equals, schemaext.AccessNarrows)
	c.Assert(reversals[0].Limitations, qt.HasLen, 1)
}

// TestSecurityPolicyReport pins the inventory counts: one policy and its
// predicates.
func TestSecurityPolicyReport(t *testing.T) {
	c := qt.New(t)

	report, err := must.Must(builtin.New()).ReportFeatures(context.Background(), schemaext.ReportingRequest{Target: "sqlserver",
		Representation: schemaext.Desired, Values: []schemaext.Value{&mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id"), policyFilter(policyInvoices, "tenant_id")}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{{Kind: mssqlschema.SecurityPolicyKind, Counts: []schemaext.MetricCount{
		{Name: "security_policies", Value: 1}, {Name: "security_predicates", Value: 2}}}})
}

// planPolicies plans changes through the SQL Server planner and renderer,
// which return each statement without its terminator.
func planPolicies(c *qt.C, changes ...schemaext.ChangeRecord) []string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{FeatureChanges: changes}, "sqlserver")
	c.Assert(err, qt.IsNil)
	return statements
}

// TestSecurityPolicyPlan_HandsATableOver pins the order of a plan that moves
// a table from one enabled policy to another, which SQL Server refuses while
// both are enabled (Msg 33264): the policy that releases the table is dropped
// before the taker is created, though the taker's change comes first.
func TestSecurityPolicyPlan_HandsATableOver(t *testing.T) {
	c := qt.New(t)
	before := &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}
	after := &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "owner_id")}}

	statements := planPolicies(c,
		schemaext.ChangeRecord{Subject: mssqlschema.SecurityPolicyRef("rls", "new"), Value: &mssqldiff.SecurityPolicy{After: after,
			Access: mssqldiff.Assess(sqlServerNames, nil, after)}},
		schemaext.ChangeRecord{Subject: mssqlschema.SecurityPolicyRef("rls", "old"), Value: &mssqldiff.SecurityPolicy{Before: before,
			Access: mssqldiff.Assess(sqlServerNames, before, nil)}})

	c.Assert(strings.Join(statements, "\n"), qt.Equals, "DROP SECURITY POLICY [rls].[old]\n"+
		"CREATE SECURITY POLICY [rls].[new]\n    ADD FILTER PREDICATE [rls].[fn_tenant](owner_id) ON [app].[orders]\n    WITH (STATE = ON, SCHEMABINDING = ON)")
}

// TestSecurityPolicyRender pins the statements of each kind of change,
// measured on SQL Server 2025.
func TestSecurityPolicyRender(t *testing.T) {
	enabled := func(predicates ...mssqlschema.Predicate) *mssqlschema.ObservedSecurityPolicy {
		return &mssqlschema.ObservedSecurityPolicy{Predicates: predicates, Enabled: true, SchemaBinding: true}
	}
	tests := []struct {
		name   string
		before *mssqlschema.ObservedSecurityPolicy
		after  *mssqlschema.DesiredSecurityPolicy
		want   string
	}{
		{name: "a new disabled policy left out of replication", after: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}, Enabled: new(false), SchemaBinding: new(false), NotForReplication: true},
			want: "CREATE SECURITY POLICY [rls].[tenancy]\n    ADD FILTER PREDICATE [rls].[fn_tenant](tenant_id) ON [app].[orders]\n" +
				"    WITH (STATE = OFF, SCHEMABINDING = OFF)\n    NOT FOR REPLICATION"},
		{name: "a dropped policy", before: enabled(policyFilter(policyOrders, "[tenant_id]")), want: "DROP SECURITY POLICY [rls].[tenancy]"},
		{name: "schema binding turned off", before: enabled(policyFilter(policyOrders, "[tenant_id]")), after: &mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}, SchemaBinding: new(false)},
			want: "DROP SECURITY POLICY [rls].[tenancy]\nCREATE SECURITY POLICY [rls].[tenancy]\n" +
				"    ADD FILTER PREDICATE [rls].[fn_tenant](tenant_id) ON [app].[orders]\n    WITH (STATE = ON, SCHEMABINDING = OFF)"},
		{name: "a predicate dropped, altered and added while disabling", before: enabled(policyFilter(policyOrders, "[tenant_id]"), policyFilter(policyInvoices, "[tenant_id]")),
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "owner_id"),
				{Type: mssqlschema.Block, Function: policyFunction, Arguments: []string{"owner_id"}, Table: policyOrders, Operation: mssqlschema.BeforeDelete}},
				Enabled: new(false)},
			want: "ALTER SECURITY POLICY [rls].[tenancy] WITH (STATE = OFF)\nALTER SECURITY POLICY [rls].[tenancy]\n" +
				"    DROP FILTER PREDICATE ON [app].[invoices],\n    ALTER FILTER PREDICATE [rls].[fn_tenant](owner_id) ON [app].[orders],\n" +
				"    ADD BLOCK PREDICATE [rls].[fn_tenant](owner_id) ON [app].[orders] BEFORE DELETE"},
		{name: "a table spelled another way while enabling", before: &mssqlschema.ObservedSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, SchemaBinding: true},
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(mssqlschema.ObjectName{Schema: "APP", Name: "Orders"}, "tenant_id")}},
			want: "ALTER SECURITY POLICY [rls].[tenancy]\n    DROP FILTER PREDICATE ON [app].[orders]\n" +
				"ALTER SECURITY POLICY [rls].[tenancy]\n    ADD FILTER PREDICATE [rls].[fn_tenant](tenant_id) ON [APP].[Orders]\n" +
				"ALTER SECURITY POLICY [rls].[tenancy] WITH (STATE = ON)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements := planPolicies(c, schemaext.ChangeRecord{Subject: mssqlschema.SecurityPolicyRef("rls", "tenancy"),
				Value: &mssqldiff.SecurityPolicy{Before: test.before, After: test.after, Access: mssqldiff.Assess(sqlServerNames, test.before, test.after)}})
			c.Assert(strings.Join(statements, "\n"), qt.Equals, test.want)
		})
	}
}

// TestSecurityPolicyDeclarations pins a whole-schema render: the policy is
// created after the table it binds, and two enabled policies on one table
// are refused before any statement is returned.
func TestSecurityPolicyDeclarations(t *testing.T) {
	declared := func(c *qt.C, policies ...schemaext.Object) *schemamodel.Database {
		c.Helper()
		return &schemamodel.Database{
			Tables:          []schemamodel.Table{{StructName: "O", Name: "orders", Schema: "app"}},
			Fields:          []schemamodel.Field{{StructName: "O", FieldName: "ID", Name: "tenant_id", Type: "INT", Primary: true}},
			FeatureObjects:  must.Must(schemaext.NewObjects(policies...)),
			FeatureCoverage: must.Must(mssqlschema.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		}
	}
	t.Run("after the table", func(t *testing.T) {
		c := qt.New(t)
		statements, err := builtin.GetOrderedCreateStatements(declared(c, desiredPolicy("tenancy", mssqlschema.DesiredSecurityPolicy{
			Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}})), "sqlserver")
		c.Assert(err, qt.IsNil)
		c.Assert(statements[len(statements)-1], qt.Equals, "CREATE SECURITY POLICY [rls].[tenancy]\n"+
			"    ADD FILTER PREDICATE [rls].[fn_tenant](tenant_id) ON [app].[orders]\n    WITH (STATE = ON, SCHEMABINDING = ON);\n")
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE TABLE [app].[orders]")
	})
	t.Run("two enabled policies on one table", func(t *testing.T) {
		c := qt.New(t)
		statements, err := builtin.GetOrderedCreateStatements(declared(c,
			desiredPolicy("first", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}}),
			desiredPolicy("second", mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "tenant_id")}})), "sqlserver")
		c.Assert(err, qt.ErrorMatches, `(?s).*enabled security policies \[rls\]\.\[first\] and \[rls\]\.\[second\] both bind table \[app\]\.\[orders\].*Msg 33264.*`)
		c.Assert(statements, qt.IsNil)
	})
}

// TestSecurityPolicyPlanFeatures pins the step a change becomes: the dependent
// phase, a read of every table, function and argument column the policy binds
// before and after the change, and the plan's transaction when the change
// needs more than one statement.
func TestSecurityPolicyPlanFeatures(t *testing.T) {
	builder := objectidentity.NewBuilder(sqlServerNames)
	function := builder.TableParts("rls", "fn_tenant")
	function.Kind = objectidentity.KindFunction
	before := &mssqlschema.ObservedSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyOrders, "[tenant_id]")}, Enabled: true, SchemaBinding: true}
	tests := []struct {
		name            string
		after           *mssqlschema.DesiredSecurityPolicy
		wantTransaction plangraph.Transaction
	}{
		{name: "an argument changed in place", wantTransaction: plangraph.TransactionAllowed,
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyInvoices, "owner_id")}}},
		{name: "schema binding turned off", wantTransaction: plangraph.TransactionRequired,
			after: &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{policyFilter(policyInvoices, "owner_id")}, SchemaBinding: new(false)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := mssqlschema.SecurityPolicyRef("rls", "tenancy")

			result, err := must.Must(builtin.New()).PlanFeatures(context.Background(), featureplan.Request{Target: "sqlserver", Identifiers: sqlServerNames,
				Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &mssqldiff.SecurityPolicy{Before: before, After: test.after,
					Access: mssqldiff.Assess(sqlServerNames, before, test.after)}}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 1)
			c.Assert(result.Contributions[0].Steps, qt.HasLen, 1)
			step := result.Contributions[0].Steps[0]
			c.Assert(step.Payload.Phase, qt.Equals, featureplan.PhaseDependent)
			c.Assert(step.Transaction, qt.Equals, test.wantTransaction)
			c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{
				{Subject: subject, Action: plangraph.Alter},
				{Subject: builder.TableParts("app", "invoices"), Action: plangraph.Read},
				{Subject: function, Action: plangraph.Read},
				{Subject: builder.ColumnParts("app", "invoices", "owner_id"), Action: plangraph.Read},
				{Subject: builder.TableParts("app", "orders"), Action: plangraph.Read},
				{Subject: builder.ColumnParts("app", "orders", "tenant_id"), Action: plangraph.Read},
			})
		})
	}
}
