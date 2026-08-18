package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// policyCatalog is a database carrying one table and one policy on it.
func policyCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "orders", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Name: "tenant_isolation", Table: "public.orders", PolicyFor: "ALL",
				ToRoles: "app", UsingExpression: "tenant_id = current_setting('app.tenant')"},
		},
	}
}

// policyDescription is a desired schema declaring the table and, optionally,
// the policy.
func policyDescription(withPolicy bool) *goschema.Database {
	description := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Order", Name: "orders"}},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "integer", Primary: true},
		},
	}
	policies := map[bool][]goschema.RLSPolicy{
		true: {{
			StructName: "Order", Name: "tenant_isolation", Table: "orders", PolicyFor: "ALL",
			ToRoles: "app", UsingExpression: "tenant_id = current_setting('app.tenant')",
		}},
		false: nil,
	}
	description.RLSPolicies = policies[withPolicy]
	return description
}

func policyStates(c *qt.C, description *goschema.Database) (current, desired *schemastate.State) {
	c.Helper()
	profile := postgresProfile()
	rawDesired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	rawCurrent, err := schemastate.FromCatalog(policyCatalog(), profile.Dialect, profile.Semantics)
	c.Assert(err, qt.IsNil)
	desired, err = schemastate.Normalize(rawDesired, profile)
	c.Assert(err, qt.IsNil)
	current, err = schemastate.Normalize(rawCurrent, profile)
	c.Assert(err, qt.IsNil)
	return current, desired
}

func policyChanges(c *qt.C, description *goschema.Database) []schemachange.Change {
	c.Helper()
	current, desired := policyStates(c, description)
	changes, err := schemachange.Compare(current, desired, postgresProfile())
	c.Assert(err, qt.IsNil)
	kept := make([]schemachange.Change, 0)
	for _, change := range changes {
		kept = appendPolicyChange(kept, change)
	}
	return kept
}

func appendPolicyChange(kept []schemachange.Change, change schemachange.Change) []schemachange.Change {
	appenders := map[bool]func() []schemachange.Change{
		true:  func() []schemachange.Change { return append(kept, change) },
		false: func() []schemachange.Change { return kept },
	}
	return appenders[change.ID.Kind == objectidentity.KindPolicy]()
}

// TestPolicySilenceIsNotARemoval is the criterion stokaro/ptah#1664 carries
// forward from #1028: for the families where absence is ambiguous, a
// description that declines to describe one is SILENT, not empty.
//
// A description that simply omits the policy is asking for it to go. A
// description that records `not-described policy` is saying it did not look,
// and reading that as absence drops a policy nobody asked to drop.
func TestPolicySilenceIsNotARemoval(t *testing.T) {
	c := qt.New(t)
	description := policyDescription(false)
	description.NotDescribed = coverage.Set{}.WithKind(coverage.Policy)

	changes := policyChanges(c, description)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "not-described")
}

// TestPolicyOmissionIsARemoval is the control the test above cannot be: a model
// that withheld every removal would satisfy it and would never drop anything.
//
// A description that describes the family and does not carry the policy IS
// asking for it to go.
func TestPolicyOmissionIsARemoval(t *testing.T) {
	c := qt.New(t)

	changes := policyChanges(c, policyDescription(false))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
}

// TestPolicyDeclaredOnBothSidesIsNoChange pins the settled case.
func TestPolicyDeclaredOnBothSidesIsNoChange(t *testing.T) {
	c := qt.New(t)

	changes := policyChanges(c, policyDescription(true))

	c.Assert(changes, qt.HasLen, 0)
}

// TestPolicyIdentityKeepsOneNameOnTwoTablesApart pins the #1276 shape in the
// canonical state: a policy name is scoped to its table, so the same name on
// two tables is two policies.
func TestPolicyIdentityKeepsOneNameOnTwoTablesApart(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	description := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Order", Name: "orders"},
			{StructName: "Invoice", Name: "invoices"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{StructName: "Order", Name: "tenant_isolation", Table: "orders", PolicyFor: "ALL"},
			{StructName: "Invoice", Name: "tenant_isolation", Table: "invoices", PolicyFor: "ALL"},
		},
	}

	state, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)

	c.Assert(err, qt.IsNil)
	c.Assert(state.OfKind(objectidentity.KindPolicy), qt.HasLen, 2)
}

// TestPolicyPropertyChangeIsAModification pins that a policy whose expression
// changed is one object modified, and that the diagnostic names what differs.
//
// "The policy changed" tells an author something is wrong and not which clause
// to look at.
func TestPolicyPropertyChangeIsAModification(t *testing.T) {
	c := qt.New(t)
	description := policyDescription(true)
	description.RLSPolicies[0].UsingExpression = "tenant_id = current_setting('app.other')"
	description.RLSPolicies[0].ToRoles = "reader"

	changes := policyChanges(c, description)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Modify)
	c.Assert(changes[0].Changed, qt.DeepEquals, []string{"role", "using"})
}

// TestPolicyChangeIsRefusedByThePlannerRatherThanDropped pins the fail-closed
// half of comparing a family this planner does not render.
//
// Emitting nothing would be the dangerous answer: the caller would receive a
// successful plan whose statements change no policy at all.
func TestPolicyChangeIsRefusedByThePlannerRatherThanDropped(t *testing.T) {
	c := qt.New(t)
	changes := policyChanges(c, policyDescription(false))
	c.Assert(changes, qt.HasLen, 1)
	changes[0].Status = schemachange.Planned

	operations, err := schemachange.Plan(changes, postgresProfile())

	c.Assert(err, qt.ErrorIs, schemachange.ErrNotRendered)
	c.Assert(operations, qt.IsNil)
}

// TestPolicyIdentityFromCatalogKeepsOneNameOnTwoTablesApart is the catalog-side
// half of the identity property.
//
// The two adapters build the identity from different components -- one resolves
// the owning table through the struct that declared the policy, the other
// parses a qualified name the catalog reported -- so a defect in either is
// invisible to a test that exercises only the other.
func TestPolicyIdentityFromCatalogKeepsOneNameOnTwoTablesApart(t *testing.T) {
	c := qt.New(t)
	profile := postgresProfile()
	catalog := policyCatalog()
	catalog.RLSPolicies = append(catalog.RLSPolicies, dbschematypes.DBRLSPolicy{
		Name: "tenant_isolation", Table: "public.invoices", PolicyFor: "ALL",
	})

	state, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)

	c.Assert(err, qt.IsNil)
	c.Assert(state.OfKind(objectidentity.KindPolicy), qt.HasLen, 2)
}
