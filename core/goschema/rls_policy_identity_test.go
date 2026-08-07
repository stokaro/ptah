package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// A PostgreSQL RLS policy name is scoped to its table, not to the schema.
// Measured on PostgreSQL 17.10: CREATE POLICY tenant_isolation succeeds on
// public.alpha_orders and again on public.zeta_orders, leaving two rows in
// pg_policy, and is refused only when repeated on the same table. Recording a
// policy by name alone therefore threw one of the two away before anything
// downstream could compare it (stokaro/ptah#1276).
const sharedPolicyNameSource = `package entities

//ptah:schema:rls:enable table="alpha_orders"
//ptah:schema:rls:policy name="tenant_isolation" table="alpha_orders" for="ALL" to="PUBLIC" using="tenant_id = 1"

//ptah:schema:table name="alpha_orders"
type AlphaOrder struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}

//ptah:schema:rls:enable table="zeta_orders"
//ptah:schema:rls:policy name="tenant_isolation" table="zeta_orders" for="ALL" to="PUBLIC" using="tenant_id = 2"

//ptah:schema:table name="zeta_orders"
type ZetaOrder struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`

func TestParseSource_KeepsOnePolicyNamePerTable(t *testing.T) {
	c := qt.New(t)

	database := mustParseSource(c, "fixture.go", sharedPolicyNameSource)

	c.Assert(database.RLSPolicies, qt.HasLen, 2)
	c.Assert(database.RLSPolicies[0].Name, qt.Equals, "tenant_isolation")
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "alpha_orders")
	c.Assert(database.RLSPolicies[0].UsingExpression, qt.Equals, "tenant_id = 1")
	c.Assert(database.RLSPolicies[1].Name, qt.Equals, "tenant_isolation")
	c.Assert(database.RLSPolicies[1].Table, qt.Equals, "zeta_orders")
	c.Assert(database.RLSPolicies[1].UsingExpression, qt.Equals, "tenant_id = 2")
}

// TestParseSource_RecordsAStructAttachedPolicyOnce is the control the
// table-scoped key must not break: a policy annotation attached to its struct
// is also visible to the file-wide comment scan, and it must still be recorded
// exactly once.
func TestParseSource_RecordsAStructAttachedPolicyOnce(t *testing.T) {
	c := qt.New(t)

	source := `package entities

//ptah:schema:rls:enable table="alpha_orders"
//ptah:schema:rls:policy name="tenant_isolation" table="alpha_orders" for="ALL" to="PUBLIC" using="tenant_id = 1"
//ptah:schema:table name="alpha_orders"
type AlphaOrder struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`

	database := mustParseSource(c, "fixture.go", source)

	c.Assert(database.RLSPolicies, qt.HasLen, 1)
	c.Assert(database.RLSPolicies[0].Name, qt.Equals, "tenant_isolation")
	c.Assert(database.RLSPolicies[0].Table, qt.Equals, "alpha_orders")
}

func TestDeduplicate_KeepsOnePolicyNamePerTable(t *testing.T) {
	tests := []struct {
		name     string
		policies []goschema.RLSPolicy
		want     []goschema.RLSPolicy
	}{
		{
			name: "one name on two tables survives whole",
			policies: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "zeta_orders", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "zeta_orders", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			name: "the same name on the same table collapses",
			policies: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			},
			want: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The key is the identity, not the definition: PostgreSQL would
			// refuse the second CREATE POLICY outright, so two declarations of
			// one policy still collapse to the first even when their
			// definitions disagree.
			name: "the same name on the same table collapses despite a different definition",
			policies: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &goschema.Database{
				Tables: []goschema.Table{
					{Name: "alpha_orders", StructName: "AlphaOrder"},
					{Name: "zeta_orders", StructName: "ZetaOrder"},
				},
				RLSPolicies: test.policies,
			}

			goschema.Deduplicate(database)

			c.Assert(database.RLSPolicies, qt.DeepEquals, test.want)
		})
	}
}
