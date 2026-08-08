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
	twoTables := []goschema.Table{
		{Name: "alpha_orders", StructName: "AlphaOrder"},
		{Name: "zeta_orders", StructName: "ZetaOrder"},
	}

	tests := []struct {
		name     string
		tables   []goschema.Table
		policies []goschema.RLSPolicy
		want     []goschema.RLSPolicy
	}{
		{
			name:   "one name on two tables survives whole",
			tables: twoTables,
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
			name:   "the same name on the same table collapses",
			tables: twoTables,
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
			name:   "the same name on the same table collapses despite a different definition",
			tables: twoTables,
			policies: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// A table declared without a schema is reached both as `orders`
			// and as `public.orders`, and PostgreSQL treats those as one
			// table: measured on PostgreSQL 17.10, `CREATE POLICY p ON orders`
			// followed by `CREATE POLICY p ON public.orders` is refused with
			// `policy "p" for table "orders" already exists`. Keying the two
			// spellings apart kept both, and `ptah schema render` then emitted
			// a pair of CREATE POLICY statements the database rejects. The
			// survivor is the first, so the applied USING expression is the
			// one written first rather than whichever spelling came last.
			name:   "two spellings of one unqualified table collapse onto the first",
			tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
			policies: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "public.orders", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The schema is not the only part of a table that has more than
			// one spelling. An unquoted PostgreSQL identifier folds to lower
			// case, so `ORDERS` names the table declared as `orders`: measured
			// on PostgreSQL 17.10, `CREATE POLICY p ON orders` followed by
			// `CREATE POLICY p ON ORDERS` is refused with `policy "p" for
			// table "orders" already exists`.
			name:   "a case variant of one unqualified table collapses onto the first",
			tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
			policies: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// Collapsing onto the first declaration is only half an answer if
			// the first declaration is the variant spelling. The survivor has
			// to name the declared table, because the renderer quotes what it
			// is given and `CREATE POLICY "p" ON "ORDERS"` is answered by
			// `relation "ORDERS" does not exist`.
			name:   "a case variant declared first still names the declared table",
			tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
			policies: []goschema.RLSPolicy{
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
			want: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			// The control that bounds the case fold: a quoted identifier keeps
			// its case, so `orders` and `"ORDERS"` are two tables. PostgreSQL
			// 17.10 accepts a policy called `p` on each and reports both rows
			// in pg_policy. An ambiguous fold must resolve to nothing rather
			// than pick one.
			name: "two tables differing only in case keep one policy each",
			tables: []goschema.Table{
				{Name: "orders", StructName: "Order"},
				{Name: "ORDERS", StructName: "ORDERSTable"},
			},
			policies: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			// A policy on a table nothing declares keeps its spelling. The
			// resolver maps a reference onto a declared table or leaves it
			// alone; it does not invent one.
			name:   "a policy on an undeclared table keeps its spelling",
			tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
			policies: []goschema.RLSPolicy{
				{Name: "p", Table: "archive.SHIPMENTS", UsingExpression: "tenant_id = 1"},
			},
			want: []goschema.RLSPolicy{
				{Name: "p", Table: "archive.SHIPMENTS", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The control that separates the fold above from folding every
			// qualified spelling together: two tables of the same name in two
			// schemas are two tables, and one policy name on each is two
			// policies. PostgreSQL accepts both.
			name: "one name on the same table name in two schemas survives whole",
			tables: []goschema.Table{
				{Name: "orders", Schema: "tenanta", StructName: "TenantAOrder"},
				{Name: "orders", Schema: "tenantb", StructName: "TenantBOrder"},
			},
			policies: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "tenanta.orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "tenantb.orders", UsingExpression: "tenant_id = 2"},
			},
			want: []goschema.RLSPolicy{
				{Name: "tenant_isolation", Table: "tenanta.orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "tenantb.orders", UsingExpression: "tenant_id = 2"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &goschema.Database{
				Tables:      test.tables,
				RLSPolicies: test.policies,
			}

			goschema.Deduplicate(database)

			c.Assert(database.RLSPolicies, qt.DeepEquals, test.want)
		})
	}
}
