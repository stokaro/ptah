package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
)

func TestDeduplicate_KeepsOnePolicyNamePerTable(t *testing.T) {
	twoTables := []schemamodel.Table{
		{Name: "alpha_orders", StructName: "AlphaOrder"},
		{Name: "zeta_orders", StructName: "ZetaOrder"},
	}

	tests := []struct {
		name     string
		tables   []schemamodel.Table
		policies []schemamodel.RLSPolicy
		want     []schemamodel.RLSPolicy
	}{
		{
			name:   "one name on two tables survives whole",
			tables: twoTables,
			policies: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "zeta_orders", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "zeta_orders", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			name:   "the same name on the same table collapses",
			tables: twoTables,
			policies: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
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
			policies: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
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
			tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "public.orders", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// A [Database] built in memory carries no quoting: this is what an
			// annotation, a YAML document or an HCL block produces, and Ptah
			// quotes every identifier it renders, so `table="ORDERS"` renders
			// `ON "ORDERS"` and names the relation `ORDERS`. That is a
			// different relation from the declared `orders` -- measured on
			// PostgreSQL 17.10, `CREATE POLICY p ON "ORDERS"` against a
			// database holding only `orders` exits 1 with `relation "ORDERS"
			// does not exist`.
			//
			// This row asserted the collapse until stokaro/ptah#1311 was
			// reviewed. Collapsing meant the author wrote `ORDERS` and Ptah
			// secured `orders`: a relocated access-control declaration, the
			// same defect the SQL frontend had, on the surface where the
			// quoting question has only one answer. Two spellings, two
			// policies, and the render reproduces the database's own answer.
			name:   "a case variant of one unqualified table is a second relation",
			tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			// The same pair in the other order, which is the control on the
			// rule being about the relations rather than about which
			// declaration came first. Neither spelling is rewritten into the
			// other, so order changes nothing.
			name:   "a case variant declared first keeps its own spelling",
			tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The direction the fold must not run. Ptah discards quoting, so
			// a table declared `ORDERS` is indistinguishable from one written
			// `"ORDERS"` -- and PostgreSQL reads those as two different
			// relations, only one of which a reference written `orders`
			// reaches. Measured on PostgreSQL 17.10, a file declaring
			// `CREATE TABLE "ORDERS"` and then `CREATE POLICY p ON orders`
			// exits 3 with `relation "orders" does not exist`. Folding the
			// declaration up to meet the reference would instead protect
			// `ORDERS` and leave the named relation unprotected, so the
			// reference keeps its spelling and nothing binds.
			name:   "a case-preserving declared table does not capture a lower-case reference",
			tables: []schemamodel.Table{{Name: "ORDERS", StructName: "OrdersTable"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The schema half of a qualified name is an identifier too, and
			// answers the same question. Measured on PostgreSQL 17.10, a file
			// declaring `CREATE SCHEMA "App"` and then naming `app.ledger`
			// exits 3 with `schema "app" does not exist`.
			name:   "a case-preserving declared schema does not capture a folded reference",
			tables: []schemamodel.Table{{Name: "orders", Schema: "App", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "app.orders", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "app.orders", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The pair to the row above, differing only in the case of the
			// declared schema, and the answer is the same for the same reason:
			// nothing here was ever written unquoted. `table="APP.ORDERS"`
			// renders `ON "APP"."ORDERS"`, which PostgreSQL 17.10 answers with
			// `relation "APP.ORDERS" does not exist`, and quietly rewriting it
			// to `app.orders` would secure a relation the author did not name.
			//
			// Case folding belongs where quoting still exists. The SQL frontend
			// folds `APP.ORDERS` written without quotes into `app.orders`
			// before it ever reaches this resolver, per component, so a schema
			// file gets PostgreSQL's answer and a schema built in memory gets
			// the one its own renderer will produce.
			name:   "a case variant of a qualified reference keeps its spelling",
			tables: []schemamodel.Table{{Name: "orders", Schema: "app", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "APP.ORDERS", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "APP.ORDERS", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The control that bounds the case fold: a quoted identifier keeps
			// its case, so `orders` and `"ORDERS"` are two tables. PostgreSQL
			// 17.10 accepts a policy called `p` on each and reports both rows
			// in pg_policy. An ambiguous fold must resolve to nothing rather
			// than pick one.
			name: "two tables differing only in case keep one policy each",
			tables: []schemamodel.Table{
				{Name: "orders", StructName: "Order"},
				{Name: "ORDERS", StructName: "ORDERSTable"},
			},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "orders", UsingExpression: "tenant_id = 1"},
				{Name: "p", Table: "ORDERS", UsingExpression: "tenant_id = 2"},
			},
		},
		{
			// A policy on a table nothing declares keeps its spelling. The
			// resolver maps a reference onto a declared table or leaves it
			// alone; it does not invent one.
			name:   "a policy on an undeclared table keeps its spelling",
			tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
			policies: []schemamodel.RLSPolicy{
				{Name: "p", Table: "archive.SHIPMENTS", UsingExpression: "tenant_id = 1"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "p", Table: "archive.SHIPMENTS", UsingExpression: "tenant_id = 1"},
			},
		},
		{
			// The control that separates the fold above from folding every
			// qualified spelling together: two tables of the same name in two
			// schemas are two tables, and one policy name on each is two
			// policies. PostgreSQL accepts both.
			name: "one name on the same table name in two schemas survives whole",
			tables: []schemamodel.Table{
				{Name: "orders", Schema: "tenanta", StructName: "TenantAOrder"},
				{Name: "orders", Schema: "tenantb", StructName: "TenantBOrder"},
			},
			policies: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "tenanta.orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "tenantb.orders", UsingExpression: "tenant_id = 2"},
			},
			want: []schemamodel.RLSPolicy{
				{Name: "tenant_isolation", Table: "tenanta.orders", UsingExpression: "tenant_id = 1"},
				{Name: "tenant_isolation", Table: "tenantb.orders", UsingExpression: "tenant_id = 2"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Tables:      test.tables,
				RLSPolicies: test.policies,
			}

			schemamodel.Deduplicate(database)

			c.Assert(database.RLSPolicies, qt.DeepEquals, test.want)
		})
	}
}
