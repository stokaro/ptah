package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// dottedPolicyCase is one pair of (table, policy name) that a delimiter-joined
// identity cannot tell apart.
type dottedPolicyCase struct {
	name     string
	tables   []goschema.Table
	policies []goschema.RLSPolicy
	wantKept int
}

// TestDeduplicate_KeepsPoliciesADelimiterWouldCollapse pins that the (table,
// policy name) identity is injective.
//
// Keying by the policy name alone dropped the second of two identically named
// policies on different tables (stokaro/ptah#1276). Repairing that by joining
// the resolved table and the policy name with a "." reintroduced the same loss
// through a different door: both components are independently valid PostgreSQL
// identifiers that may contain a literal dot, and Table.QualifiedName already
// spends the dot structurally.
//
// Measured on PostgreSQL 17.10 -- both of these exist in one database at once,
// so the pair is a real catalog state rather than a synthetic one:
//
//	CREATE TABLE a (...);      CREATE POLICY "b.c" ON a ...   -- public | a | b.c
//	CREATE SCHEMA a;
//	CREATE TABLE a.b (...);    CREATE POLICY c ON a.b ...     -- a      | b | c
//
// Joined with a dot both become `a.b.c`, and deduplication keeps one.
func TestDeduplicate_KeepsPoliciesADelimiterWouldCollapse(t *testing.T) {
	tests := []dottedPolicyCase{
		{
			// The shape the reviewer reproduced.
			name: "a dotted policy name against a schema-qualified table",
			tables: []goschema.Table{
				{StructName: "A", Name: "a"},
				{StructName: "B", Name: "b", Schema: "a"},
			},
			policies: []goschema.RLSPolicy{
				{StructName: "A", Name: "b.c", Table: "a", PolicyFor: "SELECT", UsingExpression: "true"},
				{StructName: "B", Name: "c", Table: "a.b", PolicyFor: "SELECT", UsingExpression: "true"},
			},
			wantKept: 2,
		},
		{
			// The same collision with no schema in play, so the fix cannot be
			// "always qualify the table".
			name: "a dotted table name against a dotted policy name",
			tables: []goschema.Table{
				{StructName: "X", Name: "x.y"},
				{StructName: "Y", Name: "x"},
			},
			policies: []goschema.RLSPolicy{
				{StructName: "X", Name: "z", Table: "x.y", PolicyFor: "SELECT", UsingExpression: "true"},
				{StructName: "Y", Name: "y.z", Table: "x", PolicyFor: "SELECT", UsingExpression: "true"},
			},
			wantKept: 2,
		},
		{
			// The control against the fix becoming "never deduplicate": one
			// policy declared twice on one table is still one policy.
			name: "the same policy on the same table is still one",
			tables: []goschema.Table{
				{StructName: "A", Name: "a"},
			},
			policies: []goschema.RLSPolicy{
				{StructName: "A", Name: "p", Table: "a", PolicyFor: "SELECT", UsingExpression: "true"},
				{StructName: "A", Name: "p", Table: "a", PolicyFor: "SELECT", UsingExpression: "true"},
			},
			wantKept: 1,
		},
		{
			// And the row the original fix exists to protect.
			name: "one policy name on two tables survives whole",
			tables: []goschema.Table{
				{StructName: "A", Name: "alpha"},
				{StructName: "B", Name: "beta"},
			},
			policies: []goschema.RLSPolicy{
				{StructName: "A", Name: "tenant_isolation", Table: "alpha", PolicyFor: "SELECT", UsingExpression: "true"},
				{StructName: "B", Name: "tenant_isolation", Table: "beta", PolicyFor: "SELECT", UsingExpression: "true"},
			},
			wantKept: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &goschema.Database{Tables: test.tables, RLSPolicies: test.policies}

			goschema.Deduplicate(database)

			c.Assert(database.RLSPolicies, qt.HasLen, test.wantKept,
				qt.Commentf("kept %d of %d policies", len(database.RLSPolicies), len(test.policies)))
		})
	}
}
