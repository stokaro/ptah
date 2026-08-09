package generator

// White-box testing required: the down direction is reached through
// generateDownMigrationSQL, which is unexported, and the defect these tests pin
// lives between the reversed diff and the introspected schema the rollback is
// planned against. No exported entry point reaches that pair without a live
// database and a migration directory.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// desiredPolicyOnOrders is the desired state for the rollback tests: one policy
// on one table, spelled however the caller writes it, with the USING expression
// the migration is changing to.
func desiredPolicyOnOrders(spelling string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: spelling, StructName: "Order"},
		},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "tenant_isolation",
			Table:           spelling,
			StructName:      "Order",
			PolicyFor:       "ALL",
			ToRoles:         "PUBLIC",
			UsingExpression: "tenant_id = 2",
		}},
	}
}

// introspectedPolicyOnOrders is the database side, carrying the spelling the
// PostgreSQL reader reports and the USING expression a rollback has to restore.
func introspectedPolicyOnOrders(spelling string) *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "orders", Schema: "public", RLSEnabled: true},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{{
			Name:            "tenant_isolation",
			Table:           spelling,
			PolicyFor:       "ALL",
			ToRoles:         "PUBLIC",
			UsingExpression: "tenant_id = 1",
		}},
	}
}

// TestGenerateDownMigrationSQL_RestoresAModifiedRLSPolicyAcrossSpellings pins
// the rollback of a changed row-level security policy.
//
// The comparator deliberately treats `orders` and `public.orders` as one table
// and then reports the DESIRED side's spelling in RLSPolicyDiff.TableName. The
// down direction plans against the INTROSPECTED schema, whose policy carries
// the database's spelling, so a raw-string lookup found nothing and the
// generated rollback was exactly `-- No rollback operations needed` -- a policy
// body changed on the way up and nothing put it back (stokaro/ptah#1311).
//
// The diff is produced by the real comparator rather than written out by hand:
// which spelling lands in TableName is the comparator's decision, and a
// hand-built diff would be asserting this test's own assumption instead of the
// pipeline's behavior.
//
// Every combination of the two default-schema spellings is covered, because the
// pair that fails is the pair that differs and either side may be the qualified
// one.
func TestGenerateDownMigrationSQL_RestoresAModifiedRLSPolicyAcrossSpellings(t *testing.T) {
	tests := []struct {
		name      string
		desired   string
		database  string
		wantTable string
	}{
		{
			name:      "desired qualified, database bare",
			desired:   "public.orders",
			database:  "orders",
			wantTable: "orders",
		},
		{
			name:      "desired bare, database qualified",
			desired:   "orders",
			database:  "public.orders",
			wantTable: "public.orders",
		},
		{
			name:      "both bare",
			desired:   "orders",
			database:  "orders",
			wantTable: "orders",
		},
		{
			name:      "both qualified",
			desired:   "public.orders",
			database:  "public.orders",
			wantTable: "public.orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := desiredPolicyOnOrders(test.desired)
			database := introspectedPolicyOnOrders(test.database)

			diff := schemadiff.CompareWithDialect(desired, database, "postgres")
			c.Assert(diff.RLSPoliciesModified, qt.HasLen, 1)

			downSQL, err := generateDownMigrationSQL(diff, desired, database, "postgres")
			c.Assert(err, qt.IsNil)

			// The rollback drops the changed policy and recreates it from the
			// pre-change database definition, on the table the database itself
			// reports.
			c.Assert(downSQL, qt.Not(qt.Contains), "No rollback operations needed")
			c.Assert(legacyRenderedSQL(downSQL), qt.Contains,
				"DROP POLICY IF EXISTS tenant_isolation ON "+test.wantTable+";")
			c.Assert(legacyRenderedSQL(downSQL), qt.Contains, "USING (tenant_id = 1)")
			c.Assert(downSQL, qt.Not(qt.Contains), "tenant_id = 2")
		})
	}
}
