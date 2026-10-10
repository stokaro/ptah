package mssql_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mssql"
)

// answeringPolicies scripts the security policy read and answers every other
// catalog read with no rows.
func answeringPolicies(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "sys.security_policies") || !strings.Contains(query, "is_not_for_replication") {
			return dbtest.QueryResult{}, nil
		}
		return dbtest.QueryResult{
			Columns: []string{"schema_name", "policy_name", "is_enabled", "is_schema_bound", "is_not_for_replication",
				"table_schema", "table_name", "predicate_definition", "predicate_type_desc", "operation_desc"},
			Rows: rows,
		}, nil
	}
}

// predicate is one predicate as the owner models it.
func predicate(kind mssqlschema.PredicateType, function, table string, operation mssqlschema.BlockOperation, arguments ...string) mssqlschema.Predicate {
	return mssqlschema.Predicate{Type: kind, Function: mssqlschema.ObjectName{Schema: "rls", Name: function}, Arguments: arguments,
		Table: mssqlschema.ObjectName{Schema: "app", Name: table}, Operation: operation}
}

func TestReader_ReadsSecurityPolicies_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPolicies([][]driver.Value{
		{"rls", "tenancy", true, true, false, "app", "orders", "([rls].[fn]([tenant]))", "FILTER", nil},
		{"rls", "tenancy", true, true, false, "app", "orders", "([rls].[fn_write]([tenant],CONVERT([int],[owner])+(0)))", "BLOCK", "AFTER UPDATE"},
		{"rls", "tenancy", true, true, false, "app", "invoices", "([rls].[fn]([tenant]))", "FILTER", nil},
		{"rls", "dormant", false, false, true, nil, nil, nil, nil, nil},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(live.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
		must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "dormant"),
			mssqlschema.ObservedSecurityPolicy{NotForReplication: true})),
		must.Must(mssqlschema.ObservedSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"),
			mssqlschema.ObservedSecurityPolicy{Enabled: true, SchemaBinding: true, Predicates: []mssqlschema.Predicate{
				predicate(mssqlschema.Filter, "fn", "orders", "", "[tenant]"),
				predicate(mssqlschema.Block, "fn_write", "orders", mssqlschema.AfterUpdate, "[tenant]", "CONVERT([int],[owner])+(0)"),
				predicate(mssqlschema.Filter, "fn", "invoices", "", "[tenant]"),
			}})),
	})
	c.Assert(live.FeatureCoverage.Lookup(mssqlschema.SecurityPolicyKind, mssqlschema.SecurityPolicyRef("rls", "other")),
		qt.DeepEquals, schemaext.Knowledge{State: schemaext.Complete})
	c.Assert(live.RLSPolicies, qt.HasLen, 0)
}

// A predicate the reader cannot read as a function call leaves its policy
// recorded rather than described, so a comparison neither drops nor changes
// it; the other policies are still described.
func TestReader_RecordsAPolicyItCannotRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringPolicies([][]driver.Value{
		{"rls", "odd", true, true, false, "app", "orders", "(1)", "FILTER", nil},
		{"rls", "tenancy", true, true, false, "app", "orders", "([rls].[fn]([tenant]))", "FILTER", nil},
	}))

	live, err := mssql.NewSQLServerReader(db.SQL, "").ReadSchema()

	c.Assert(err, qt.IsNil)
	c.Assert(live.FeatureObjects.Refs(), qt.HasLen, 1)
	c.Assert(live.FeatureCoverage.Lookup(mssqlschema.SecurityPolicyKind, mssqlschema.SecurityPolicyRef("rls", "odd")),
		qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected, Reason: mssql.UnreadablePredicateReason})
}
