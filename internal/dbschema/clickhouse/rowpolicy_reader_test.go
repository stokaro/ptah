package clickhouse_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/dbschema/dbtest"
)

// rowPolicyColumns are the columns the reader selects from
// system.row_policies, in its order.
var rowPolicyColumns = []string{
	"short_name", "table", "select_filter", "is_restrictive", "apply_to_all", "apply_to_list", "apply_to_except",
}

// answerRowPolicies answers the row policy read with rows, and every other
// statement as a database of views does.
func answerRowPolicies(rows [][]driver.Value, refusal error) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "FROM system.row_policies") {
			return clickHouseViewReaderQuery(query, args)
		}
		if refusal != nil {
			return dbtest.QueryResult{}, refusal
		}
		return dbtest.QueryResult{Columns: rowPolicyColumns, Rows: rows}, nil
	}
}

// TestReaderReadSchema_ReadsRowPoliciesAsOwnerObjects reads each policy on a
// table of the database as the ClickHouse owner's observed row policy, under
// a reference that leaves the database to the connection, as its table's
// does: the composition from is_restrictive, so a restrictive policy is never
// read as permissive (stokaro/ptah#4343), a NULL filter as none, and ALL
// EXCEPT from apply_to_all and its exceptions. The read describes every row
// policy, so one it does not report is absent.
func TestReaderReadSchema_ReadsRowPoliciesAsOwnerObjects(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answerRowPolicies([][]driver.Value{
		{"tenant", "orders", "tenant_id = 1", uint8(1), uint8(1), []string{}, []string{"admin"}},
		{"open", "orders", nil, uint8(0), uint8(0), []string{"alice", "bob"}, []string{}},
	}, nil))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	objects, err := schema.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		{Ref: chschema.RowPolicyRef("", "orders", "open"), Value: &chschema.ObservedRowPolicy{
			Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice", "bob"}, Except: []string{}},
		}},
		{Ref: chschema.RowPolicyRef("", "orders", "tenant"), Value: &chschema.ObservedRowPolicy{
			Filter: new("tenant_id = 1"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{All: true, Names: []string{}, Except: []string{"admin"}},
		}},
	})
	c.Assert(schema.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "other")).State,
		qt.Equals, schemaext.Complete)
	c.Assert(schema.RLSPolicies, qt.HasLen, 0)
}

// TestReaderReadSchema_RowPoliciesTheAccountMayNotReadAreUninspected records
// the row policies an account may not read as uninspected rather than as
// none, and keeps the rest of the description: a declared policy is then
// undecided, not created over one the read did not see.
func TestReaderReadSchema_RowPoliciesTheAccountMayNotReadAreUninspected(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answerRowPolicies(nil,
		&clickhousedriver.Exception{Code: 497, Name: "ACCESS_DENIED", Message: "lowpriv: Not enough privileges."}))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(schema.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "tenant")),
		qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the account may not read system.row_policies"})
	c.Assert(schema.Views, qt.HasLen, 1)
}
