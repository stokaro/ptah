package clickhouse_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
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
// statement as a database of views and one MergeTree table, orders, does.
func answerRowPolicies(rows [][]driver.Value, refusal error) dbtest.QueryHandler {
	return answerRowPolicyRelations(rows, refusal, nil, errors.New("the read asked which relations exist"))
}

// answerRowPolicyRelations is [answerRowPolicies] with the relations of the
// database, by name and engine, answering the read that asks which exist, or
// relationsErr refusing it.
func answerRowPolicyRelations(rows [][]driver.Value, refusal error, relations [][]driver.Value, relationsErr error) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM system.row_policies") && refusal != nil:
			return dbtest.QueryResult{}, refusal
		case strings.Contains(query, "FROM system.row_policies"):
			return dbtest.QueryResult{Columns: rowPolicyColumns, Rows: rows}, nil
		case strings.Contains(query, "SELECT name, engine") && relationsErr != nil:
			return dbtest.QueryResult{}, relationsErr
		case strings.Contains(query, "SELECT name, engine"):
			return dbtest.QueryResult{Columns: []string{"name", "engine"}, Rows: relations}, nil
		case strings.Contains(query, "engine LIKE '%MergeTree'"):
			return dbtest.QueryResult{Columns: []string{"name", "comment", "sorting_key", "primary_key", "engine_full", "partition_key", "sampling_key", "create_table_query"},
				Rows: [][]driver.Value{{"orders", "", "id", "id", "MergeTree ORDER BY id", "", "", "CREATE TABLE default.orders (`id` UInt64) ENGINE = MergeTree ORDER BY id"}}}, nil
		default:
			return clickHouseViewReaderQuery(query, args)
		}
	}
}

// TestReaderReadSchema_ReadsRowPoliciesAsOwnerObjects reads each policy on a
// table of the database as the ClickHouse owner's observed row policy, under
// a reference that leaves the database to the connection, as its table's
// does: the composition from is_restrictive, so a restrictive policy is never
// read as permissive (stokaro/ptah#4343), a NULL filter as none, and ALL
// EXCEPT from apply_to_all and its exceptions. The read describes every row
// policy, so one it does not report is absent. Every policy is on a table the
// read holds, so it does not ask which other relations exist: the fixture
// refuses that read.
func TestReaderReadSchema_ReadsRowPoliciesAsOwnerObjects(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answerRowPolicies([][]driver.Value{
		{"tenant", "orders", "tenant_id = 1", uint8(1), uint8(1), make([]string, 0), []string{"admin"}},
		{"open", "orders", nil, uint8(0), uint8(0), []string{"alice", "bob"}, make([]string, 0)},
	}, nil))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	objects, err := schema.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		{Ref: chschema.RowPolicyRef("", "orders", "open"), Value: &chschema.ObservedRowPolicy{
			Composition: chschema.Permissive, Roles: chschema.RoleSelection{Names: []string{"alice", "bob"}, Except: make([]string, 0)},
		}},
		{Ref: chschema.RowPolicyRef("", "orders", "tenant"), Value: &chschema.ObservedRowPolicy{
			Filter: new("tenant_id = 1"), Composition: chschema.Restrictive,
			Roles: chschema.RoleSelection{All: true, Names: make([]string, 0), Except: []string{"admin"}},
		}},
	})
	c.Assert(schema.FeatureCoverage.Lookup(chschema.RowPolicyKind, chschema.RowPolicyRef("", "orders", "other")).State,
		qt.Equals, schemaext.Complete)
	c.Assert(schema.RLSPolicies, qt.HasLen, 0)
}

// TestReaderReadSchema_ReadsAPolicyItsTableOutlived reads a policy on a name
// no relation of the database has, which is what the server keeps when the
// table is dropped, as an observed policy like any other, so the comparison
// can plan it; and records a policy on a relation the read does not hold as a
// table -- a view, a Distributed table, Ptah's own bookkeeping table -- or on
// every table of the database as one it cannot describe, naming the relation,
// so the comparison keeps and reports it. A policy on a held table stays
// described.
func TestReaderReadSchema_ReadsAPolicyItsTableOutlived(t *testing.T) {
	c := qt.New(t)
	policy := func(name, table string) []driver.Value {
		return []driver.Value{name, table, "1", uint8(0), uint8(1), make([]string, 0), make([]string, 0)}
	}
	db := dbtest.Open(t, answerRowPolicyRelations([][]driver.Value{
		policy("tenant", "orders"), policy("leak", "dropped"), policy("on_view", "active_users"),
		policy("on_dist", "events_all"), policy("on_log", "ptah_schema_apply_log"), policy("wide", ""),
	}, nil, [][]driver.Value{
		{"orders", "MergeTree"}, {"active_users", "View"}, {"events_all", "Distributed"}, {"ptah_schema_apply_log", "MergeTree"},
	}, nil))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	objects, err := schema.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	var refs []string
	for _, object := range objects {
		refs = append(refs, object.Ref.String())
	}
	c.Assert(refs, qt.DeepEquals, []string{chschema.RowPolicyRef("", "dropped", "leak").String(), chschema.RowPolicyRef("", "orders", "tenant").String()})
	for _, test := range []struct {
		ref  objectidentity.ID
		want schemaext.Knowledge
	}{
		{chschema.RowPolicyRef("", "orders", "tenant"), schemaext.Knowledge{State: schemaext.Complete}},
		{chschema.RowPolicyRef("", "dropped", "leak"), schemaext.Knowledge{State: schemaext.Complete}},
		{chschema.RowPolicyRef("", "active_users", "on_view"), schemaext.Knowledge{State: schemaext.Unrepresentable,
			Reason: `the policy is on "active_users" (engine View), which the read does not describe as a table; the plan keeps the policy`}},
		{chschema.RowPolicyRef("", "events_all", "on_dist"), schemaext.Knowledge{State: schemaext.Unrepresentable,
			Reason: `the policy is on "events_all" (engine Distributed), which the read does not describe as a table; the plan keeps the policy`}},
		{chschema.RowPolicyRef("", "ptah_schema_apply_log", "on_log"), schemaext.Knowledge{State: schemaext.Unrepresentable,
			Reason: `the policy is on "ptah_schema_apply_log", Ptah's own bookkeeping table, which the read leaves out; the plan keeps the policy`}},
		{chschema.RowPolicyRef("", "*", "wide"), schemaext.Knowledge{State: schemaext.Unrepresentable,
			Reason: "the policy is written ON analytics.*, for every table of the database, which Ptah does not model; the plan keeps the policy"}},
	} {
		c.Assert(schema.FeatureCoverage.Lookup(chschema.RowPolicyKind, test.ref), qt.DeepEquals, test.want, qt.Commentf("%s", test.ref))
	}
}

// TestReaderReadSchema_RowPolicyRelationsTheReadCannotListFailTheRead fails
// the read when it cannot ask which relations hold the names of policies on
// no table it holds, rather than reading those policies as outlived ones the
// plan would drop.
func TestReaderReadSchema_RowPolicyRelationsTheReadCannotListFailTheRead(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answerRowPolicyRelations([][]driver.Value{
		{"leak", "dropped", "1", uint8(0), uint8(1), make([]string, 0), make([]string, 0)},
	}, nil, nil, errors.New("catalog unavailable")))

	schema, err := clickhouse.NewClickHouseReader(db.SQL, "analytics").ReadSchemaContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `clickhouse: read row policies: read the relations the row policies are on: .*catalog unavailable`)
	c.Assert(schema, qt.IsNil)
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
