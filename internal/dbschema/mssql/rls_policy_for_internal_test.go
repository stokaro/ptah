package mssql

// White-box testing required: the predicate grouping is package-local and the
// FOR clause it produces is not reachable through an exported API.

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

var errUnaskedOperation = errors.New("the RLS read stopped asking for operation_desc")

// A filter-only policy reads back as FOR ALL -- stokaro/ptah#2211.
//
// The catalog reports operation_desc as NULL for a filter predicate, because
// `ADD FILTER PREDICATE` has no per-operation form. The BLOCK arm has always
// mapped that empty operation onto ALL; the FILTER arm left PolicyFor empty,
// and the two sides of the comparison then disagreed with each other: the HCL
// the reader itself writes carries `for = "ALL"`, and re-applying that file
// planned a DROP SECURITY POLICY and a CREATE SECURITY POLICY forever, leaving
// the table with no row-level security in between.
//
// Measured on SQL Server 2025 (RTM-CU8), 17.0.4075.5.
func TestReadRLSPolicies_FilterOnlyPolicyCoversEveryOperation(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringRLSPredicates([][]driver.Value{
		{"dbo", "tenant_filter", "docs", "([dbo].[fn_pred]([tenant]))", "FILTER", ""},
	}))
	reader := NewSQLServerReader(db.SQL, "dbo")

	policies, err := reader.readRLSPolicies(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(policies, qt.HasLen, 1)
	c.Assert(policies[0].PolicyFor, qt.Equals, "ALL")
	c.Assert(policies[0].UsingExpression, qt.Not(qt.Equals), "")
	c.Assert(policies[0].WithCheckExpression, qt.Equals, "")
}

// The block half names the operation, and the filter half beside it does not
// take that name away.
//
// The two predicates of one policy arrive as two rows. PolicyFor is seeded when
// the pair is created rather than assigned by the FILTER arm precisely so this
// holds in both row orders: a policy whose block predicate is written first
// would otherwise have AFTER UPDATE overwritten by the filter row arriving
// second, and the operation would read as ALL.
func TestReadRLSPolicies_TheFilterHalfDoesNotOverwriteTheBlockOperation(t *testing.T) {
	filter := []driver.Value{"dbo", "both", "docs", "([dbo].[fn_read]([tenant]))", "FILTER", ""}
	block := []driver.Value{"dbo", "both", "docs", "([dbo].[fn_write]([tenant]))", "BLOCK", "AFTER UPDATE"}

	tests := []struct {
		name string
		rows [][]driver.Value
	}{
		{name: "filter first", rows: [][]driver.Value{filter, block}},
		{name: "block first", rows: [][]driver.Value{block, filter}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db := dbtest.Open(t, answeringRLSPredicates(test.rows))
			reader := NewSQLServerReader(db.SQL, "dbo")

			policies, err := reader.readRLSPolicies(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(policies, qt.HasLen, 1)
			c.Assert(policies[0].PolicyFor, qt.Equals, "UPDATE")
			c.Assert(policies[0].UsingExpression, qt.Not(qt.Equals), "")
			c.Assert(policies[0].WithCheckExpression, qt.Not(qt.Equals), "")
		})
	}
}

// answeringRLSPredicates scripts the predicate read, and refuses a projection
// that does not ask for the operation it returns.
//
// A fake answering a fixed row set whatever the query selects cannot tell a
// reader that stopped asking for operation_desc from one that still does, and
// the operation is what these tests are about.
func answeringRLSPredicates(rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if !strings.Contains(query, "operation_desc") {
			return dbtest.QueryResult{}, errUnaskedOperation
		}
		return dbtest.QueryResult{
			Columns: []string{
				"schema_name", "policy_name", "table_name",
				"predicate_definition", "predicate_type_desc", "operation_desc",
			},
			Rows: rows,
		}, nil
	}
}
