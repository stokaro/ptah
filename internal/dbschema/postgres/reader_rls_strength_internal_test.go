package postgres

// White-box testing required: readRLSPoliciesForSchema and readTablesForSchema
// are unexported, and reaching them through ReadSchema would make a fake server
// answer every query the whole read issues, so a failure would no longer name
// the projection under test.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// rlsStrengthFakeServer answers the queries readRLSPoliciesForSchema and
// readTablesForSchema issue, reporting the given strength flags.
//
// It records each of the two queries, because a fake server answers by column
// name and hands back whatever the caller supplied: swapping the projection for
// a constant produces the same rows, so only the text says which question the
// reader asked the server.
func rlsStrengthFakeServer(
	restrictive, forced bool,
	policyQuery, tablesQuery *string,
) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "p.proname = 'pg_relation_size'"):
			return dbtest.QueryResult{
				Columns: []string{"exists"},
				Rows:    [][]driver.Value{{true}},
			}, nil
		case strings.Contains(query, "FROM pg_policy pol"):
			*policyQuery = query
			return dbtest.QueryResult{
				Columns: []string{
					"schema_name", "policy_name", "table_name", "policy_for",
					"to_roles", "using_expression", "with_check_expression",
					"comment", "restrictive",
				},
				Rows: [][]driver.Value{
					{"public", "p", "docs", "ALL", "PUBLIC", "true", "", "", restrictive},
				},
			}, nil
		case strings.Contains(query, "FROM information_schema.tables"):
			*tablesQuery = query
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema", "table_name", "table_type", "table_comment",
					"estimated_rows", "row_stats_unknown", "partitioned",
					"rls_enabled", "rls_forced", "unlogged", "row_ttl_options", "row_deletion_policy",
				},
				Rows: [][]driver.Value{
					{"public", "docs", "BASE TABLE", "", int64(0), false, false, true, forced, false, "[]", ""},
				},
			}, nil
		case strings.Contains(query, "FROM information_schema.columns"):
			return dbtest.QueryResult{
				Columns: []string{
					"table_name", "column_name", "data_type", "udt_name", "udt_schema",
					"element_udt_name", "element_udt_schema", "is_nullable", "column_default",
					"character_maximum_length", "numeric_precision", "numeric_scale",
					"datetime_precision", "collation_name", "ordinal_position",
					"generated_kind", "generated_expression", "identity_kind",
					"column_comment", "not_null_constraint_name", "owned_sequence_name",
				},
				Rows: [][]driver.Value{
					{"docs", "id", "integer", "int4", "", "", "", "NO", nil, nil, nil, nil, nil, "", int64(1), "", "", "", "", "", ""},
				},
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
}

// TestReadRLSPolicies_ReadsBackTheAsClause pins that a policy's strength
// survives the read.
//
// The renderer emits AS RESTRICTIVE, so a reader that did not report the flag
// would hand the comparison a permissive policy for every restrictive one on
// the server. Every apply would then plan the same change again, and each
// would leave the table permissive between the DROP and the CREATE.
func TestReadRLSPolicies_ReadsBackTheAsClause(t *testing.T) {
	rows := []struct {
		name        string
		restrictive bool
	}{
		{name: "restrictive", restrictive: true},
		{name: "permissive", restrictive: false},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			var policyQuery, tablesQuery string
			db := dbtest.Open(t, rlsStrengthFakeServer(row.restrictive, false, &policyQuery, &tablesQuery))
			reader := NewPostgreSQLReader(db.SQL, "public")

			policies, err := reader.readRLSPoliciesForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(policies, qt.HasLen, 1)
			c.Assert(policies[0].Restrictive, qt.Equals, row.restrictive)
			c.Assert(policyQuery, qt.Contains, "pol.polpermissive")
		})
	}
}

// TestReadTables_ReadsBackWhetherTheOwnerIsBound pins that a table's FORCE flag
// survives the read.
//
// It is a separate column from the one reporting enablement, and a reader that
// returned only the first would report a forced table as an ordinary enabled
// one, so a comparison could never see the difference.
func TestReadTables_ReadsBackWhetherTheOwnerIsBound(t *testing.T) {
	rows := []struct {
		name   string
		forced bool
	}{
		{name: "forced", forced: true},
		{name: "enabled only", forced: false},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			var policyQuery, tablesQuery string
			db := dbtest.Open(t, rlsStrengthFakeServer(false, row.forced, &policyQuery, &tablesQuery))
			reader := NewPostgreSQLReader(db.SQL, "public")

			tables, err := reader.readTablesForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.HasLen, 1)
			c.Assert(tables[0].RLSEnabled, qt.IsTrue)
			c.Assert(tables[0].RLSForced, qt.Equals, row.forced)
			c.Assert(tablesQuery, qt.Contains, "c.relforcerowsecurity")
		})
	}
}
