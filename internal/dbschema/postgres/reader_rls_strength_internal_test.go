package postgres

// White-box testing required: readPolicies, readRowSecurity and readTablesForSchema
// are unexported, and reaching them through ReadSchema would make a fake server
// answer every query the whole read issues, so a failure would no longer name
// the projection under test.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/dbschema/dbtest"
)

// rlsStrengthFakeServer answers the queries readPolicies and
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
					"table_name", "policy_name", "command", "roles",
					"using_expression", "with_check_expression", "comment", "permissive",
				},
				Rows: [][]driver.Value{
					{"docs", "p", "*", nil, "true", nil, "", !restrictive},
					{"docs", "q", "r", `["App", "reader"]`, nil, nil, "reads", true},
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
					"column_comment", "not_null_constraint_name", "owned_sequence_name", "udt_schema",
				},
				Rows: [][]driver.Value{
					{"docs", "id", "integer", "int4", "", "", "", "NO", nil, nil, nil, nil, nil, "", int64(1), "", "", "", "", "", "", "pg_catalog"},
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

			var schema catalog.Database
			err := reader.readPolicies(t.Context(), &schema, "public")

			c.Assert(err, qt.IsNil)
			composition := map[bool]pgpolicy.Composition{true: pgpolicy.Restrictive, false: pgpolicy.Permissive}[row.restrictive]
			c.Assert(observedPolicies(c, schema), qt.DeepEquals, map[string]pgpolicy.ObservedPolicy{
				"p": {Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Using: new("true"), Composition: composition},
				"q": {Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "App"}, {Name: "reader"}}, Composition: pgpolicy.Permissive, Comment: "reads"},
			})
			c.Assert(policyQuery, qt.Contains, "pol.polpermissive")
		})
	}
}

// observedPolicies returns the policies a read reported, by name.
func observedPolicies(c *qt.C, schema catalog.Database) map[string]pgpolicy.ObservedPolicy {
	c.Helper()
	objects, err := schema.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	policies := make(map[string]pgpolicy.ObservedPolicy, len(objects))
	for _, object := range objects {
		policies[object.Ref.Name.Source] = *object.Value.(*pgpolicy.ObservedPolicy)
	}
	return policies
}

// TestReadRowSecurity_ReportsSwitchesToTheOwner pins that a table's switches
// leave the shared model and reach the row-security owner as a facet, with
// complete knowledge claimed for both models: a table without the facet has
// both switches off.
func TestReadRowSecurity_ReportsSwitchesToTheOwner(t *testing.T) {
	c := qt.New(t)
	var policyQuery, tablesQuery string
	db := dbtest.Open(t, rlsStrengthFakeServer(false, true, &policyQuery, &tablesQuery))
	reader := NewPostgreSQLReader(db.SQL, "public")
	schema := catalog.Database{Tables: []catalog.Table{{Name: "docs", RLSEnabled: true, RLSForced: true}, {Name: "plain"}}}

	err := reader.readRowSecurity(t.Context(), &schema)

	c.Assert(err, qt.IsNil)
	value, found, err := schema.Tables[0].Facets.Get(pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value, qt.DeepEquals, &pgpolicy.ObservedTableState{Enabled: true, Forced: true})
	c.Assert(schema.Tables[0].RLSEnabled || schema.Tables[0].RLSForced, qt.IsFalse)
	c.Assert(schema.Tables[1].Facets.Kinds(), qt.HasLen, 0)
	c.Assert(schema.FeatureCoverage.Lookup(pgpolicy.TableStateKind, pgpolicy.PolicyRef("", "plain", "x")).State, qt.Equals, schemaext.Complete)
	c.Assert(observedPolicies(c, schema), qt.HasLen, 2)
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
