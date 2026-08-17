package postgres

// White-box testing required: what this file asserts is a property of the SQL
// the reader SENDS and of the projection it scans, and the exported surface
// cannot show either. rowTTLOptionsExpr and readRowTTL are unexported, and
// whether a table has no TTL because the catalog said so or because the
// projection was never asked for is invisible from outside -- which is exactly
// the difference the capability gate makes.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadRowTTL_DecodesTheCatalogProjection pins the decode of the JSON array
// the projection fetches.
//
// The inputs are what `array_to_json(c.reloptions)::text` returned on live
// CockroachDB v26.2.5, transcribed rather than invented. The escape-string row
// is the one that matters: an expression containing a quote comes back
// backslash-escaped, and a decoder that assumed doubled quotes would corrupt
// the most common non-trivial expression there is.
func TestReadRowTTL_DecodesTheCatalogProjection(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		want    *ast.RowTTLSpec
	}{
		{
			name:    "a target that was never asked, or a table with no parameters",
			encoded: "[]",
			want:    nil,
		},
		{
			name:    "a table carrying only parameters this reader does not model",
			encoded: `["schema_locked=true"]`,
			want:    nil,
		},
		{
			name:    "the issue's reproducer as v26.2.5 reports it",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'", "schema_locked=true"]`,
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:    "the same table on v25.4.14, which adds no schema_locked",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'"]`,
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:    "an expression carrying a quote",
			encoded: `["ttl='on'", "ttl_expiration_expression=e'expires_at + INTERVAL \\'1 day\\''"]`,
			want:    &ast.RowTTLSpec{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
		},
		{
			name: "a policy with knobs",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'", ` +
				`"ttl_job_cron='@daily'", "ttl_select_batch_size=500"]`,
			want: &ast.RowTTLSpec{
				ExpirationExpression: "expires_at",
				JobCron:              "@daily",
				SelectBatchSize:      new(int64(500)),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			spec, err := readRowTTL(test.encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(spec, qt.DeepEquals, test.want)
		})
	}
}

// TestRowTTLOptionsExpr_IsAskedOnlyWhereItCanBeAnswered pins the capability
// gate on the projection itself.
//
// The column exists on PostgreSQL too, so an ungated projection would be valid
// there -- but a read that asks a target about a feature it does not have is a
// read that has to be right about a catalog nobody exercises, and the Spanner
// PostgreSQL interface has already shown that a pg_catalog column existing is
// not the same as it being readable (stokaro/ptah#942).
func TestRowTTLOptionsExpr_IsAskedOnlyWhereItCanBeAnswered(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "cockroachdb asks the catalog", caps: capability.CockroachDB26(), wantAsked: true},
		{name: "postgres does not", caps: capability.Postgres17(), wantAsked: false},
		{name: "yugabytedb does not", caps: capability.YugabyteDB25(), wantAsked: false},
		{name: "spanner does not", caps: capability.SpannerPostgres(), wantAsked: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reader := &Reader{caps: test.caps}

			c.Assert(strings.Contains(reader.rowTTLOptionsExpr(), "c.reloptions"), qt.Equals, test.wantAsked)
		})
	}
}

// TestReadTablesForSchema_CarriesRowTTLThroughTheRead is the behavioral half:
// the projection, the scan and the decode together, against a server that
// answers the way CockroachDB answers.
//
// Asserting the decode alone would pass against a reader that fetched the
// column and threw it away, which is the shape a refactor most easily produces.
func TestReadTablesForSchema_CarriesRowTTLThroughTheRead(t *testing.T) {
	tests := []struct {
		name       string
		caps       capability.Capabilities
		reloptions string
		want       *ast.RowTTLSpec
	}{
		{
			name:       "a CockroachDB table carrying a policy",
			caps:       capability.CockroachDB26(),
			reloptions: `["ttl='on'", "ttl_expiration_expression='expires_at'", "schema_locked=true"]`,
			want:       &ast.RowTTLSpec{ExpirationExpression: "expires_at"},
		},
		{
			name:       "a CockroachDB table carrying none",
			caps:       capability.CockroachDB26(),
			reloptions: `["schema_locked=true"]`,
			want:       nil,
		},
		{
			// The gate answers "[]" whatever the server holds, so a PostgreSQL
			// read reports no policy without asking.
			name:       "a PostgreSQL table is never described as carrying one",
			caps:       capability.Postgres17(),
			reloptions: "[]",
			want:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db := dbtest.Open(c, ttlTableServer(test.reloptions))
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps)

			tables, err := reader.readTablesForSchema("public")

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.HasLen, 1)
			c.Assert(tables[0].RowTTL, qt.DeepEquals, test.want)
		})
	}
}

// ttlTableServer answers the table read with one table carrying the given
// reloptions projection, and answers every other read of the table scan with
// nothing.
func ttlTableServer(reloptions string) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "FROM information_schema.tables"):
			return dbtest.QueryResult{
				Columns: []string{
					"table_schema", "table_name", "table_type", "table_comment",
					"estimated_rows", "row_stats_unknown", "partitioned", "rls_enabled",
					"row_ttl_options",
				},
				Rows: [][]driver.Value{
					{"public", "sessions", "BASE TABLE", "", int64(0), false, false, false, reloptions},
				},
			}, nil
		case strings.Contains(query, "FROM information_schema.columns"),
			strings.Contains(query, "information_schema.columns"):
			return dbtest.QueryResult{}, nil
		case strings.Contains(query, "SELECT"):
			return dbtest.QueryResult{Columns: []string{"ok"}, Rows: [][]driver.Value{{true}}}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
}

// TestHiddenColumnFilter_IsAskedOnlyWhereHiddenColumnsExist pins the gate on
// the column read.
//
// attishidden is a CockroachDB column: measured, PostgreSQL 18.4 and YugabyteDB
// 2026.1 have neither pg_attribute.attishidden nor
// information_schema.columns.is_hidden, so naming it unconditionally would break
// every column read on both engines rather than only changing what they report.
func TestHiddenColumnFilter_IsAskedOnlyWhereHiddenColumnsExist(t *testing.T) {
	tests := []struct {
		name       string
		caps       capability.Capabilities
		wantFilter bool
	}{
		{name: "cockroachdb filters them", caps: capability.CockroachDB26(), wantFilter: true},
		{name: "postgres has no such column", caps: capability.Postgres17(), wantFilter: false},
		{name: "yugabytedb has no such column", caps: capability.YugabyteDB25(), wantFilter: false},
		{name: "spanner has no such column", caps: capability.SpannerPostgres(), wantFilter: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reader := &Reader{caps: test.caps}

			c.Assert(strings.Contains(reader.hiddenColumnFilter(), "attishidden"), qt.Equals, test.wantFilter)
		})
	}
}

// TestReadColumnsForSchema_LeavesOutTheColumnsTheEngineOwns is the behavioral
// half: the filter has to reach the statement, not only exist.
//
// The two hidden columns CockroachDB creates are both here. crdb_internal_expiration
// is the one ttl_expire_after adds, and rowid is the one a table with no
// declared primary key gets -- older than row-level TTL and already leaking
// before this change, which `ptah db read` showed as a third column
// `"rowid" bigint PRIMARY KEY NOT NULL DEFAULT unique_rowid()`.
func TestReadColumnsForSchema_LeavesOutTheColumnsTheEngineOwns(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "cockroachdb asks for visible columns only", caps: capability.CockroachDB26(), wantAsked: true},
		{name: "postgres asks for all of them", caps: capability.Postgres17(), wantAsked: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var sent []string
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				sent = append(sent, query)
				return dbtest.QueryResult{}, nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps)

			_, err := reader.readColumnsForSchema("public")

			c.Assert(err, qt.IsNil)
			c.Assert(sent, qt.HasLen, 1)
			c.Assert(strings.Contains(sent[0], "attishidden"), qt.Equals, test.wantAsked)
		})
	}
}
