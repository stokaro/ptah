package postgres

// White-box testing required: what this file asserts is a property of the SQL
// the reader SENDS and of the projection it scans, and the exported surface
// cannot show either. rowTTLOptionsExpr, rowTTLFacets and rowTTLCoverage are
// unexported, and
// whether a table has no TTL because the catalog said so or because the
// projection was never asked for is invisible from outside -- which is exactly
// the difference the capability gate makes.

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbrender"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/internal/dbschema/dbtest"
)

// TestRowTTLFacets_DecodesTheCatalogProjection pins the decode of the JSON
// array the projection fetches into the owner's observed policy.
//
// The inputs are what `array_to_json(c.reloptions)::text` returned on live
// CockroachDB v25.4.14 and v26.2.5, transcribed rather than invented. The
// escape-string row is the one that matters: an expression containing a quote
// comes back backslash-escaped, and a decoder that assumed doubled quotes would
// corrupt the most common non-trivial expression there is.
func TestRowTTLFacets_DecodesTheCatalogProjection(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
		want    *crdbschema.Policy
	}{
		{
			name:    "a table with no parameters",
			encoded: "[]",
			want:    nil,
		},
		{
			// v26.2.5 puts schema_locked on every table, and v25.4.14 nothing.
			// Neither is a TTL parameter, and both are ignored rather than
			// refused: a server that adds a storage parameter must not break a
			// read.
			name:    "a table carrying only parameters the owner does not model",
			encoded: `["schema_locked=true", "fillfactor=70"]`,
			want:    nil,
		},
		{
			name:    "the derived marker alone is no policy",
			encoded: `["ttl='on'", "schema_locked=true"]`,
			want:    nil,
		},
		{
			name:    "the issue's reproducer as v26.2.5 reports it",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'", "schema_locked=true"]`,
			want:    &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			name:    "the same table on v25.4.14, which adds no schema_locked",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'"]`,
			want:    &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			name:    "an expression carrying a quote, in the escape-string form",
			encoded: `["ttl='on'", "ttl_expiration_expression=e'expires_at + INTERVAL \\'1 day\\''"]`,
			want:    &crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 day'"},
		},
		{
			// Measured: `ttl_expire_after = '3 days'` is stored as
			// `ttl_expire_after='3 days':::INTERVAL`, the one parameter the
			// catalog writes a type annotation on (stokaro/ptah#1605).
			name:    "the interval enabler, with the type annotation the catalog adds",
			encoded: `["ttl='on'", "ttl_expire_after='3 days':::INTERVAL"]`,
			want:    &crdbschema.Policy{ExpireAfter: "3 days"},
		},
		{
			// The server's own spellings, read back verbatim. Only the
			// comparison reads them as an interval and a duration.
			name:    "an interval and a duration the server rewrote on the way in",
			encoded: `["ttl='on'", "ttl_expire_after='72:00:00':::INTERVAL", "ttl_row_stats_poll_interval='10m0s'"]`,
			want:    &crdbschema.Policy{ExpireAfter: "72:00:00", RowStatsPollInterval: "10m0s"},
		},
		{
			name: "every managed parameter beside an unmodeled one",
			encoded: `["ttl='on'", "ttl_expiration_expression='expires_at'", "ttl_job_cron='@daily'", ` +
				`"ttl_select_batch_size=500", "ttl_delete_batch_size=100", "ttl_select_rate_limit=200", ` +
				`"ttl_delete_rate_limit=300", "ttl_pause=true", "ttl_label_metrics=true", ` +
				`"ttl_disable_changefeed_replication=true", "schema_locked=true"]`,
			want: &crdbschema.Policy{
				ExpirationExpression: "expires_at", JobCron: "@daily",
				SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)),
				SelectRateLimit: new(int64(200)), DeleteRateLimit: new(int64(300)),
				Pause: true, LabelMetrics: true, DisableChangefeedReplication: true,
			},
		},
		{
			name:    "a malformed element with no equals sign is skipped",
			encoded: `["ttl='on'", "garbage", "ttl_expiration_expression='expires_at'"]`,
			want:    &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reader := &Reader{caps: capability.CockroachDB26()}
			facets, err := reader.rowTTLFacets(test.encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(observedPolicy(c, facets), qt.DeepEquals, test.want)
		})
	}
}

// observedPolicy returns the policy a facet collection observes, nil for none.
// An observed value is bound to the CockroachDB target that read it.
func observedPolicy(c *qt.C, facets schemaext.Facets) *crdbschema.Policy {
	c.Helper()
	value, found, err := schemaext.FacetAs[*crdbschema.ObservedRowTTL](facets, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	if !found {
		c.Assert(facets.IsZero(), qt.IsTrue)
		return nil
	}
	c.Assert(facets.TargetScope(crdbschema.RowTTLKind), qt.DeepEquals, []string{"cockroachdb"})
	return &value.Policy
}

// TestRowTTLFacets_RoundTripsWhatTheOwnerRenders is the assertion the whole
// convergence guarantee rests on: what the owner writes into a statement has to
// be what this reader decodes out of the catalog, or the plan never empties.
//
// It is a round trip through both sides rather than two expectations, because a
// defect here is a disagreement between them, and each side looks right alone.
// The server sits between them and rewrites the quoting: a value carrying a
// quote reaches it doubled and is stored as an escape-string literal, measured
// on v26.2.5, so catalogElement models that step rather than feeding the
// statement straight into the decoder.
func TestRowTTLFacets_RoundTripsWhatTheOwnerRenders(t *testing.T) {
	tests := []struct {
		name   string
		policy crdbschema.Policy
	}{
		{name: "the enabler alone", policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		{name: "an expression carrying a quote", policy: crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 day'"}},
		{name: "an expression ending in a quote", policy: crdbschema.Policy{ExpirationExpression: "expires_at + '1 day'"}},
		{name: "an expression carrying whitespace and a cast", policy: crdbschema.Policy{ExpirationExpression: "  (expires_at)::TIMESTAMPTZ  "}},
		{name: "every managed parameter", policy: crdbschema.Policy{
			ExpirationExpression: "expires_at", JobCron: "0 3 * * *",
			SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)),
			SelectRateLimit: new(int64(200)), DeleteRateLimit: new(int64(300)),
			Pause: true, LabelMetrics: true, DisableChangefeedReplication: true,
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets := must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: test.policy}))
			clause, err := crdbrender.CreateTableClause("cockroachdb", capability.CockroachDB26(), "t", facets)
			c.Assert(err, qt.IsNil)
			elements := []string{"ttl='on'"}
			for option := range strings.SplitSeq(strings.TrimSuffix(strings.TrimPrefix(clause, " WITH ("), ")"), ", ") {
				elements = append(elements, catalogElement(option))
			}
			encoded, err := json.Marshal(elements)
			c.Assert(err, qt.IsNil)

			reader := &Reader{caps: capability.CockroachDB26()}
			decoded, err := reader.rowTTLFacets(string(encoded))

			c.Assert(err, qt.IsNil)
			c.Assert(observedPolicy(c, decoded), qt.DeepEquals, &test.policy)
		})
	}
}

// catalogElement is the reloptions element the catalog holds after a statement
// carrying one rendered `name = value` option runs. A value carrying no quote of
// its own is stored as the statement spelled it; a doubled quote is stored as
// an escape-string literal with backslashes.
func catalogElement(option string) string {
	name, value, _ := strings.Cut(option, " = ")
	if !strings.Contains(value, "''") {
		return name + "=" + value
	}
	// Exactly one delimiter comes off each end. strings.Trim would eat the
	// doubled quote that is part of the value when the expression ends with one.
	unquoted := strings.ReplaceAll(value[1:len(value)-1], "''", "'")
	return name + "=e'" + strings.ReplaceAll(unquoted, "'", "\\'") + "'"
}

// TestRowTTLCoverage_KnowsOnlyTheTablesTheReadReturned pins what makes a
// missing facet an observed absence: complete knowledge for each returned
// table, and none for a table the read did not return.
func TestRowTTLCoverage_KnowsOnlyTheTablesTheReadReturned(t *testing.T) {
	identities := objectidentity.NewBuilder(identifier.ForDialect("cockroachdb"))
	tests := []struct {
		name     string
		caps     capability.Capabilities
		returned schemaext.KnowledgeState
		other    schemaext.KnowledgeState
	}{
		{name: "cockroachdb", caps: capability.CockroachDB26(), returned: schemaext.Complete, other: schemaext.Uninspected},
		{name: "postgres records nothing", caps: capability.Postgres17(), returned: schemaext.Uninspected, other: schemaext.Uninspected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema := &catalog.Database{Tables: []catalog.Table{{Name: "sessions"}, {Name: "events", Schema: "audit"}}}
			reader := &Reader{caps: test.caps}

			c.Assert(reader.rowTTLCoverage(schema), qt.IsNil)
			c.Assert(schema.FeatureCoverage.Lookup(crdbschema.RowTTLKind, identities.TableParts("", "sessions")).State, qt.Equals, test.returned)
			c.Assert(schema.FeatureCoverage.Lookup(crdbschema.RowTTLKind, identities.TableParts("audit", "events")).State, qt.Equals, test.returned)
			c.Assert(schema.FeatureCoverage.Lookup(crdbschema.RowTTLKind, identities.TableParts("", "other")).State, qt.Equals, test.other)
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
		want       *crdbschema.Policy
	}{
		{
			name:       "a CockroachDB table carrying a policy",
			caps:       capability.CockroachDB26(),
			reloptions: `["ttl='on'", "ttl_expiration_expression='expires_at'", "schema_locked=true"]`,
			want:       &crdbschema.Policy{ExpirationExpression: "expires_at"},
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

			tables, err := reader.readTablesForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.HasLen, 1)
			c.Assert(observedPolicy(c, tables[0].Facets), qt.DeepEquals, test.want)
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
					"estimated_rows", "row_stats_unknown", "partitioned", "rls_enabled", "rls_forced",
					"unlogged",
					"row_ttl_options",
					"row_deletion_policy",
				},
				Rows: [][]driver.Value{
					{"public", "sessions", "BASE TABLE", "", int64(0), false, false, false, false, false, reloptions, ""},
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

			_, err := reader.readColumnsForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(sent, qt.HasLen, 1)
			c.Assert(strings.Contains(sent[0], "attishidden"), qt.Equals, test.wantAsked)
		})
	}
}

// TestReadBasicConstraintsForSchema_LeavesOutTheKeyOverHiddenColumns pins the
// constraint half of the hidden-column rule: the statement the reader sends
// drops a constraint whose every column is hidden, and only where the target
// has hidden columns.
//
// Whether the clause keeps the right constraints is a question for the server,
// which the live test answers: the key over rowid goes, a hash-sharded key that
// spans the hidden shard column and a declared one stays. What a fake can
// answer is that the clause reaches the statement, spelled with the predicate
// the column read uses, and that PostgreSQL and YugabyteDB, which have no
// pg_attribute.attishidden, are never asked about it.
func TestReadBasicConstraintsForSchema_LeavesOutTheKeyOverHiddenColumns(t *testing.T) {
	const clause = "HAVING NOT COALESCE(bool_and(COALESCE(local_column.attishidden, false)), false)"
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "cockroachdb leaves the engine's key out", caps: capability.CockroachDB263(), wantAsked: true},
		{name: "postgres has no hidden columns", caps: capability.Postgres18(), wantAsked: false},
		{name: "yugabytedb has no hidden columns", caps: capability.YugabyteDB25(), wantAsked: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var sent []string
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				sent = append(sent, strings.Join(strings.Fields(query), " "))
				return dbtest.QueryResult{}, nil
			})
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", test.caps)

			_, err := reader.readBasicConstraintsForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(sent, qt.HasLen, 1)
			c.Assert(strings.Contains(sent[0], clause), qt.Equals, test.wantAsked)
			c.Assert(strings.Contains(sent[0], "attishidden"), qt.Equals, test.wantAsked)
		})
	}
}
