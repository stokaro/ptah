package postgres

// White-box testing required: hashShardBuckets and the constraint read's key
// filter are unexported, and a fake server cannot evaluate the filter, so the
// statement text is the only place its presence shows. The live test in
// integration/dbschema/postgres reads a real hash-sharded key.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
)

// TestHashShardBuckets_ReadsTheCountTheServerPrinted pins the parse against the
// definitions CockroachDB v26.3.1 prints, and against the PostgreSQL hash
// access method, which has the word and no bucket count.
func TestHashShardBuckets_ReadsTheCountTheServerPrinted(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		definition string
		want       int
	}{
		{
			name:       "a hash-sharded primary key",
			dialect:    platform.CockroachDB,
			definition: "PRIMARY KEY (id ASC) USING HASH WITH (bucket_count=16)",
			want:       16,
		},
		{
			name:       "a hash-sharded index",
			dialect:    platform.CockroachDB,
			definition: "CREATE INDEX plain_v_idx ON public.plain USING btree (v ASC) USING HASH WITH (bucket_count=8)",
			want:       8,
		},
		{
			name:       "an ordinary key",
			dialect:    platform.CockroachDB,
			definition: "PRIMARY KEY (id ASC)",
			want:       0,
		},
		{
			name:       "the PostgreSQL hash access method",
			dialect:    platform.Postgres,
			definition: "CREATE INDEX h ON public.t USING hash (v)",
			want:       0,
		},
		{
			name:       "the clause outside CockroachDB",
			dialect:    platform.Postgres,
			definition: "PRIMARY KEY (id ASC) USING HASH WITH (bucket_count=16)",
			want:       0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(hashShardBuckets(test.dialect, test.definition), qt.Equals, test.want)
		})
	}
}

// TestReadBasicConstraintsForSchema_ListsOnlyVisibleKeyColumns pins the
// constraint half of the rule: on CockroachDB a key column the engine hides is
// left out of the key's column list, so a hash-sharded key lists the columns
// it was declared with; PostgreSQL and YugabyteDB are never asked, because
// neither has pg_attribute.attishidden.
func TestReadBasicConstraintsForSchema_ListsOnlyVisibleKeyColumns(t *testing.T) {
	const filter = "FILTER (WHERE local_column.attname IS NOT NULL AND NOT COALESCE(local_column.attishidden, false))"
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "cockroachdb", caps: capability.CockroachDB263(), wantAsked: true},
		{name: "postgres", caps: capability.Postgres18(), wantAsked: false},
		{name: "yugabytedb", caps: capability.YugabyteDB25(), wantAsked: false},
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
			c.Assert(strings.Contains(sent[0], filter), qt.Equals, test.wantAsked)
		})
	}
}
