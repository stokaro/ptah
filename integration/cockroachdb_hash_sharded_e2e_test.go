//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestCockroachDBHashShardedE2E_HappyPath reads a CockroachDB database holding
// a hash-sharded primary key and a hash-sharded index, applies the description
// to a fresh database, and compares both databases with it.
//
// A key or index built USING HASH spans a hidden shard column. Described over
// it, `ptah db read` names a column the description does not have, and applying
// the description fails with `column "crdb_internal_id_shard_16" does not
// exist` (stokaro/ptah#3771). The description carries each over its declared
// columns, the note on stderr names the sharding it cannot carry, and the
// description applies.
func TestCockroachDBHashShardedE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	sourceURL, targetURL := newHashShardedDatabases(c, ctx)

	const note = "note: 2 CockroachDB keys and indexes built USING HASH are described without it," +
		" because no schema source can declare hash sharding; a description applied to another" +
		" database builds them unsharded, and a diff between the two reports no difference:" +
		" events.events_at_idx (8 buckets), orders.orders_pkey (16 buckets).\n"
	read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", sourceURL})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	c.Assert(read, qt.Not(qt.Contains), "crdb_internal_")
	c.Assert(readErr, qt.Contains, note)

	inspected, inspectErr, err := runCompatSQLInspect(ctx, sourceURL)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", inspectErr))
	c.Assert(inspected, qt.Not(qt.Contains), "crdb_internal_")
	c.Assert(inspectErr, qt.Contains, note)

	rendered := filepath.Join(c.TempDir(), "rendered.sql")
	c.Assert(os.WriteFile(rendered, []byte(read), 0o600), qt.IsNil)
	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", rendered, "--db-url", targetURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))

	for _, dbURL := range []string{sourceURL, targetURL} {
		compared, comparedErr, err := runPtahSplitStreams(ctx, []string{
			"schema", "compare", "--schema-file", rendered, "--db-url", dbURL, "--exit-code",
		})
		c.Assert(err, qt.IsNil, qt.Commentf("%s\nstdout:\n%s\nstderr:\n%s", dbURL, compared, comparedErr))
	}
	c.Assert(primaryKeyColumns(c, ctx, targetURL, "orders"), qt.Equals, "id")
}

// newHashShardedDatabases creates a source database holding a table with a
// hash-sharded primary key and a table with a hash-sharded index, and an empty
// target database, on the CockroachDB server. Both are dropped when the test
// ends.
func newHashShardedDatabases(c *qt.C, ctx context.Context) (sourceURL, targetURL string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	source := fmt.Sprintf("ptah_hs_source_%d", stamp)
	target := fmt.Sprintf("ptah_hs_target_%d", stamp)
	for _, name := range []string{source, target} {
		createE2EDatabase(c, ctx, admin, name)
		c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	}
	sourceURL = replaceDatabaseName(c, adminURL, source)
	targetURL = replaceDatabaseName(c, adminURL, target)

	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, sourceURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range []string{
		"CREATE TABLE orders (id INT8 PRIMARY KEY USING HASH, total INT8)",
		"CREATE TABLE events (id INT8 PRIMARY KEY, at INT8)",
		"CREATE INDEX events_at_idx ON events (at) USING HASH WITH (bucket_count = 8)",
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return sourceURL, targetURL
}

// primaryKeyColumns names the columns of a table's primary key as pg_constraint
// records them, hidden ones included, in key order.
func primaryKeyColumns(c *qt.C, ctx context.Context, dbURL, table string) string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var columns string
	c.Assert(db.QueryRowContext(ctx, `
		SELECT string_agg(a.attname, ',' ORDER BY k.ordinality)
		FROM pg_constraint con
		JOIN pg_class cls ON cls.oid = con.conrelid
		CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ordinality)
		JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
		WHERE cls.relname = $1 AND con.contype = 'p'`, table,
	).Scan(&columns), qt.IsNil)
	return columns
}
