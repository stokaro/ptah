//go:build integration

// A chunked corpus keyed by two columns.
//
// Everything #2625 shipped is built by looping over the key fields: the
// relation Ptah creates and its foreign key, the conflict target the insert
// upserts on, the conditions the surplus removal deletes by, and the tuple the
// resolution reads back. A one-element loop renders identically however its
// parts are combined, so a single-column key cannot tell a conjunction from a
// disjunction, a right key from a first component, or a conflict target that
// forgot a column from one that did not.
//
// The rows here share a component pairwise -- ("a",1), ("b",1), ("a",2) -- so
// no single component identifies a row, which is what makes each of those
// observable.

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// compositeChunkTable is the relation these specifications ask Ptah to make.
const compositeChunkTable = "document_chunks"

// TestInferenceChunksACompositeKeyedRowE2E is the feature over a two-column
// key, end to end.
func TestInferenceChunksACompositeKeyedRowE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshCompositeChunkableDatabase(c, ctx, dbURL, "ptah_composite_chunked")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := compositeChunkingSpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "composite-chunk")

	// The relation Ptah made is keyed by both components AND the ordinal, and
	// references the source on both. A foreign key on one component would be
	// refused by PostgreSQL, and a primary key missing one would let a second
	// tenant's chunk overwrite the first's.
	c.Assert(primaryKeyColumnsOf(c, ctx, db, compositeChunkTable), qt.DeepEquals,
		[]string{"tenant", "id", "embedding_chunk_ordinal"})
	c.Assert(referencedRelationOf(c, ctx, db, compositeChunkTable), qt.Equals, "documents")

	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "composite-chunk")

	// Every one of the three source rows holds its own set, and no set is
	// missing. A conflict target that dropped a key component would collapse
	// ("a",1) and ("b",1) into one set and this count would be short.
	perKey := chunksPerCompositeKey(c, ctx, db)
	c.Assert(perKey, qt.HasLen, 3)
	for key, count := range perKey {
		c.Assert(count > 1, qt.IsTrue, qt.Commentf("%s holds %d chunks, so it was not split", key, count))
	}

	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "composite-chunk")
	verified := runInference(c, ctx, "verify", "--spec", spec, "--db-url", dbName, "--run-id", "composite-chunk")

	c.Assert(verified, qt.Contains, "3 source rows")
	c.Assert(verified, qt.Contains, "every deterministic layer passed")
}

// TestInferenceAShorterCompositeKeyedRowLosesOnlyItsOwnChunksE2E is the surplus
// removal addressed by every key component.
//
// The removal deletes by the key and the ordinal. Built from a loop, a version
// that bound only the first component would delete ("b",1)'s chunks along with
// ("a",1)'s -- and every count in the test above would still hold, because the
// sets it checks are written after.
func TestInferenceAShorterCompositeKeyedRowLosesOnlyItsOwnChunksE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshCompositeChunkableDatabase(c, ctx, dbURL, "ptah_composite_shrink")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := compositeChunkingSpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "composite-shrink")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "composite-shrink")

	before := chunksPerCompositeKey(c, ctx, db)
	c.Assert(before["a/1"] > 1, qt.IsTrue)
	c.Assert(before["b/1"] > 1, qt.IsTrue)

	// ("a",1) shrinks. ("b",1) shares its id and ("a",2) shares its tenant, so
	// a removal addressed by either component alone takes one of them with it.
	_, err := db.ExecContext(ctx,
		`UPDATE documents SET body = 'short', updated_at = '8' WHERE tenant = 'a' AND id = 1`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "composite-shrink")

	after := chunksPerCompositeKey(c, ctx, db)
	c.Assert(after["a/1"], qt.Equals, 1)
	c.Assert(after["b/1"], qt.Equals, before["b/1"])
	c.Assert(after["a/2"], qt.Equals, before["a/2"])
}

// compositeChunkingSpec is the specification both tests run.
func compositeChunkingSpec(c *qt.C, endpoint string) string {
	c.Helper()
	spec := defaultCLISpec(endpoint)
	spec.sourceTable, spec.targetTable = "documents", compositeChunkTable
	spec.keyFields = []string{"tenant", "id"}
	spec.layout = "own_table"
	spec.truncate = "chunk"
	spec.maxInputBytes, spec.overlapBytes = chunkBound, chunkOverlap
	return writeCLISpecFrom(c, spec)
}

// freshCompositeChunkableDatabase seeds a two-column-keyed source whose bodies
// are long enough to split, and no target relation at all.
func freshCompositeChunkableDatabase(
	c *qt.C, ctx context.Context, dbURL, prefix string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { adminDB.Close() })

	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { db.Close() })

	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE documents (
			tenant TEXT NOT NULL, id BIGINT NOT NULL,
			title TEXT, body TEXT, updated_at TEXT NOT NULL,
			PRIMARY KEY (tenant, id))`,
	} {
		_, execErr := db.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	// Pairwise-sharing keys, so neither component identifies a row.
	rows := []struct {
		tenant string
		id     int64
	}{{"a", 1}, {"b", 1}, {"a", 2}}
	for index, row := range rows {
		_, execErr := db.ExecContext(ctx,
			`INSERT INTO documents (tenant, id, title, body, updated_at)
			 VALUES ($1, $2, $3, $4, '7')`,
			row.tenant, row.id, fmt.Sprintf("Doc %s/%d", row.tenant, row.id),
			longCompositeBody(index))
		c.Assert(execErr, qt.IsNil)
	}
	return db, dbName
}

// longCompositeBody is text several chunks long, different per row.
func longCompositeBody(index int) string {
	var body strings.Builder
	for sentence := range 30 {
		fmt.Fprintf(&body, "doc%d-line%02d ", index, sentence)
	}
	return body.String()
}

// chunksPerCompositeKey counts each source key's stored rows, keyed
// "<tenant>/<id>".
//
// Both components in the map key, because the whole subject is that a row is
// identified by both: a helper grouping on one would report the pair as one
// entry and every assertion here would be about a set that does not exist.
func chunksPerCompositeKey(c *qt.C, ctx context.Context, db *sql.DB) map[string]int {
	c.Helper()
	// #nosec G201 -- the relation is this file's own constant.
	rows, err := db.QueryContext(ctx,
		"SELECT tenant, id, count(*) FROM "+compositeChunkTable+" GROUP BY tenant, id")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	counts := make(map[string]int)
	for rows.Next() {
		var tenant string
		var id int64
		var count int
		c.Assert(rows.Scan(&tenant, &id, &count), qt.IsNil)
		counts[fmt.Sprintf("%s/%d", tenant, id)] = count
	}
	c.Assert(rows.Err(), qt.IsNil)
	return counts
}
