//go:build integration

// One source row to many chunk vectors.
//
// stokaro/ptah#2625, on the model ADR 0017 decides: the chunk set of a source
// row is the unit of correctness rather than the chunk, the ordinal orders but
// does not identify, and a set write makes a source key's stored rows equal to
// the set its text produces -- creating, updating and removing as that requires.
//
// Every assertion here is one an earlier stage could satisfy by accident, so
// the counts come from the catalog rather than from what a verb printed. The
// one that matters most is the shrink: a row whose text gets shorter produces
// fewer chunks, and the rows the new set does not have must go. Nothing else in
// the lifecycle would ever visit them again.

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

// chunkedTargetTable is the relation these specifications ask Ptah to make.
const chunkedTargetTable = "article_chunks"

// chunkBound and chunkOverlap are the split these tests declare.
//
// Small, so a body of a few hundred bytes produces a handful of chunks rather
// than one: the whole subject is a source row holding more than one vector, and
// a bound the fixture never reaches would test the unchunked path under a
// chunking specification's name.
const (
	chunkBound   = 64
	chunkOverlap = 16
)

// TestInferenceEmbedsASourceRowAsASetOfChunksE2E is the feature end to end.
func TestInferenceEmbedsASourceRowAsASetOfChunksE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshChunkableDatabase(c, ctx, dbURL, "ptah_chunked")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecChunking(c, endpoint.URL, chunkedTargetTable, chunkBound, chunkOverlap)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "chunked")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "chunked")

	// More stored rows than source rows is the whole claim. Three source rows
	// with long bodies, and a bound they exceed several times over.
	c.Assert(rowsIn(c, ctx, db, "articles"), qt.Equals, 3)
	c.Assert(rowsIn(c, ctx, db, chunkedTargetTable) > 3, qt.IsTrue,
		qt.Commentf("the corpus holds %d rows for 3 source rows, so nothing was chunked",
			rowsIn(c, ctx, db, chunkedTargetTable)))
	// And every stored row carries a vector: a set whose members were created
	// and not written is the shape an INSERT without the provider call leaves.
	c.Assert(vectorsIn(c, ctx, db, chunkedTargetTable),
		qt.Equals, rowsIn(c, ctx, db, chunkedTargetTable))

	// The ordinals of each key are 0, 1, 2 ... with no repeat and no gap, which
	// is what makes them a set rather than rows that happen to share a key.
	c.Assert(malformedSetsIn(c, ctx, db), qt.HasLen, 0)

	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "chunked")
	verified := runInference(c, ctx, "verify", "--spec", spec, "--db-url", dbName, "--run-id", "chunked")

	// The verification counts the source rows once each, however many chunks
	// stand beside them. Before the walk folded by source key, a joined read
	// over a chunked corpus repeated the source row per chunk and reported a
	// corpus several times its own size.
	c.Assert(verified, qt.Contains, "3 source rows")
	c.Assert(verified, qt.Contains, "every deterministic layer passed")
	c.Assert(verified, qt.Not(qt.Contains), "hold rows their chunk set does not declare")
}

// TestInferenceAShorterRowLosesItsSurplusChunksE2E is the set write's other
// half, and the one no coverage assertion reaches.
//
// A row whose text shrinks produces fewer chunks. The rows the new set does not
// have are not stale, not missing and not another generation's -- they are rows
// no source text produces any more, and nothing but the set write would ever
// visit them again. Left behind, they are what a verification reports as
// outside the generation's scope forever.
func TestInferenceAShorterRowLosesItsSurplusChunksE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshChunkableDatabase(c, ctx, dbURL, "ptah_chunked_shrink")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecChunking(c, endpoint.URL, chunkedTargetTable, chunkBound, chunkOverlap)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "shrink")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "shrink")

	before := chunksOfKey(c, ctx, db, 1)
	c.Assert(before > 1, qt.IsTrue, qt.Commentf("row 1 holds %d chunks, so it was not split", before))

	// The row's body collapses to something one chunk holds, and its version
	// advances so the write is not discarded as a late answer.
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET body = 'short', updated_at = '8' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "shrink")

	after := chunksOfKey(c, ctx, db, 1)
	c.Assert(after < before, qt.IsTrue,
		qt.Commentf("row 1 held %d chunks and holds %d after shrinking", before, after))
	c.Assert(after, qt.Equals, 1)
	c.Assert(malformedSetsIn(c, ctx, db), qt.HasLen, 0)

	// The other rows are untouched, which is what separates "removed the
	// surplus of this key" from "removed rows".
	c.Assert(chunksOfKey(c, ctx, db, 2), qt.Equals, before)
}

// TestInferenceRefusesChunkingIntoTheSourceRowE2E is the refusal that keeps a
// set from being written over itself.
//
// The columns beside a source row hold one vector. A specification that splits
// that row into four and stores them there would embed all four, write each
// over the last, and report a covered corpus holding one piece of every row.
func TestInferenceRefusesChunkingIntoTheSourceRowE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, dbName := freshChunkableDatabase(c, ctx, dbURL, "ptah_chunked_refuse")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// Everything the working fixture has except the layout.
	spec := writeCLISpecChunkingIntoTheSource(c, endpoint.URL, chunkBound, chunkOverlap)
	refused, err := runInferenceExpectingFailure(c, ctx,
		"prepare", "--spec", spec, "--db-url", dbName, "--run-id", "refused")

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", refused))
	c.Assert(err.Error(), qt.Contains, "splits a row into a set of chunks")
	c.Assert(err.Error(), qt.Contains, "own_table")
}

// freshChunkableDatabase seeds a database whose article bodies are long enough
// to split.
//
// The default fixture's bodies are three words, so a chunking specification
// over it produces one chunk per row and every assertion about a set would be
// an assertion about a single row wearing the word set.
func freshChunkableDatabase(
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
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`,
	} {
		_, execErr := db.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("%s", statement))
	}
	for id := 1; id <= 3; id++ {
		_, execErr := db.ExecContext(ctx,
			`INSERT INTO articles (id, title, body, updated_at) VALUES ($1, $2, $3, '7')`,
			id, fmt.Sprintf("Article %d", id), longBody(id))
		c.Assert(execErr, qt.IsNil)
	}
	return db, dbName
}

// longBody is text several chunks long, and different per row.
//
// Different per row because a fixture whose rows are identical cannot tell a
// set written for the right key from one written for every key.
func longBody(id int) string {
	var body strings.Builder
	for index := range 30 {
		fmt.Fprintf(&body, "row%d-sentence%02d ", id, index)
	}
	return body.String()
}

// chunksOfKey counts the stored rows one source key holds.
func chunksOfKey(c *qt.C, ctx context.Context, db *sql.DB, id int64) int {
	c.Helper()
	var count int
	// #nosec G201 -- the relation is this file's own constant.
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM "+chunkedTargetTable+" WHERE id = $1", id).Scan(&count), qt.IsNil)
	return count
}

// malformedSetsIn names every source key whose stored ordinals are not
// 0, 1, 2 ... in order.
//
// Asked of the database rather than read from the verification's report,
// because the report is one of the things under test: a walk that had stopped
// looking would say nothing and this would still answer.
func malformedSetsIn(c *qt.C, ctx context.Context, db *sql.DB) []int64 {
	c.Helper()
	// #nosec G201 -- the relation is this file's own constant.
	rows, err := db.QueryContext(ctx, `SELECT id FROM `+chunkedTargetTable+`
		GROUP BY id
		HAVING count(*) <> count(DISTINCT embedding_chunk_ordinal)
		    OR min(embedding_chunk_ordinal) <> 0
		    OR max(embedding_chunk_ordinal) <> count(*) - 1
		ORDER BY id`)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	keys := make([]int64, 0)
	for rows.Next() {
		var key int64
		c.Assert(rows.Scan(&key), qt.IsNil)
		keys = append(keys, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return keys
}
