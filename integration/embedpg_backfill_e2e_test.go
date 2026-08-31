//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
	"go.5x5.cz/ptah/internal/embedverify"
)

// TestEmbedPGBackfillE2E runs a whole backfill against a live database and then
// verifies the corpus it produced.
//
// Every part of this is tested in isolation elsewhere. What only a live server
// can answer is whether the pieces still fit when the vectors are real: whether
// pgvector accepts what the target renders, whether a keyset scan over a
// composite key resumes where it stopped, and -- the one that matters -- whether
// the checkpoint and the vectors are in the same transaction when the
// transaction is a real BEGIN.
//
// It runs against the TimescaleDB target because that image ships pgvector, and
// the extension is what the test needs; nothing here is about TimescaleDB
// (stokaro/ptah#2068).
func TestEmbedPGBackfillE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_embedpg_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := liveSpec()
	seedArticles(c, ctx, db, spec)

	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)

	run := embedrun.Run{
		ID: "live-run", SpecDigest: "spec-1", GenerationIdentity: spec.Identity().Digest,
		Environment: "test", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	c.Assert(store.CreateRun(ctx, run), qt.IsNil)

	engine := &embedengine.Engine{
		Spec: spec, Source: source, Provider: &liveProvider{dimension: 4},
		Target: target, Store: store,
		// Two rows a scan and two inputs a request, so the backfill is several
		// transactions rather than one. A single-transaction run cannot show
		// whether resuming works.
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2},
		Worker: "worker-a",
	}

	finished, err := engine.Backfill(ctx, "live-run")

	c.Assert(err, qt.IsNil)
	c.Assert(finished.Progress.RowsScanned, qt.Equals, int64(5))
	c.Assert(finished.Progress.RowsEmbedded, qt.Equals, int64(4))
	c.Assert(finished.Progress.RowsSkipped, qt.Equals, int64(1))
	c.Assert(finished.Cursor, qt.DeepEquals, []string{"5"})

	assertVectorsAreReadable(c, ctx, db, spec)
	assertVerificationPasses(c, ctx, db, spec, finished)
	assertResumeReadsNothingTwice(c, ctx, db, spec, store)
	assertARefusedWriteTakesItsCheckpointWithIt(c, ctx, db, spec, store)
	assertAFencedCommitWritesNothing(c, ctx, db, spec, store)
}

// assertAFencedCommitWritesNothing is the takeover, in the transaction where it
// can actually happen.
//
// The worker read the run, embedded a batch, and by the time its transaction
// runs another worker holds the run. The refusal has to be inside that
// transaction: a check before it is a check with a window after it, and the
// window is exactly where a superseded worker overwrites its successor's work.
func assertAFencedCommitWritesNothing(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, store *embedpg.Store,
) {
	c.Helper()
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	held, err := store.Run(ctx, "live-run")
	c.Assert(err, qt.IsNil)
	takenOver := held
	takenOver.FencingToken = held.FencingToken + 1
	takenOver.LeaseOwner = "worker-b"
	c.Assert(store.SaveRun(ctx, takenOver), qt.IsNil)

	stale := held
	stale.Cursor = []string{"1"}
	err = target.Commit(ctx, []embedrun.TargetWrite{{
		Key: []string{"1"}, Generation: "a generation nobody asked for",
		InputHash: "stale", Version: "7", Kind: embedrun.WriteUpsert,
		Vector: make([]float32, spec.Model.ReportedDimension),
	}}, stale)

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	// Neither half landed: not the vector, and not the cursor it carried.
	c.Assert(generationsInTable(c, ctx, db, spec)[0], qt.Equals, spec.Identity().Digest)
	current, readErr := store.Run(ctx, "live-run")
	c.Assert(readErr, qt.IsNil)
	c.Assert(current.Cursor, qt.DeepEquals, []string{"5"})
	c.Assert(current.LeaseOwner, qt.Equals, "worker-b")
}

// assertARefusedWriteTakesItsCheckpointWithIt is the claim the whole design
// rests on, measured against a real BEGIN.
//
// A second run declares eight dimensions against a four-dimension column. The
// provider answers eight, so nothing upstream refuses it -- the server is what
// says no, halfway through a transaction that has already written the run's
// advanced cursor. If the checkpoint were a separate statement the cursor would
// survive, and the resumed run would skip four rows nothing embedded, looking
// perfectly healthy while it did.
func assertARefusedWriteTakesItsCheckpointWithIt(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, store *embedpg.Store,
) {
	c.Helper()
	wrong := spec
	wrong.Model.ReportedDimension = 8
	source, err := embedpg.NewSource(db, wrong)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, wrong)
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "refused-run", SpecDigest: "spec-2", GenerationIdentity: wrong.Identity().Digest,
		Environment: "test", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}), qt.IsNil)

	engine := &embedengine.Engine{
		Spec: wrong, Source: source, Provider: &liveProvider{dimension: 8},
		Target: target, Store: store,
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2}, Worker: "worker-a",
	}

	_, err = engine.Backfill(ctx, "refused-run")

	c.Assert(err, qt.ErrorMatches, `target: .*`)
	stored, readErr := store.Run(ctx, "refused-run")
	c.Assert(readErr, qt.IsNil)
	c.Assert(stored.Cursor, qt.HasLen, 0)
	c.Assert(stored.Progress.RowsScanned, qt.Equals, int64(0))
	c.Assert(stored.Progress.BatchesCommitted, qt.Equals, int64(0))
	// And the corpus the first run built is untouched: the refused transaction
	// wrote no generation column either.
	c.Assert(generationsInTable(c, ctx, db, spec), qt.DeepEquals,
		[]string{spec.Identity().Digest, spec.Identity().Digest, spec.Identity().Digest,
			spec.Identity().Digest, spec.Identity().Digest})
}

// generationsInTable lists the generation each target row belongs to.
func generationsInTable(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT COALESCE(%s, '') FROM articles ORDER BY id`,
		spec.Target.Column+embedpg.GenerationSuffix))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var generations []string
	for rows.Next() {
		var generation string
		c.Assert(rows.Scan(&generation), qt.IsNil)
		generations = append(generations, generation)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return generations
}

// liveSpec is the generation the live test builds: a composite input, four
// dimensions, an empty article skipped rather than embedded.
func liveSpec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields:       []string{"id"},
			InputFields:     []string{"title", "body"},
			VersionStrategy: embedgen.VersionUpdatedAt,
			VersionField:    "updated_at",
		},
		Preprocessing: embedgen.Preprocessing{
			Separator: "\n", NullPolicy: embedgen.NullAsEmpty, EmptyPolicy: embedgen.EmptySkipRow,
		},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "fake-model", Revision: "1", ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: "public", Table: "articles", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}

// seedArticles creates the source table, the generation's columns, and five
// rows -- one of which is empty and gets skipped.
func seedArticles(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY,
			title TEXT,
			body TEXT,
			updated_at TEXT NOT NULL
		)`,
		fmt.Sprintf(`ALTER TABLE articles
			ADD COLUMN %s vector(%d),
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT`,
			spec.Target.Column,
			spec.Model.ReportedDimension,
			spec.Target.Column+embedpg.GenerationSuffix,
			spec.Target.Column+embedpg.InputHashSuffix,
			spec.Target.Column+embedpg.VersionSuffix,
			spec.Target.Column+embedpg.StateSuffix),
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'First',  'about pricing', '7'),
			(2, 'Second', 'about support', '7'),
			(3, '',       '',              '7'),
			(4, 'Fourth', 'about billing', '7'),
			(5, 'Fifth',  NULL,            '7')`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// assertVectorsAreReadable reads the corpus back through the server rather than
// through Ptah.
//
// A test that only checked what Ptah believes it wrote would agree with a
// target that rendered a vector pgvector could not parse -- the write would
// have failed, and the assertion would have been about the value in memory.
func assertVectorsAreReadable(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	column := spec.Target.Column
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, %s::text, %s, %s FROM articles ORDER BY id`,
		column, column+embedpg.StateSuffix, column+embedpg.GenerationSuffix))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var states []string
	var vectors []string
	for rows.Next() {
		var id int64
		var vector, state, generation sql.NullString
		c.Assert(rows.Scan(&id, &vector, &state, &generation), qt.IsNil)
		states = append(states, state.String)
		vectors = append(vectors, vector.String)
		c.Assert(generation.String, qt.Equals, spec.Identity().Digest)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(states, qt.DeepEquals, []string{"upsert", "upsert", "skip", "upsert", "upsert"})
	// The skipped row has no vector, and NULL is what that means. A zero vector
	// would be a point at the origin and the nearest neighbour of everything
	// near it.
	c.Assert(vectors[2], qt.Equals, "")
	// The fake's vectors start at the input's length, so the value says which
	// text produced it -- and the server is what read it back.
	c.Assert(vectors[0], qt.Equals, "[19,20,21,22]")
	// "Fifth" and a NULL body: six characters, because the null policy keeps
	// the separator. The row proves the policy reached the provider rather
	// than being applied somewhere the vector could not see.
	c.Assert(vectors[4], qt.Equals, "[6,7,8,9]")
}

// assertVerificationPasses runs the deterministic layers over what the backfill
// actually produced.
func assertVerificationPasses(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, run embedrun.Run,
) {
	c.Helper()
	source, target := readVerificationRows(c, ctx, db, spec)
	report := embedverify.Verify(
		embedverify.Expectation{
			Generation: spec.Identity().Digest,
			ColumnType: fmt.Sprintf("vector(%d)", spec.Model.ReportedDimension),
			Dimension:  spec.Model.ReportedDimension,
		},
		embedverify.Structure{
			ColumnExists: true, ColumnType: fmt.Sprintf("vector(%d)", spec.Model.ReportedDimension),
			Dimension: spec.Model.ReportedDimension, ExtensionPresent: true,
		},
		source, target,
		embedverify.RunState{SnapshotComplete: true, CatchUpReached: true},
	)

	c.Assert(report.Blocking(), qt.HasLen, 0, qt.Commentf("%v", report.Findings))
	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.SourceRows, qt.Equals, 5)
	c.Assert(report.TargetRows, qt.Equals, 5)
	c.Assert(run.Progress.RowsEmbedded+run.Progress.RowsSkipped, qt.Equals, int64(report.SourceRows))
}

// readVerificationRows reads both sides out of the same table.
func readVerificationRows(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec,
) ([]embedverify.SourceRow, []embedverify.TargetRow) {
	c.Helper()
	column := spec.Target.Column
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT id::text, title, body, updated_at, %s, %s, %s, %s
		 FROM articles ORDER BY id`,
		column+embedpg.GenerationSuffix, column+embedpg.InputHashSuffix,
		column+embedpg.VersionSuffix, column+embedpg.StateSuffix))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var sources []embedverify.SourceRow
	var targets []embedverify.TargetRow
	for rows.Next() {
		var key, version string
		var title, body, generation, inputHash, storedVersion, state sql.NullString
		c.Assert(rows.Scan(&key, &title, &body, &version,
			&generation, &inputHash, &storedVersion, &state), qt.IsNil)

		input, canonicalErr := spec.Canonicalize(embedgen.Row{
			Key: []string{key}, Fields: []*string{nullableOf(title), nullableOf(body)},
		})
		c.Assert(canonicalErr, qt.IsNil)
		sources = append(sources, embedverify.SourceRow{
			Key: key, Version: version, InputHash: spec.SourceInputHash(input), Skipped: input.Skipped,
		})
		targets = append(targets, embedverify.TargetRow{
			Key: key, Generation: generation.String, Version: storedVersion.String,
			InputHash: inputHash.String, Skipped: state.String == "skip",
			Dimension: storedWidth(spec, state.String),
		})
	}
	c.Assert(rows.Err(), qt.IsNil)
	return sources, targets
}

// storedWidth is how wide the stored vector is, or zero where there is none.
//
// Verification's vector layer asks about the width, and about finiteness only
// where a caller actually read the values back. Parsing pgvector's text form
// here would be a second parser to get wrong, and a zero-filled slice of the
// right length would be the allocation the production reader stopped making --
// six gigabytes over a million rows at 1536 dimensions, carrying nothing this
// integer does not (stokaro/ptah#2068).
func storedWidth(spec embedgen.Spec, state string) int {
	if state != "upsert" {
		return 0
	}
	return spec.Model.ReportedDimension
}

// nullableOf turns a SQL string into a field value, keeping NULL as nil.
func nullableOf(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

// assertResumeReadsNothingTwice starts a second run from the finished cursor.
//
// The scan is asked to continue past the last key, and a keyset that works
// answers with nothing. One that does not -- an OFFSET, a comparison that
// reads `>=` -- hands back the last row, and the engine's stall guard is what
// would notice.
func assertResumeReadsNothingTwice(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, store *embedpg.Store,
) {
	c.Helper()
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)

	page, err := source.Scan(ctx, []string{"5"}, 2)

	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows, qt.HasLen, 0)
	c.Assert(page.Done, qt.IsTrue)

	// And the middle of the scan resumes where it stopped rather than where it
	// started.
	page, err = source.Scan(ctx, []string{"2"}, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows, qt.HasLen, 2)
	c.Assert(page.Rows[0].Key, qt.DeepEquals, []string{"3"})
	c.Assert(page.Versions, qt.DeepEquals, []string{"7", "7"})

	stored, err := store.Run(ctx, "live-run")
	c.Assert(err, qt.IsNil)
	c.Assert(stored.Cursor, qt.DeepEquals, []string{"5"})
	c.Assert(stored.Progress.BatchesCommitted, qt.Equals, int64(3))
}

// liveProvider answers with a vector derived from the input, so a value read
// back from the server says which text produced it.
type liveProvider struct {
	dimension int
}

// Profile describes the endpoint.
func (p *liveProvider) Profile() embedprovider.Profile {
	return embedprovider.Profile{Provider: "fake", Model: "fake-model", Dimension: p.dimension}
}

// Embed answers one vector per input.
func (p *liveProvider) Embed(_ context.Context, inputs []string) (embedprovider.Result, error) {
	vectors := make([]embedprovider.Vector, 0, len(inputs))
	for _, input := range inputs {
		vector := make(embedprovider.Vector, p.dimension)
		for component := range vector {
			vector[component] = float32(len(input) + component)
		}
		vectors = append(vectors, vector)
	}
	return embedprovider.Result{
		Vectors: vectors,
		Usage:   embedprovider.Usage{PromptTokens: len(inputs), TotalTokens: len(inputs) * 2},
	}, nil
}
