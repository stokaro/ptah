//go:build integration

package integration_test

// Live PostgreSQL coverage for the vector index a generation is queried
// through.
//
// Nothing built one until stokaro/ptah#2415: the plan listed `[index] build the
// vector index and wait for it to be valid`, `Spec.TargetObjects` derived the
// index, and the only consumers read it -- to verify one, to drop one. These
// tests read the catalog rather than what the verb said about itself, because
// the failure this is about is a verb reporting work it did not do.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedstore"
)

// TestEnsureIndex_BuildsAValidIndexLive is the happy path.
//
// The method, the operator class and the build options all come from the
// specification, and all three are asserted: an index built with the wrong
// operator class is one PostgreSQL will not use for the metric the generation
// is queried with, and it is valid, so `indisvalid` alone would call it done.
func TestEnsureIndex_BuildsAValidIndexLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := indexedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	outcome, err := embedpg.EnsureIndex(ctx, db, spec)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, embedpg.IndexBuilt)
	method, valid, definition := indexInCatalog(c, ctx, db, spec)
	c.Assert(valid, qt.IsTrue)
	c.Assert(method, qt.Equals, "hnsw")
	c.Assert(definition, qt.Contains, "vector_cosine_ops")
	c.Assert(definition, qt.Contains, "m='16'")
	c.Assert(definition, qt.Contains, "ef_construction='64'")
}

// TestEnsureIndex_IsIdempotentLive is the second run.
func TestEnsureIndex_IsIdempotentLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := indexedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	_, err := embedpg.EnsureIndex(ctx, db, spec)
	c.Assert(err, qt.IsNil)

	outcome, err := embedpg.EnsureIndex(ctx, db, spec)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, embedpg.IndexAlreadyValid)
}

// TestEnsureIndex_RebuildsAnInvalidIndexLive is the case the read-back exists
// for.
//
// A concurrent build that fails leaves an index behind, and PostgreSQL will not
// use it. An implementation that asked only whether the index EXISTS would
// report the generation ready while every query over it is a sequential scan
// over the whole corpus.
func TestEnsureIndex_RebuildsAnInvalidIndexLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := indexedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)
	_, err := embedpg.EnsureIndex(ctx, db, spec)
	c.Assert(err, qt.IsNil)
	invalidateIndex(c, ctx, db, spec)

	outcome, err := embedpg.EnsureIndex(ctx, db, spec)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, embedpg.IndexRebuilt)
	_, valid, _ := indexInCatalog(c, ctx, db, spec)
	c.Assert(valid, qt.IsTrue)
}

// TestEnsureIndex_ASpecificationWithNoMethodBuildsNothingLive is an answer
// rather than a failure.
//
// A generation may be queried by sequential scan, and an author who declared no
// method asked for that. It is also the control for every row above: an
// implementation that built an index regardless would satisfy them and fail
// here.
func TestEnsureIndex_ASpecificationWithNoMethodBuildsNothingLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)
	spec := loadTargetSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

	outcome, err := embedpg.EnsureIndex(ctx, db, spec)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, embedpg.IndexNotDeclared)
	c.Assert(indexNames(c, ctx, db, table), qt.HasLen, 0)
}

// TestEnsureIndex_RefusesABuildOptionItCannotPlaceLive is the input guard.
//
// PostgreSQL takes no parameter in a WITH clause, so an option reaches the
// statement as text. pgvector's build options are all whole numbers, and
// anything else is refused by shape rather than escaped -- guessing would put
// author-controlled text into a statement.
func TestEnsureIndex_RefusesABuildOptionItCannotPlaceLive(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		wantErr string
	}{
		{
			name:    "a value that is not a number",
			options: map[string]string{"m": "16); DROP TABLE docs; --"},
			wantErr: "is not a value this can place in a WITH clause",
		},
		{
			name:    "a name that is not an identifier",
			options: map[string]string{"m = 1, x": "16"},
			wantErr: "is not a name this can place in a WITH clause",
		},
		{
			name:    "an empty value",
			options: map[string]string{"m": ""},
			wantErr: "is not a value this can place in a WITH clause",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			db, table := targetColumnsDatabase(c, ctx, withVector)
			spec := indexedSpec(c, table)
			spec.Target.IndexOptions = test.options
			c.Assert(embedpg.EnsureTarget(ctx, db, spec), qt.IsNil)

			_, err := embedpg.EnsureIndex(ctx, db, spec)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.wantErr)
			// And it refused before running anything: the table is still there
			// and no index was left half-built.
			c.Assert(indexNames(c, ctx, db, table), qt.HasLen, 0)
		})
	}
}

// indexedSpec is a specification that declares an index.
func indexedSpec(c *qt.C, table string) embedgen.Spec {
	c.Helper()
	spec := loadTargetSpec(c, table)
	spec.Target.IndexMethod = "hnsw"
	spec.Target.IndexOptions = map[string]string{"m": "16", "ef_construction": "64"}
	return spec
}

// indexInCatalog reports the method, validity and definition of the
// generation's index.
func indexInCatalog(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec,
) (string, bool, string) {
	c.Helper()
	objects, err := spec.TargetObjects()
	c.Assert(err, qt.IsNil)

	var method, definition string
	var valid bool
	c.Assert(db.QueryRowContext(ctx, `SELECT am.amname, i.indisvalid,
			pg_get_indexdef(i.indexrelid)
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_am am ON am.oid = ic.relam
		WHERE ic.relname = $1`, objects.Index.Name).
		Scan(&method, &valid, &definition), qt.IsNil)
	return method, valid, definition
}

// invalidateIndex marks the generation's index the way a failed concurrent
// build leaves it.
//
// Directly in the catalog, because the honest way to produce one -- starting a
// concurrent build and killing it -- is a race, and a fixture that sometimes
// sets up the state under test is a test that sometimes measures nothing.
func invalidateIndex(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	objects, err := spec.TargetObjects()
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		"UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass",
		objects.Index.Name)
	c.Assert(err, qt.IsNil)
}

// indexNames lists the indexes over a table that are not its primary key.
func indexNames(c *qt.C, ctx context.Context, db *sql.DB, table string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `SELECT ic.relname
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_class tc ON tc.oid = i.indrelid
		WHERE tc.relname = $1 AND NOT i.indisprimary`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestRetireIndex_DropsTheIndexTheGenerationBuiltLive is stokaro/ptah#2642.
//
// The retirement built the index name from the CURRENT specification with only
// `Target.Column` swapped in. `Target.Column` is an identity field, so the
// digest in the generated name belonged to a hybrid that was no generation at
// all, and the `DROP INDEX IF EXISTS` matched nothing. With
// `--drop-column=false` -- the only mode in which dropping the index IS the
// operation -- the index survived while the verb reported the generation gone.
//
// Retiring an old generation while holding the new specification is the
// documented workflow. The signature is now what closes it -- a retirement takes
// the registry row and no specification, so there is no wrong one to pass -- and
// the test asserts the name the old code WOULD have built is a different name,
// so a change that reintroduced the hybrid would have to redden here.
func TestRetireIndex_DropsTheIndexTheGenerationBuiltLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	retiring := indexedSpec(c, table)
	c.Assert(embedpg.EnsureTarget(ctx, db, retiring), qt.IsNil)
	_, err := embedpg.EnsureIndex(ctx, db, retiring)
	c.Assert(err, qt.IsNil)
	_, valid, _ := indexInCatalog(c, ctx, db, retiring)
	c.Assert(valid, qt.IsTrue, qt.Commentf("the index must exist, or the drop asserts nothing"))

	registered := registryRowFor(retiring)
	c.Assert(embedpg.GenerationIndexName(registered), qt.Equals, indexNameOf(c, retiring))

	// The name the retirement used to build: the operator's CURRENT
	// specification -- a different generation -- with the retired column
	// swapped in. Target.Column is an identity field, so the digest belongs to
	// a hybrid that is no generation, and the DROP matched nothing.
	holding := indexedSpec(c, table)
	holding.Model.Revision = "2"
	holding.Target.Column = retiring.Target.Column + "_v2"
	hybrid := holding
	hybrid.Target.Column = retiring.Target.Column
	c.Assert(indexNameOf(c, hybrid), qt.Not(qt.Equals), indexNameOf(c, retiring),
		qt.Commentf("the hybrid must name a different index, or this test cannot fail"))

	exists, err := embedpg.GenerationIndexExists(ctx, db, registered)
	c.Assert(err, qt.IsNil)
	c.Assert(exists, qt.IsTrue)

	c.Assert(embedpg.RetireIndex(ctx, db, registered), qt.IsNil)

	gone, err := embedpg.GenerationIndexExists(ctx, db, registered)
	c.Assert(err, qt.IsNil)
	c.Assert(gone, qt.IsFalse)
}

// TestRetireIndex_ReportsAGenerationThatBuiltNoIndexLive is the control.
//
// `DropsIndex` was a literal `true`, so a plan promised to drop an index
// whether or not one existed and the record afterwards claimed one had been
// dropped. A generation with no index method builds none, and the question has
// to be answered by the catalog rather than by the caller's specification --
// which describes a different generation.
func TestRetireIndex_ReportsAGenerationThatBuiltNoIndexLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, table := targetColumnsDatabase(c, ctx, withVector)

	unindexed := loadTargetSpec(c, table)
	c.Assert(unindexed.Target.IndexMethod, qt.Equals, "")
	c.Assert(embedpg.EnsureTarget(ctx, db, unindexed), qt.IsNil)

	registered := registryRowFor(unindexed)
	exists, err := embedpg.GenerationIndexExists(ctx, db, registered)

	c.Assert(err, qt.IsNil)
	c.Assert(exists, qt.IsFalse)
	// And retiring it is still a clean no-op rather than an error.
	c.Assert(embedpg.RetireIndex(ctx, db, registered), qt.IsNil)
}

// registryRowFor is the row the registry holds for a generation, which is all a
// retirement has to work from.
func registryRowFor(spec embedgen.Spec) embedstore.Generation {
	return embedstore.Generation{
		Identity:     spec.Identity().Digest,
		TargetSchema: spec.Target.Schema,
		TargetTable:  spec.Target.Table,
		TargetColumn: spec.Target.Column,
	}
}

// indexNameOf is the name the specification itself builds.
func indexNameOf(c *qt.C, spec embedgen.Spec) string {
	c.Helper()
	objects, err := spec.TargetObjects()
	c.Assert(err, qt.IsNil)
	return objects.Index.Name
}
