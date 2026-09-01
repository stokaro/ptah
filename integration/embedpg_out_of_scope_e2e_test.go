//go:build integration

// A vector outside the generation's source scope is reported.
//
// stokaro/ptah#2649 finding 2: `VerificationCorpus` builds both sides from one
// `SELECT ... WHERE (<filter>)`, so every target row it produced was in scope
// by construction and `embedverify.reportOutOfScope` could not fire through the
// shipped reader. A generation carrying vectors for rows the specification
// excludes passed every layer, and the reported target-row count was not the
// number of vectors in the column.
//
// The out-of-scope vector is written directly rather than produced by a defect.
// How it got there is not the question -- catch-up made them before #2638, a
// hand-run UPDATE makes them now -- and a test that reproduced one particular
// cause would stop covering the check the moment that cause was fixed.

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedverify"
)

// TestEmbedPGVerificationSeesAVectorOutsideTheFilterE2E is the finding, and its
// control: an in-scope corpus with no stray vector reports nothing.
func TestEmbedPGVerificationSeesAVectorOutsideTheFilterE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_out_of_scope_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)
	engine, _, _ := aFilteredCatchUpEngine(c, ctx, db, spec)

	_, backfilled, err := engine.Backfill(ctx, filterRunID)
	c.Assert(err, qt.IsNil)
	c.Assert(backfilled.RowsEmbedded, qt.Equals, int64(2))

	generation := spec.Identity().Digest

	// The control runs first, and against the same corpus: two published rows
	// with vectors and nothing else. A check that reported every corpus would
	// satisfy the assertion below and this is where it reddens.
	clean := verifyFilteredCorpus(c, ctx, db, spec, generation)
	c.Assert(outOfScopeSummaries(clean), qt.HasLen, 0,
		qt.Commentf("a corpus with no stray vector reported one: %v", clean.Findings))
	c.Assert(clean.TargetRows, qt.Equals, 2)

	writeAnOutOfScopeVector(c, ctx, db, spec, generation, 3)

	found := verifyFilteredCorpus(c, ctx, db, spec, generation)

	c.Assert(outOfScopeSummaries(found), qt.DeepEquals,
		[]string{"1 target rows are outside the generation's source scope"})
	// And the count now includes it, which is the other half of the finding:
	// the reported number was the in-scope rows rather than the vectors.
	c.Assert(found.TargetRows, qt.Equals, 3)
}

// TestEmbedPGVerificationIgnoresAnotherGenerationsVectorE2E is the control that
// keeps the check about THIS generation.
//
// A previous generation's vectors sit in their own columns and carry their own
// identity. Reporting them would make every second migration's verification
// blame its predecessor, and the assertion above cannot tell the two apart.
func TestEmbedPGVerificationIgnoresAnotherGenerationsVectorE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_out_of_scope_other_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)
	engine, _, _ := aFilteredCatchUpEngine(c, ctx, db, spec)

	_, _, err = engine.Backfill(ctx, filterRunID)
	c.Assert(err, qt.IsNil)

	generation := spec.Identity().Digest
	writeAnOutOfScopeVector(c, ctx, db, spec, "a-generation-that-came-before", 3)

	report := verifyFilteredCorpus(c, ctx, db, spec, generation)

	c.Assert(outOfScopeSummaries(report), qt.HasLen, 0,
		qt.Commentf("a previous generation's vector was reported: %v", report.Findings))
}

// writeAnOutOfScopeVector puts a vector on a row the filter excludes, marked
// with the generation given.
func writeAnOutOfScopeVector(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, generation string, id int64,
) {
	c.Helper()
	// #nosec G201 -- the identifiers are the specification's own, and the
	// values are bound.
	statement := fmt.Sprintf(
		`UPDATE articles SET %s = $1, %s = $2, %s = 'x', %s = '7', %s = 'embedded'
		   WHERE id = $3`,
		spec.Target.Column,
		spec.Target.Column+embedpg.GenerationSuffix,
		spec.Target.Column+embedpg.InputHashSuffix,
		spec.Target.Column+embedpg.VersionSuffix,
		spec.Target.Column+embedpg.StateSuffix)
	result, err := db.ExecContext(ctx, statement, "[1,2,3,4]", generation, id)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	c.Assert(affected, qt.Equals, int64(1),
		qt.Commentf("the fixture wrote no vector, so the assertion would measure nothing"))
}

// verifyFilteredCorpus reads both sides through the shipped reader and runs the
// deterministic layers over them.
//
// Through the reader rather than around it: the finding is that the READER made
// the check unreachable, so a test assembling the two sides itself would pass
// against the defect.
func verifyFilteredCorpus(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, generation string,
) embedverify.Report {
	c.Helper()
	corpus, err := embedpg.VerificationCorpus(ctx, db, spec)
	c.Assert(err, qt.IsNil)
	structure, err := embedpg.ReadStructure(ctx, db, spec, "")
	c.Assert(err, qt.IsNil)
	report, err := embedverify.Verify(
		embedverify.Expectation{
			Generation: generation,
			ColumnType: spec.Target.Representation,
			Dimension:  spec.Model.ReportedDimension,
		},
		structure, corpus, embedverify.RunState{},
	)
	c.Assert(err, qt.IsNil)
	return report
}

// outOfScopeSummaries is the finding this file is about, and nothing else.
func outOfScopeSummaries(report embedverify.Report) []string {
	var found []string
	for _, finding := range report.Findings {
		if strings.Contains(finding.Summary, "outside the generation's source scope") {
			found = append(found, finding.Summary)
		}
	}
	return found
}
