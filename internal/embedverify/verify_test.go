package embedverify_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedverify"
)

const generation = "gen-1"

// healthy is a generation with nothing wrong with it. Every test below breaks
// exactly one thing, which is what makes each row measure that thing.
func healthy() (
	embedverify.Expectation,
	embedverify.Structure,
	[]embedverify.SourceRow,
	[]embedverify.TargetRow,
	embedverify.RunState,
) {
	expectation := embedverify.Expectation{
		Generation: generation, ColumnType: "vector(3)", Dimension: 3,
		IndexMethod: "hnsw", OperatorClass: "vector_cosine_ops", RequireIndex: true,
	}
	structure := embedverify.Structure{
		ColumnExists: true, ColumnType: "vector(3)", Dimension: 3,
		IndexExists: true, IndexMethod: "hnsw", OperatorClass: "vector_cosine_ops",
		IndexValid: true, ExtensionPresent: true,
	}
	source := []embedverify.SourceRow{
		{Key: "1", Version: "7", InputHash: "hash-1"},
		{Key: "2", Version: "7", InputHash: "hash-2"},
	}
	target := []embedverify.TargetRow{
		{Key: "1", Generation: generation, Version: "7", InputHash: "hash-1", Vector: []float32{1, 2, 3}},
		{Key: "2", Generation: generation, Version: "7", InputHash: "hash-2", Vector: []float32{4, 5, 6}},
	}
	state := embedverify.RunState{SnapshotComplete: true, CatchUpReached: true}
	return expectation, structure, source, target, state
}

// summaries lists what a report said.
func summaries(report embedverify.Report) []string {
	lines := make([]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		lines = append(lines, finding.Summary)
	}
	return lines
}

// TestVerify_AHealthyGenerationPasses is the control every other test needs.
//
// A verifier that failed everything would satisfy each row below and stop the
// product working, which is the failure mode a suite of only-negative rows
// cannot see.
func TestVerify_AHealthyGenerationPasses(t *testing.T) {
	c := qt.New(t)

	report := embedverify.Verify(healthy())

	c.Assert(report.Findings, qt.HasLen, 0, qt.Commentf("%v", summaries(report)))
	c.Assert(report.Passed(), qt.IsTrue)
	c.Assert(report.SourceRows, qt.Equals, 2)
	c.Assert(report.TargetRows, qt.Equals, 2)
}

// TestVerify_CountsMatchingIsNotCoverage is the finding a count comparison
// cannot make, and the reason coverage is answered key by key.
//
// The totals agree -- two source rows, two target rows -- and one of them is for
// a key the source does not have while the key it does have is missing. A
// verifier comparing counts reports success over a corpus that answers half its
// queries with somebody else's document.
func TestVerify_CountsMatchingIsNotCoverage(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	target[1].Key = "999"

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.SourceRows, qt.Equals, report.TargetRows)
	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(summaries(report), qt.Contains, "1 in-scope source rows have no vector in this generation")
	c.Assert(summaries(report), qt.Contains, "1 target rows are outside the generation's source scope")
}

// TestVerify_ARowCountMatchWithStaleVectorsFails is the epic's sentence, tested.
//
// Every key is present and every count agrees; one vector was computed from
// text the source has since changed. That corpus retrieves a document by what
// it used to say.
func TestVerify_ARowCountMatchWithStaleVectorsFails(t *testing.T) {
	tests := []struct {
		name   string
		change func([]embedverify.TargetRow)
	}{
		{name: "the input hash moved", change: func(rows []embedverify.TargetRow) { rows[0].InputHash = "hash-old" }},
		{name: "the source version moved", change: func(rows []embedverify.TargetRow) { rows[0].Version = "6" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, state := healthy()
			test.change(target)

			report := embedverify.Verify(expectation, structure, source, target, state)

			c.Assert(report.SourceRows, qt.Equals, report.TargetRows)
			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(summaries(report), qt.Contains,
				"1 target rows were computed from a source state that has since changed")
		})
	}
}

// TestVerify_StructuralFailuresBlock walks layer 1.
//
// The operator-class row is the one worth naming: a wrong class answers every
// query with the wrong distance, which produces plausible results in the wrong
// order -- the failure that looks least like a failure.
func TestVerify_StructuralFailuresBlock(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedverify.Structure)
		want   string
	}{
		{
			name: "no extension", change: func(s *embedverify.Structure) { s.ExtensionPresent = false },
			want: "the vector extension is not installed, so nothing here can be queried",
		},
		{
			name: "no column", change: func(s *embedverify.Structure) { s.ColumnExists = false },
			want: "the generation's vector column does not exist",
		},
		{
			name: "the wrong type", change: func(s *embedverify.Structure) { s.ColumnType = "halfvec(3)" },
			want: "the column is halfvec(3) and the generation expects vector(3)",
		},
		{
			name: "the wrong dimension", change: func(s *embedverify.Structure) { s.Dimension = 1024 },
			want: "the column holds 1024 dimensions and the generation expects 3",
		},
		{
			name: "an invalid index", change: func(s *embedverify.Structure) { s.IndexValid = false },
			want: "the index exists and is not valid, so queries fall back to a sequential scan",
		},
		{
			name: "the wrong index method", change: func(s *embedverify.Structure) { s.IndexMethod = "ivfflat" },
			want: "the index uses ivfflat and the generation expects hnsw",
		},
		{
			name:   "the wrong operator class",
			change: func(s *embedverify.Structure) { s.OperatorClass = "vector_l2_ops" },
			want:   "the index uses operator class vector_l2_ops and the generation expects vector_cosine_ops",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, state := healthy()
			test.change(&structure)

			report := embedverify.Verify(expectation, structure, source, target, state)

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(summaries(report), qt.Contains, test.want)
		})
	}
}

// TestVerify_AMovedActivePointerBlocksTheCutover is Scenario 13.
//
// The plan was built against one pointer and somebody else has cut over since.
// Executing it now would undo their work, and the plan cannot tell that from
// its own success.
func TestVerify_AMovedActivePointerBlocksTheCutover(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	expectation.PreviousPointer = "gen-0"
	structure.ActivePointer = "gen-other"

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(summaries(report), qt.Contains,
		`the active pointer is "gen-other" and the cutover plan was built against "gen-0"`)
}

// TestVerify_VectorValidityBlocks walks layer 4: what makes a stored vector
// unusable.
func TestVerify_VectorValidityBlocks(t *testing.T) {
	tests := []struct {
		name   string
		change func([]embedverify.TargetRow)
		want   string
	}{
		{
			name: "no payload", change: func(rows []embedverify.TargetRow) { rows[0].Vector = nil },
			want: "1 rows carry no vector and are not marked skipped or deleted",
		},
		{
			name:   "the wrong dimension",
			change: func(rows []embedverify.TargetRow) { rows[0].Vector = []float32{1, 2} },
			want:   "1 stored vectors do not have the generation's dimension",
		},
		{
			name:   "NaN",
			change: func(rows []embedverify.TargetRow) { rows[0].Vector = []float32{1, float32(math.NaN()), 3} },
			want:   "1 stored vectors carry NaN or an infinity, which makes every distance over them meaningless",
		},
		{
			name:   "an infinity",
			change: func(rows []embedverify.TargetRow) { rows[0].Vector = []float32{1, float32(math.Inf(1)), 3} },
			want:   "1 stored vectors carry NaN or an infinity, which makes every distance over them meaningless",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, state := healthy()
			test.change(target)

			report := embedverify.Verify(expectation, structure, source, target, state)

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(summaries(report), qt.Contains, test.want)
		})
	}
}

// TestVerify_ConsistencyFailuresBlock walks layer 5: whether the run finished
// what it started.
//
// The last row is the epic's cutover rule -- a mutable source with no
// consistency mode cannot be declared ready, because nothing establishes that
// what was read is what is there.
func TestVerify_ConsistencyFailuresBlock(t *testing.T) {
	tests := []struct {
		name   string
		change func(*embedverify.RunState)
		want   string
	}{
		{
			name: "the snapshot is unfinished", change: func(s *embedverify.RunState) { s.SnapshotComplete = false },
			want: "the backfill has not reached the end of its snapshot",
		},
		{
			name:   "catch-up has not reached the barrier",
			change: func(s *embedverify.RunState) { s.CatchUpReached = false },
			want:   "catch-up has not reached the barrier, so changes after the snapshot are unprocessed",
		},
		{
			name:   "a batch was never reconciled",
			change: func(s *embedverify.RunState) { s.UnreconciledBatches = 2 },
			want:   "2 batches were never reconciled, so what they wrote is unknown",
		},
		{
			name:   "a stale worker still holds a lease",
			change: func(s *embedverify.RunState) { s.StaleLeaseHolder = "worker-a" },
			want:   `worker "worker-a" still holds a lease on this run and could still write`,
		},
		{
			name:   "a mutable source with no consistency mode",
			change: func(s *embedverify.RunState) { s.SourceMutable = true },
			want: "the source is mutable and the run has no consistency mode, so nothing establishes that " +
				"the backfill covers the source as it is now",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, state := healthy()
			test.change(&state)

			report := embedverify.Verify(expectation, structure, source, target, state)

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(summaries(report), qt.Contains, test.want)
		})
	}
}

// TestVerify_AMutableSourceWithAConsistencyModeIsAllowed is the control for the
// last row above: the rule is about the ABSENCE of a mode, not about mutability.
func TestVerify_AMutableSourceWithAConsistencyModeIsAllowed(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	state.SourceMutable = true
	state.ConsistencyMode = "outbox"

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_ASkippedRowIsNotACoverageGap keeps a deliberate gap from reading
// as a failure, and from going unsaid.
//
// The specification declined to embed the row -- an empty input under a skip
// policy -- and verification has to tell that from a row nobody got to. It is
// still reported: a policy that skips nine rows in ten passes every layer here
// and answers a tenth of the queries the generation was built for, and the
// operator is the one who can tell which of those two happened.
func TestVerify_ASkippedRowIsNotACoverageGap(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	source[1].Skipped = true
	target = target[:1]

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
	c.Assert(report.Blocking(), qt.HasLen, 0)
	c.Assert(summaries(report), qt.DeepEquals, []string{
		"1 in-scope source rows were skipped by the specification and carry no vector",
	})
	c.Assert(report.Findings[0].Severity, qt.Equals, embedverify.Advisory)
}

// TestVerify_ADuplicateTargetKeyBlocks is the case a per-key walk finds and a
// count does not: two rows for one key, where a query gets whichever the index
// reaches first.
func TestVerify_ADuplicateTargetKeyBlocks(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	target = append(target, target[0])

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(summaries(report), qt.Contains, "1 target keys appear more than once")
}

// TestVerify_ARowFromAnotherGenerationBlocks is Decision 6 at verification
// time: a row in this column belonging to another generation means two runs
// have written to one place.
func TestVerify_ARowFromAnotherGenerationBlocks(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	target[0].Generation = "gen-0"

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(summaries(report), qt.Contains, "1 target rows belong to another generation")
}

// TestVerify_TheReportIsBoundedAndStable keeps a report about a large failure
// readable, and keeps two runs over one state producing one document.
func TestVerify_TheReportIsBoundedAndStable(t *testing.T) {
	c := qt.New(t)
	expectation, structure, _, _, state := healthy()
	var source []embedverify.SourceRow
	for index := range 100 {
		source = append(source, embedverify.SourceRow{
			Key: string(rune('a'+index%26)) + string(rune('a'+index/26)), InputHash: "hash",
		})
	}

	first := embedverify.Verify(expectation, structure, source, nil, state)
	second := embedverify.Verify(expectation, structure, source, nil, state)

	c.Assert(first.Blocking(), qt.Not(qt.HasLen), 0)
	c.Assert(first.Findings[0].Count, qt.Equals, 100)
	c.Assert(first.Findings[0].Keys, qt.HasLen, embedverify.MaxReportedKeys)
	c.Assert(summaries(first), qt.DeepEquals, summaries(second))
	c.Assert(first.Findings[0].Keys, qt.DeepEquals, second.Findings[0].Keys)
}

// TestVerify_ASourceWithoutVersionsIsNotStale keeps the version comparison from
// firing where there is nothing to compare.
//
// Under a strategy that establishes no source version -- a content hash, say --
// every source row reports an empty one, and a verifier comparing it against
// what the target recorded would call the entire generation stale on every run.
func TestVerify_ASourceWithoutVersionsIsNotStale(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	source[0].Version = ""
	source[1].Version = ""

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_ATombstoneForADeletedSourceRowIsCorrect pins the state a delete
// leaves behind.
//
// The source row is gone and the target says so. That is what a tombstone is
// for, and a verifier reading it as a row outside the source's scope would fail
// every generation that ever saw a deletion.
func TestVerify_ATombstoneForADeletedSourceRowIsCorrect(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	target = append(target, embedverify.TargetRow{Key: "gone", Generation: generation, Tombstone: true})

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_ATombstoneOverALiveSourceRowIsAGap is the other side, and it is
// Scenario 4 read back.
//
// A row was deleted, the tombstone was written, and the source has it again.
// The target still refuses to answer for a key the source has, which is a
// coverage gap and not a freshness one -- there is no vector to be stale.
func TestVerify_ATombstoneOverALiveSourceRowIsAGap(t *testing.T) {
	tests := []struct {
		name string
		mark func(*embedverify.TargetRow)
	}{
		{name: "a tombstone", mark: func(row *embedverify.TargetRow) { row.Tombstone = true }},
		{name: "a skip", mark: func(row *embedverify.TargetRow) { row.Skipped = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			expectation, structure, source, target, state := healthy()
			target[1] = embedverify.TargetRow{Key: "2", Generation: generation}
			test.mark(&target[1])

			report := embedverify.Verify(expectation, structure, source, target, state)

			c.Assert(report.Passed(), qt.IsFalse)
			c.Assert(summaries(report), qt.Contains,
				"1 in-scope source rows have no vector in this generation")
		})
	}
}

// TestVerify_TheListedKeysAreTheSortedPrefix makes the bound mean something.
//
// Twenty keys out of a hundred thousand is only useful if it is the same twenty
// every time and an operator can find them. Whatever order the rows were walked
// in, the report lists the first twenty in key order.
func TestVerify_TheListedKeysAreTheSortedPrefix(t *testing.T) {
	c := qt.New(t)
	expectation, structure, _, _, state := healthy()
	source := make([]embedverify.SourceRow, 0, 100)
	for index := range 100 {
		// Walk order and key order deliberately disagree: this emits "aa",
		// "ba", "ca" ... so a report listing what it happened to visit first
		// would start "aa", "ba", "ca" rather than "aa", "ab", "ac".
		source = append(source, embedverify.SourceRow{
			Key: string(rune('a'+index%26)) + string(rune('a'+index/26)), InputHash: "hash",
		})
	}

	report := embedverify.Verify(expectation, structure, source, nil, state)

	c.Assert(report.Findings[0].Keys, qt.DeepEquals, []string{
		"aa", "ab", "ac", "ad", "ba", "bb", "bc", "bd", "ca", "cb",
		"cc", "cd", "da", "db", "dc", "dd", "ea", "eb", "ec", "ed",
	})
}

// TestVerify_ASkippedRowWrittenAsSkippedPasses is the state a skip policy
// leaves behind once the run has been over the row.
//
// Both sides say the same thing -- the specification declined to embed it -- so
// the absent vector is the correct answer rather than a missing one.
func TestVerify_ASkippedRowWrittenAsSkippedPasses(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	source[1].Skipped = true
	target[1] = embedverify.TargetRow{Key: "2", Generation: generation, Skipped: true}

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_AGenerationNotYetIndexedIsNotJudgedOnItsIndex separates the phases.
//
// Backfill runs before the index is built, and verification during backfill has
// to be able to say something useful about coverage without failing on an index
// nobody has asked for yet.
func TestVerify_AGenerationNotYetIndexedIsNotJudgedOnItsIndex(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	expectation.RequireIndex = false
	structure.IndexExists = false
	structure.IndexMethod = ""
	structure.OperatorClass = ""
	structure.IndexValid = false

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_VerificationOutsideACutoverIgnoresThePointer keeps the drift check
// tied to the plan that gives it meaning.
//
// Asking whether a generation is sound is not asking to cut over to it. Without
// a plan there is no pointer the answer was built against, and whatever queries
// currently read is somebody else's business.
func TestVerify_VerificationOutsideACutoverIgnoresThePointer(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	expectation.PreviousPointer = ""
	structure.ActivePointer = "gen-other"

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsTrue, qt.Commentf("%v", summaries(report)))
}

// TestVerify_AGenerationWithoutADimensionSaysSo is what replaced two silent
// skips.
//
// A dimension is what both the column check and every stored vector are
// measured against. Guarding each of them on having one meant an expectation
// that arrived without a dimension passed two layers by asking them nothing,
// and the report said the generation was sound.
func TestVerify_AGenerationWithoutADimensionSaysSo(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	expectation.Dimension = 0

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(report.Passed(), qt.IsFalse)
	c.Assert(summaries(report), qt.Contains,
		"the generation declares no dimension, so nothing can be checked against one")
	c.Assert(summaries(report), qt.Not(qt.Contains),
		"2 stored vectors do not have the generation's dimension")
}

// TestVerify_AMissingColumnReportsOnceRatherThanCascading keeps the report
// honest about what is actually wrong.
//
// A catalog that has no column reports nothing about its type, its dimension or
// its index either. Carrying on down the layer turns one fact into four
// findings, three of which are restatements of the first, and an operator
// reading the report has to work out that they are the same problem.
func TestVerify_AMissingColumnReportsOnceRatherThanCascading(t *testing.T) {
	c := qt.New(t)
	expectation, _, source, target, state := healthy()

	report := embedverify.Verify(
		expectation, embedverify.Structure{ExtensionPresent: true}, source, target, state)

	c.Assert(summaries(report), qt.DeepEquals,
		[]string{"the generation's vector column does not exist"})
}

// TestVerify_AMissingIndexReportsOnceRatherThanCascading is the column case one
// level down.
//
// An index that does not exist has no method and no operator class either, so
// continuing past it produces three findings for one fact -- and two of them
// name a mismatch against an empty string, which reads like a different problem
// than the one there is.
func TestVerify_AMissingIndexReportsOnceRatherThanCascading(t *testing.T) {
	c := qt.New(t)
	expectation, structure, source, target, state := healthy()
	structure.IndexExists = false
	structure.IndexValid = false
	structure.IndexMethod = ""
	structure.OperatorClass = ""

	report := embedverify.Verify(expectation, structure, source, target, state)

	c.Assert(summaries(report), qt.DeepEquals, []string{"the generation's index does not exist"})
}
