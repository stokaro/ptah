package embedverify

import (
	"iter"
	"slices"
	"sort"
	"strings"
)

// SourceRow is one in-scope source row as it is NOW.
//
// "Now" is the whole point: freshness is answered against the source's current
// state, not against what the run believed when it read it.
type SourceRow struct {
	// Key identifies the row.
	Key string
	// Version is its current source version, empty under a strategy that
	// establishes none.
	Version string
	// InputHash is its current source-input hash under the generation being
	// verified.
	InputHash string
	// Skipped marks a row the specification declined to embed, which is a
	// deliberate gap rather than missing coverage.
	Skipped bool
}

// TargetRow is one row of the generation being verified.
type TargetRow struct {
	// Key identifies the row.
	Key string
	// Generation is the generation the row belongs to.
	Generation string
	// Version and InputHash are what the vector was computed from.
	Version   string
	InputHash string
	// Dimension is the width of the stored vector, zero when there is none --
	// a skip, a tombstone, or a row nothing ever wrote.
	//
	// The width rather than the vector, because a corpus is millions of rows
	// and the width is the whole of what the layer below asks about. A caller
	// that answered it with a zero-filled slice of the right length allocated
	// the corpus twice over -- 1536 float32s per row, carrying no information
	// the integer does not -- and a verification over a few million rows ran
	// the process out of memory (stokaro/ptah#2068).
	Dimension int
	// Tombstone marks a row whose source is gone.
	Tombstone bool
	// Skipped marks a row deliberately without a vector.
	Skipped bool
}

// Pair is one position in the walk over the corpus: what the specification
// asks for at a key, and what the column holds there.
//
// The two sides arrive together because they are read together. A reader that
// produced them as two sequences would let a row written between the two reads
// appear on one side and not the other, and the report would name it as a
// coverage gap that nothing created.
//
// Either side may be absent, and each absence means something different:
//
//   - Source nil -- the key is outside the specification's scope. The column
//     holds a vector for a row the generation was never asked to cover.
//   - Target nil -- nothing in the column stands at this key at all.
//
// Both nil is not a position a walk can be at, and a corpus that yields one is
// reporting nothing about nothing; it is ignored rather than counted.
//
// NEITHER POINTER IS RETAINED past the yield that produced it. Verify copies
// what it needs -- a finding holds key strings, which are immutable -- so a
// corpus may point both fields at storage it reuses for the next position, and
// a reader that allocates a pair per row is paying for a guarantee nothing
// here asks for.
type Pair struct {
	// Source is the in-scope source row, nil when the key is out of scope.
	Source *SourceRow
	// Target is the stored row, nil when nothing stands at the key.
	Target *TargetRow
}

// Corpus is the walk Verify folds over.
//
// It is a sequence rather than two slices because verification is the one place
// in the lifecycle where the work is proportional to the corpus rather than to
// a batch: the backfill bounds every batch by keyset, and a verification that
// materialized both sides held a million-row corpus, plus a map over it, in
// memory at once (stokaro/ptah#2621).
//
// A yielded error ends the walk. Verify returns it rather than reporting on
// what it managed to read, because a partial corpus produces coverage findings
// that describe the read rather than the data.
//
// EQUAL KEYS MUST BE ADJACENT. That is what lets a duplicate be found without a
// map, and it is a weaker requirement than sorted order: a reader ordering by
// the key COLUMNS satisfies it even though the encoded key strings sort
// differently -- `ORDER BY id` gives 1, 2, 10 where the strings give "1", "10",
// "2". Nothing here requires the second order, and a reader must not be changed
// to produce it on the belief that this does.
type Corpus = iter.Seq2[Pair, error]

// Structure is what the target's schema actually looks like, read back.
type Structure struct {
	// ColumnExists and ColumnType are the vector column as the catalog reports
	// it.
	ColumnExists bool
	ColumnType   string
	// Dimension is the column's declared dimension.
	Dimension int
	// IndexExists, IndexMethod and OperatorClass are the index over it.
	IndexExists   bool
	IndexMethod   string
	OperatorClass string
	// IndexValid reports whether the index finished building. An invalid index
	// is one PostgreSQL will not use, and a generation whose index is invalid
	// answers queries by sequential scan -- correctly, and far too slowly to
	// cut over to.
	IndexValid bool
	// ExtensionPresent reports whether the vector extension is installed.
	ExtensionPresent bool
	// ActivePointer is the generation queries currently read, which a cutover
	// plan is bound to.
	ActivePointer string
}

// Expectation is what the generation's specification says the structure should
// be.
type Expectation struct {
	Generation      string
	ColumnType      string
	Dimension       int
	IndexMethod     string
	OperatorClass   string
	RequireIndex    bool
	PreviousPointer string
}

// RunState is what the run itself says about whether it finished.
type RunState struct {
	// SnapshotComplete reports that the backfill reached the end of its
	// snapshot.
	SnapshotComplete bool
	// CatchUpReached reports that catch-up processed everything past the
	// snapshot boundary.
	CatchUpReached bool
	// UnreconciledBatches counts batches whose outcome was never decided.
	UnreconciledBatches int
	// StaleLeaseHolder names a worker still holding a lease the run has moved
	// past, empty when none does.
	StaleLeaseHolder string
	// ConsistencyMode is the mode the run was configured with, empty when none
	// was.
	ConsistencyMode string
	// SourceMutable reports whether the source can change during the run.
	SourceMutable bool
}

// Verify runs every layer and returns one report.
//
// The layers are independent on purpose: a structural failure does not stop
// coverage from being measured, because an operator deciding what to do wants
// the whole picture rather than the first thing that went wrong.
//
// The corpus is walked once and never held. An error the walk yields is
// returned rather than reported, because a report built from a corpus that
// stopped halfway carries coverage findings describing the read rather than
// the data -- every unread row reads as an in-scope row with no vector.
func Verify(
	expectation Expectation,
	structure Structure,
	corpus Corpus,
	state RunState,
) (Report, error) {
	report := Report{Generation: expectation.Generation}
	verifyStructure(&report, expectation, structure)
	if err := walkCorpus(&report, expectation, corpus); err != nil {
		return Report{}, err
	}
	// Said on every run rather than conditionally, because it is true on every
	// run: the read reports each stored vector's width and never its values.
	//
	// It is not a gap. Measured on pgvector 0.8.1, every representation this
	// build targets refuses a non-finite component on write -- `NaN not allowed
	// in vector`, and the same for `halfvec` and `sparsevec` -- so a value that
	// could fail such a check cannot be in the corpus to begin with. What keeps
	// it out is `embedprovider.validateVector`, which refuses the provider's
	// answer before anything is written (stokaro/ptah#2622).
	report.Unmeasured = append(report.Unmeasured,
		"the stored vectors were not read back, so their dimension was checked and their "+
			"values were not; the target refuses a non-finite component on write, so nothing "+
			"here could have carried one")
	verifyConsistency(&report, state)
	report.sortFindings()
	return report, nil
}

// verifyStructure answers layer 1.
func verifyStructure(report *Report, expectation Expectation, structure Structure) {
	if !structure.ExtensionPresent {
		report.addf(LayerStructural, Blocking, 0, nil,
			"the vector extension is not installed, so nothing here can be queried")
	}
	if !structure.ColumnExists {
		report.addf(LayerStructural, Blocking, 0, nil, "the generation's vector column does not exist")
		return
	}
	if !strings.EqualFold(structure.ColumnType, expectation.ColumnType) {
		report.addf(LayerStructural, Blocking, 0, nil,
			"the column is %s and the generation expects %s", structure.ColumnType, expectation.ColumnType)
	}
	switch {
	case expectation.Dimension <= 0:
		// Without a declared dimension neither the column nor a single stored
		// vector can be checked against anything, so two layers go quiet at
		// once. That is worth saying out loud rather than passing.
		report.addf(LayerStructural, Blocking, 0, nil,
			"the generation declares no dimension, so nothing can be checked against one")
	case structure.Dimension != expectation.Dimension:
		report.addf(LayerStructural, Blocking, 0, nil,
			"the column holds %d dimensions and the generation expects %d",
			structure.Dimension, expectation.Dimension)
	}
	verifyIndex(report, expectation, structure)
	if expectation.PreviousPointer != "" && structure.ActivePointer != expectation.PreviousPointer {
		// The cutover plan was written against a pointer that has since moved,
		// so somebody else has cut over in the meantime and this plan would
		// undo their work.
		report.addf(LayerStructural, Blocking, 0, nil,
			"the active pointer is %q and the cutover plan was built against %q",
			structure.ActivePointer, expectation.PreviousPointer)
	}
}

// verifyIndex answers the index half of layer 1.
func verifyIndex(report *Report, expectation Expectation, structure Structure) {
	if !expectation.RequireIndex {
		return
	}
	if !structure.IndexExists {
		report.addf(LayerStructural, Blocking, 0, nil, "the generation's index does not exist")
		return
	}
	if !structure.IndexValid {
		report.addf(LayerStructural, Blocking, 0, nil,
			"the index exists and is not valid, so queries fall back to a sequential scan")
	}
	if !strings.EqualFold(structure.IndexMethod, expectation.IndexMethod) {
		report.addf(LayerStructural, Blocking, 0, nil,
			"the index uses %s and the generation expects %s", structure.IndexMethod, expectation.IndexMethod)
	}
	if !strings.EqualFold(structure.OperatorClass, expectation.OperatorClass) {
		// A wrong operator class answers every query with the wrong distance,
		// which produces plausible results in the wrong order.
		report.addf(LayerStructural, Blocking, 0, nil,
			"the index uses operator class %s and the generation expects %s",
			structure.OperatorClass, expectation.OperatorClass)
	}
}

// keySample keeps the keys one finding will list, and counts the rest.
//
// A fold that appended every offending key and truncated at the end is bounded
// in the REPORT and unbounded in memory, which is the same defect one layer
// down from the one this walk exists to fix: measured, a million-row corpus
// with a tenth of it stale held 8.9 MB of keys nobody would ever read. What a
// finding lists is [MaxReportedKeys] of them, so that is what is kept.
//
// The kept keys are the smallest, which is what the report has always listed --
// `Report.addf` sorts before it truncates. Keeping the first ones seen would
// have been cheaper and would have quietly changed which twenty an operator is
// shown, depending on the order a server happened to return rows in.
type keySample struct {
	// total is every key offered, which is what the finding's count reports.
	total int
	// kept is at most MaxReportedKeys of them, ascending.
	kept []string
}

// add offers a key.
func (s *keySample) add(key string) {
	s.total++
	if len(s.kept) == MaxReportedKeys && key >= s.kept[len(s.kept)-1] {
		return
	}
	position := sort.SearchStrings(s.kept, key)
	s.kept = slices.Insert(s.kept, position, key)
	if len(s.kept) > MaxReportedKeys {
		s.kept = s.kept[:MaxReportedKeys]
	}
}

// walkCorpus answers layers 2, 3 and 4 in one pass over the corpus.
//
// One pass because the layers share a question -- which source key does this
// stored row belong to -- and answering it more than once would let the answers
// disagree. It used to be three passes over two materialized slices plus a map
// joining them, which is what made a verification's memory proportional to the
// corpus (stokaro/ptah#2621).
func walkCorpus(report *Report, expectation Expectation, corpus Corpus) error {
	walk := corpusWalk{expectation: expectation}
	for pair, err := range corpus {
		if err != nil {
			return err
		}
		walk.take(pair)
	}
	walk.report(report)
	return nil
}

// corpusWalk is everything the fold has to remember.
//
// What it holds is the findings, which are bounded by `boundedKeys` where they
// are reported, plus a handful of counters and the previous key. Nothing here
// grows with the corpus except a finding list, and a corpus that produced an
// unbounded one is a corpus where every row is wrong.
type corpusWalk struct {
	expectation    Expectation
	sourceRows     int
	targetRows     int
	targetVectors  int
	tombstones     int
	skippedTargets int
	skipped        int
	// lastKey is the previous STORED row's key, and haveLast says whether there
	// was one. A source-only position leaves both alone, so a key stored twice
	// is still adjacent across one.
	lastKey  string
	haveLast bool

	duplicates      keySample
	missing         keySample
	stale           keySample
	wrongGeneration keySample
	unexpected      keySample
	missingPayload  keySample
	wrongDimension  keySample
}

// take folds one position of the walk in.
func (w *corpusWalk) take(pair Pair) {
	if pair.Source == nil && pair.Target == nil {
		return
	}
	if pair.Source != nil {
		w.sourceRows++
		if pair.Source.Skipped {
			w.skipped++
		}
	}
	if pair.Target != nil {
		w.targetRows++
		w.takeTargetShape(*pair.Target)
		w.takeDuplicate(*pair.Target)
		w.takeVector(*pair.Target)
		if pair.Source == nil {
			w.takeOutOfScope(*pair.Target)
		}
	}
	if pair.Source != nil {
		w.takeCoverage(*pair.Source, pair.Target)
	}
}

// takeTargetShape counts what one stored row actually holds.
//
// By the row's own flags rather than as a partition of targetRows: a tombstone
// that still holds a vector is both, and it is a finding rather than a category
// to pick between (stokaro/ptah#2734).
func (w *corpusWalk) takeTargetShape(row TargetRow) {
	if row.Dimension > 0 {
		w.targetVectors++
	}
	if row.Tombstone {
		w.tombstones++
	}
	if row.Skipped {
		w.skippedTargets++
	}
}

// takeDuplicate reports a key the column holds twice.
//
// A duplicate is reported rather than resolved: which of two rows for one key
// is the answer is not a question this layer can settle, and picking one would
// verify a corpus against a row somebody's query may not read. Each of the two
// is then judged on its own merits, which is the one thing that changed when
// this stopped being a map -- the second row used to be judged against the
// first, silently.
func (w *corpusWalk) takeDuplicate(row TargetRow) {
	if w.haveLast && row.Key == w.lastKey {
		w.duplicates.add(row.Key)
	}
	w.lastKey, w.haveLast = row.Key, true
}

// takeCoverage decides what one in-scope source row's stored row says.
func (w *corpusWalk) takeCoverage(row SourceRow, found *TargetRow) {
	if found == nil {
		if !row.Skipped {
			w.missing.add(row.Key)
		}
		return
	}
	switch classifyRow(w.expectation, row, *found) {
	case rowMissing:
		if !row.Skipped {
			w.missing.add(row.Key)
		}
	case rowWrongGeneration:
		w.wrongGeneration.add(row.Key)
	case rowStale:
		w.stale.add(row.Key)
	case rowCovered:
	}
}

// takeOutOfScope records a stored row this generation wrote for a key the
// specification does not ask for.
//
// The generation is compared as well as the scope. A row holding another
// generation's vector is out of this one's scope by definition -- that is what
// a previous generation looks like -- and reporting it would make every
// migration's verification blame its predecessor.
//
// A TOMBSTONE is exempt because it is Ptah's own record that a row left scope:
// catch-up writes one for exactly this, so reporting it would turn the normal
// outcome into a blocking finding. What is not exempt is a tombstone that still
// holds a vector. Ptah never writes that shape -- the state column and the
// vector are assigned in one UPDATE -- so a tombstoned row with a vector was
// written by something else, and it is searchable: measured, a similarity query
// returned it first while verification reported every layer passing
// (stokaro/ptah#2649 finding 2).
func (w *corpusWalk) takeOutOfScope(row TargetRow) {
	if row.Generation != w.expectation.Generation {
		return
	}
	if row.Tombstone && row.Dimension == 0 {
		return
	}
	w.unexpected.add(row.Key)
}

// takeVector answers layer 4 for one stored row.
func (w *corpusWalk) takeVector(row TargetRow) {
	if row.Tombstone || row.Skipped {
		return
	}
	switch {
	case row.Dimension == 0:
		w.missingPayload.add(row.Key)
	case w.expectation.Dimension > 0 && row.Dimension != w.expectation.Dimension:
		w.wrongDimension.add(row.Key)
	}
}

// report writes what the fold found, in the order the three separate passes
// wrote it in, so the report a reader sees is unchanged by the rewrite.
func (w *corpusWalk) report(report *Report) {
	report.SourceRows = w.sourceRows
	report.TargetRows = w.targetRows
	report.TargetVectors = w.targetVectors
	report.Tombstones = w.tombstones
	report.SkippedTargets = w.skippedTargets
	if w.duplicates.total > 0 {
		report.addf(LayerCoverage, Blocking, w.duplicates.total, w.duplicates.kept,
			"%d target keys appear more than once", w.duplicates.total)
	}
	reportCoverage(report, w.missing, w.stale, w.wrongGeneration)
	if w.skipped > 0 {
		// Not a failure -- the specification asked for this. It is still worth
		// saying, because a policy that skips nine rows in ten produces a
		// generation that passes every layer here and answers a tenth of the
		// queries it was built for.
		report.addf(LayerCoverage, Advisory, w.skipped, nil,
			"%d in-scope source rows were skipped by the specification and carry no vector", w.skipped)
	}
	if w.unexpected.total > 0 {
		report.addf(LayerCoverage, Blocking, w.unexpected.total, w.unexpected.kept,
			"%d target rows are outside the generation's source scope", w.unexpected.total)
	}
	if w.missingPayload.total > 0 {
		report.addf(LayerVectorValidity, Blocking, w.missingPayload.total, w.missingPayload.kept,
			"%d rows carry no vector and are not marked skipped or deleted", w.missingPayload.total)
	}
	if w.wrongDimension.total > 0 {
		report.addf(LayerVectorValidity, Blocking, w.wrongDimension.total, w.wrongDimension.kept,
			"%d stored vectors do not have the generation's dimension", w.wrongDimension.total)
	}
}

// rowVerdict is what one source row's target row turned out to be.
type rowVerdict int

const (
	// rowCovered is a target row this generation wrote from the source as it is
	// now.
	rowCovered rowVerdict = iota
	// rowMissing is a source row this generation has no vector for.
	rowMissing
	// rowWrongGeneration is a target row another generation wrote.
	rowWrongGeneration
	// rowStale is a vector computed from a source state that has since moved.
	rowStale
)

// classifyRow is the decision itself, with no reporting in it.
func classifyRow(expectation Expectation, row SourceRow, found TargetRow) rowVerdict {
	switch {
	case found.Generation == "":
		// Nothing ever wrote this row. Reporting it as belonging to another
		// generation named a generation that does not exist and sent an
		// operator looking for one -- which is what a corpus before its first
		// backfill produced, on every row, in the sentence a reader meets
		// first (stokaro/ptah#2068).
		return rowMissing
	case found.Generation != expectation.Generation:
		return rowWrongGeneration
	case found.Tombstone, found.Skipped:
		// A tombstone or a skip against a live source row is a coverage gap:
		// the source has it and this generation does not.
		return rowMissing
	case found.InputHash != row.InputHash,
		row.Version != "" && found.Version != "" && found.Version != row.Version:
		// Both sides have to carry a version for the comparison to mean
		// anything. A target row written with none -- under the input_hash
		// strategy, or before a strategy that records one -- has no earlier
		// version to have moved FROM, so a source that has one now is a
		// strategy change rather than evidence the source moved.
		//
		// Without the second guard, switching to a versioned strategy reports
		// every existing row stale and recomputes a corpus whose text has not
		// changed. The input hash above is what answers freshness in that case,
		// and it answers it correctly (stokaro/ptah#2474).
		return rowStale
	}
	return rowCovered
}

// reportOutOfScope names target rows the source no longer accounts for.
// reportCoverage records what the walk above found.
func reportCoverage(report *Report, missing, stale, wrongGeneration keySample) {
	if missing.total > 0 {
		report.addf(LayerCoverage, Blocking, missing.total, missing.kept,
			"%d in-scope source rows have no vector in this generation", missing.total)
	}
	if stale.total > 0 {
		// This is the finding a count comparison cannot make: the row exists,
		// the totals match, and the vector answers for text the source no
		// longer has.
		report.addf(LayerFreshness, Blocking, stale.total, stale.kept,
			"%d target rows were computed from a source state that has since changed", stale.total)
	}
	if wrongGeneration.total > 0 {
		report.addf(LayerCoverage, Blocking, wrongGeneration.total, wrongGeneration.kept,
			"%d target rows belong to another generation", wrongGeneration.total)
	}
}

// verifyConsistency answers layer 5: whether the run finished what it started.
func verifyConsistency(report *Report, state RunState) {
	if !state.SnapshotComplete {
		report.addf(LayerConsistency, Blocking, 0, nil, "the backfill has not reached the end of its snapshot")
	}
	if !state.CatchUpReached {
		report.addf(LayerConsistency, Blocking, 0, nil,
			"catch-up has not reached the barrier, so changes after the snapshot are unprocessed")
	}
	if state.UnreconciledBatches > 0 {
		report.addf(LayerConsistency, Blocking, state.UnreconciledBatches, nil,
			"%d batches were never reconciled, so what they wrote is unknown", state.UnreconciledBatches)
	}
	if state.StaleLeaseHolder != "" {
		report.addf(LayerConsistency, Blocking, 0, nil,
			"worker %q still holds a lease on this run and could still write", state.StaleLeaseHolder)
	}
	if state.SourceMutable && state.ConsistencyMode == "" {
		// The epic's cutover rule: a mutable source with no consistency mode
		// cannot be declared ready, because nothing establishes that what was
		// read is what is there.
		report.addf(LayerConsistency, Blocking, 0, nil,
			"the source is mutable and the run has no consistency mode, so nothing establishes that the "+
				"backfill covers the source as it is now")
	}
}
