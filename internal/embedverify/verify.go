package embedverify

import (
	"math"
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
	// Vector is the stored embedding, present only where a caller actually read
	// the values back. It is never the answer to how wide the vector is; see
	// [RunState.VectorValuesRead], which is how a caller says which it did.
	Vector []float32
	// Tombstone marks a row whose source is gone.
	Tombstone bool
	// Skipped marks a row deliberately without a vector.
	Skipped bool
}

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
	// VectorValuesRead reports whether the target rows carry the vectors
	// themselves or only their shape.
	//
	// Stated rather than inferred, because a caller that passed a zero-filled
	// placeholder of the right length gets a vector layer that checks the
	// length and silently answers "finite" about numbers it never saw. Where a
	// target refuses a non-finite vector on write -- pgvector does -- reading
	// every vector back to prove it proves nothing, and saying so once beats a
	// check that cannot fail.
	VectorValuesRead bool
}

// Verify runs every layer and returns one report.
//
// The layers are independent on purpose: a structural failure does not stop
// coverage from being measured, because an operator deciding what to do wants
// the whole picture rather than the first thing that went wrong.
func Verify(
	expectation Expectation,
	structure Structure,
	source []SourceRow,
	target []TargetRow,
	state RunState,
) Report {
	report := Report{
		Generation: expectation.Generation,
		SourceRows: len(source),
		TargetRows: len(target),
	}
	verifyStructure(&report, expectation, structure)
	verifyCoverageAndFreshness(&report, expectation, source, target)
	verifyVectors(&report, expectation, target)
	if !state.VectorValuesRead {
		report.Unmeasured = append(report.Unmeasured,
			"the stored vectors were not read back, so their dimension was checked and their "+
				"values were not")
	}
	verifyConsistency(&report, state)
	report.sortFindings()
	return report
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

// verifyCoverageAndFreshness answers layers 2 and 3 in one walk.
//
// They share a walk because they share a question -- which source key does this
// target row belong to -- and answering it twice would let the two disagree.
func verifyCoverageAndFreshness(report *Report, expectation Expectation, source []SourceRow, target []TargetRow) {
	byKey := make(map[string]TargetRow, len(target))
	var duplicates []string
	for _, row := range target {
		if _, seen := byKey[row.Key]; seen {
			duplicates = append(duplicates, row.Key)
			continue
		}
		byKey[row.Key] = row
	}
	if len(duplicates) > 0 {
		report.addf(LayerCoverage, Blocking, len(duplicates), duplicates,
			"%d target keys appear more than once", len(duplicates))
	}

	var missing, stale, wrongGeneration []string
	skipped := 0
	inScope := make(map[string]bool, len(source))
	for _, row := range source {
		inScope[row.Key] = true
		if row.Skipped {
			skipped++
		}
		found, ok := byKey[row.Key]
		if !ok {
			if !row.Skipped {
				missing = append(missing, row.Key)
			}
			continue
		}
		switch {
		case found.Generation != expectation.Generation:
			wrongGeneration = append(wrongGeneration, row.Key)
		case found.Tombstone, found.Skipped:
			// A tombstone or a skip against a live source row is a coverage
			// gap: the source has it and this generation does not.
			if !row.Skipped {
				missing = append(missing, row.Key)
			}
		case found.InputHash != row.InputHash,
			row.Version != "" && found.Version != "" && found.Version != row.Version:
			// Both sides have to carry a version for the comparison to mean
			// anything. A target row written with none -- under the input_hash
			// strategy, or before a strategy that records one -- has no earlier
			// version to have moved FROM, so a source that has one now is a
			// strategy change rather than evidence the source moved.
			//
			// Without the second guard, switching to a versioned strategy
			// reports every existing row stale and recomputes a corpus whose
			// text has not changed. The input hash above is what answers
			// freshness in that case, and it answers it correctly
			// (stokaro/ptah#2474).
			stale = append(stale, row.Key)
		}
	}
	reportCoverage(report, missing, stale, wrongGeneration)
	if skipped > 0 {
		// Not a failure -- the specification asked for this. It is still worth
		// saying, because a policy that skips nine rows in ten produces a
		// generation that passes every layer here and answers a tenth of the
		// queries it was built for.
		report.addf(LayerCoverage, Advisory, skipped, nil,
			"%d in-scope source rows were skipped by the specification and carry no vector", skipped)
	}

	var unexpected []string
	for _, row := range target {
		if !inScope[row.Key] && !row.Tombstone {
			unexpected = append(unexpected, row.Key)
		}
	}
	if len(unexpected) > 0 {
		report.addf(LayerCoverage, Blocking, len(unexpected), unexpected,
			"%d target rows are outside the generation's source scope", len(unexpected))
	}
}

// reportCoverage records what the walk above found.
func reportCoverage(report *Report, missing, stale, wrongGeneration []string) {
	if len(missing) > 0 {
		report.addf(LayerCoverage, Blocking, len(missing), missing,
			"%d in-scope source rows have no vector in this generation", len(missing))
	}
	if len(stale) > 0 {
		// This is the finding a count comparison cannot make: the row exists,
		// the totals match, and the vector answers for text the source no
		// longer has.
		report.addf(LayerFreshness, Blocking, len(stale), stale,
			"%d target rows were computed from a source state that has since changed", len(stale))
	}
	if len(wrongGeneration) > 0 {
		report.addf(LayerCoverage, Blocking, len(wrongGeneration), wrongGeneration,
			"%d target rows belong to another generation", len(wrongGeneration))
	}
}

// verifyVectors answers layer 4.
func verifyVectors(report *Report, expectation Expectation, target []TargetRow) {
	var wrongDimension, notFinite, missingPayload []string
	for _, row := range target {
		if row.Tombstone || row.Skipped {
			continue
		}
		switch {
		case row.Dimension == 0:
			missingPayload = append(missingPayload, row.Key)
		case expectation.Dimension > 0 && row.Dimension != expectation.Dimension:
			wrongDimension = append(wrongDimension, row.Key)
		case len(row.Vector) > 0 && !finite(row.Vector):
			// Only where the values were read. A caller that reported the width
			// alone has said so, and asking about numbers it did not fetch
			// would answer "finite" about a vector nobody looked at.
			//
			// No caller reads them today, and the report says so on every run.
			// Measured on pgvector 0.8.1: `vector`, `halfvec` and `sparsevec`
			// each refuse a NaN and an infinity on write -- `NaN not allowed in
			// vector` and so on -- so against every target this build supports,
			// reading the values back would measure the write path a second
			// time. It stays because the layer is the general one and a target
			// that permits such a value is what RunState.VectorValuesRead
			// exists to describe.
			notFinite = append(notFinite, row.Key)
		}
	}
	if len(missingPayload) > 0 {
		report.addf(LayerVectorValidity, Blocking, len(missingPayload), missingPayload,
			"%d rows carry no vector and are not marked skipped or deleted", len(missingPayload))
	}
	if len(wrongDimension) > 0 {
		report.addf(LayerVectorValidity, Blocking, len(wrongDimension), wrongDimension,
			"%d stored vectors do not have the generation's dimension", len(wrongDimension))
	}
	if len(notFinite) > 0 {
		report.addf(LayerVectorValidity, Blocking, len(notFinite), notFinite,
			"%d stored vectors carry NaN or an infinity, which makes every distance over them meaningless",
			len(notFinite))
	}
}

// finite reports whether every component is a usable number.
func finite(vector []float32) bool {
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return false
		}
	}
	return true
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
