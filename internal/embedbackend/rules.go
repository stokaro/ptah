package embedbackend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedcutover"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedspec"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
)

// RecordedSpec parses the document a generation was registered with.
//
// It returns the whole [embedspec.Loaded] rather than the specification alone
// because the consistency mode is on it, and the mode a generation was built
// with is a property of THAT generation. Read off the invocation's file
// instead, retiring an outbox-built generation while passing an immutable
// specification skipped the outbox removal entirely and said nothing
// (stokaro/ptah#2649).
//
// The what argument completes the sentence "so nothing can measure whether it
// is ...", so a refusal names the question its caller was answering rather than
// a generic absence.
func RecordedSpec(
	registered embedstore.Generation, what string,
) (embedspec.Loaded, error) {
	if strings.TrimSpace(registered.SpecDocument) == "" {
		return embedspec.Loaded{}, fmt.Errorf(
			"generation %s records no specification, so nothing can measure whether it is %s",
			registered.Identity, what)
	}
	return embedspec.ParsePublished(
		[]byte(registered.SpecDocument), "the recorded specification", registered.SpecDigest)
}

// ValidatePrepare refuses a prepare whose arguments disagree with each other,
// before any engine is asked anything.
//
// Every engine runs it first, so the same arguments are refused in the same
// words whichever database the run would have used.
func ValidatePrepare(
	spec embedgen.Spec, generation embedstore.Generation, run embedrun.Run, mode embedcatchup.Mode,
) error {
	identity := spec.Identity().Digest
	if generation.Identity != identity {
		return fmt.Errorf(
			"%w: generation registry identity %s does not match specification %s",
			embedrun.ErrGeneration, generation.Identity, identity)
	}
	if err := run.DescribesGeneration(identity); err != nil {
		return err
	}
	if run.Phase != embedrun.PhasePrepared {
		return fmt.Errorf(
			"prepare run %s: %w: starting phase is %s, want %s",
			run.ID, embedrun.ErrPhase, run.Phase, embedrun.PhasePrepared)
	}
	if run.Terminal() {
		return fmt.Errorf(
			"prepare run %s: %w: run is already %s",
			run.ID, embedrun.ErrTerminal, run.Status)
	}
	if generation.ConsistencyMode != string(mode) {
		return fmt.Errorf(
			"%w: generation %s records consistency mode %q, prepare selected %q",
			embedstore.ErrConflict, identity, generation.ConsistencyMode, mode)
	}
	return nil
}

// ValidatePreparedGeneration refuses a prepare whose generation row records a
// different location, dimension or mode than the one being prepared.
func ValidatePreparedGeneration(registered, requested embedstore.Generation) error {
	type field struct {
		name       string
		registered string
		requested  string
	}
	fields := []field{
		{"dimension", strconv.Itoa(registered.Dimension), strconv.Itoa(requested.Dimension)},
		{"target schema", registered.TargetSchema, requested.TargetSchema},
		{"target table", registered.TargetTable, requested.TargetTable},
		{"target column", registered.TargetColumn, requested.TargetColumn},
		{"source schema", registered.SourceSchema, requested.SourceSchema},
		{"source table", registered.SourceTable, requested.SourceTable},
		{"consistency mode", registered.ConsistencyMode, requested.ConsistencyMode},
	}
	for _, candidate := range fields {
		if candidate.registered != candidate.requested {
			return fmt.Errorf(
				"%w: generation %s records %s %q, prepare requested %q",
				embedstore.ErrConflict, registered.Identity, candidate.name,
				candidate.registered, candidate.requested)
		}
	}
	return nil
}

// ValidatePreparedRun decides whether an existing run is a completed prepare of
// the run being prepared, from the run alone.
//
// A row that merely carries the requested identifier is not one: a run created
// before its generation was registered has no committed boundary, and treating
// it as prepared would skip every object prepare owns. It is asked before the
// generation is read, so such a run is refused for its missing boundary rather
// than for the registry row it never had.
func ValidatePreparedRun(wantGeneration embedstore.Generation, existing embedrun.Run) error {
	if existing.Terminal() {
		return fmt.Errorf("%w: run %s is %s",
			embedrun.ErrTerminal, existing.ID, existing.Status)
	}
	if err := existing.DescribesGeneration(wantGeneration.Identity); err != nil {
		return err
	}
	if !existing.Reached(embedrun.PhaseBoundaryCaptured) {
		return fmt.Errorf(
			"prepare run %s: %w: existing run is at %s and has no committed prepare boundary",
			existing.ID, embedrun.ErrPhase, existing.Phase)
	}
	return nil
}

// ValidatePreparedRegistration is the second half of that decision: the
// generation row the existing run belongs to has to record what is being
// prepared now. An engine also checks that the objects the prepare created are
// still there; that half needs the database and is the engine's.
func ValidatePreparedRegistration(
	wantGeneration, registered embedstore.Generation, wantRun, existing embedrun.Run,
) error {
	if registered.Retired() {
		return fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, registered.Identity,
			registered.RetiredAt.UTC().Format(time.RFC3339))
	}
	if err := ValidatePreparedGeneration(registered, wantGeneration); err != nil {
		return err
	}
	if err := embedstore.RunSourceMatches(existing, registered); err != nil {
		return err
	}
	if existing.SpecDigest != wantRun.SpecDigest {
		return fmt.Errorf(
			"%w: run %s records specification %s, prepare requested %s",
			embedstore.ErrConflict, existing.ID, existing.SpecDigest, wantRun.SpecDigest)
	}
	if existing.Target != wantRun.Target {
		return fmt.Errorf(
			"%w: run %s records target %s, prepare requested %s",
			embedstore.ErrConflict, existing.ID, existing.Target, wantRun.Target)
	}
	return nil
}

// ValidateIndexRun refuses an index build for a run that cannot record it.
func ValidateIndexRun(run embedrun.Run, identity string) error {
	if run.Terminal() {
		return fmt.Errorf("%w: run %s is %s", embedrun.ErrTerminal, run.ID, run.Status)
	}
	return run.DescribesGeneration(identity)
}

// IndexedRun returns the exact row to persist after an index build.
//
// The fencing token is raised and the lease cleared, so a worker still holding
// its pre-build snapshot is fenced rather than allowed to write a batch the
// index was not built over.
func IndexedRun(run embedrun.Run, identity string, now time.Time) (embedrun.Run, error) {
	if err := ValidateIndexRun(run, identity); err != nil {
		return embedrun.Run{}, err
	}
	run.FencingToken++
	run.LeaseOwner = ""
	run.LeaseExpires = time.Time{}
	if err := run.Reach(run.FencingToken, embedrun.PhaseIndexed); err != nil {
		return embedrun.Run{}, err
	}
	// Reach is deliberately a no-op when the run is already beyond indexed.
	// Raising the token still changed the row and needs its own timestamp.
	run.UpdatedAt = now.UTC()
	return run, nil
}

// Validate refuses a destruction that names both removals.
//
// They are alternatives rather than options: a generation's vectors live in
// one place, and a caller asking for both has decided the layout twice.
func (d RetirementDestruction) Validate() error {
	if d.DropColumns && d.DropTable {
		return fmt.Errorf(
			"%w: a retirement drops the generation's columns or its table, not both",
			embedstore.ErrConflict)
	}
	return nil
}

// MeasureRollback reads what is true about a previous generation now.
//
// Every field is measured. Rollback must not be reported as available merely
// because the old tables still exist, so none of this comes from a status
// column somebody set.
//
// It takes no specification, deliberately. The question is about ONE
// generation -- the one the pointer names as its way back -- and the only
// specification that can answer it is that generation's own, which the
// registry records. Taking the caller's is what produced stokaro/ptah#2630:
// the answer changed with whichever file the operator happened to pass.
func MeasureRollback(
	ctx context.Context, backend Backend, generation string, pointer embedstore.Pointer,
) (embedcutover.RollbackState, error) {
	registered, err := backend.Store().Generation(ctx, generation)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}
	loaded, err := RecordedSpec(registered, "still fresh enough to roll back to")
	if err != nil {
		return embedcutover.RollbackState{}, err
	}
	previous := loaded.Spec
	structure, err := backend.ReadStructure(ctx, previous, pointer.Active)
	if err != nil {
		return embedcutover.RollbackState{}, err
	}

	// Freshness only where the decision can still use it. A retired generation
	// has no columns left and an absent one never had any, so the walk would
	// fail on a missing column before the decision layer gives its designed
	// refusal (stokaro/ptah#2647). The generation queries already read is
	// excluded for cost: the refusal for it is unconditional.
	alreadyActive := pointer.Active == generation
	var stale, missing int
	if structure.ColumnExists && !registered.Retired() && !alreadyActive {
		stale, missing, err = generationFreshness(ctx, backend, previous, registered)
		if err != nil {
			return embedcutover.RollbackState{}, err
		}
	}
	return embedcutover.RollbackState{
		Present:       structure.ColumnExists,
		AlreadyActive: alreadyActive,
		IndexReady:    structure.IndexExists && structure.IndexValid,
		Retired:       registered.Retired(),
		CutOverAt:     pointer.CutOverAt,
		// Both recorded rather than inferred: a generation nobody verified is
		// unmeasured, and one nobody is feeding drifts with every write.
		VerifiedAt:  registered.VerifiedAt,
		Maintained:  registered.Maintained(time.Now().UTC()),
		StaleRows:   stale,
		MissingRows: missing,
	}, nil
}

// generationFreshness counts what is wrong with a generation right now.
//
// It runs the verification layers rather than walking the rows itself.
// Coverage and freshness are exactly what those layers decide, and a second
// walk here would be a second answer to them. The structure is passed as
// satisfied because the caller reads it separately; what this wants is the
// two counts.
func generationFreshness(
	ctx context.Context, backend Backend, spec embedgen.Spec, registered embedstore.Generation,
) (stale, missing int, err error) {
	corpus, err := backend.VerificationCorpus(ctx, spec)
	if err != nil {
		return 0, 0, err
	}
	report, err := embedverify.Verify(
		embedverify.Expectation{Generation: registered.Identity, Dimension: registered.Dimension},
		embedverify.Structure{
			ColumnExists: true, ExtensionPresent: true, Dimension: registered.Dimension,
		},
		corpus,
		embedverify.RunState{SnapshotComplete: true, CatchUpReached: true})
	if err != nil {
		return 0, 0, err
	}
	for _, finding := range report.Blocking() {
		switch finding.Layer {
		case embedverify.LayerCoverage:
			missing += finding.Count
		case embedverify.LayerFreshness:
			stale += finding.Count
		}
	}
	return stale, missing, nil
}
