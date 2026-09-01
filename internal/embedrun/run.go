package embedrun

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.5x5.cz/ptah/internal/embeddigest"
)

// Phase is where a run has got to in the lifecycle.
//
// The names are the epic's phases. They are a state machine rather than a
// label: [Run.Advance] refuses a move that is not on the path, because a run
// that jumped from backfilling to cutover would cut over a corpus nobody
// verified.
type Phase string

const (
	// PhaseResolved is a specification whose provider has answered: the model
	// identity and the reported dimension are known.
	PhaseResolved Phase = "resolved"
	// PhasePlanned is a run whose target objects and scope are decided.
	PhasePlanned Phase = "planned"
	// PhasePrepared is a run whose target column exists.
	PhasePrepared Phase = "prepared"
	// PhaseBoundaryCaptured is a run holding a snapshot watermark: the point
	// the backfill covers up to, and the point catch-up starts from.
	PhaseBoundaryCaptured Phase = "boundary_captured"
	// PhaseBackfilling is a run walking the snapshot.
	PhaseBackfilling Phase = "backfilling"
	// PhaseBackfilled is a run whose walk reached the end of the snapshot.
	//
	// It exists because nothing recorded that fact. The phase was set to
	// `backfilling` AFTER the walk finished, and verification asked
	// `Phase != PhaseBackfilling` -- so a backfill that had embedded every row
	// was told it had not reached the end of its snapshot, while a run that had
	// never backfilled at all, sitting at `boundary_captured`, was told it had
	// (stokaro/ptah#2649).
	PhaseBackfilled Phase = "backfilled"
	// PhaseCaughtUp is a run whose catch-up watermark has reached the source.
	PhaseCaughtUp Phase = "caught_up"
	// PhaseIndexed is a run whose target index exists.
	PhaseIndexed Phase = "indexed"
	// PhaseVerified is a run whose verification passed or was explicitly
	// accepted.
	PhaseVerified Phase = "verified"
	// PhaseCutOver is a run whose active pointer names this generation.
	PhaseCutOver Phase = "cut_over"
	// PhaseRetired is a run whose previous generation has been removed.
	PhaseRetired Phase = "retired"
	// PhaseRolledBack is a run returned to the previous generation.
	PhaseRolledBack Phase = "rolled_back"
)

// Status is whether a run is moving.
type Status string

const (
	// StatusRunning is a run a worker holds.
	StatusRunning Status = "running"
	// StatusPaused is a run stopped at a safe batch boundary, resumable.
	StatusPaused Status = "paused"
	// StatusFailed is a run stopped by a failure, with the classification
	// recorded.
	StatusFailed Status = "failed"
	// StatusComplete is a run that reached its terminal phase.
	StatusComplete Status = "complete"
)

// Errors a caller distinguishes.
var (
	// ErrFenced is a mutating operation from a worker the run has moved past.
	ErrFenced = errors.New("the run has a newer fencing token")
	// ErrPhase is a transition that is not on the lifecycle's path.
	ErrPhase = errors.New("phase transition is not on the lifecycle path")
	// ErrCheckpoint is a checkpoint whose preconditions are not met.
	ErrCheckpoint = errors.New("checkpoint refused")
	// ErrGeneration is a run asked to work on a generation it is not for.
	ErrGeneration = errors.New("the run was prepared for a different generation")
)

// Run is one embedding migration's durable state.
//
// Every field here answers a question a restart has to ask, which is why the
// list is long: a run that resumed without one of them would either repeat work
// it already paid for or skip work it never did.
type Run struct {
	// ID identifies this run.
	ID string
	// SpecDigest and GenerationIdentity pin what is being built. They are
	// separate because a specification may be recorded before its provider has
	// answered, and the identity is only complete once it has.
	SpecDigest         string
	GenerationIdentity string
	// Environment, Source, Target and ProviderProfile identify where the run
	// operates and through what.
	Environment     string
	Source          string
	Target          string
	ProviderProfile string
	// ResolvedModel is the model identity the provider reported, which is what
	// makes the generation reproducible or explains why it is not.
	ResolvedModel string
	// PtahVersion and PolicyDigest record what produced and governed the run,
	// so a later reader can tell whether the rules have moved since.
	PtahVersion  string
	PolicyDigest string

	// Phase and Status are where the run is and whether it is moving.
	Phase  Phase
	Status Status

	// LeaseOwner names the worker that holds the run, and FencingToken is what
	// makes the lease enforceable. See [Run.Fence].
	LeaseOwner   string
	LeaseExpires time.Time
	FencingToken int64

	// SnapshotWatermark is the boundary the backfill covers up to.
	// CatchUpWatermark is how far catch-up has processed past it.
	SnapshotWatermark string
	CatchUpWatermark  string

	// Cursor is the keyset position the backfill resumes from: the last
	// completed key, in the specification's key order.
	//
	// Keyset rather than an offset, because an offset over a mutable table
	// silently skips or repeats rows as the table changes underneath a scan
	// that takes hours.
	Cursor []string

	// Progress counts what has happened.
	Progress Progress

	// VerificationRef, CutoverPlanRef and ApprovalRef point at the artifacts a
	// cutover requires, rather than embedding them.
	VerificationRef string
	CutoverPlanRef  string
	ApprovalRef     string

	// ActivePointer names the generation queries currently read, and
	// RollbackEligible records whether the previous one is still there to
	// return to.
	ActivePointer    string
	RollbackEligible bool

	// FailureClass classifies why a failed run stopped, and FailureDetail says
	// what happened.
	FailureClass  string
	FailureDetail string

	// CreatedAt and UpdatedAt are the run's timestamps.
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Progress is what a run has done, in counts.
type Progress struct {
	// RowsScanned, RowsEmbedded, RowsSkipped and RowsDeleted count the source
	// rows this run has handled.
	//
	// Skipped is separate from embedded because a skipped row is a deliberate
	// gap the specification asked for, and verification reads it as such rather
	// than as missing coverage.
	RowsScanned  int64
	RowsEmbedded int64
	RowsSkipped  int64
	RowsDeleted  int64
	// BatchesCommitted counts the checkpoints behind the cursor.
	BatchesCommitted int64
	// ProviderPromptTokens and ProviderTotalTokens accumulate what the provider
	// reported, for an operator's cost view.
	ProviderPromptTokens int64
	ProviderTotalTokens  int64
	// RetryCount is how many times a batch has been retried since the last
	// checkpoint, which is what tells a stuck run from a slow one.
	RetryCount int
}

// nextPhases is the lifecycle's path.
//
// Written as the allowed moves rather than as a rule, because the interesting
// part is which moves are missing: nothing reaches cutover except through
// verification, and nothing reaches retirement except through cutover.
var nextPhases = map[Phase][]Phase{
	PhaseResolved:         {PhasePlanned},
	PhasePlanned:          {PhasePrepared},
	PhasePrepared:         {PhaseBoundaryCaptured},
	PhaseBoundaryCaptured: {PhaseBackfilling},
	PhaseBackfilling:      {PhaseBackfilled},
	PhaseBackfilled:       {PhaseCaughtUp},
	// Indexing is a step a specification may not have. One that declares no
	// index method has nothing to build -- every query over that generation is
	// a sequential scan, which is what its author asked for -- so verification
	// follows the catch-up directly. Making the edge conditional on a
	// specification is not open to a static table, and refusing it would leave
	// such a run unable to reach a phase it legitimately completed.
	//
	// It costs nothing that matters: a DECLARED index that is missing is
	// refused by verification itself, which is where the check belongs, and
	// cut_over is still reachable only from verified.
	PhaseCaughtUp:   {PhaseIndexed, PhaseVerified},
	PhaseIndexed:    {PhaseVerified},
	PhaseVerified:   {PhaseCutOver},
	PhaseCutOver:    {PhaseRetired, PhaseRolledBack},
	PhaseRetired:    nil,
	PhaseRolledBack: nil,
}

// Reach records that the run has got as far as a phase.
//
// The phase is a high-water mark rather than a cursor, and that is what makes
// it usable by verbs an operator may run more than once and out of order. A
// catch-up run after a verification is ordinary -- the source keeps moving --
// and it must not drag the run backwards to say so. So a phase already at or
// past the one being reached is left alone, and nothing is reported: the
// caller's work still happened, it simply told the run nothing new.
//
// Forward it is still one step at a time, which is the guarantee worth having:
// a jump is refused even where a path exists, so nothing reaches a cutover
// without passing through verification. What the earlier shape lacked -- and
// why it had no caller -- is the no-op. `catchup` may run twenty times, and
// after a verification each of those asks to reach a phase already passed; a
// rule that answered that with an error could not be obeyed by the lifecycle it
// described (stokaro/ptah#2441).
//
// The token is checked first, and before the ordering, because a stale worker
// must not be able to move a run at all -- a phase change is the most
// consequential move there is, and a worker whose lease was taken is not the
// one to make it.
func (r *Run) Reach(token int64, to Phase) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if _, known := nextPhases[to]; !known {
		return fmt.Errorf("%w: %s is not a phase of this lifecycle", ErrPhase, to)
	}
	// Already there, or behind. A catch-up run after a verification is
	// ordinary -- the source keeps moving -- and it asks to reach a phase the
	// run has passed. Refusing that would make re-running an earlier verb an
	// error; the work happened, it simply told the run nothing new.
	if to == r.Phase || reaches(to, r.Phase) {
		return nil
	}
	// Forward, and exactly one step. A jump is refused even though a path
	// exists, because the steps it skips are the ones whose absence is
	// invisible afterwards: a corpus cut over without verification looks
	// exactly like one that passed. Every verb completes one phase and reaches
	// that one, so nothing in the lifecycle needs to jump.
	if !slices.Contains(nextPhases[r.Phase], to) {
		return fmt.Errorf("%w: %s cannot move to %s", ErrPhase, r.Phase, to)
	}
	r.Phase = to
	r.UpdatedAt = time.Now().UTC()
	return nil
}

// Reached reports whether the run is at the given phase or past it.
//
// Ask this rather than comparing phases with `!=`. A phase is a position on a
// path, not a scalar, and `run.Phase != PhaseBackfilling` reads as "the
// backfill is done" while also being true for every phase BEFORE it -- which
// is how a run that had never backfilled came to be told its snapshot was
// complete (stokaro/ptah#2649).
func (r Run) Reached(phase Phase) bool {
	return r.Phase == phase || reaches(phase, r.Phase)
}

// reaches reports whether one phase is ahead of another along the lifecycle.
//
// It walks nextPhases rather than comparing against a second list of the order.
// The table already says what may follow what, and an ordering written beside
// it would be a second answer to the same question -- the one that goes stale
// when a phase is added.
func reaches(from, to Phase) bool {
	seen := map[Phase]bool{from: true}
	frontier := []Phase{from}
	for len(frontier) > 0 {
		current := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		for _, next := range nextPhases[current] {
			if next == to {
				return true
			}
			if seen[next] {
				continue
			}
			seen[next] = true
			frontier = append(frontier, next)
		}
	}
	return false
}

// Fence refuses a mutating operation from a worker the run has moved past.
//
// A lease says who should be working. This says who may still COMMIT, which is
// the question that matters after a worker was paused long enough for its lease
// to lapse and then resumed: it still believes it holds the run, and the only
// thing that can stop it is the state refusing its token.
func (r *Run) Fence(token int64) error {
	if token < r.FencingToken {
		return fmt.Errorf("%w: the worker holds %d and the run is at %d",
			ErrFenced, token, r.FencingToken)
	}
	return nil
}

// Claim takes the run for a worker, issuing a token that fences every worker
// that held it before.
func (r *Run) Claim(owner string, lease time.Duration) int64 {
	r.FencingToken++
	r.LeaseOwner = owner
	r.LeaseExpires = time.Now().UTC().Add(lease)
	r.UpdatedAt = time.Now().UTC()
	return r.FencingToken
}

// DescribesGeneration reports whether this run is the one a caller holding that
// generation identity should be driving, and says what differs when it is not.
//
// A run records the generation it was prepared for, and every verb takes both a
// specification and a run id. Nothing compared them, which is stokaro/ptah#2637:
// the documented second-generation workflow leaves the run id alone -- the guide
// derives it from a date and the quick start exports PTAH_RUN_ID -- so a second
// `prepare` registered the new generation, created its columns, and left the run
// naming the first with its finished cursor. The `backfill` after it reported
// "3 scanned, 3 embedded" at exit 0, having made no provider request and written
// no vector: it resumed a completed run.
//
// The other direction was accepted too. A backfill handed a specification for a
// different generation than the run was prepared for wrote that generation's
// identity into a column prepared and registered for the run's own -- and the
// cross-generation refusal further down could not fire, because the column was
// still empty. The error surfaced three verbs later at `verify`, about a
// generation with no run and no registry row.
//
// The message names both identities. "The run is for a different generation" is
// a diagnostic an operator cannot act on without going and looking up which.
func (r Run) DescribesGeneration(identity string) error {
	if r.GenerationIdentity == identity {
		return nil
	}
	return fmt.Errorf(
		"%w: run %s is for generation %s and this specification produces %s",
		ErrGeneration, r.ID,
		embeddigest.Short(r.GenerationIdentity), embeddigest.Short(identity))
}
