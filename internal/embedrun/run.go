package embedrun

import (
	"errors"
	"fmt"
	"slices"
	"time"
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
	PhaseBackfilling:      {PhaseCaughtUp},
	PhaseCaughtUp:         {PhaseIndexed},
	PhaseIndexed:          {PhaseVerified},
	PhaseVerified:         {PhaseCutOver},
	PhaseCutOver:          {PhaseRetired, PhaseRolledBack},
	PhaseRetired:          nil,
	PhaseRolledBack:       nil,
}

// Advance moves the run one phase along the lifecycle.
//
// The token is checked first: a stale worker must not be able to move a run at
// all, and a phase change is the most consequential move there is.
func (r *Run) Advance(token int64, to Phase) error {
	if err := r.Fence(token); err != nil {
		return err
	}
	if !slices.Contains(nextPhases[r.Phase], to) {
		return fmt.Errorf("%w: %s cannot move to %s", ErrPhase, r.Phase, to)
	}
	r.Phase = to
	r.UpdatedAt = time.Now().UTC()
	return nil
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
