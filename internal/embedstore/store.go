package embedstore

import (
	"context"
	"errors"
	"time"

	"go.5x5.cz/ptah/internal/embedrun"
)

// Errors a caller distinguishes.
var (
	// ErrNotFound is a row that is not there.
	ErrNotFound = errors.New("not found")
	// ErrConflict is a write that lost a race it was required to win: a stale
	// fencing token, or a pointer that moved between reading and writing.
	ErrConflict = errors.New("the state changed underneath this write")
	// ErrRetired is an operation on a generation that was destroyed.
	ErrRetired = errors.New("the generation was retired")
)

// Generation is a registry row.
type Generation struct {
	// Identity is the content address.
	Identity string
	// SpecDigest is the specification it came from.
	SpecDigest string
	// Name is a display name, outside the identity.
	Name string
	// Reproducibility and ReproducibilityReason are what the identity could
	// promise.
	Reproducibility       string
	ReproducibilityReason string
	// ResolvedModel is what the provider reported.
	ResolvedModel string
	// Dimension is the vector dimension.
	Dimension int
	// TargetTable and TargetColumn are where its vectors live.
	TargetTable  string
	TargetColumn string
	// CreatedAt is when Ptah first recorded it.
	CreatedAt time.Time
	// RetiredAt is when it was destroyed, zero while it exists.
	RetiredAt time.Time
	// VerifiedAt is when a verification last passed over it, zero when none
	// has.
	//
	// It is what a rollback rests on, and it is recorded by the verification
	// rather than claimed by anything else: a generation somebody says is fine
	// is not a generation anybody measured.
	VerifiedAt time.Time
	// MaintainedUntil is how long something is keeping this generation current,
	// zero when nothing is.
	//
	// This is the difference between a generation whose tables exist and one
	// you can go back to. After a cutover the old generation stops receiving
	// changes unless somebody keeps feeding it, and from that moment it drifts
	// from the source with every write.
	MaintainedUntil time.Time
}

// Maintained reports whether something is keeping this generation current.
func (g Generation) Maintained(now time.Time) bool {
	return !g.MaintainedUntil.IsZero() && now.Before(g.MaintainedUntil)
}

// Retired reports whether the generation was destroyed.
func (g Generation) Retired() bool {
	return !g.RetiredAt.IsZero()
}

// Pointer is which generation a target's queries read.
type Pointer struct {
	// TargetTable is what the pointer is about.
	TargetTable string
	// Active is the generation queries read.
	Active string
	// Previous is what they read before.
	Previous string
	// CutOverAt is when it moved.
	CutOverAt time.Time
	// CutOverBy names who moved it.
	CutOverBy string
	// PlanDigest is the plan that authorized the move, which is what makes a
	// pointer's history auditable against the approval that permitted it.
	PlanDigest string
}

// Store is where run state lives.
//
// Every method takes a context because every one of them is a database round
// trip in the implementation that matters, and a backfill that cannot be
// cancelled is a backfill that holds a lease until it expires.
type Store interface {
	// RegisterGeneration records a generation, or returns the existing row
	// unchanged.
	//
	// Unchanged is the point: a generation is a content address, so two
	// registrations of one identity are the same registration, and a second
	// one overwriting the first would let a display name rewrite history.
	RegisterGeneration(ctx context.Context, generation Generation) (Generation, error)
	// Generation reads one back.
	Generation(ctx context.Context, identity string) (Generation, error)
	// RetireGeneration marks one destroyed, which is terminal.
	RetireGeneration(ctx context.Context, identity string, at time.Time) error
	// RecordVerification records that a verification passed over a generation.
	RecordVerification(ctx context.Context, identity string, at time.Time) error
	// Maintain records how long something will keep a generation current.
	//
	// A zero time clears it, which is what stops a generation being reported as
	// a way back the moment nobody is feeding it.
	Maintain(ctx context.Context, identity string, until time.Time) error

	// CreateRun records a new run.
	CreateRun(ctx context.Context, run embedrun.Run) error
	// Run reads one back.
	Run(ctx context.Context, id string) (embedrun.Run, error)
	// SaveRun writes a run's state, refusing a stale fencing token.
	//
	// The token is what makes this safe rather than the lease: a worker whose
	// lease expired mid-request still holds the token it was given, and the
	// store is the only place that can tell it has been superseded.
	SaveRun(ctx context.Context, run embedrun.Run) error

	// AppendEvent records what happened.
	AppendEvent(ctx context.Context, event embedrun.Event) error
	// Events reads a run's history in order.
	Events(ctx context.Context, runID string) ([]embedrun.Event, error)

	// Pointer reads which generation a target's queries currently read.
	Pointer(ctx context.Context, targetTable string) (Pointer, error)
	// MovePointer moves it, refusing when it is not where the caller thinks.
	//
	// Compare-and-set rather than a write, because a cutover decided against
	// one pointer and executed against another is exactly the case the decision
	// layer refuses and the store must not reintroduce.
	MovePointer(ctx context.Context, pointer Pointer, expectedActive string) error
}
