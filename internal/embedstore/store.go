package embedstore

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embeddigest"
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
	// ErrNoLiveRun is a generation that has run history but no usable live
	// feeder. For an outbox generation, a merely non-terminal run is not enough:
	// it must name the registered source and carry a valid resume position. A
	// generation with no run history is not covered: callers may register and
	// point at generations imported by a store this process never saw.
	ErrNoLiveRun = errors.New("the generation has no usable live feeder")
)

// Generation is a registry row.
type Generation struct {
	// Identity is the content address.
	Identity string
	// SpecDigest is the specification it came from.
	SpecDigest string
	// SpecDocument is that specification's bytes.
	//
	// Recorded because a PREVIOUS generation has to be measurable, and nothing
	// else in this row can reconstruct it. Rollback asks whether the generation
	// the pointer names as its way back is still fresh, and freshness is each
	// row's stored input hash against a hash recomputed from the source -- which
	// needs that generation's own source fields, preprocessing and identity.
	//
	// Without it, rollback measured the retired generation against WHICHEVER
	// specification the operator passed. The documented command passes the
	// current one, so every row's expected hash was computed under an identity
	// belonging to no generation at all, every row mismatched, and the rollback
	// the guide describes was refused with a false count -- while `verify` on
	// the same generation at the same instant reported every layer passing
	// (stokaro/ptah#2630).
	SpecDocument string
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
	// TargetSchema, TargetTable and TargetColumn are where its vectors live.
	//
	// The schema is recorded because retirement reads the location from HERE
	// rather than from a specification, and a bare relation name resolves
	// through the server's search_path: without it, retiring a generation in
	// one schema dropped the columns of a live generation in another
	// (stokaro/ptah#2629). Empty means the specification named no schema, so
	// search_path is what its author asked for.
	TargetSchema string
	TargetTable  string
	TargetColumn string
	// SourceSchema and SourceTable are the relation it reads.
	//
	// Recorded because an outbox belongs to a SOURCE table -- two generations
	// over one source share one set of triggers -- so retirement has to ask
	// whether the generation it is destroying was the last reader of that
	// source. The target cannot answer it. A specification whose target table
	// differs from its source is accepted, and asking the target counted zero
	// readers for a source another live generation was still being fed from:
	// retiring one generation took the shared outbox away, and the survivor's
	// catch-up then failed on a relation that no longer existed
	// (stokaro/ptah#2649).
	//
	// Empty schema means the specification named none, so search_path is what
	// its author asked for -- the same convention TargetSchema carries.
	SourceSchema string
	SourceTable  string
	// ConsistencyMode is the mode it was built with, as
	// [embedcatchup.Mode] spells it.
	//
	// Recorded for the same reason the source is, and it closes the same
	// question from the other side: only an outbox-mode generation is fed by
	// the outbox, so only one of those counts as a reader of it. Counting every
	// live generation over the source instead, an `immutable` generation over
	// the same table kept the change capture installed forever -- and retiring
	// that generation could not remove it either, because there was no outbox
	// to remove for a mode that never installs one.
	ConsistencyMode string
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
	return !g.MaintainedUntil.IsZero() && !now.After(g.MaintainedUntil)
}

// Retired reports whether the generation was destroyed.
func (g Generation) Retired() bool {
	return !g.RetiredAt.IsZero()
}

// Pointer is which generation a target's queries read.
type Pointer struct {
	// TargetSchema and TargetTable are what the pointer is about.
	//
	// Two generations over same-named tables in different schemas are two
	// pointers, not one: the table name alone made them share a row, so a
	// cutover in one schema moved the other's readers.
	TargetSchema string
	TargetTable  string
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

// CutoverMove is the time-bearing state committed by an atomic pointer move.
// The store owns these timestamps because it samples time only after acquiring
// the lifecycle locks that can delay the operation.
type CutoverMove struct {
	CutOverAt               time.Time
	PreviousMaintainedUntil time.Time
}

// QualifiedName names a target the way a diagnostic should say it.
//
// One function, used by both stores, because a message is a thing an operator
// compares between them: the SQL store rendered a pointer refusal through its
// own SQL quoting and the in-memory store printed the bare table, so the two
// disagreed about which target had refused. The SQL renderer stays separate --
// that one has to quote, and this one has to read.
func QualifiedName(schema, table string) string {
	if trimmed := strings.TrimSpace(schema); trimmed != "" {
		return trimmed + "." + table
	}
	return table
}

// SourceIdentity is the canonical identity of a source relation. Runs created
// before the source lock carried the bare table name; lifecycle operations may
// still recognize that spelling, but new locks always use this qualified,
// collision-safe identity.
func SourceIdentity(schema, table string) string {
	return embeddigest.Of(schema, table)
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
	// RecordVerification records that a verification passed over a generation.
	RecordVerification(ctx context.Context, identity string, at time.Time) error
	// Maintain records how long something will keep a generation current.
	//
	// A zero time clears it, which is what stops a generation being reported as
	// a way back the moment nobody is feeding it.
	//
	// A non-zero time never moves the deadline EARLIER. Maintenance is a
	// renewal, and the flag that drives it is documented as extending the
	// window; written as a plain assignment it made `--maintain-for 1h` after a
	// `--stabilize-for 24h` take twenty-three hours of rollback eligibility
	// away without saying so (stokaro/ptah#2647). Shortening a window is not a
	// thing any caller asks for: clearing it is what zero is for.
	Maintain(ctx context.Context, identity string, until time.Time) error

	// CreateRun records a new run.
	CreateRun(ctx context.Context, run embedrun.Run) error
	// Run reads one back.
	Run(ctx context.Context, id string) (embedrun.Run, error)
	// RunsForGeneration reads every run that built one generation, newest
	// first, and an empty slice when none did.
	//
	// The lookup the run table's generation index was created for -- its own
	// comment says "a run is almost always looked up by the generation it
	// builds" -- and which nothing implemented, so the two terminal phases had
	// no producer: `rollback` and `retire` name a generation and had no run to
	// advance (stokaro/ptah#2649 finding 6).
	//
	// It returns a slice rather than one run because the index is not unique: a
	// generation identity is a digest of the specification, so a second run of
	// the same specification builds the same generation.
	RunsForGeneration(ctx context.Context, identity string) ([]embedrun.Run, error)
	// SaveRun writes a non-terminal run's state, refusing a stale fencing token.
	//
	// The token is what makes this safe rather than the lease: a worker whose
	// lease expired mid-request still holds the token it was given, and the
	// store is the only place that can tell it has been superseded.
	// Terminal membership changes belong to AbandonRun and the atomic generation
	// retirement operation; SaveRun cannot create them.
	SaveRun(ctx context.Context, run embedrun.Run) error
	// ClaimRun takes a non-terminal run for a worker and returns it as the store
	// now holds it, with the fencing token the store assigned. Complete and
	// abandoned runs are refused atomically rather than claimed and checked
	// afterwards.
	//
	// It writes the LEASE ALONE -- owner, expiry, token -- and not the run.
	// Claiming used to be a read-modify-write of the whole row, so a worker
	// that committed a checkpoint between the claimer's read and its write was
	// still unfenced, its transaction landed, and the claim then overwrote the
	// cursor and every progress counter with the snapshot it had read. A live
	// backfill reproduced it: twenty vectors committed, four checkpoints in the
	// event trail, and a run row saying three batches and fifteen rows. On
	// resume the rows behind the rewound cursor were read and paid for again,
	// and nothing reported the rewind (stokaro/ptah#2636).
	//
	// The token is assigned by the STORE rather than computed by the caller,
	// for the second half of the same race: two claimers reading one token both
	// compute the same successor, and the second write passes the `<=` guard.
	//
	// The returned run is what the row holds after the claim, so a caller that
	// goes on to write it back carries whatever landed while it was reading.
	ClaimRun(ctx context.Context, id, worker string, leaseExpires time.Time) (embedrun.Run, int64, error)
	// AbandonRun permanently ends one run without destroying its generation.
	// It fences the current worker and writes the terminal status in the same
	// atomic operation, preserving every checkpoint and progress counter.
	//
	// If the generation is active or maintained, the store permits the
	// abandonment only while another usable live run can keep feeding it.
	// The check and terminal write are serialized with claims, saves, pointer
	// moves, maintenance changes and retirement. The store samples the persisted
	// abandonment time only after acquiring those lifecycle locks.
	AbandonRun(ctx context.Context, id, reason string) (embedrun.Run, error)

	// AppendEvent records what happened.
	AppendEvent(ctx context.Context, event embedrun.Event) error
	// Events reads a run's history in order.
	Events(ctx context.Context, runID string) ([]embedrun.Event, error)

	// Pointer reads which generation a target's queries currently read.
	Pointer(ctx context.Context, targetSchema, targetTable string) (Pointer, error)
	// MovePointer moves it, refusing when it is not where the caller thinks.
	//
	// Compare-and-set rather than a write, because a cutover decided against
	// one pointer and executed against another is exactly the case the decision
	// layer refuses and the store must not reintroduce.
	MovePointer(ctx context.Context, pointer Pointer, expectedActive string) error
	// MovePointerWithMaintenance moves the pointer and, when stabilizeFor is
	// positive, opens or extends the maintenance window over pointer.Previous in
	// the same atomic operation. It samples the cutover time after acquiring the
	// lifecycle locks, derives the deadline from that sample, commits both, and
	// returns the committed values. When pointer.Previous is nonempty, a zero
	// stabilizeFor explicitly clears its existing maintenance deadline in that
	// same operation; zero is not a no-op.
	//
	// Keeping these writes together prevents an abandonment in the gap between
	// cutover and maintenance from leaving queries moved but the requested
	// rollback window absent. A non-zero window therefore refuses before moving
	// when the previous generation has run history but no usable live feeder.
	// requiredRunID is the run whose verified state authorizes the cutover; the
	// same transaction requires it to be non-terminal, fences it, and records
	// PhaseCutOver. That keeps an abandonment from landing after the pointer
	// moved but before the authorizing run recorded the move.
	MovePointerWithMaintenance(
		ctx context.Context, pointer Pointer, expectedActive, requiredRunID string,
		stabilizeFor time.Duration,
	) (CutoverMove, error)
	// MovePointerWithRollback moves the pointer and records PhaseRolledBack on
	// every non-terminal run of expectedActive for which the lifecycle declares
	// that transition. The compare-and-set, run fences and phase changes are one
	// atomic operation, so abandonment cannot leave a successful pointer move
	// followed by a failed run update. The store samples the move time after it
	// holds the lifecycle locks, rechecks both the exact maintenance deadline and
	// eligibilityNotAfter, records that sampled time on the pointer, and returns
	// it. A zero eligibilityNotAfter means the policy has no independent expiry.
	MovePointerWithRollback(
		ctx context.Context, pointer Pointer, expectedActive string,
		expectedMaintainedUntil, eligibilityNotAfter time.Time,
	) (time.Time, error)
}
