package embedengine

import (
	"context"
	"fmt"
	"time"

	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// Runs is what can be done to a run without a source, a provider or a target:
// taking it for a worker, pausing it, resuming it, and ending it without
// destroying its generation.
//
// It is separate from [Engine] because of what building an Engine costs. An
// Engine resolves the provider, and resolving the provider reads the credential
// the specification names, so an operator pausing a rate-limited backfill would
// have had to supply a token to stop one from being used. What pause needs is
// the store and a name to claim under.
type Runs struct {
	// Store is where the run lives.
	Store embedstore.Store
	// Worker names this process in the lease and in the audit trail.
	Worker string
	// LeaseFor is how long a claim is good for. Zero uses defaultLease.
	LeaseFor time.Duration
}

// Claim takes the run for this worker and persists the token that fences the
// last holder.
//
// The claim is saved before the caller does anything with the run, because a
// claim this process holds and the store does not is a claim that fences
// nobody.
func (r Runs) Claim(ctx context.Context, runID string) (embedrun.Run, int64, error) {
	lease := r.LeaseFor
	if lease <= 0 {
		lease = defaultLease
	}
	// One statement in the store rather than a read here, a change in memory
	// and a write back. The read-modify-write wrote the whole row, so a
	// checkpoint the working worker committed between the read and the write
	// landed and was then erased by the claim (stokaro/ptah#2636).
	run, token, err := r.Store.ClaimRun(ctx, runID, r.Worker, time.Now().UTC().Add(lease))
	if err != nil {
		// The store names the operation it failed at, so naming it again here
		// printed the same clause twice: `claim run r: claim run r: not found:
		// run r`. An operator searching the documentation for what they saw
		// found nothing, because nobody writes a sentence that stutters
		// (stokaro/ptah#2648 finding 3).
		return embedrun.Run{}, 0, err
	}
	return run, token, nil
}

// Pause stops a run at the boundary its last checkpoint reached, and fences
// whoever was working on it.
//
// The claim is what makes a pause take effect rather than take note. A pause
// written without one lands in a row the running worker overwrites at its next
// checkpoint, so the run reads paused for a few seconds and then reads running
// again while the provider bill goes on. Claiming moves the fencing token past
// the token that worker holds, and the store refuses its next commit.
//
// So an operator pausing a live backfill sees that backfill fail, which is the
// intended outcome and not a defect: the run is durable at its last checkpoint,
// and a resume picks it up from there.
func (r Runs) Pause(ctx context.Context, runID, reason string) (embedrun.Run, error) {
	run, token, err := r.Claim(ctx, runID)
	if err != nil {
		return embedrun.Run{}, err
	}
	if err := run.Pause(token, reason); err != nil {
		return embedrun.Run{}, fmt.Errorf("pause run %s: %w", runID, err)
	}
	if err := r.Store.SaveRun(ctx, run); err != nil {
		return embedrun.Run{}, fmt.Errorf("pause run %s: %w", runID, err)
	}
	return run, nil
}

// Resume returns a paused run to running, clearing the reason it stopped for.
//
// It claims for the same reason Pause does, and for one more: the worker that
// was fenced when the run was paused is not necessarily gone. A resume that
// left the token where the pause put it would return the run to running with
// the old holder still able to commit into it, which is the state the fence
// exists to prevent.
//
// Nothing starts working here. Resume makes the run workable again; the verb
// that does the work claims it in turn.
func (r Runs) Resume(ctx context.Context, runID string) (embedrun.Run, error) {
	run, token, err := r.Claim(ctx, runID)
	if err != nil {
		return embedrun.Run{}, err
	}
	if err := run.Resume(token); err != nil {
		return embedrun.Run{}, fmt.Errorf("resume run %s: %w", runID, err)
	}
	if err := r.Store.SaveRun(ctx, run); err != nil {
		return embedrun.Run{}, fmt.Errorf("resume run %s: %w", runID, err)
	}
	return run, nil
}

// Abandon permanently ends one run while preserving its generation.
//
// It is deliberately distinct from retirement. Retirement destroys vectors;
// abandonment keeps them and only releases this run's claim on shared outbox
// history. An active generation cannot stop receiving source changes, and a
// maintained generation is an explicit promise to keep doing so, so the store
// refuses abandonment when it would remove their last usable live feeder.
//
// The store increments the fencing token and writes the terminal status in one
// atomic operation. That keeps every checkpoint committed before the fence and
// prevents a worker from overwriting the abandonment afterwards.
func (r Runs) Abandon(ctx context.Context, runID, reason string) (embedrun.Run, error) {
	if reason == "" {
		return embedrun.Run{}, fmt.Errorf(
			"abandon run %s: %w: an abandonment without a reason cannot be acted on",
			runID, embedrun.ErrCheckpoint)
	}

	run, err := r.Store.AbandonRun(ctx, runID, reason)
	if err != nil {
		return embedrun.Run{}, err
	}
	return run, nil
}
