// Package embedengine runs a backfill: it reads the source, canonicalizes what
// it read, asks the provider, and commits the vectors with the checkpoint that
// records them.
//
// Every part it composes already exists and is tested on its own. What this
// package adds is the ORDER, and the order is where a resumable migration is
// won or lost -- a checkpoint written before the vectors it claims produces a
// resumed run that skips them, and nothing about the resumed run looks wrong
// (stokaro/ptah#2068).
package embedengine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedprovider"
	"go.5x5.cz/ptah/internal/embedrun"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Source is where the rows come from.
type Source interface {
	// Scan returns the rows after a cursor, in key order, and the cursor to
	// resume from.
	//
	// Keyset rather than offset: an offset over a table that changes during a
	// scan taking hours silently skips and repeats rows, and neither shows up
	// in a count.
	Scan(ctx context.Context, after []string, limit int) (Page, error)
}

// Page is one keyset page.
type Page struct {
	// Rows are the source rows, in key order.
	Rows []embedgen.Row
	// Versions are the rows' source versions, positionally, and may be empty
	// under a strategy that establishes none.
	Versions []string
	// Cursor is the key to resume after, and is what the next Scan is given.
	Cursor []string
	// Done reports that the scan reached the end of the snapshot. It is
	// separate from an empty page because a filtered scan can answer with no
	// rows and still have more to read.
	Done bool
}

// Target is where the vectors go.
type Target interface {
	// Commit writes a batch's target writes AND the run that checkpoints them,
	// in one transaction, refusing a stale fencing token.
	//
	// One method rather than two, because two is the bug: a caller given
	// separate Write and Checkpoint calls will eventually make them separate
	// transactions, and the resumed run then skips whichever half did not land.
	// The signature is the guarantee, and it is why the engine never saves a
	// checkpoint through the store -- a second write would be a second
	// transaction, which is the thing this interface exists to prevent.
	//
	// It returns an error satisfying errors.Is(err, embedstore.ErrConflict)
	// when another worker has taken the run over. That check belongs here
	// rather than in the engine because it has to happen inside the same
	// transaction: a token read before the write is a token that can go stale
	// before the write lands.
	Commit(ctx context.Context, writes []embedrun.TargetWrite, run embedrun.Run) error
}

// Errors a caller distinguishes.
var (
	// ErrAborted is a run that stopped because its context was cancelled.
	ErrAborted = errors.New("the run was cancelled")
	// ErrFenced is a run another worker took over.
	ErrFenced = errors.New("the run was taken over by another worker")
	// ErrStalled is a source whose cursor does not move.
	//
	// It is a contract violation rather than an unlucky page: a scan that
	// answers from the same position forever makes the engine embed one page
	// of rows until somebody notices the provider bill, and every count in the
	// run keeps rising while the corpus does not grow.
	ErrStalled = errors.New("the source did not advance past the cursor it was given")
)

// Engine drives one run.
type Engine struct {
	// Spec is what is being built.
	Spec embedgen.Spec
	// Source, Provider, Target and Store are what it works through.
	Source   Source
	Provider embedprovider.Provider
	Target   Target
	Store    embedstore.Store
	// Bounds limit what one batch may hold.
	Bounds embedrun.BatchBounds
	// Worker names this process in the lease and in the audit trail.
	Worker string
	// LeaseFor is how long this worker's claim on the run is good for. Zero
	// uses defaultLease.
	//
	// The lease says who SHOULD be working; the fencing token the claim issues
	// says who may still commit. A lease that expires does not stop the holder
	// -- only a later claim does, by moving the token past it.
	LeaseFor time.Duration
	// Now supplies the clock, so a test does not have to wait and a run does
	// not have to guess. Nil uses time.Now.
	Now func() time.Time
}

// now answers the current time.
// defaultLease is how long a claim is good for when the caller names no
// duration.
//
// Long enough that an ordinary batch does not outlive it, short enough that a
// worker which died leaves a lease somebody can see has lapsed. It bounds
// nothing on its own: what stops a lapsed holder committing is a later claim
// moving the fencing token, not the clock.
const defaultLease = 15 * time.Minute

// claim takes the run for this worker and persists the token that fences the
// last holder.
//
// Before any work rather than after it. The fencing token is enforced in the
// store's own WHERE clause, so a worker whose token is behind is refused at the
// moment it commits -- but only if somebody moved the token, and until
// stokaro/ptah#2474 nobody did: `prepare` wrote 1 into the run and every verb
// read it back unchanged. The mechanism was real and the event that exercises
// it did not occur.
//
// The claim is saved before the loop starts, because a claim this process holds
// and the store does not is a claim that fences nobody.
func (e *Engine) claim(ctx context.Context, runID string) (embedrun.Run, int64, error) {
	run, token, err := e.runs().Claim(ctx, runID)
	if err != nil {
		return embedrun.Run{}, 0, err
	}
	// The one place both facts are in hand, and every loop this engine runs
	// passes through it. A backfill handed a specification for a generation the
	// run was not prepared for either resumed a finished run and embedded
	// nothing, or wrote a foreign generation's identity into this run's column
	// (stokaro/ptah#2637).
	if err := run.DescribesGeneration(e.Spec.Identity().Digest); err != nil {
		return embedrun.Run{}, 0, err
	}
	return run, token, nil
}

// runs is the store-only half of this engine, which is also what the pause and
// resume verbs hold. One claim implementation rather than two, because a second
// one that forgot to save would fence nobody and look identical from here.
func (e *Engine) runs() Runs {
	return Runs{Store: e.Store, Worker: e.Worker, LeaseFor: e.LeaseFor}
}

func (e *Engine) now() time.Time {
	if e.Now == nil {
		return time.Now()
	}
	return e.Now()
}

// Backfill runs the scan-embed-commit loop until the source is exhausted.
//
// It returns the run as the store last accepted it, so a caller that stopped
// early can see exactly how far the work got rather than how far this process
// believed it got.
func (e *Engine) Backfill(ctx context.Context, runID string) (embedrun.Run, embedrun.Progress, error) {
	run, token, err := e.claim(ctx, runID)
	if err != nil {
		return embedrun.Run{}, embedrun.Progress{}, err
	}
	// What THIS invocation did, which is not what the run has done. The run's
	// counters are cumulative and a verb printing them as its own work told an
	// operator that a catch-up which processed nothing had processed the
	// backfill's rows -- so the documented completion signal, "0 changed
	// rows", was unreachable on any run whose backfill scanned anything
	// (stokaro/ptah#2645).
	started := run.Progress
	final, err := e.backfillLoop(ctx, runID, run, token)
	return final, progressSince(started, final.Progress), err
}

// backfillLoop is the scan-embed-commit loop itself.
//
// Separated from [Engine.Backfill] so that every way out of it -- an abort, a
// failure, a stall, exhaustion -- passes through one place that can subtract
// what the run had done before from what it has done now. Seven returns, and a
// per-return subtraction would be seven chances to forget one.
func (e *Engine) backfillLoop(
	ctx context.Context, runID string, run embedrun.Run, token int64,
) (embedrun.Run, error) {
	for {
		if err := ctx.Err(); err != nil {
			// Stopping is not failing. The run is durable at its last
			// checkpoint and another process can pick it up, so this returns
			// what is on disk rather than marking the run failed.
			return e.reload(context.WithoutCancel(ctx), runID, errors.Join(ErrAborted, err))
		}
		page, err := e.Source.Scan(ctx, run.Cursor, e.Bounds.MaxRows)
		if err != nil {
			return e.fail(ctx, run, token, "source", err)
		}
		if stalled(run.Cursor, page) {
			// Checked before the page is embedded rather than after, because
			// after is a page of provider spend later and the answer is
			// already knowable: a scan told to start past a key must not hand
			// that key back.
			return e.fail(ctx, run, token, "source", fmt.Errorf("%w: %v", ErrStalled, run.Cursor))
		}
		run, err = e.advancePast(ctx, run, token, page)
		if err != nil {
			return run, err
		}
		if page.Done {
			return run, nil
		}
	}
}

// stalled reports whether a page failed to move past the cursor it was given.
//
// A scan asked to start after a key that answers with that key has not moved,
// and the loop cannot tell that from progress: the rows are real, the batch
// embeds, the counts rise. Only the cursor it was handed says otherwise.
func stalled(after []string, page Page) bool {
	if page.Done {
		return false
	}
	if len(page.Rows) == 0 {
		// A page with no rows has to say where it got to, and it has to be
		// somewhere new. One that says nothing, or says the position it was
		// handed, leaves the next scan asking the same question.
		return len(page.Cursor) == 0 || slices.Equal(page.Cursor, after)
	}
	return slices.ContainsFunc(page.Rows, func(row embedgen.Row) bool {
		return slices.Equal(row.Key, after)
	})
}

// advancePast handles one page, whether or not it has rows in it.
//
// A page with no rows is not the end of the scan: a filter that matches nothing
// for a stretch answers exactly that, and the position it reached still has to
// become durable or the next scan reads the same stretch again.
func (e *Engine) advancePast(
	ctx context.Context, run embedrun.Run, token int64, page Page,
) (embedrun.Run, error) {
	if len(page.Rows) > 0 {
		return e.processPage(ctx, run, token, page)
	}
	if page.Done {
		return run, nil
	}
	// A page that is not done and has no cursor was refused above, so what
	// reaches here always has somewhere to record.
	return e.commitProgress(ctx, run, token, nil, embedrun.BatchOutcome{
		Cursor: page.Cursor, TargetCommitted: true, DeletesCommitted: true,
	})
}

// processPage embeds and commits one page, batch by batch.
func (e *Engine) processPage(
	ctx context.Context, run embedrun.Run, token int64, page Page,
) (embedrun.Run, error) {
	rows, err := e.prepare(page)
	if err != nil {
		return e.fail(ctx, run, token, "canonicalization", err)
	}
	batches, err := embedrun.Assemble(rows, e.Bounds)
	if err != nil {
		return e.fail(ctx, run, token, "batching", err)
	}
	for _, batch := range batches {
		run, err = e.commitBatch(ctx, run, token, batch)
		if err != nil {
			return run, err
		}
	}
	return run, nil
}

// prepare canonicalizes a page's rows and hashes their inputs.
func (e *Engine) prepare(page Page) ([]embedrun.BatchRow, error) {
	rows := make([]embedrun.BatchRow, 0, len(page.Rows))
	for index, row := range page.Rows {
		input, err := e.Spec.Canonicalize(row)
		if err != nil {
			return nil, fmt.Errorf("canonicalize row %v: %w", row.Key, err)
		}
		rows = append(rows, embedrun.BatchRow{
			Key:        row.Key,
			Input:      input.Text,
			Version:    versionAt(page.Versions, index),
			InputHash:  e.Spec.SourceInputHash(input),
			Skipped:    input.Skipped,
			SkipReason: input.SkipReason,
		})
	}
	return rows, nil
}

// versionAt reads a row's version, tolerating a strategy that establishes none.
func versionAt(versions []string, index int) string {
	if index >= len(versions) {
		return ""
	}
	return versions[index]
}

// commitBatch embeds one batch and commits it with its checkpoint.
//
// The order is the whole point of the package: embed, then write the vectors
// and the checkpoint together, then record the event. A checkpoint that moved
// first would leave a resumed run past rows nothing embedded.
func (e *Engine) commitBatch(
	ctx context.Context, run embedrun.Run, token int64, batch embedrun.Batch,
) (embedrun.Run, error) {
	writes, outcome, err := e.embed(ctx, batch)
	if err != nil {
		return e.fail(ctx, run, token, "provider", err)
	}
	return e.commitProgress(ctx, run, token, writes, outcome)
}

// commitProgress writes what a batch produced and the checkpoint that records
// it, in that one transaction, and then says so in the trail.
func (e *Engine) commitProgress(
	ctx context.Context,
	run embedrun.Run,
	token int64,
	writes []embedrun.TargetWrite,
	outcome embedrun.BatchOutcome,
) (embedrun.Run, error) {
	if err := run.Checkpoint(token, outcome); err != nil {
		return run, fmt.Errorf("checkpoint: %w", err)
	}
	run.UpdatedAt = e.now()

	if err := e.Target.Commit(ctx, writes, run); err != nil {
		if errors.Is(err, embedstore.ErrConflict) {
			// Another worker holds the run. This one's transaction did not
			// land, and what it must not do is keep going.
			return e.reload(ctx, run.ID, ErrFenced)
		}
		// The vectors and the checkpoint were one transaction, so neither
		// landed. The in-memory run has already advanced past them, which is
		// why the store's copy is what gets reloaded rather than trusted here.
		return e.fail(ctx, run, token, "target", err)
	}
	// The transaction this records has already committed, so the event is
	// written on a context the caller's cancellation cannot reach. An interrupt
	// arriving here otherwise printed `append event: context canceled` about a
	// checkpoint that landed, and left the trail missing the one entry that
	// says it did.
	if err := e.Store.AppendEvent(context.WithoutCancel(ctx),
		embedrun.NewEvent(&run, embedrun.EventCheckpoint, e.Worker, "")); err != nil {
		return run, fmt.Errorf("append event: %w", err)
	}
	return run, nil
}

// embed asks the provider for one batch and turns the answer into writes.
func (e *Engine) embed(
	ctx context.Context, batch embedrun.Batch,
) ([]embedrun.TargetWrite, embedrun.BatchOutcome, error) {
	inputs, embedded := embeddableInputs(batch)
	var result embedprovider.Result
	if len(inputs) > 0 {
		answered, err := e.Provider.Embed(ctx, inputs)
		if err != nil {
			return nil, embedrun.BatchOutcome{}, err
		}
		if err := embedprovider.ValidateResult(answered, len(inputs), e.Spec.Model.ReportedDimension); err != nil {
			return nil, embedrun.BatchOutcome{}, err
		}
		result = answered
	}

	writes := make([]embedrun.TargetWrite, 0, len(batch.Rows))
	generation := e.Spec.Identity().Digest
	position := 0
	for _, row := range batch.Rows {
		write := embedrun.TargetWrite{
			Key: row.Key, Generation: generation, InputHash: row.InputHash, Version: row.Version,
		}
		if row.Skipped {
			write.Kind = embedrun.WriteSkip
			write.SkipReason = row.SkipReason
			writes = append(writes, write)
			continue
		}
		write.Kind = embedrun.WriteUpsert
		write.Vector = result.Vectors[position]
		position++
		writes = append(writes, write)
	}

	outcome := embedrun.BatchOutcome{
		Cursor:           batch.Rows[len(batch.Rows)-1].Key,
		RowsScanned:      int64(len(batch.Rows)),
		RowsEmbedded:     int64(embedded),
		RowsSkipped:      int64(len(batch.Rows) - embedded),
		PromptTokens:     int64(result.Usage.PromptTokens),
		TotalTokens:      int64(result.Usage.TotalTokens),
		TargetCommitted:  true,
		DeletesCommitted: true,
	}
	return writes, outcome, nil
}

// embeddableInputs collects the texts a provider is actually asked about.
//
// A skipped row is not sent. It still travels in the batch and still produces a
// write, because verification reads a skip as a deliberate gap rather than as
// missing coverage -- dropping it here would make it indistinguishable from a
// row nobody got to.
func embeddableInputs(batch embedrun.Batch) ([]string, int) {
	inputs := make([]string, 0, len(batch.Rows))
	for _, row := range batch.Rows {
		if row.Skipped {
			continue
		}
		inputs = append(inputs, row.Input)
	}
	return inputs, len(inputs)
}

// fail records why a run stopped and returns what the store holds.
func (e *Engine) fail(
	ctx context.Context, run embedrun.Run, token int64, class string, cause error,
) (embedrun.Run, error) {
	// An interrupt is not a failure of whichever subsystem happened to be
	// mid-request when it arrived. Ctrl-C during a provider call reported
	// `embedding endpoint unreachable`, during a target write `write [229]`,
	// during a store write `save run`: twelve interrupts, six different causes,
	// none of them the cause. The loop head already answers this correctly --
	// stopping is not failing, the run is durable at its last checkpoint -- and
	// this is the same answer for an interrupt that lands between two of its
	// passes (stokaro/ptah#2649).
	//
	// The parent context is the discriminator rather than `errors.Is(cause,
	// context.Canceled)`, because `--provider-timeout` is an
	// `http.Client.Timeout` and surfaces as a deadline on the request alone. A
	// request that timed out is a provider problem and must keep saying so.
	if interrupted := ctx.Err(); interrupted != nil {
		return e.reload(context.WithoutCancel(ctx), run.ID, errors.Join(ErrAborted, interrupted))
	}

	// Everything below records what happened, on a context the caller's
	// cancellation cannot reach.
	//
	// The check above already turns a cancelled context into an abort, so what
	// this guards is the narrower case: a DEADLINE that expires between here
	// and the last of the three store calls. Bookkeeping abandoned that way
	// leaves the run `running` with no failure class -- the state this whole
	// function exists to prevent -- and the caller is handed a second error
	// line about a reload nobody asked for.
	//
	// No test covers it, and none can through `embedstore.Memory`, which
	// ignores the context it is given. Reaching it needs a store that honors
	// one and a deadline landing inside a three-call window, which is a race
	// rather than a fixture. Said here rather than left to look covered.
	recording := context.WithoutCancel(ctx)

	// The run in hand may have advanced past what was committed, so the failure
	// is recorded against a fresh copy: marking the in-memory one failed and
	// saving it would persist a cursor whose work never landed.
	stored, err := e.Store.Run(recording, run.ID)
	if err != nil {
		return run, errors.Join(cause, fmt.Errorf("reload run %s: %w", run.ID, err))
	}
	if err := stored.Fail(token, class, cause.Error()); err != nil {
		return stored, errors.Join(cause, err)
	}
	stored.UpdatedAt = e.now()
	if err := e.Store.SaveRun(recording, stored); err != nil {
		return stored, errors.Join(cause, err)
	}
	if err := e.Store.AppendEvent(recording,
		embedrun.NewEvent(&stored, embedrun.EventFailed, e.Worker, cause.Error())); err != nil {
		return stored, errors.Join(cause, err)
	}
	return stored, fmt.Errorf("%s: %w", class, cause)
}

// reload returns what the store holds, with the reason the loop stopped.
func (e *Engine) reload(ctx context.Context, runID string, cause error) (embedrun.Run, error) {
	stored, err := e.Store.Run(ctx, runID)
	if err != nil {
		return embedrun.Run{}, errors.Join(cause, err)
	}
	return stored, cause
}

// progressSince is what happened between two readings of a run's counters.
//
// Subtraction rather than accumulation, and it is exact: the reading it starts
// from is the row the claim returned, and no other worker can commit against a
// run this one holds the token for. A worker that took the run over fences this
// one, and the loop stops.
//
// RetryCount is deliberately absent. It is per-batch rather than cumulative --
// a batch that finally committed says nothing about the next one -- so a
// difference between two readings of it is not a count of anything.
func progressSince(started, now embedrun.Progress) embedrun.Progress {
	return embedrun.Progress{
		RowsScanned:          now.RowsScanned - started.RowsScanned,
		RowsEmbedded:         now.RowsEmbedded - started.RowsEmbedded,
		RowsSkipped:          now.RowsSkipped - started.RowsSkipped,
		RowsDeleted:          now.RowsDeleted - started.RowsDeleted,
		BatchesCommitted:     now.BatchesCommitted - started.BatchesCommitted,
		ProviderPromptTokens: now.ProviderPromptTokens - started.ProviderPromptTokens,
		ProviderTotalTokens:  now.ProviderTotalTokens - started.ProviderTotalTokens,
	}
}
