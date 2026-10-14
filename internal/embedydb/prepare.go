package embedydb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// CheckMode refuses a consistency mode this server cannot provide.
//
// The outbox mode captures each source change with a row trigger, in the
// transaction that makes the change, so its event commits or rolls back with
// it. YDB has no triggers, and nothing else here records a change in the
// writer's own transaction, so `immutable` is the mode a YDB generation runs
// under.
func (b *Backend) CheckMode(mode embedcatchup.Mode) error {
	if mode != embedcatchup.ModeOutbox {
		return nil
	}
	return b.require(capability.Triggers, "the outbox consistency mode records each source change with a "+
		"row trigger in the transaction that makes it; use consistency mode immutable, which is the mode a "+
		"YDB generation supports")
}

// Outbox refuses: no YDB line can capture changes the way the outbox mode
// requires. See [Backend.CheckMode].
func (b *Backend) Outbox(context.Context, embedgen.Spec) (embedbackend.Outbox, error) {
	if err := b.CheckMode(embedcatchup.ModeOutbox); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: the outbox", ErrUnsupported)
}

// PrepareRun creates the generation's storage, registers the generation and
// creates the run.
//
// YDB runs a schema statement outside any transaction, so the storage comes
// first and the records second, in one serializable transaction. A prepare
// stopped between them is completed by running it again: the storage step is
// idempotent, and the records are written only once. A run that already
// exists is an idempotent retry only when its recorded state and the storage
// both show the first prepare finished.
func (b *Backend) PrepareRun(
	ctx context.Context, spec embedgen.Spec, generation embedstore.Generation,
	run embedrun.Run, mode embedcatchup.Mode,
) (embedbackend.PrepareResult, error) {
	if err := embedbackend.ValidatePrepare(spec, generation, run, mode); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if err := b.CheckMode(mode); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if _, err := TargetObjects(spec); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	source, err := b.ResolveRelation(ctx, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if run.Source != embedstore.SourceIdentity(source.Schema, source.Table) {
		return embedbackend.PrepareResult{}, fmt.Errorf(
			"%w: run %s records source %s, specification uses %s",
			embedstore.ErrConflict, run.ID, run.Source, source)
	}

	existing, err := b.store.Run(ctx, run.ID)
	switch {
	case err == nil:
		return b.preparedRetry(ctx, spec, generation, run, existing)
	case !errors.Is(err, embedstore.ErrNotFound):
		return embedbackend.PrepareResult{}, err
	}
	registered, err := b.store.Generation(ctx, generation.Identity)
	switch {
	case err == nil && registered.Retired():
		return embedbackend.PrepareResult{}, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, registered.Identity, registered.RetiredAt.UTC().Format(time.RFC3339))
	case err == nil:
		if err := embedbackend.ValidatePreparedGeneration(registered, generation); err != nil {
			return embedbackend.PrepareResult{}, err
		}
	case !errors.Is(err, embedstore.ErrNotFound):
		return embedbackend.PrepareResult{}, err
	}
	if _, err := b.sourceKey(ctx, spec); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if err := b.ensureTarget(ctx, spec); err != nil {
		return embedbackend.PrepareResult{}, err
	}

	// The immutable mode records no boundary, so the storage being in place
	// is the whole of what prepare captures; the run is created at
	// boundary_captured in the same transaction that registers the
	// generation, so no worker can claim it before both are durable.
	run.Phase = embedrun.PhaseBoundaryCaptured
	var raced bool
	after, err := b.store.decide(ctx, everything, "prepare run "+run.ID,
		func(memory *embedstore.Memory) error {
			raced = false
			if _, err := memory.Run(ctx, run.ID); err == nil {
				raced = true
				return nil
			}
			if _, err := memory.RegisterGeneration(ctx, generation); err != nil {
				return err
			}
			run.UpdatedAt = clock()
			return memory.CreateRun(ctx, run)
		})
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	created, err := runIn(after, run.ID)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if raced {
		// Another prepare of the same run committed first; this one is its
		// retry, and answers as a retry does.
		return b.preparedRetry(ctx, spec, generation, run, created)
	}
	return embedbackend.PrepareResult{Run: created, Created: true}, nil
}

// preparedRetry decides whether an existing run is a finished prepare of the
// one being prepared, and answers it unchanged when it is.
func (b *Backend) preparedRetry(
	ctx context.Context, spec embedgen.Spec, generation embedstore.Generation,
	run, existing embedrun.Run,
) (embedbackend.PrepareResult, error) {
	if err := embedbackend.ValidatePreparedRun(generation, existing); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	registered, err := b.store.Generation(ctx, generation.Identity)
	if err != nil {
		return embedbackend.PrepareResult{}, fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	}
	if err := embedbackend.ValidatePreparedRegistration(generation, registered, run, existing); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if err := b.requirePreparedTarget(ctx, spec); err != nil {
		return embedbackend.PrepareResult{}, fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	}
	return embedbackend.PrepareResult{Run: existing}, nil
}
