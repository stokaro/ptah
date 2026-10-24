package embedrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// PrepareRun creates the generation's storage, registers the generation and
// creates the run.
//
// Preparing storage and recording the run are two calls: the storage step is
// idempotent and the records are written once, so a prepare stopped between
// them is finished by running it again. A run that already exists is an
// idempotent retry only when its record and the storage both show the first
// prepare finished.
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
	record, err := specRecord(spec)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	source, err := b.ResolveRelation(ctx, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if run.Source != embedstore.SourceIdentity(source.Schema, source.Table) {
		return embedbackend.PrepareResult{}, fmt.Errorf("%w: run %s records source %s, specification uses %s",
			embedstore.ErrConflict, run.ID, run.Source, source)
	}

	existing, err := b.store.Run(ctx, run.ID)
	switch {
	case err == nil:
		return b.preparedRetry(ctx, record, generation, run, existing)
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
	if err := b.prepareTarget(ctx, record, protocol.PrepareEmbeddingTargetRequest_CREATE); err != nil {
		return embedbackend.PrepareResult{}, err
	}

	// The immutable mode records no boundary, so the storage being in place is
	// the whole of what prepare captures; the run is created at
	// boundary_captured in the same compare-and-write that registers the
	// generation, so no worker can claim it before both are durable.
	run.Phase = embedrun.PhaseBoundaryCaptured
	var raced bool
	after, err := b.store.decide(ctx, everything(), "prepare run "+run.ID, func(memory *embedstore.Memory) error {
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
		// Another prepare of the same run recorded it first; this one is its
		// retry, and answers as a retry does.
		return b.preparedRetry(ctx, record, generation, run, created)
	}
	return embedbackend.PrepareResult{Run: created, Created: true}, nil
}

// prepareTarget creates or verifies a generation's storage.
func (b *Backend) prepareTarget(ctx context.Context, record *protocol.EmbeddingSpec, mode protocol.PrepareEmbeddingTargetRequest_Mode) error {
	result, err := b.session.Store.PrepareEmbeddingTarget(ctx, &protocol.PrepareEmbeddingTargetRequest{
		SessionId: b.session.ID, RequestId: requestID(), Spec: record, Mode: mode,
	})
	return writeError("prepare the storage of generation "+record.GetGeneration()+" on "+b.engineName(), result, err)
}

// preparedRetry decides whether an existing run is a finished prepare of the
// one being prepared, and answers it unchanged when it is.
func (b *Backend) preparedRetry(
	ctx context.Context, record *protocol.EmbeddingSpec, generation embedstore.Generation, run, existing embedrun.Run,
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
	if err := b.prepareTarget(ctx, record, protocol.PrepareEmbeddingTargetRequest_VERIFY); err != nil {
		return embedbackend.PrepareResult{}, fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	}
	return embedbackend.PrepareResult{Run: existing}, nil
}

// indexOutcomes are the service's answers in the host's words.
var indexOutcomes = map[protocol.EmbeddingWriteResult_IndexOutcome]embedbackend.IndexOutcome{
	protocol.EmbeddingWriteResult_NOT_DECLARED:  embedbackend.IndexNotDeclared,
	protocol.EmbeddingWriteResult_ALREADY_VALID: embedbackend.IndexAlreadyValid,
	protocol.EmbeddingWriteResult_BUILT:         embedbackend.IndexBuilt,
	protocol.EmbeddingWriteResult_REBUILT:       embedbackend.IndexRebuilt,
}

// EnsureRunIndex builds the generation's index and records the run's index
// phase.
//
// The phase is checked before the build, because a run that has not caught up
// cannot truthfully record indexing, and building first would leave an index
// beside a refused transition. The phase is recorded after the build in a
// compare-and-write of its own that raises the fencing token, so a worker still
// holding its pre-build lease cannot write a batch the index was not built
// over. A run abandoned or a generation retired during the build is seen when
// the phase is recorded, and the record is refused then.
func (b *Backend) EnsureRunIndex(ctx context.Context, runID string, spec embedgen.Spec) (embedbackend.IndexOutcome, error) {
	identity := spec.Identity().Digest
	initial, err := b.store.Run(ctx, runID)
	if err != nil {
		return "", err
	}
	if _, err := embedbackend.IndexedRun(initial, identity, clock()); err != nil {
		return "", err
	}
	record, err := specRecord(spec)
	if err != nil {
		return "", err
	}
	result, err := b.session.Store.BuildEmbeddingIndex(ctx, &protocol.BuildEmbeddingIndexRequest{SessionId: b.session.ID, RequestId: requestID(), Spec: record})
	if err := writeError("build the index of generation "+spec.Identity().Short()+" on "+b.engineName(), result, err); err != nil {
		return "", err
	}
	outcome := indexOutcomes[result.GetIndexOutcome()]
	if _, err := b.store.decide(ctx, oneRun(runID), "record the index phase of run "+runID, func(memory *embedstore.Memory) error {
		stored, err := memory.Run(ctx, runID)
		if err != nil {
			return err
		}
		indexed, err := embedbackend.IndexedRun(stored, identity, clock())
		if err != nil {
			return err
		}
		return memory.SaveRun(ctx, indexed)
	}); err != nil {
		return "", fmt.Errorf("the index operation completed as %q, but run %s did not record its index phase: %w", outcome, runID, err)
	}
	return outcome, nil
}
