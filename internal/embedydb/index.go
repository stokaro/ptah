package embedydb

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/ydbindex"
)

// EnsureRunIndex builds the generation's vector index and records the run's
// index phase.
//
// The phase is checked before the build, because a run that has not caught up
// cannot truthfully record indexing, and building first would leave an index
// beside a refused transition. The build is `ALTER TABLE ... ADD INDEX`, which
// returns once YDB has built the index over every row. The phase is recorded
// after it in a transaction of its own, which raises the fencing token, so a
// worker still holding its pre-build lease cannot write a batch the index was
// not built over.
//
// YDB holds no lock across a schema statement and a transaction, so a run
// abandoned or a generation retired during the build is seen when the phase is
// recorded, and the record is refused then.
func (b *Backend) EnsureRunIndex(
	ctx context.Context, runID string, spec embedgen.Spec,
) (embedbackend.IndexOutcome, error) {
	identity := spec.Identity().Digest
	initial, err := b.store.Run(ctx, runID)
	if err != nil {
		return "", err
	}
	if _, err := embedbackend.IndexedRun(initial, identity, clock()); err != nil {
		return "", err
	}
	outcome, err := b.ensureIndex(ctx, spec)
	if err != nil {
		return "", err
	}
	if _, err := b.store.decide(ctx, oneRun(runID), "record the index phase of run "+runID,
		func(memory *embedstore.Memory) error {
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
		return "", fmt.Errorf("the index operation completed as %q, but run %s did not record its index phase: %w",
			outcome, runID, err)
	}
	return outcome, nil
}

// ensureIndex builds the index the specification declares, if it is not
// there and ready.
//
// An index that exists and is not ready is one whose build has not finished;
// it is dropped and built again, the counterpart of the PostgreSQL index a
// failed concurrent build leaves invalid.
func (b *Backend) ensureIndex(ctx context.Context, spec embedgen.Spec) (embedbackend.IndexOutcome, error) {
	objects, err := TargetObjects(spec)
	if err != nil {
		return "", err
	}
	if !objects.HasIndex {
		return embedbackend.IndexNotDeclared, nil
	}
	if err := b.require(capability.VectorIndexes, "building the generation's "+ydbindex.VectorMethod+
		" index needs it; YDB 25.1 builds one with the EnableVectorIndex feature flag on"); err != nil {
		return "", err
	}
	settings, err := VectorSettings(spec)
	if err != nil {
		return "", err
	}
	described, exists, err := b.describe(ctx, spec.Target.Schema, spec.Target.Table)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", fmt.Errorf("target table %s does not exist; run prepare first",
			tablePath(spec.Target.Schema, spec.Target.Table))
	}
	table := quoteTable(spec.Target.Schema, spec.Target.Table)
	outcome := embedbackend.IndexBuilt
	if index, found := described.Index(objects.Index.Name); found {
		if index.Ready {
			return embedbackend.IndexAlreadyValid, nil
		}
		if _, err := b.db.ExecContext(ctx, "ALTER TABLE "+table+" DROP INDEX "+quote(objects.Index.Name)); err != nil {
			return "", fmt.Errorf("drop the index %s that did not finish building: %w", objects.Index.Name, err)
		}
		outcome = embedbackend.IndexRebuilt
	}
	statement := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s GLOBAL USING %s ON (%s) %s",
		table, quote(objects.Index.Name), ydbindex.VectorMethod, quote(spec.Target.Column),
		ydbindex.VectorClause(settings))
	if _, err := b.db.ExecContext(ctx, statement); err != nil {
		return "", fmt.Errorf("build the index %s: %w", objects.Index.Name, err)
	}
	return outcome, nil
}
