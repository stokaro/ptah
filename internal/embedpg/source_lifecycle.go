package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// PrepareRun creates the target, installs change capture, records its boundary,
// registers the generation, and creates the run behind one source-and-generation
// lifecycle lock.
//
// The operations need several transactions: the outbox filter probe is always
// rolled back, while target and outbox DDL must survive it. Session advisory
// locks bridge those boundaries on the same pinned connection. Using a lock-only
// transaction from one pool connection and running the action through the pool
// deadlocks when MaxOpenConns is one and can hold an old snapshot while DDL waits
// for it. Keeping the work on this connection also means prune cannot delete
// below a floor until the new run and its boundary are visible.
func (s *Store) PrepareRun(
	ctx context.Context,
	spec embedgen.Spec,
	generation embedstore.Generation,
	run embedrun.Run,
	mode embedcatchup.Mode,
) (embedbackend.PrepareResult, error) {
	identity := spec.Identity().Digest
	if err := embedbackend.ValidatePrepare(spec, generation, run, mode); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	// Resolved before it is compared, and against this connection: the run's
	// digest is taken over the physical relation, so comparing it to a digest
	// of the authored spelling refuses a run this same call just created from
	// a specification whose schema was omitted (stokaro/ptah#2806).
	source, _, err := ResolveRelation(ctx, s.db, spec.Source.Schema, spec.Source.Table)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if run.Source != SourceIdentity(source.Schema, source.Table) {
		return embedbackend.PrepareResult{}, fmt.Errorf(
			"%w: run %s records source %s, specification uses %s",
			embedstore.ErrConflict, run.ID, run.Source, source)
	}

	var result embedbackend.PrepareResult
	scopes := []lifecycleSessionScope{
		{kind: "source", identity: run.Source},
		{kind: "generation", identity: identity},
	}
	err = s.withLifecycleSessionLocks(ctx, scopes, func(conn *sql.Conn) error {
		prepared, err := s.prepareRunLocked(
			ctx, conn, spec, generation, run, mode, identity)
		result = prepared
		return err
	})
	return result, err
}

// prepareOutboxForSource builds the outbox this run will feed, and the live
// contracts already installed on its source, or nothing at all when the run
// records no changes.
//
// Separate from prepareRunLocked because it is the one part of preparation
// that is about the SOURCE rather than about the run: it resolves the physical
// relation, names the outbox after it, and refuses an event format the source's
// existing readers cannot share.
func prepareOutboxForSource(
	ctx context.Context, conn *sql.Conn, spec embedgen.Spec, mode embedcatchup.Mode,
) (*Outbox, []embedgen.Spec, error) {
	if mode != embedcatchup.ModeOutbox {
		return nil, nil, nil
	}
	// The outbox is named after the physical relation, so the specification is
	// resolved against this connection first: an omitted schema means whatever
	// the session's search_path says it means (stokaro/ptah#2806).
	physical, err := WithResolvedRelations(ctx, conn, spec)
	if err != nil {
		return nil, nil, err
	}
	outbox, err := NewOutbox(conn, physical)
	if err != nil {
		return nil, nil, err
	}
	contracts, err := liveOutboxContracts(ctx, conn, physical)
	if err != nil {
		return nil, nil, err
	}
	// Contract incompatibility is knowable without DDL. Refuse it before
	// creating target columns or registering a generation that cannot be fed
	// by the source's existing event format.
	if err := outbox.validateSharedSpecs(contracts); err != nil {
		return nil, nil, err
	}
	return outbox, contracts, nil
}

func (s *Store) prepareRunLocked(
	ctx context.Context,
	conn *sql.Conn,
	spec embedgen.Spec,
	generation embedstore.Generation,
	run embedrun.Run,
	mode embedcatchup.Mode,
	identity string,
) (embedbackend.PrepareResult, error) {
	existing, err := scanRun(conn.QueryRowContext(ctx, selectRunSQL, run.ID), run.ID)
	switch {
	case err == nil:
		if err := validatePreparedRunRetry(
			ctx, conn, spec, generation, run, existing, mode); err != nil {
			return embedbackend.PrepareResult{}, err
		}
		return embedbackend.PrepareResult{Run: existing}, nil
	case !errors.Is(err, embedstore.ErrNotFound):
		return embedbackend.PrepareResult{}, err
	}

	registered, err := readGeneration(ctx, conn, identity)
	switch {
	case err == nil && registered.Retired():
		return embedbackend.PrepareResult{}, fmt.Errorf("%w: generation %s was retired at %s",
			embedstore.ErrRetired, identity,
			registered.RetiredAt.UTC().Format(time.RFC3339))
	case err == nil:
		if err := embedbackend.ValidatePreparedGeneration(registered, generation); err != nil {
			return embedbackend.PrepareResult{}, err
		}
	case errors.Is(err, embedstore.ErrNotFound):
	default:
		return embedbackend.PrepareResult{}, err
	}

	outbox, contracts, err := prepareOutboxForSource(ctx, conn, spec, mode)
	if err != nil {
		return embedbackend.PrepareResult{}, err
	}

	if err := EnsureTarget(ctx, conn, spec); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	if _, err := registerGeneration(ctx, conn, generation); err != nil {
		return embedbackend.PrepareResult{}, err
	}

	if outbox != nil {
		if len(contracts) == 1 {
			err = outbox.InstallForIsolatedSource(ctx)
		} else {
			err = outbox.installForSpecs(ctx, contracts)
		}
		if err != nil {
			return embedbackend.PrepareResult{}, err
		}
		boundary, err := outbox.Horizon(ctx)
		if err != nil {
			return embedbackend.PrepareResult{}, err
		}
		run.SnapshotWatermark = strconv.FormatUint(boundary, 10)
	}
	// Target preparation and boundary capture have both completed. Recording
	// the resulting phase in the INSERT avoids exposing a prepared row between
	// two commits, where another worker could claim it before the boundary is
	// durable.
	run.Phase = embedrun.PhaseBoundaryCaptured
	run.UpdatedAt = time.Now().UTC()
	if err := createRunRecord(ctx, conn, run); err != nil {
		return embedbackend.PrepareResult{}, err
	}
	return createdPrepareResult(ctx, conn, run.ID)
}

func createdPrepareResult(
	ctx context.Context, conn *sql.Conn, runID string,
) (embedbackend.PrepareResult, error) {
	durable, err := scanRun(conn.QueryRowContext(ctx, selectRunSQL, runID), runID)
	if err != nil {
		return embedbackend.PrepareResult{}, fmt.Errorf("read prepared run %s: %w", runID, err)
	}
	return embedbackend.PrepareResult{Run: durable, Created: true}, nil
}

// validatePreparedRunRetry distinguishes a completed prepare from a row that
// merely happens to use the requested run identifier.
//
// CreateRun deliberately accepts an unpositioned prepared row before a
// generation is registered. Treating that row as an idempotent PrepareRun
// result skipped every operation PrepareRun owns: no target columns, outbox,
// generation registry, or durable boundary existed, yet the CLI returned
// success. A retry is successful only when both the durable state and the
// PostgreSQL objects prove the original prepare reached its commit point.
func validatePreparedRunRetry(
	ctx context.Context,
	conn *sql.Conn,
	spec embedgen.Spec,
	wantGeneration embedstore.Generation,
	wantRun, existing embedrun.Run,
	mode embedcatchup.Mode,
) error {
	if err := embedbackend.ValidatePreparedRun(wantGeneration, existing); err != nil {
		return err
	}
	registered, err := readGeneration(ctx, conn, wantGeneration.Identity)
	if err != nil {
		return fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	}
	if err := embedbackend.ValidatePreparedRegistration(
		wantGeneration, registered, wantRun, existing); err != nil {
		return err
	}

	if err := requirePreparedTarget(ctx, conn, spec); err != nil {
		return fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	}
	if mode != embedcatchup.ModeOutbox {
		return nil
	}
	if _, ok, err := embedcatchup.ResumeFrom(
		existing.CatchUpWatermark, existing.SnapshotWatermark); err != nil {
		return fmt.Errorf("validate prepared run %s: %w", existing.ID, err)
	} else if !ok {
		return fmt.Errorf(
			"prepare run %s: %w: outbox mode has no durable resume position",
			existing.ID, embedrun.ErrCheckpoint)
	}
	physical, err := WithResolvedRelations(ctx, conn, spec)
	if err != nil {
		return err
	}
	outbox, err := NewOutbox(conn, physical)
	if err != nil {
		return err
	}
	installed, err := outbox.Installed(ctx)
	if err != nil {
		return err
	}
	if !installed {
		return fmt.Errorf(
			"%w: run %s records a prepared outbox generation, but its source triggers "+
				"are absent or disabled for ordinary writes",
			embedstore.ErrConflict, existing.ID)
	}
	var tableExists bool
	if err := conn.QueryRowContext(ctx,
		`SELECT to_regclass($1) IS NOT NULL`, quoteIdentifier(outbox.TableName())).Scan(
		&tableExists); err != nil {
		return fmt.Errorf("read outbox table %s: %w", outbox.TableName(), err)
	}
	if !tableExists {
		return fmt.Errorf(
			"%w: run %s records a prepared outbox generation, but table %s is absent",
			embedstore.ErrConflict, existing.ID, outbox.TableName())
	}
	return nil
}

func requirePreparedTarget(
	ctx context.Context, conn *sql.Conn, spec embedgen.Spec,
) error {
	target := qualify(spec.Target.Schema, spec.Target.Table)
	columns := []string{spec.Target.Column}
	for _, suffix := range MetadataSuffixes() {
		columns = append(columns, spec.Target.Column+suffix)
	}
	for _, column := range columns {
		const query = `SELECT EXISTS (
			SELECT 1 FROM pg_attribute
			WHERE attrelid = to_regclass($1) AND attname = $2
			  AND attnum > 0 AND NOT attisdropped)`
		var exists bool
		if err := conn.QueryRowContext(ctx, query, target, column).Scan(&exists); err != nil {
			return fmt.Errorf("read target column %s.%s: %w", target, column, err)
		}
		if !exists {
			return fmt.Errorf("%w: prepared target column %s.%s is absent",
				embedstore.ErrConflict, target, column)
		}
	}
	return nil
}

// liveOutboxContracts reconstructs every nonterminal generation sharing the
// source. One trigger serves them all, so reinstalling it from only the newest
// specification would make an older generation stop seeing changes to fields
// only it reads.
func liveOutboxContracts(
	ctx context.Context, conn *sql.Conn, current embedgen.Spec,
) ([]embedgen.Spec, error) {
	sourceIdentity := SourceIdentity(current.Source.Schema, current.Source.Table)
	const orphan = `SELECT r.id
		FROM ` + embedstore.RunTable + ` r
		LEFT JOIN ` + embedstore.GenerationTable + ` g ON g.identity = r.generation_identity
		WHERE (r.source = $1 OR r.source = $2)
		  AND r.status NOT IN ($3, $4)
		  AND (NULLIF(r.catch_up_watermark, '') IS NOT NULL
		       OR NULLIF(r.snapshot_watermark, '') IS NOT NULL)
		  AND g.identity IS NULL
		ORDER BY r.id LIMIT 1`
	var orphanRun string
	err := conn.QueryRowContext(ctx, orphan,
		sourceIdentity, current.Source.Table,
		string(embedrun.StatusComplete), string(embedrun.StatusAbandoned)).Scan(&orphanRun)
	if err == nil {
		return nil, fmt.Errorf(
			"install outbox for %s: live run %s has no generation registry row, so its "+
				"source contract cannot be preserved",
			embedstore.QualifiedName(current.Source.Schema, current.Source.Table), orphanRun)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("read orphan outbox contracts for %s: %w",
			embedstore.QualifiedName(current.Source.Schema, current.Source.Table), err)
	}

	const query = `SELECT g.identity, g.spec_digest, g.spec_document
		FROM ` + embedstore.GenerationTable + ` g
		WHERE g.source_schema = $1 AND g.source_table = $2
		  AND g.consistency_mode = $3 AND g.retired_at IS NULL
		  AND EXISTS (
		        SELECT 1 FROM ` + embedstore.RunTable + ` r
		        WHERE r.generation_identity = g.identity
		          AND r.status NOT IN ($4, $5))
		ORDER BY g.identity`
	rows, err := conn.QueryContext(ctx, query,
		current.Source.Schema, current.Source.Table, string(embedcatchup.ModeOutbox),
		string(embedrun.StatusComplete), string(embedrun.StatusAbandoned))
	if err != nil {
		return nil, fmt.Errorf("read live outbox contracts for %s: %w",
			embedstore.QualifiedName(current.Source.Schema, current.Source.Table), err)
	}
	defer rows.Close()

	identity := current.Identity().Digest
	contracts := make([]embedgen.Spec, 0)
	currentIncluded := false
	for rows.Next() {
		var registered embedstore.Generation
		if err := rows.Scan(
			&registered.Identity, &registered.SpecDigest, &registered.SpecDocument); err != nil {
			return nil, fmt.Errorf("read live outbox contracts for %s: %w",
				embedstore.QualifiedName(current.Source.Schema, current.Source.Table), err)
		}
		if registered.Identity == identity {
			contracts = append(contracts, current)
			currentIncluded = true
			continue
		}
		loaded, err := embedbackend.RecordedSpec(registered,
			"preserved while another generation reinstalls its shared outbox")
		if err != nil {
			return nil, err
		}
		contracts = append(contracts, loaded.Spec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live outbox contracts for %s: %w",
			embedstore.QualifiedName(current.Source.Schema, current.Source.Table), err)
	}
	if !currentIncluded {
		contracts = append(contracts, current)
	}
	return contracts, nil
}
