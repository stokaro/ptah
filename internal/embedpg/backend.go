package embedpg

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver the PostgreSQL vertical connects with

	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
)

// A signature that drifts from the contract is a compile error here rather
// than at the verb that reaches it.
var _ embedbackend.Backend = (*Backend)(nil)

// Backend is [embedbackend.Backend] over PostgreSQL with pgvector.
//
// It adds no behavior of its own: every method is one of this package's
// functions or a [Store] method, so the verbs reach PostgreSQL through the same
// code whether they hold a Backend or the package.
type Backend struct {
	db    *sql.DB
	store *Store
}

// Open connects to the PostgreSQL database a URL names and checks that it
// answers.
//
// The two failures are worded apart because they send an operator to
// different places: a URL the driver cannot parse is a string to fix, and a
// server that does not answer is a network or a server.
func Open(ctx context.Context, dbURL string) (*Backend, error) {
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dburldisplay.Format(dbURL), err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to %s: %w", dburldisplay.Format(dbURL), err)
	}
	return NewBackend(db), nil
}

// NewBackend returns a backend over an open database. Closing the backend
// closes db.
func NewBackend(db *sql.DB) *Backend {
	return &Backend{db: db, store: NewStore(db)}
}

// Dialect names the engine.
func (b *Backend) Dialect() string { return Dialect }

// Close releases the connection.
func (b *Backend) Close() error { return b.db.Close() }

// Store is the run state.
func (b *Backend) Store() embedbackend.Store { return b.store }

// EnsureSchema creates the run-state tables, rendered by service for the
// PostgreSQL baseline profile.
func (b *Backend) EnsureSchema(ctx context.Context, service renderer.Service) error {
	return b.store.EnsureSchema(ctx, service, capability.ForDialect(Dialect))
}

// ResolveRelation resolves a spelling through the session's search_path.
func (b *Backend) ResolveRelation(ctx context.Context, schema, table string) (embedbackend.Relation, error) {
	relation, _, err := ResolveRelation(ctx, b.db, schema, table)
	return relation, err
}

// WithResolvedRelations names the physical relations.
func (b *Backend) WithResolvedRelations(ctx context.Context, spec embedgen.Spec) (embedgen.Spec, error) {
	return WithResolvedRelations(ctx, b.db, spec)
}

// TargetObjects is the pgvector column and index.
func (b *Backend) TargetObjects(spec embedgen.Spec) (embedgen.TargetObjects, error) {
	return spec.TargetObjects()
}

// CheckMode accepts every mode this build has: the outbox is triggers on the
// source, which PostgreSQL has.
func (b *Backend) CheckMode(embedcatchup.Mode) error { return nil }

// PrepareRun creates the target, installs change capture, records the
// boundary, registers the generation and creates the run.
func (b *Backend) PrepareRun(
	ctx context.Context, spec embedgen.Spec, generation embedstore.Generation,
	run embedrun.Run, mode embedcatchup.Mode,
) (embedbackend.PrepareResult, error) {
	return b.store.PrepareRun(ctx, spec, generation, run, mode)
}

// NewSource reads the source by keyset.
func (b *Backend) NewSource(spec embedgen.Spec) (embedbackend.Source, error) {
	return NewSource(b.db, spec)
}

// NewTarget writes vectors and their checkpoint together.
func (b *Backend) NewTarget(spec embedgen.Spec) (embedengine.Target, error) {
	return NewTarget(b.db, spec)
}

// Outbox is the trigger-fed outbox on the physical source.
func (b *Backend) Outbox(ctx context.Context, spec embedgen.Spec) (embedbackend.Outbox, error) {
	physical, err := WithResolvedRelations(ctx, b.db, spec)
	if err != nil {
		return nil, err
	}
	outbox, err := NewOutbox(b.db, physical)
	if err != nil {
		return nil, err
	}
	return prunableOutbox{Outbox: outbox, store: b.store}, nil
}

// EnsureRunIndex builds the index and records the run's index phase.
func (b *Backend) EnsureRunIndex(
	ctx context.Context, runID string, spec embedgen.Spec,
) (embedbackend.IndexOutcome, error) {
	return b.store.EnsureRunIndex(ctx, runID, spec)
}

// NewSearcher searches a generation through its index and without it.
func (b *Backend) NewSearcher(spec embedgen.Spec) (embedbackend.Searcher, error) {
	return NewSearcher(b.db, spec)
}

// TargetColumnExists asks the catalog for the generation's column.
func (b *Backend) TargetColumnExists(ctx context.Context, spec embedgen.Spec) (bool, error) {
	return ColumnExists(ctx, b.db, spec.Target.Table, spec.Target.Column)
}

// ReadStructure reads the column and index back from the catalog.
func (b *Backend) ReadStructure(
	ctx context.Context, spec embedgen.Spec, activePointer string,
) (embedverify.Structure, error) {
	return ReadStructure(ctx, b.db, spec, activePointer)
}

// VerificationCorpus walks both sides in one statement.
func (b *Backend) VerificationCorpus(ctx context.Context, spec embedgen.Spec) (embedverify.Corpus, error) {
	return VerificationCorpus(ctx, b.db, spec)
}

// CountGenerationRows counts a registered generation's vectors.
func (b *Backend) CountGenerationRows(ctx context.Context, generation embedstore.Generation) (int, error) {
	return CountGenerationRows(ctx, b.db, generation)
}

// GenerationIndexExists asks the catalog for a registered generation's index.
func (b *Backend) GenerationIndexExists(ctx context.Context, generation embedstore.Generation) (bool, error) {
	return GenerationIndexExists(ctx, b.db, generation)
}

// RetireGeneration destroys the generation and records it in one transaction.
func (b *Backend) RetireGeneration(
	ctx context.Context, identity string, expectedPointer embedstore.Pointer,
	expectedRows int, destruction embedbackend.RetirementDestruction,
) (embedbackend.Retirement, error) {
	release, err := b.store.RetireGenerationObjects(ctx, identity, expectedPointer, expectedRows, destruction)
	if err != nil {
		return embedbackend.Retirement{}, err
	}
	retirement := embedbackend.Retirement{RetiredAt: release.RetiredAt}
	if sentence := release.Sentence(); sentence != "" {
		retirement.Notes = append(retirement.Notes, sentence)
	}
	return retirement, nil
}

// Measure answers what a plan reports: the relations, the row count, the
// extension and its index methods, and whether this index can be built.
func (b *Backend) Measure(ctx context.Context, spec embedgen.Spec) (embedbackend.Measurement, error) {
	var measured embedbackend.Measurement
	var err error
	if measured.TargetColumnExists, err = ColumnExists(ctx, b.db, spec.Target.Table, spec.Target.Column); err != nil {
		return embedbackend.Measurement{}, err
	}
	if measured.SourceExists, err = SourceTableExists(ctx, b.db, spec); err != nil {
		return embedbackend.Measurement{}, err
	}
	if measured.TargetTableExists, err = TargetTableExists(ctx, b.db, spec); err != nil {
		return embedbackend.Measurement{}, err
	}
	if measured.Rows, err = CountRows(ctx, b.db, spec); err != nil {
		return embedbackend.Measurement{}, err
	}
	if measured.Capabilities, err = VectorCapabilities(ctx, b.db); err != nil {
		return embedbackend.Measurement{}, err
	}
	if measured.IndexBuildable, err = VectorIndexBuildable(ctx, b.db, spec); err != nil {
		return embedbackend.Measurement{}, err
	}
	return measured, nil
}

// prunableOutbox is an [Outbox] whose prune runs through the store, which owns
// the durable reader set and binds its floor to the deleting statement.
type prunableOutbox struct {
	*Outbox
	store *Store
}

// Prune removes the events every live reader has passed.
func (o prunableOutbox) Prune(ctx context.Context) (embedbackend.Pruned, error) {
	floor, found, removed, err := o.store.PruneOutbox(ctx, o.Outbox)
	if err != nil || !found {
		return embedbackend.Pruned{}, err
	}
	pruned := embedbackend.Pruned{Found: true, Position: floor.Position, Removed: removed}
	for _, holder := range floor.Holders {
		pruned.Holders = append(pruned.Holders, embedbackend.OutboxFloorHolder{
			RunID: holder.RunID, Generation: holder.Generation,
		})
	}
	return pruned, nil
}
