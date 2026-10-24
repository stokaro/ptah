package embedrpc

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
)

// verificationPage bounds the rows one page of the verification walk carries.
// A row never carries a vector, so a page is small however wide the vectors
// are.
const verificationPage = 1000

// structure reads a generation's storage as the engine reports it.
func (b *Backend) structure(ctx context.Context, location *protocol.EmbeddingLocation, count bool) (*protocol.EmbeddingStructure, error) {
	structure, err := b.session.Store.ReadEmbeddingStructure(ctx, &protocol.ReadEmbeddingStructureRequest{
		SessionId: b.session.ID, Location: location, CountRows: count,
	})
	if err != nil {
		return nil, fmt.Errorf("read the storage of generation %s on %s: %w", location.GetGeneration(), b.engineName(), err)
	}
	return structure, nil
}

// specStructure reads the storage a specification's generation has.
func (b *Backend) specStructure(ctx context.Context, spec embedgen.Spec) (*protocol.EmbeddingStructure, error) {
	record, err := specRecord(spec)
	if err != nil {
		return nil, err
	}
	return b.structure(ctx, specLocation(spec, record), false)
}

// TargetColumnExists reports whether the generation's vector column is there.
func (b *Backend) TargetColumnExists(ctx context.Context, spec embedgen.Spec) (bool, error) {
	structure, err := b.specStructure(ctx, spec)
	if err != nil {
		return false, err
	}
	return structure.GetColumnExists(), nil
}

// ReadStructure reads back the column and index a generation's verification
// is measured against.
func (b *Backend) ReadStructure(ctx context.Context, spec embedgen.Spec, activePointer string) (embedverify.Structure, error) {
	structure, err := b.specStructure(ctx, spec)
	if err != nil {
		return embedverify.Structure{ActivePointer: activePointer}, err
	}
	return embedverify.Structure{
		ActivePointer: activePointer, ExtensionPresent: structure.GetVectorSupport(),
		ColumnExists: structure.GetColumnExists(), ColumnType: structure.GetColumnType(), Dimension: int(structure.GetDimension()),
		IndexExists: structure.GetIndexExists(), IndexValid: structure.GetIndexValid(),
		IndexMethod: structure.GetIndexMethod(), OperatorClass: structure.GetIndexOperator(),
	}, nil
}

// CountGenerationRows counts the vectors a registered generation holds.
func (b *Backend) CountGenerationRows(ctx context.Context, generation embedstore.Generation) (int, error) {
	structure, err := b.structure(ctx, locationOf(generation), true)
	if err != nil {
		return 0, err
	}
	return int(structure.GetRows()), nil
}

// GenerationIndexExists reports whether a registered generation's index is
// there. The location comes from the record, because a retirement holds no
// specification for the generation it destroys.
func (b *Backend) GenerationIndexExists(ctx context.Context, generation embedstore.Generation) (bool, error) {
	structure, err := b.structure(ctx, locationOf(generation), false)
	if err != nil {
		return false, err
	}
	return structure.GetIndexExists(), nil
}

// Measure answers what a plan reports about the database.
func (b *Backend) Measure(ctx context.Context, spec embedgen.Spec) (embedbackend.Measurement, error) {
	record, err := specRecord(spec)
	if err != nil {
		return embedbackend.Measurement{}, err
	}
	measured, err := b.session.Store.MeasureEmbedding(ctx, &protocol.MeasureEmbeddingRequest{SessionId: b.session.ID, Spec: record})
	if err != nil {
		return embedbackend.Measurement{}, fmt.Errorf("measure generation %s on %s: %w", spec.Identity().Short(), b.engineName(), err)
	}
	result := embedbackend.Measurement{
		TargetColumnExists: measured.GetTargetColumnExists(), SourceExists: measured.GetSourceExists(),
		TargetTableExists: measured.GetTargetTableExists(), Rows: -1, Capabilities: maps.Clone(measured.GetCapabilities()),
		IndexBuildable: measured.GetIndexBuildable(), Refusals: slices.Clone(measured.GetRefusals()),
	}
	if measured.GetRowsCounted() {
		result.Rows = measured.GetRows()
	}
	return result, nil
}

// VerificationCorpus walks both sides of the comparison a page at a time.
//
// Each page reads both sides of its keys in one read, so a row written between
// two reads cannot appear on one side only, and a page never splits a key's
// rows. The walk is handed back as a sequence that asks for the next page when
// the range loop over it reaches the end of the last, so nothing between here
// and the report holds the corpus. The provider sends the width of a stored
// vector and never the vector; the host computes the input hash.
func (b *Backend) VerificationCorpus(ctx context.Context, spec embedgen.Spec) (embedverify.Corpus, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	record, err := specRecord(spec)
	if err != nil {
		return nil, err
	}
	return func(yield func(embedverify.Pair, error) bool) {
		token := ""
		for {
			page, err := b.session.Store.ReadEmbeddingVerification(ctx, &protocol.ReadEmbeddingVerificationRequest{
				SessionId: b.session.ID, Spec: record, PageToken: token, MaxRows: verificationPage,
			})
			if err != nil {
				yield(embedverify.Pair{}, fmt.Errorf("read generation %s for verification on %s: %w", spec.Identity().Short(), b.engineName(), err))
				return
			}
			for _, row := range page.GetRows() {
				pair, err := pairOf(spec, row)
				if err != nil {
					yield(embedverify.Pair{}, err)
					return
				}
				if !yield(pair, nil) {
					return
				}
			}
			if page.GetComplete() {
				return
			}
			token = page.GetNextPageToken()
		}
	}, nil
}

// pairOf is one position of the walk as verification reads it.
func pairOf(spec embedgen.Spec, row *protocol.EmbeddingVerificationRow) (embedverify.Pair, error) {
	components := row.GetKey().GetComponents()
	identity := embedverify.KeyIdentity(components...)
	var pair embedverify.Pair
	if stored := row.GetStored(); stored != nil {
		kind := stored.GetKind()
		pair.Target = &embedverify.TargetRow{
			Key: identity, Generation: stored.GetGeneration(), Version: stored.GetVersion(), InputHash: stored.GetInputHash(),
			Dimension: int(stored.GetDimension()), Ordinal: int(stored.GetOrdinal()),
			Tombstone: kind == protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE,
			Skipped:   kind == protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP,
		}
	}
	source := row.GetSource()
	if source == nil {
		return pair, nil
	}
	fields := rowOf(&protocol.EmbeddingSourceRow{Key: row.GetKey(), Fields: source.GetFields()}).Fields
	set, err := spec.CanonicalInputs(embedgen.Row{Key: components, Fields: fields})
	if err != nil {
		return embedverify.Pair{}, fmt.Errorf("canonicalize %v for verification: %w", components, err)
	}
	pair.Source = &embedverify.SourceRow{
		Key: identity, Version: source.GetVersion(), InputHash: spec.SourceInputHash(set.Whole), Skipped: set.Whole.Skipped,
	}
	return pair, nil
}

// RetireGeneration destroys one generation and records that it did.
//
// The record comes first: the generation is stamped retired and its runs
// completed in one compare-and-write, decided by the rules every store
// applies, after the row count and index the retirement was approved against
// are measured again. From that moment no pointer can move to it. Its storage
// is dropped second, by the provider, which checks that the storage is the
// generation's before it drops any. A retirement whose drop did not finish is
// completed by running retire again: a generation already recorded as retired
// skips the record and drops what is left.
func (b *Backend) RetireGeneration(
	ctx context.Context, identity string, expectedPointer embedstore.Pointer, expectedRows int,
	destruction embedbackend.RetirementDestruction,
) (embedbackend.Retirement, error) {
	if err := destruction.Validate(); err != nil {
		return embedbackend.Retirement{}, err
	}
	registered, err := b.store.Generation(ctx, identity)
	if err != nil {
		return embedbackend.Retirement{}, err
	}
	retirement := embedbackend.Retirement{RetiredAt: registered.RetiredAt}
	if registered.Retired() {
		retirement.Notes = append(retirement.Notes, fmt.Sprintf(
			"generation %s was recorded as retired at %s; this run dropped what was left of it",
			identity, registered.RetiredAt.UTC().Format(time.RFC3339)))
	} else {
		if err := b.revalidateArtifacts(ctx, registered, expectedRows, destruction); err != nil {
			return embedbackend.Retirement{}, err
		}
		after, err := b.store.decide(ctx, everything(), "retire generation "+identity, func(memory *embedstore.Memory) error {
			_, err := memory.RetireGeneration(ctx, identity, expectedPointer)
			return err
		})
		if err != nil {
			return embedbackend.Retirement{}, err
		}
		retired, err := generationIn(after, identity)
		if err != nil {
			return embedbackend.Retirement{}, err
		}
		retirement.RetiredAt = retired.RetiredAt
	}
	storage := protocol.DropEmbeddingGenerationRequest_INDEX_ONLY
	switch {
	case destruction.DropTable:
		storage = protocol.DropEmbeddingGenerationRequest_TABLE
	case destruction.DropColumns:
		storage = protocol.DropEmbeddingGenerationRequest_COLUMNS
	}
	result, err := b.session.Store.DropEmbeddingGeneration(ctx, &protocol.DropEmbeddingGenerationRequest{
		SessionId: b.session.ID, RequestId: requestID(), Location: locationOf(registered), Storage: storage,
	})
	if err := writeError("drop the storage of generation "+identity, result, err); err != nil {
		return embedbackend.Retirement{}, fmt.Errorf(
			"generation %s is recorded as retired, and dropping its storage failed; run retire again to finish: %w", identity, err)
	}
	return retirement, nil
}

// revalidateArtifacts refuses a retirement whose approved row count or index
// state is no longer what the engine holds.
func (b *Backend) revalidateArtifacts(
	ctx context.Context, generation embedstore.Generation, rows int, approved embedbackend.RetirementDestruction,
) error {
	structure, err := b.structure(ctx, locationOf(generation), true)
	if err != nil {
		return err
	}
	if counted := int(structure.GetRows()); counted != rows {
		return fmt.Errorf("%w: generation %s row count changed from approved %d to %d",
			embedstore.ErrConflict, generation.Identity, rows, counted)
	}
	if present := structure.GetIndexExists(); present != approved.IndexExists {
		return fmt.Errorf("%w: generation %s index state changed from approved %t to %t",
			embedstore.ErrConflict, generation.Identity, approved.IndexExists, present)
	}
	return nil
}
