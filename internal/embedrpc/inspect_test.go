package embedrpc_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedstore"
)

// retiringStore is a run-state store whose generation holds rows, and that
// records the drops the backend asks for.
type retiringStore struct {
	*fakeStore

	rows  int64
	index bool
	drops []*protocol.DropEmbeddingGenerationRequest
}

func (r *retiringStore) ReadEmbeddingStructure(_ context.Context, request *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error) {
	structure := &protocol.EmbeddingStructure{TableExists: true, ColumnExists: true, ColumnType: "BLOB", IndexExists: r.index, RowsCounted: request.GetCountRows()}
	if r.index {
		structure.IndexMethod = "flat"
	}
	if request.GetCountRows() {
		structure.Rows = r.rows
	}
	return structure, nil
}

func (r *retiringStore) DropEmbeddingGeneration(_ context.Context, request *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error) {
	r.drops = append(r.drops, request)
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED}, nil
}

func retiring(c *qt.C) *retiringStore {
	c.Helper()
	return &retiringStore{fakeStore: seeded(c), rows: 5}
}

// TestBackend_RetireGeneration_DropsWhatTheApprovalNames records the
// retirement first and then drops the storage the approval names, at the
// location the record gives.
func TestBackend_RetireGeneration_DropsWhatTheApprovalNames(t *testing.T) {
	tests := []struct {
		name        string
		destruction embedbackend.RetirementDestruction
		storage     protocol.DropEmbeddingGenerationRequest_Storage
	}{
		{name: "the index alone", storage: protocol.DropEmbeddingGenerationRequest_INDEX_ONLY},
		{name: "the columns", destruction: embedbackend.RetirementDestruction{DropColumns: true}, storage: protocol.DropEmbeddingGenerationRequest_COLUMNS},
		{name: "the table", destruction: embedbackend.RetirementDestruction{DropTable: true}, storage: protocol.DropEmbeddingGenerationRequest_TABLE},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			store := retiring(c)

			retirement, err := backendOver(store).RetireGeneration(c.Context(), "gen-1", embedstore.Pointer{}, 5, test.destruction)

			c.Assert(err, qt.IsNil)
			c.Assert(retirement.RetiredAt.IsZero(), qt.IsFalse)
			c.Assert(store.mutations, qt.HasLen, 1)
			c.Assert(store.mutations[0].GetRecords().GetGenerations()[0].GetRetiredAt(), qt.IsNotNil)
			c.Assert(store.drops, qt.HasLen, 1)
			c.Assert(store.drops[0].GetStorage(), qt.Equals, test.storage)
			c.Assert(store.drops[0].GetLocation().GetNames().GetOwnershipMark(), qt.Equals, "ptah embedding target for generation gen-1")
			c.Assert(store.drops[0].GetLocation().GetNames().GetIndex(), qt.Equals, "articles_embedding_gen-1_idx")
		})
	}
}

// TestBackend_RetireGeneration_FailurePath refuses a retirement whose
// approved row count or index state is not what the engine holds now, before
// anything is recorded or dropped.
func TestBackend_RetireGeneration_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		rows  int
		index bool
		want  string
	}{
		{name: "a row count that changed", rows: 4, want: `the state changed underneath this write: generation gen-1 row count changed from approved 4 to 5`},
		{name: "an index that appeared", rows: 5, index: true, want: `the state changed underneath this write: generation gen-1 index state changed from approved false to true`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			store := retiring(c)
			store.index = test.index

			_, err := backendOver(store).RetireGeneration(c.Context(), "gen-1", embedstore.Pointer{}, test.rows, embedbackend.RetirementDestruction{})

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
			c.Assert(store.mutations, qt.HasLen, 0)
			c.Assert(store.drops, qt.HasLen, 0)
		})
	}
}
