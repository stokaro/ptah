package embedrpc_test

import (
	"context"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// fakeBatches is a provider's batch commit held in memory: the run and what
// the target holds, read with a token that names the read, and the commits the
// backend sent. The first conflicts commits answer CONFLICT.
type fakeBatches struct {
	provider.EmbeddingStore

	run       *protocol.EmbeddingRun
	held      []*protocol.EmbeddingHeldTarget
	reads     int
	conflicts int
	commits   []*protocol.CommitEmbeddingBatchRequest
}

func (f *fakeBatches) ReadEmbeddingBatchState(_ context.Context, request *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error) {
	f.reads++
	return &protocol.EmbeddingBatchState{RunId: request.GetRunId(), Run: f.run, Held: f.held, Token: "batch-" + strconv.Itoa(f.reads)}, nil
}

func (f *fakeBatches) CommitEmbeddingBatch(_ context.Context, request *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error) {
	f.commits = append(f.commits, request)
	if f.conflicts > 0 {
		f.conflicts--
		result := &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_CONFLICT}
		return result, provider.CheckEmbeddingWrite(result)
	}
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED,
		Written: &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{request.GetRun()}}}, nil
}

// targetSpec is a generation of two-element vectors on the articles table.
func targetSpec() embedgen.Spec {
	return embedgen.Spec{
		Name:   "articles",
		Source: embedgen.Source{Table: "articles", KeyFields: []string{"id"}, InputFields: []string{"body"}},
		Model:  embedgen.Model{Identifier: "m", ReportedDimension: 2},
		Target: embedgen.Target{Table: "articles", Column: "embedding", Representation: "float", Metric: embedgen.MetricCosine},
	}
}

// storedRun is the run as the provider holds it, at a fencing token.
func storedRun(spec embedgen.Spec, token int64) *protocol.EmbeddingRun {
	return &protocol.EmbeddingRun{Id: "run-1", GenerationIdentity: spec.Identity().Digest, Source: embedstore.SourceIdentity("", "articles"),
		Phase: string(embedrun.PhaseBackfilling), Status: string(embedrun.StatusRunning), FencingToken: token, Progress: &protocol.EmbeddingProgress{}}
}

// checkpoint is the run a worker commits a batch with.
func checkpoint(spec embedgen.Spec, token int64) embedrun.Run {
	return embedrun.Run{ID: "run-1", GenerationIdentity: spec.Identity().Digest, Source: embedstore.SourceIdentity("", "articles"),
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning, FencingToken: token, Cursor: []string{"2"},
		Progress: embedrun.Progress{RowsScanned: 2, RowsEmbedded: 2, BatchesCommitted: 1}}
}

func write(spec embedgen.Spec, id, hash string) embedrun.TargetWrite {
	return embedrun.TargetWrite{Key: []string{id}, Generation: spec.Identity().Digest, InputHash: hash, Version: "1",
		Kind: embedrun.WriteUpsert, Vector: []float32{1, 0}}
}

func committed(c *qt.C, store provider.EmbeddingStore, spec embedgen.Spec, run embedrun.Run, writes ...embedrun.TargetWrite) error {
	c.Helper()
	target, err := backendOver(store).NewTarget(spec)
	c.Assert(err, qt.IsNil)
	return target.Commit(c.Context(), writes, run)
}

// TestTarget_SendsOnlyTheSetsThatWin decides each key against what the
// target holds: the same work arriving again is left out of the commit, a new
// key is written, and both keys stay under the token.
func TestTarget_SendsOnlyTheSetsThatWin(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	store := &fakeBatches{run: storedRun(spec, 3), held: []*protocol.EmbeddingHeldTarget{{
		Key: &protocol.EmbeddingKey{Components: []string{"1"}}, Generation: spec.Identity().Digest, InputHash: "hash-1", Version: "1",
		Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT,
	}}}

	c.Assert(committed(c, store, spec, checkpoint(spec, 3), write(spec, "1", "hash-1"), write(spec, "2", "hash-2")), qt.IsNil)

	c.Assert(store.commits, qt.HasLen, 1)
	commit := store.commits[0]
	c.Assert(commit.GetExpectedToken(), qt.Equals, "batch-1")
	c.Assert(keyComponents(commit.GetKeys()), qt.DeepEquals, []string{"1", "2"})
	c.Assert(commit.GetWrites(), qt.HasLen, 1)
	c.Assert(commit.GetWrites()[0].GetKey().GetComponents(), qt.DeepEquals, []string{"2"})
	c.Assert(commit.GetWrites()[0].GetVector(), qt.DeepEquals, []float32{1, 0})
	c.Assert(commit.GetRun().GetCursor(), qt.DeepEquals, []string{"2"})
}

// TestTarget_DecidesAgainAfterAConflict reads the batch state again after a
// CONFLICT and commits on the token of that read.
func TestTarget_DecidesAgainAfterAConflict(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	store := &fakeBatches{run: storedRun(spec, 3), conflicts: 1}

	c.Assert(committed(c, store, spec, checkpoint(spec, 3), write(spec, "1", "hash-1")), qt.IsNil)

	c.Assert(store.commits, qt.HasLen, 2)
	c.Assert([]string{store.commits[0].GetExpectedToken(), store.commits[1].GetExpectedToken()}, qt.DeepEquals, []string{"batch-1", "batch-2"})
}

// TestTarget_FailurePath refuses before anything is sent: a worker whose
// token the run moved past, and a write over a row another generation holds.
func TestTarget_FailurePath(t *testing.T) {
	spec := targetSpec()
	tests := []struct {
		name  string
		store *fakeBatches
		want  string
	}{
		{name: "a stale fencing token", store: &fakeBatches{run: storedRun(spec, 5)},
			want: `the state changed underneath this write: run run-1 is fenced at token 5 and this write carries 3`},
		{name: "another generation's row", store: &fakeBatches{run: storedRun(spec, 3), held: []*protocol.EmbeddingHeldTarget{{
			Key: &protocol.EmbeddingKey{Components: []string{"1"}}, Generation: "gen-other", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT,
		}}}, want: `write \[1\]: target write: the row belongs to generation gen-other and the write is for .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := committed(c, test.store, spec, checkpoint(spec, 3), write(spec, "1", "hash-1"))
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(test.store.commits, qt.HasLen, 0)
		})
	}
}

// TestTarget_RefusesATerminalRun refuses a batch of a run that is over before
// the provider is asked anything.
func TestTarget_RefusesATerminalRun(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	store := &fakeBatches{run: storedRun(spec, 3)}
	abandoned := checkpoint(spec, 3)
	abandoned.Status = embedrun.StatusAbandoned

	err := committed(c, store, spec, abandoned, write(spec, "1", "hash-1"))

	c.Assert(err, qt.ErrorIs, embedrun.ErrTerminal)
	c.Assert(err, qt.ErrorMatches, `save run run-1 with target writes: the run is terminal: run is abandoned`)
	c.Assert(store.reads, qt.Equals, 0)
}

// TestTarget_RefusesABatchThatDoesNotFitOneMessage refuses, before anything
// is sent, a batch whose commit is larger than one message to the provider:
// the commit is atomic with its checkpoint, so it cannot be split, and a
// message the transport refused would leave its outcome unknown.
func TestTarget_RefusesABatchThatDoesNotFitOneMessage(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	spec.Model.ReportedDimension = 1024
	store := &fakeBatches{run: storedRun(spec, 3)}
	writes := make([]embedrun.TargetWrite, 0, 1100)
	for id := range 1100 {
		written := write(spec, strconv.Itoa(id), "hash")
		written.Vector = make([]float32, 1024)
		writes = append(writes, written)
	}

	err := committed(c, store, spec, checkpoint(spec, 3), writes...)

	c.Assert(err, qt.ErrorMatches, `commit a batch of run run-1: the batch is \d+ bytes, more than the 4194304 bytes one message to the provider carries; commit fewer rows per batch`)
	c.Assert(store.commits, qt.HasLen, 0)
}

func keyComponents(keys []*protocol.EmbeddingKey) []string {
	components := make([]string, len(keys))
	for i, key := range keys {
		components[i] = key.GetComponents()[0]
	}
	return components
}
