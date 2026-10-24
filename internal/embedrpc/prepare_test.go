package embedrpc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// resolvingStore answers each relation with its name in upper case, the way a
// server that folds unquoted names spells them.
type resolvingStore struct {
	*fakeStore

	built int
}

func (r *resolvingStore) ResolveEmbeddingRelations(_ context.Context, request *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error) {
	resolved := &protocol.EmbeddingRelations{}
	for _, relation := range request.GetRelations() {
		resolved.Relations = append(resolved.Relations, &protocol.Reference{Kind: relation.GetKind(), Schema: "app", Name: strings.ToUpper(relation.GetName())})
	}
	return resolved, nil
}

func (r *resolvingStore) BuildEmbeddingIndex(_ context.Context, request *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error) {
	r.built++
	return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_APPLIED,
		IndexOutcome: protocol.EmbeddingWriteResult_NOT_DECLARED}, nil
}

// TestBackend_WithResolvedRelations names the source and the target as the
// server names them.
func TestBackend_WithResolvedRelations(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	spec.Target.Table = "article_vectors"

	resolved, err := backendOver(&resolvingStore{fakeStore: &fakeStore{}}).WithResolvedRelations(c.Context(), spec)

	c.Assert(err, qt.IsNil)
	c.Assert([]string{resolved.Source.Schema, resolved.Source.Table, resolved.Target.Schema, resolved.Target.Table}, qt.DeepEquals,
		[]string{"app", "ARTICLES", "app", "ARTICLE_VECTORS"})
}

// TestBackend_PrepareRunRefusesAnotherSource refuses a run that records
// another source than the one the specification resolves to, before the
// provider is asked to create anything.
func TestBackend_PrepareRunRefusesAnotherSource(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	identity := spec.Identity().Digest
	generation := embedstore.Generation{Identity: identity, ConsistencyMode: string(embedcatchup.ModeImmutable)}
	run := embedrun.Run{ID: "run-1", GenerationIdentity: identity, Source: embedstore.SourceIdentity("", "articles"),
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning}

	_, err := backendOver(&resolvingStore{fakeStore: &fakeStore{}}).PrepareRun(c.Context(), spec, generation, run, embedcatchup.ModeImmutable)

	c.Assert(err, qt.ErrorIs, embedstore.ErrConflict)
	c.Assert(err, qt.ErrorMatches, `.*run run-1 records source .*, specification uses app.ARTICLES`)
}

// TestBackend_EnsureRunIndexRecordsThePhase builds the index and then records
// the run's index phase with a raised fencing token and no lease, so a worker
// holding its pre-build lease cannot commit a batch the index was not built
// over.
func TestBackend_EnsureRunIndexRecordsThePhase(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	store := &resolvingStore{fakeStore: &fakeStore{}}
	backend := backendOver(store)
	caughtUp := embedrun.Run{ID: "run-1", GenerationIdentity: spec.Identity().Digest, Phase: embedrun.PhaseCaughtUp, Status: embedrun.StatusRunning,
		FencingToken: 3, LeaseOwner: "worker", LeaseExpires: created.Add(time.Hour)}
	c.Assert(backend.Store().CreateRun(c.Context(), caughtUp), qt.IsNil)
	store.mutations = nil

	outcome, err := backend.EnsureRunIndex(c.Context(), "run-1", spec)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, embedbackend.IndexNotDeclared)
	c.Assert(store.built, qt.Equals, 1)
	c.Assert(store.mutations, qt.HasLen, 1)
	recorded := store.mutations[0].GetRecords().GetRuns()[0]
	c.Assert(recorded.GetPhase(), qt.Equals, string(embedrun.PhaseIndexed))
	c.Assert(recorded.GetFencingToken(), qt.Equals, int64(4))
	c.Assert(recorded.GetLeaseOwner(), qt.Equals, "")
	c.Assert(recorded.GetLeaseExpires(), qt.IsNil)
}
