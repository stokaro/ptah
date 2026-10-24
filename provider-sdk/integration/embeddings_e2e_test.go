//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

// embeddingOptions starts the fixture that serves the embedding store, with a
// host that lists the feature.
func embeddingOptions(c *qt.C) transport.Options {
	c.Helper()
	binary := buildFixture(c, "embeddings")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.EmbeddingStoreFeature}, HostFeatures: []string{provider.EmbeddingStoreFeature}}
}

// embeddingDatabase opens a session on the database file path, under a fault
// the fixture reads from the address.
func embeddingDatabase(c *qt.C, client *transport.Client, path, fault string) string {
	c.Helper()
	address := &url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: url.Values{"fault": {fault}}.Encode()}
	// Terminate the process before the temporary directory is removed, because
	// Windows refuses to remove an open database file.
	c.Cleanup(client.Close)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: address.String()})
	c.Assert(err, qt.IsNil)
	return session.GetId()
}

// seedArticles creates the source table, five rows keyed by id, and the run-state store.
func seedArticles(c *qt.C, client *transport.Client, session string) {
	c.Helper()
	units := []*protocol.Unit{
		{ChangeId: "table", Body: &protocol.Unit_Sql{Sql: `CREATE TABLE "articles" ("id" INTEGER NOT NULL, "body" TEXT, "updated_at" TEXT, PRIMARY KEY ("id"));`}},
		{ChangeId: "rows", Body: &protocol.Unit_Sql{Sql: `INSERT INTO "articles" VALUES (1, 'pricing', '1'), (2, 'support', '1'), (3, 'billing', '1'), (4, NULL, '1'), (5, 'renewal', '1');`}},
	}
	result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session, RequestId: "seed", Units: units})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	prepared, err := client.PrepareEmbeddingStore(c.Context(), &protocol.PrepareEmbeddingStoreRequest{SessionId: session, RequestId: "store"})
	c.Assert(err, qt.IsNil)
	c.Assert(prepared.GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)
}

func articlesReference(name string) *protocol.Reference {
	return &protocol.Reference{Kind: "table", Name: name}
}

// columnsSpec is a generation that writes its columns on the articles table.
func columnsSpec() *protocol.EmbeddingSpec {
	return &protocol.EmbeddingSpec{Generation: "gen-columns",
		Source: &protocol.EmbeddingSource{Table: articlesReference("articles"), KeyFields: []string{"id"}, InputFields: []string{"body"}, VersionField: "updated_at"},
		Target: &protocol.EmbeddingTarget{Table: articlesReference("articles"), Column: "embedding", Layout: protocol.EmbeddingTarget_SOURCE_COLUMNS,
			Representation: "float", Metric: protocol.EmbeddingTarget_COSINE, Dimension: 3},
		Names: &protocol.EmbeddingNames{Index: "articles_embedding_idx", ColumnComment: "embedding generation gen-columns"}}
}

// chunksSpec is a chunked generation in a table of its own.
func chunksSpec() *protocol.EmbeddingSpec {
	return &protocol.EmbeddingSpec{Generation: "gen-chunks", Chunked: true,
		Source: &protocol.EmbeddingSource{Table: articlesReference("articles"), KeyFields: []string{"id"}, InputFields: []string{"body"}, VersionField: "updated_at"},
		Target: &protocol.EmbeddingTarget{Table: articlesReference("article_chunks"), Column: "embedding", Layout: protocol.EmbeddingTarget_OWN_TABLE,
			Representation: "float", Metric: protocol.EmbeddingTarget_COSINE, Dimension: 3},
		Names: &protocol.EmbeddingNames{Index: "article_chunks_embedding_idx", OwnershipMark: "ptah embedding target for generation gen-chunks"}}
}

func embeddingKey(id string) *protocol.EmbeddingKey {
	return &protocol.EmbeddingKey{Components: []string{id}}
}

func prepareTarget(c *qt.C, client *transport.Client, session string, spec *protocol.EmbeddingSpec, mode protocol.PrepareEmbeddingTargetRequest_Mode) *protocol.EmbeddingWriteResult {
	c.Helper()
	result, err := client.PrepareEmbeddingTarget(c.Context(), &protocol.PrepareEmbeddingTargetRequest{SessionId: session, RequestId: "target-" + mode.String(), Spec: spec, Mode: mode})
	c.Assert(err, qt.IsNil)
	return result
}

// readEverything reads the whole run-state store.
func readEverything(c *qt.C, client *transport.Client, session string) *protocol.EmbeddingState {
	c.Helper()
	state, err := client.ReadEmbeddingState(c.Context(), &protocol.ReadEmbeddingStateRequest{SessionId: session, MaxRecords: 100,
		Scope: &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Everything{Everything: &protocol.EmbeddingStateScope_AllRecords{}}}})
	c.Assert(err, qt.IsNil)
	return state
}

// recordRun writes a run of the generation through a compare-and-write over
// everything.
func recordRun(c *qt.C, client *transport.Client, session, generation string) {
	c.Helper()
	before := readEverything(c, client, session)
	written, err := client.MutateEmbeddingState(c.Context(), &protocol.MutateEmbeddingStateRequest{SessionId: session, RequestId: "run",
		Scope: before.GetScope(), ExpectedToken: before.GetToken(), Records: &protocol.EmbeddingRecords{
			Generations: []*protocol.EmbeddingGeneration{{Identity: generation}},
			Runs:        []*protocol.EmbeddingRun{{Id: "run-1", GenerationIdentity: generation, Phase: "boundary_captured", FencingToken: 1}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(written.GetWritten().GetRuns(), qt.HasLen, 1)
}

// batch reads the batch state of the keys and the checkpointed run.
func batch(c *qt.C, client *transport.Client, session string, spec *protocol.EmbeddingSpec, keys ...string) (*protocol.EmbeddingBatchState, []*protocol.EmbeddingKey) {
	c.Helper()
	requested := make([]*protocol.EmbeddingKey, len(keys))
	for i, key := range keys {
		requested[i] = embeddingKey(key)
	}
	state, err := client.ReadEmbeddingBatchState(c.Context(), &protocol.ReadEmbeddingBatchStateRequest{SessionId: session, Spec: spec, RunId: "run-1", Keys: requested})
	c.Assert(err, qt.IsNil)
	return state, requested
}

// upsert is a set of one vector at a key.
func upsert(spec *protocol.EmbeddingSpec, key string, ordinal uint32, vector ...float32) *protocol.EmbeddingTargetWrite {
	return &protocol.EmbeddingTargetWrite{Key: embeddingKey(key), Ordinal: ordinal, Generation: spec.GetGeneration(), InputHash: "hash-" + key, Version: "1",
		Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT, Vector: vector}
}

// commitRequest checkpoints the run at cursor with the writes, on the token
// the batch state was read with.
func commitRequest(session string, spec *protocol.EmbeddingSpec, state *protocol.EmbeddingBatchState, keys []*protocol.EmbeddingKey, cursor string,
	writes ...*protocol.EmbeddingTargetWrite,
) *protocol.CommitEmbeddingBatchRequest {
	run := state.GetRun()
	checkpoint := &protocol.EmbeddingRun{Id: run.GetId(), GenerationIdentity: run.GetGenerationIdentity(), Phase: "backfilling", FencingToken: run.GetFencingToken(),
		Cursor: []string{cursor}, Progress: &protocol.EmbeddingProgress{RowsEmbedded: int64(len(writes)), BatchesCommitted: 1}}
	return &protocol.CommitEmbeddingBatchRequest{SessionId: session, RequestId: "commit-" + cursor, Spec: spec, RunId: run.GetId(), Keys: keys,
		ExpectedToken: state.GetToken(), Run: checkpoint, Writes: writes}
}

// TestIndependentProviderEmbeddingStorePreparesAndPagesTheSource prepares a
// generation's columns, scans the source a page at a time from where the last
// page ended, and rereads rows by key.
func TestIndependentProviderEmbeddingStorePreparesAndPagesTheSource(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	again, err := client.PrepareEmbeddingStore(c.Context(), &protocol.PrepareEmbeddingStoreRequest{SessionId: session, RequestId: "again"})
	c.Assert(err, qt.IsNil)
	c.Assert(again.GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)

	objects, err := client.DescribeEmbeddingTarget(c.Context(), &protocol.DescribeEmbeddingTargetRequest{Engine: "fixture", Spec: columnsSpec()})
	c.Assert(err, qt.IsNil)
	c.Assert(objects.GetColumnType(), qt.Equals, "BLOB")
	c.Assert(objects.GetOwnsTable(), qt.IsFalse)
	c.Assert(prepareTarget(c, client, session, columnsSpec(), protocol.PrepareEmbeddingTargetRequest_CREATE).GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)
	c.Assert(prepareTarget(c, client, session, columnsSpec(), protocol.PrepareEmbeddingTargetRequest_CREATE).GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)
	c.Assert(prepareTarget(c, client, session, columnsSpec(), protocol.PrepareEmbeddingTargetRequest_VERIFY).GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)

	keys, pages := scanSource(c, client, session, columnsSpec().GetSource(), 2)
	c.Assert(keys, qt.DeepEquals, []string{"1", "2", "3", "4", "5"})
	c.Assert(pages, qt.Equals, 3)

	reread, err := client.ReadEmbeddingSourceRows(c.Context(), &protocol.ReadEmbeddingSourceRowsRequest{SessionId: session, Source: columnsSpec().GetSource(),
		Keys: []*protocol.EmbeddingKey{embeddingKey("4"), embeddingKey("9")}})
	c.Assert(err, qt.IsNil)
	c.Assert(reread.GetRows(), qt.HasLen, 1)
	c.Assert(reread.GetRows()[0].GetFields()[0].GetNull(), qt.IsNotNil)
	c.Assert(reread.GetRows()[0].GetVersion(), qt.Equals, "1")
}

// scanSource reads the source a page at a time, each page resuming after the
// cursor the one before it returned, and answers the keys and the pages.
func scanSource(c *qt.C, client *transport.Client, session string, source *protocol.EmbeddingSource, rows uint32) (keys []string, pages int) {
	c.Helper()
	var after *protocol.EmbeddingKey
	for range 10 {
		page, err := client.ScanEmbeddingSource(c.Context(), &protocol.ScanEmbeddingSourceRequest{SessionId: session, Source: source, After: after, MaxRows: rows})
		c.Assert(err, qt.IsNil)
		pages++
		for _, row := range page.GetRows() {
			keys = append(keys, row.GetKey().GetComponents()[0])
		}
		after = page.GetCursor()
		if page.GetDone() {
			break
		}
	}
	return keys, pages
}

// TestIndependentProviderEmbeddingStoreRefusesWhatItCannotHold is the
// fixture's refusals, each before anything is created: an index method, a
// representation other than float, and a source key that is not the primary
// key.
func TestIndependentProviderEmbeddingStoreRefusesWhatItCannotHold(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	indexed, represented, keyed := columnsSpec(), columnsSpec(), columnsSpec()
	indexed.Target.IndexMethod = "hnsw"
	represented.Target.Representation = "vector"
	keyed.Source.KeyFields = []string{"body"}
	tests := []struct {
		name   string
		spec   *protocol.EmbeddingSpec
		reason protocol.EmbeddingRefusal_Reason
	}{
		{name: "an index method", spec: indexed, reason: protocol.EmbeddingRefusal_UNSUPPORTED},
		{name: "another representation", spec: represented, reason: protocol.EmbeddingRefusal_UNSUPPORTED},
		{name: "a key that is not the primary key", spec: keyed, reason: protocol.EmbeddingRefusal_UNSUPPORTED},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := client.PrepareEmbeddingTarget(c.Context(), &protocol.PrepareEmbeddingTargetRequest{SessionId: session, RequestId: "refused", Spec: test.spec,
				Mode: protocol.PrepareEmbeddingTargetRequest_CREATE})
			c.Assert(err, qt.ErrorIs, provider.ErrExecution)
			c.Assert(result.GetRefusal().GetReason(), qt.Equals, test.reason)
		})
	}
	structure, err := client.ReadEmbeddingStructure(c.Context(), &protocol.ReadEmbeddingStructureRequest{SessionId: session, Location: location(columnsSpec())})
	c.Assert(err, qt.IsNil)
	c.Assert(structure.GetColumnExists(), qt.IsFalse)
}

// location is where a specification's generation is, as its record says.
func location(spec *protocol.EmbeddingSpec) *protocol.EmbeddingLocation {
	return &protocol.EmbeddingLocation{Generation: spec.GetGeneration(), Table: spec.GetTarget().GetTable(), Column: spec.GetTarget().GetColumn(),
		Dimension: spec.GetTarget().GetDimension(), Names: spec.GetNames()}
}

// TestIndependentProviderEmbeddingStoreCommitsABatchOnce commits a batch with
// its checkpoint, refuses a second commit on the same token and a state
// mutation on a stale one, and reports what it holds: the structure and the
// count, the verification walk a page at a time, and the nearest keys.
func TestIndependentProviderEmbeddingStoreCommitsABatchOnce(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	spec := columnsSpec()
	prepareTarget(c, client, session, spec, protocol.PrepareEmbeddingTargetRequest_CREATE)
	stale := readEverything(c, client, session)
	recordRun(c, client, session, spec.GetGeneration())

	state, keys := batch(c, client, session, spec, "1", "2", "3")
	c.Assert(state.GetHeld(), qt.HasLen, 0)
	request := commitRequest(session, spec, state, keys, "3", upsert(spec, "1", 0, 1, 0, 0), upsert(spec, "2", 0, 0, 1, 0), upsert(spec, "3", 0, 0, 0, 1))
	committed, err := client.CommitEmbeddingBatch(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(committed.GetWritten().GetRuns()[0].GetCursor(), qt.DeepEquals, []string{"3"})
	request.RequestId = "commit-again"
	_, err = client.CommitEmbeddingBatch(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrEmbeddingConflict)
	_, err = client.MutateEmbeddingState(c.Context(), &protocol.MutateEmbeddingStateRequest{SessionId: session, RequestId: "stale", Scope: stale.GetScope(),
		ExpectedToken: stale.GetToken(), Records: &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{{Identity: "gen-other"}}}})
	c.Assert(err, qt.ErrorIs, provider.ErrEmbeddingConflict)
	held, _ := batch(c, client, session, spec, "1", "4")
	c.Assert(held.GetHeld(), qt.HasLen, 1)
	c.Assert(held.GetHeld()[0].GetInputHash(), qt.Equals, "hash-1")
	c.Assert(readEverything(c, client, session).GetRecords().GetRuns()[0].GetCursor(), qt.DeepEquals, []string{"3"})

	counted := location(spec)
	structure, err := client.ReadEmbeddingStructure(c.Context(), &protocol.ReadEmbeddingStructureRequest{SessionId: session, Location: counted, CountRows: true})
	c.Assert(err, qt.IsNil)
	c.Assert(structure.GetColumnType(), qt.Equals, "BLOB")
	c.Assert(structure.GetRows(), qt.Equals, int64(3))
	c.Assert(structure.GetIndexExists(), qt.IsFalse)

	walk := verificationWalk(c, client, session, spec, 2)
	c.Assert(walk, qt.DeepEquals, []string{"1:3", "2:3", "3:3", "4:0", "5:0"})

	found, err := client.SearchEmbeddings(c.Context(), &protocol.SearchEmbeddingsRequest{SessionId: session, Spec: spec, Vector: []float32{0.1, 0.2, 0.9}, Depth: 2})
	c.Assert(err, qt.IsNil)
	c.Assert(found.GetOutcome(), qt.Equals, protocol.EmbeddingSearchResult_COMPLETED)
	c.Assert(keyComponents(found.GetExact()), qt.DeepEquals, []string{"3", "2"})
	c.Assert(keyComponents(found.GetIndexed()), qt.DeepEquals, []string{"3", "2"})
	c.Assert(found.GetIndexUsed(), qt.IsFalse)
}

// verificationWalk reads the walk a page at a time and renders each row as
// its key and the width of its stored vector.
func verificationWalk(c *qt.C, client *transport.Client, session string, spec *protocol.EmbeddingSpec, rows uint32) []string {
	c.Helper()
	var walk []string
	token := ""
	for range 10 {
		page, err := client.ReadEmbeddingVerification(c.Context(), &protocol.ReadEmbeddingVerificationRequest{SessionId: session, Spec: spec, PageToken: token, MaxRows: rows})
		c.Assert(err, qt.IsNil)
		for _, row := range page.GetRows() {
			walk = append(walk, row.GetKey().GetComponents()[0]+":"+strconv.FormatUint(uint64(row.GetStored().GetDimension()), 10))
		}
		token = page.GetNextPageToken()
		if page.GetComplete() {
			break
		}
	}
	return walk
}

func keyComponents(keys []*protocol.EmbeddingKey) []string {
	components := make([]string, len(keys))
	for i, key := range keys {
		components[i] = strings.Join(key.GetComponents(), ",")
	}
	return components
}

// TestIndependentProviderEmbeddingStoreKeepsASetTogether writes a chunked set
// into a table the generation owns, pages the walk with room for two rows so
// the set of two is never split, and searches a key as near as its nearest
// chunk.
func TestIndependentProviderEmbeddingStoreKeepsASetTogether(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	spec := chunksSpec()
	c.Assert(prepareTarget(c, client, session, spec, protocol.PrepareEmbeddingTargetRequest_CREATE).GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)
	recordRun(c, client, session, spec.GetGeneration())
	state, keys := batch(c, client, session, spec, "1", "2")
	_, err := client.CommitEmbeddingBatch(c.Context(), commitRequest(session, spec, state, keys, "2",
		upsert(spec, "1", 0, 1, 0, 0), upsert(spec, "1", 1, 0, 0, 1), upsert(spec, "2", 0, 0, 1, 0)))
	c.Assert(err, qt.IsNil)

	c.Assert(verificationWalk(c, client, session, spec, 2), qt.DeepEquals, []string{"1:3", "1:3", "2:3", "3:0", "4:0", "5:0"})
	found, err := client.SearchEmbeddings(c.Context(), &protocol.SearchEmbeddingsRequest{SessionId: session, Spec: spec, Vector: []float32{0, 0.1, 1}, Depth: 5})
	c.Assert(err, qt.IsNil)
	c.Assert(keyComponents(found.GetExact()), qt.DeepEquals, []string{"1", "2"})
}

// TestIndependentProviderEmbeddingStoreDropsOnlyWhatItOwns refuses to drop a
// table whose mark names another generation and leaves it in place, then
// drops it for the generation the mark names.
func TestIndependentProviderEmbeddingStoreDropsOnlyWhatItOwns(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	spec := chunksSpec()
	prepareTarget(c, client, session, spec, protocol.PrepareEmbeddingTargetRequest_CREATE)
	other := location(spec)
	other.Generation = "gen-other"
	other.Names = &protocol.EmbeddingNames{Index: other.GetNames().GetIndex(), OwnershipMark: "ptah embedding target for generation gen-other"}

	refused, err := client.DropEmbeddingGeneration(c.Context(), &protocol.DropEmbeddingGenerationRequest{SessionId: session, RequestId: "drop-other", Location: other,
		Storage: protocol.DropEmbeddingGenerationRequest_TABLE})
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(refused.GetRefusal().GetReason(), qt.Equals, protocol.EmbeddingRefusal_NOT_OWNED)
	kept, err := client.ReadEmbeddingStructure(c.Context(), &protocol.ReadEmbeddingStructureRequest{SessionId: session, Location: location(spec)})
	c.Assert(err, qt.IsNil)
	c.Assert(kept.GetTableExists(), qt.IsTrue)

	dropped, err := client.DropEmbeddingGeneration(c.Context(), &protocol.DropEmbeddingGenerationRequest{SessionId: session, RequestId: "drop", Location: location(spec),
		Storage: protocol.DropEmbeddingGenerationRequest_TABLE})
	c.Assert(err, qt.IsNil)
	c.Assert(dropped.GetOutcome(), qt.Equals, protocol.EmbeddingWriteResult_APPLIED)
	gone, err := client.ReadEmbeddingStructure(c.Context(), &protocol.ReadEmbeddingStructureRequest{SessionId: session, Location: location(spec)})
	c.Assert(err, qt.IsNil)
	c.Assert(gone.GetTableExists(), qt.IsFalse)
}

// TestIndependentProviderEmbeddingStoreSequencesEvents appends two events of
// one run, pages them in sequence order, and refuses an event of a run that
// is not recorded.
func TestIndependentProviderEmbeddingStoreSequencesEvents(t *testing.T) {
	c := qt.New(t)
	client := start(c, embeddingOptions(c))
	session := embeddingDatabase(c, client, filepath.Join(c.TempDir(), "database.db"), "")
	seedArticles(c, client, session)
	recordRun(c, client, session, "gen-columns")
	for _, kind := range []string{"prepared", "claimed"} {
		appended, err := client.AppendEmbeddingEvent(c.Context(), &protocol.AppendEmbeddingEventRequest{SessionId: session, RequestId: kind,
			Event: &protocol.EmbeddingEvent{RunId: "run-1", Kind: kind}})
		c.Assert(err, qt.IsNil)
		c.Assert(appended.GetSequence(), qt.Not(qt.Equals), int64(0))
	}
	first, err := client.ReadEmbeddingEvents(c.Context(), &protocol.ReadEmbeddingEventsRequest{SessionId: session, RunId: "run-1", MaxEvents: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(first.GetComplete(), qt.IsFalse)
	rest, err := client.ReadEmbeddingEvents(c.Context(), &protocol.ReadEmbeddingEventsRequest{SessionId: session, RunId: "run-1", AfterSequence: first.GetEvents()[0].GetSequence(), MaxEvents: 5})
	c.Assert(err, qt.IsNil)
	c.Assert(rest.GetComplete(), qt.IsTrue)
	c.Assert([]string{first.GetEvents()[0].GetKind(), rest.GetEvents()[0].GetKind()}, qt.DeepEquals, []string{"prepared", "claimed"})
	c.Assert([]int64{first.GetEvents()[0].GetSequence(), rest.GetEvents()[0].GetSequence()}, qt.DeepEquals, []int64{1, 2})

	orphan, err := client.AppendEmbeddingEvent(c.Context(), &protocol.AppendEmbeddingEventRequest{SessionId: session, RequestId: "orphan",
		Event: &protocol.EmbeddingEvent{RunId: "run-missing", Kind: "prepared"}})
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(orphan.GetRefusal().GetReason(), qt.Equals, protocol.EmbeddingRefusal_ABSENT)
}

// TestIndependentProviderEmbeddingStoreCrashAfterCommit ends the provider
// after a batch committed and before it answered: the host is told the outcome
// is unknown, and a fresh provider on the same file shows the checkpoint and
// the vectors the commit wrote.
func TestIndependentProviderEmbeddingStoreCrashAfterCommit(t *testing.T) {
	c := qt.New(t)
	options := embeddingOptions(c)
	path := filepath.Join(c.TempDir(), "database.db")
	setup := start(c, options)
	seeded := embeddingDatabase(c, setup, path, "")
	seedArticles(c, setup, seeded)
	spec := columnsSpec()
	prepareTarget(c, setup, seeded, spec, protocol.PrepareEmbeddingTargetRequest_CREATE)
	recordRun(c, setup, seeded, spec.GetGeneration())
	setup.Close()

	crashing := start(c, options)
	session := embeddingDatabase(c, crashing, path, "embeddings-crash-after-commit")
	state, keys := batch(c, crashing, session, spec, "1")
	_, err := crashing.CommitEmbeddingBatch(c.Context(), commitRequest(session, spec, state, keys, "1", upsert(spec, "1", 0, 1, 0, 0)))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)

	fresh := start(c, options)
	reopened := embeddingDatabase(c, fresh, path, "")
	c.Assert(readEverything(c, fresh, reopened).GetRecords().GetRuns()[0].GetCursor(), qt.DeepEquals, []string{"1"})
	held, _ := batch(c, fresh, reopened, spec, "1")
	c.Assert(held.GetHeld(), qt.HasLen, 1)
}
