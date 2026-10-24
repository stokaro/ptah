package embedrpc_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/internal/embedeval"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrpc"
	"ptah.run/internal/embedverify"
)

// fakeRows answers the calls about rows, vectors and target objects with what
// a test sets, and records what the backend asked.
type fakeRows struct {
	provider.EmbeddingStore

	objects  *protocol.EmbeddingTargetObjects
	search   *protocol.EmbeddingSearchResult
	searched int
	reread   [][]*protocol.EmbeddingKey
	pages    []*protocol.EmbeddingVerificationPage
}

func (f *fakeRows) DescribeEmbeddingTarget(context.Context, *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error) {
	return f.objects, nil
}

func (f *fakeRows) SearchEmbeddings(context.Context, *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error) {
	f.searched++
	return f.search, nil
}

func (f *fakeRows) ReadEmbeddingSourceRows(_ context.Context, request *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error) {
	f.reread = append(f.reread, request.GetKeys())
	return &protocol.EmbeddingSourceRows{}, nil
}

func (f *fakeRows) ReadEmbeddingVerification(context.Context, *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error) {
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func keyed(components ...string) *protocol.EmbeddingKey {
	return &protocol.EmbeddingKey{Components: components}
}

// TestTargetObjects_HappyPath reads the engine's answer into the objects a
// generation needs, with the names the host derives.
func TestTargetObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	spec.Target.IndexMethod = "flat"
	store := &fakeRows{objects: &protocol.EmbeddingTargetObjects{ColumnType: "BLOB", HasIndex: true, IndexMethod: "flat", IndexOperator: "distance=cosine",
		RequiredExtensions: []string{"vectors"}}}

	objects, err := embedrpc.TargetObjects(c.Context(), store, "fixture", spec)

	c.Assert(err, qt.IsNil)
	c.Assert(objects.Column.Type, qt.Equals, "BLOB")
	c.Assert(objects.Column.Comment, qt.Equals, spec.ColumnComment())
	c.Assert(objects.HasIndex, qt.IsTrue)
	c.Assert(objects.Index.Name, qt.Equals, embedgen.IndexName("articles", "embedding", spec.Identity().Digest))
	c.Assert(objects.Index.Type, qt.Equals, "flat")
	c.Assert(objects.Index.Operator, qt.Equals, "distance=cosine")
	c.Assert(objects.RequiredExtensions, qt.DeepEquals, []string{"vectors"})
	c.Assert(objects.OwnsTable, qt.IsFalse)
}

// TestTargetObjects_FailurePath is the engine's refusal, named, and nothing
// derived.
func TestTargetObjects_FailurePath(t *testing.T) {
	c := qt.New(t)
	store := &fakeRows{objects: &protocol.EmbeddingTargetObjects{Refusal: &protocol.EmbeddingRefusal{
		Reason: protocol.EmbeddingRefusal_UNSUPPORTED, Message: "the fixture builds no vector index"}}}

	objects, err := embedrpc.TargetObjects(c.Context(), store, "fixture", targetSpec())

	c.Assert(err, qt.ErrorIs, embedrpc.ErrUnsupported)
	c.Assert(err, qt.ErrorMatches, `.*engine "fixture" cannot hold this specification \(UNSUPPORTED\): the fixture builds no vector index`)
	c.Assert(objects, qt.DeepEquals, embedgen.TargetObjects{})
}

func searcher(c *qt.C, store *fakeRows) interface {
	Search(context.Context, embedeval.Case, []float32, int) (embedeval.Result, error)
} {
	c.Helper()
	found, err := backendOver(store).NewSearcher(targetSpec())
	c.Assert(err, qt.IsNil)
	return found
}

// TestSearcher_Search_HappyPath answers both lists of a completed search,
// each key joined the way a corpus file writes it.
func TestSearcher_Search_HappyPath(t *testing.T) {
	c := qt.New(t)
	store := &fakeRows{search: &protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_COMPLETED,
		Indexed: []*protocol.EmbeddingKey{keyed("1", "a"), keyed("2", "b")}, Exact: []*protocol.EmbeddingKey{keyed("2", "b")}}}

	result, err := searcher(c, store).Search(c.Context(), embedeval.Case{ID: "pricing"}, []float32{1, 0}, 2)

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, embedeval.Result{CaseID: "pricing", Keys: []string{"1" + embedverify.KeyFieldSeparator + "a", "2" + embedverify.KeyFieldSeparator + "b"},
		ExactKeys: []string{"2" + embedverify.KeyFieldSeparator + "b"}, ExactRun: true})
}

// TestSearcher_Search_FailurePath records a search that could not run
// against its case rather than failing the evaluation, and asks the provider
// nothing for a depth it would refuse.
func TestSearcher_Search_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		depth    int
		want     string
		searched int
	}{
		{name: "a search the provider failed", depth: 2, want: "search articles: the index is not ready", searched: 1},
		{name: "no depth", depth: 0, want: "a search depth of 0 is outside 1 to 10000"},
		{name: "a depth past the bound", depth: provider.MaxEmbeddingSearchDepth + 1, want: "a search depth of 10001 is outside 1 to 10000"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			store := &fakeRows{search: &protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_FAILED, Message: "the index is not ready"}}

			result, err := searcher(c, store).Search(c.Context(), embedeval.Case{ID: "pricing"}, []float32{1, 0}, test.depth)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, embedeval.Result{CaseID: "pricing", Err: test.want})
			c.Assert(store.searched, qt.Equals, test.searched)
		})
	}
}

// TestSource_CurrentAsksForEachKeyOnce rereads a key the changes name twice
// once: the service refuses a repeated key.
func TestSource_CurrentAsksForEachKeyOnce(t *testing.T) {
	c := qt.New(t)
	store := &fakeRows{}
	source, err := backendOver(store).NewSource(targetSpec())
	c.Assert(err, qt.IsNil)

	_, _, err = source.Current(c.Context(), [][]string{{"1"}, {"2"}, {"1"}})

	c.Assert(err, qt.IsNil)
	c.Assert(store.reread, qt.HasLen, 1)
	c.Assert(keyComponents(store.reread[0]), qt.DeepEquals, []string{"1", "2"})
}

// TestBackend_VerificationCorpus reads each page of the walk into the pairs
// verification folds over: the source side's input hash computed by the host,
// a skip and a tombstone told apart, and a stored row without a source.
func TestBackend_VerificationCorpus(t *testing.T) {
	c := qt.New(t)
	spec := targetSpec()
	text := &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: "pricing"}}
	store := &fakeRows{pages: []*protocol.EmbeddingVerificationPage{
		{Rows: []*protocol.EmbeddingVerificationRow{
			{Key: keyed("1"), Source: &protocol.EmbeddingVerificationSource{Fields: []*protocol.DataValue{text}},
				Stored: &protocol.EmbeddingStoredRow{Generation: "g", InputHash: "h", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT, Dimension: 2}},
			{Key: keyed("2"), Stored: &protocol.EmbeddingStoredRow{Generation: "g", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP}},
		}, NextPageToken: "2"},
		{Rows: []*protocol.EmbeddingVerificationRow{
			{Key: keyed("3"), Stored: &protocol.EmbeddingStoredRow{Generation: "g", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE, Ordinal: 0}},
		}, Complete: true},
	}}
	corpus, err := backendOver(store).VerificationCorpus(c.Context(), spec)
	c.Assert(err, qt.IsNil)

	var walked []string
	for pair, err := range corpus {
		c.Assert(err, qt.IsNil)
		walked = append(walked, describePair(pair))
	}

	set, err := spec.CanonicalInputs(embedgen.Row{Key: []string{"1"}, Fields: []*string{new("pricing")}})
	c.Assert(err, qt.IsNil)
	c.Assert(walked, qt.DeepEquals, []string{
		"source " + spec.SourceInputHash(set.Whole) + " target g h 2 upsert",
		"target g  0 skip",
		"target g  0 tombstone",
	})
}

// describePair renders a pair as the sides it has.
func describePair(pair embedverify.Pair) string {
	var parts []string
	if pair.Source != nil {
		parts = append(parts, "source "+pair.Source.InputHash)
	}
	if pair.Target != nil {
		kind := "upsert"
		if pair.Target.Skipped {
			kind = "skip"
		}
		if pair.Target.Tombstone {
			kind = "tombstone"
		}
		parts = append(parts, "target "+pair.Target.Generation+" "+pair.Target.InputHash+" "+strconv.Itoa(pair.Target.Dimension)+" "+kind)
	}
	return strings.Join(parts, " ")
}
