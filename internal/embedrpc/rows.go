package embedrpc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	providerruntime "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedeval"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
	"ptah.run/internal/embedverify"
)

// validateSource refuses a specification that cannot be scanned.
func validateSource(spec embedgen.Spec) error {
	switch {
	case strings.TrimSpace(spec.Source.Table) == "":
		return fmt.Errorf("the specification names no source table")
	case len(spec.Source.KeyFields) == 0:
		return fmt.Errorf("the specification names no key fields, so a scan has nothing to resume after")
	case len(spec.Source.InputFields) == 0:
		return fmt.Errorf("the specification names no input fields, so there is nothing to embed")
	}
	return nil
}

// Source is [embedbackend.Source] over the provider's keyset scan.
type Source struct {
	backend *Backend
	source  *protocol.EmbeddingSource
}

// NewSource returns a source for a specification.
func (b *Backend) NewSource(spec embedgen.Spec) (embedbackend.Source, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	record, err := specRecord(spec)
	if err != nil {
		return nil, err
	}
	return &Source{backend: b, source: record.GetSource()}, nil
}

// rowOf is a source row as the engine reads it: each input field null or text.
func rowOf(record *protocol.EmbeddingSourceRow) embedgen.Row {
	row := embedgen.Row{Key: record.GetKey().GetComponents(), Fields: make([]*string, len(record.GetFields()))}
	for i, field := range record.GetFields() {
		if value, text := field.GetValue().(*protocol.DataValue_StringValue); text {
			row.Fields[i] = &value.StringValue
		}
	}
	return row
}

// Scan returns the rows after a cursor, in key order. A page is at most the
// SDK's bound; the scan resumes after its cursor.
func (s *Source) Scan(ctx context.Context, after []string, limit int) (embedengine.Page, error) {
	if limit <= 0 {
		return embedengine.Page{}, fmt.Errorf("a scan limit of %d would read the whole table into memory", limit)
	}
	request := &protocol.ScanEmbeddingSourceRequest{SessionId: s.backend.session.ID, Source: s.source, MaxRows: uint32(min(limit, provider.MaxEmbeddingRows))} // #nosec G115 -- bounded above
	if len(after) > 0 {
		request.After = key(after)
	}
	page, err := s.backend.session.Store.ScanEmbeddingSource(ctx, request)
	if err != nil {
		return embedengine.Page{}, fmt.Errorf("scan %s: %w", s.source.GetTable().GetName(), err)
	}
	result := embedengine.Page{Cursor: page.GetCursor().GetComponents(), Done: page.GetDone()}
	for _, record := range page.GetRows() {
		result.Rows = append(result.Rows, rowOf(record))
		result.Versions = append(result.Versions, record.GetVersion())
	}
	return result, nil
}

// Current returns the rows still present out of a set of keys, under the
// filter, so a row that stopped qualifying reads as gone too.
func (s *Source) Current(ctx context.Context, keys [][]string) ([]embedgen.Row, []string, error) {
	var requested []*protocol.EmbeddingKey
	seen := make(map[string]bool, len(keys))
	for _, components := range keys {
		if id := embedverify.KeyIdentity(components...); !seen[id] {
			seen[id] = true
			requested = append(requested, key(components))
		}
	}
	var rows []embedgen.Row
	var versions []string
	for chunk := range slices.Chunk(requested, provider.MaxEmbeddingRows) {
		found, err := s.backend.session.Store.ReadEmbeddingSourceRows(ctx, &protocol.ReadEmbeddingSourceRowsRequest{
			SessionId: s.backend.session.ID, Source: s.source, Keys: chunk,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("reread %s: %w", s.source.GetTable().GetName(), err)
		}
		for _, record := range found.GetRows() {
			rows = append(rows, rowOf(record))
			versions = append(versions, record.GetVersion())
		}
	}
	return rows, versions, nil
}

// Target is [embedengine.Target] over the provider's batch commit.
type Target struct {
	backend *Backend
	spec    embedgen.Spec
	record  *protocol.EmbeddingSpec
}

// NewTarget returns a target for a specification.
func (b *Backend) NewTarget(spec embedgen.Spec) (embedengine.Target, error) {
	if len(spec.Source.KeyFields) == 0 {
		return nil, errors.New("the specification names no key fields, so a target row cannot be addressed")
	}
	record, err := specRecord(spec)
	if err != nil {
		return nil, err
	}
	return &Target{backend: b, spec: spec, record: record}, nil
}

// Commit writes a batch's target writes and the run that checkpoints them in
// one compare-and-write over the run and the batch's keys.
//
// The run is decided first, as the in-memory store decides a save: a worker
// whose lease was taken carries a stale token and is refused before any row
// it would write is looked at. What the target holds at the batch's keys is
// read with the run, so [embedrun.ResolveWrite] decides against what is there;
// a concurrent writer of the same rows makes the provider answer CONFLICT, and
// this one reads and decides again.
func (t *Target) Commit(ctx context.Context, writes []embedrun.TargetWrite, run embedrun.Run) error {
	if run.Terminal() {
		return fmt.Errorf("save run %s with target writes: %w: run is %s", run.ID, embedrun.ErrTerminal, run.Status)
	}
	sets := setsByKey(writes)
	keys := make([]*protocol.EmbeddingKey, len(sets))
	for i, set := range sets {
		keys[i] = key(set[0].Key)
	}
	if len(keys) > provider.MaxEmbeddingRows {
		return fmt.Errorf("a batch of %d keys is more than one commit carries, which is %d", len(keys), provider.MaxEmbeddingRows)
	}
	what := "commit a batch of run " + run.ID
	for attempt := range maxDecisions {
		if attempt > 0 {
			if err := pause(ctx, attempt); err != nil {
				return fmt.Errorf("%s: %w", what, err)
			}
		}
		state, err := t.backend.session.Store.ReadEmbeddingBatchState(ctx, &protocol.ReadEmbeddingBatchStateRequest{
			SessionId: t.backend.session.ID, Spec: t.record, RunId: run.ID, Keys: keys,
		})
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		winners, err := t.decide(ctx, state, sets, run)
		if err != nil {
			return err
		}
		request := &protocol.CommitEmbeddingBatchRequest{
			SessionId: t.backend.session.ID, RequestId: requestID(), Spec: t.record, RunId: run.ID, Keys: keys,
			ExpectedToken: state.GetToken(), Run: runRecord(run), Writes: winners,
		}
		// A batch is one atomic commit with its checkpoint, so it travels in one
		// message and is refused before it is sent when it does not fit one: a
		// message the transport refuses would leave the outcome unknown.
		if size := proto.Size(request); size > providerruntime.MaxMessageBytes {
			return fmt.Errorf("%s: the batch is %d bytes, more than the %d bytes one message to the provider carries; "+
				"commit fewer rows per batch", what, size, providerruntime.MaxMessageBytes)
		}
		result, err := t.backend.session.Store.CommitEmbeddingBatch(ctx, request)
		if errors.Is(err, provider.ErrEmbeddingConflict) {
			continue
		}
		return writeError(what, result, err)
	}
	return fmt.Errorf("%s: %w: %w after %d decisions", what, embedstore.ErrConflict, errContended, maxDecisions)
}

// decide applies the fencing check of a save and [embedrun.ResolveWrite] to
// what the batch state holds, and returns the writes of every set that wins.
func (t *Target) decide(ctx context.Context, state *protocol.EmbeddingBatchState, sets [][]embedrun.TargetWrite, run embedrun.Run) ([]*protocol.EmbeddingTargetWrite, error) {
	var snapshot embedstore.Snapshot
	if stored := state.GetRun(); stored != nil {
		snapshot.Runs = []embedrun.Run{runOf(stored)}
	}
	if err := embedstore.NewMemoryFrom(snapshot, clock).SaveRun(ctx, run); err != nil {
		return nil, err
	}
	held := make(map[string]*embedrun.TargetWrite, len(state.GetHeld()))
	for _, target := range state.GetHeld() {
		components := target.GetKey().GetComponents()
		held[embedverify.KeyIdentity(components...)] = &embedrun.TargetWrite{
			Key: components, Generation: target.GetGeneration(), InputHash: target.GetInputHash(),
			Version: target.GetVersion(), Kind: writeKindOf(target.GetKind()),
		}
	}
	var winners []*protocol.EmbeddingTargetWrite
	for _, set := range sets {
		_, wins, err := embedrun.ResolveWrite(held[embedverify.KeyIdentity(set[0].Key...)], set[0], t.spec.Source.VersionStrategy.VersionOrder())
		if err != nil {
			return nil, fmt.Errorf("write %v: %w", set[0].Key, err)
		}
		if !wins {
			continue
		}
		for _, write := range set {
			winners = append(winners, &protocol.EmbeddingTargetWrite{
				Key: key(write.Key), Ordinal: uint32(max(write.Ordinal, 0)), // #nosec G115 -- a chunk's position in its set
				Generation: write.Generation, InputHash: write.InputHash, Version: write.Version,
				Kind: writeKinds[write.Kind], Vector: write.Vector,
			})
		}
	}
	return winners, nil
}

// setsByKey groups a batch's writes into one set per source key, in the order
// the keys first appear, each set in ordinal order.
func setsByKey(writes []embedrun.TargetWrite) [][]embedrun.TargetWrite {
	order := make([]string, 0, len(writes))
	byKey := make(map[string][]embedrun.TargetWrite, len(writes))
	for _, write := range writes {
		id := embedverify.KeyIdentity(write.Key...)
		if _, seen := byKey[id]; !seen {
			order = append(order, id)
		}
		byKey[id] = append(byKey[id], write)
	}
	sets := make([][]embedrun.TargetWrite, 0, len(order))
	for _, id := range order {
		set := byKey[id]
		slices.SortStableFunc(set, func(a, b embedrun.TargetWrite) int { return a.Ordinal - b.Ordinal })
		sets = append(sets, set)
	}
	return sets
}

// Searcher runs an evaluation corpus against a generation.
type Searcher struct {
	backend *Backend
	spec    embedgen.Spec
	record  *protocol.EmbeddingSpec

	once     sync.Once
	settings string
	err      error
}

// NewSearcher returns a searcher for a generation.
func (b *Backend) NewSearcher(spec embedgen.Spec) (embedbackend.Searcher, error) {
	if len(spec.Source.KeyFields) == 0 {
		return nil, fmt.Errorf("the specification names no key fields, so a result cannot be named")
	}
	record, err := specRecord(spec)
	if err != nil {
		return nil, err
	}
	return &Searcher{backend: b, spec: spec, record: record}, nil
}

// QueryParameters reports the settings a search runs under, as the provider
// reads them from the session.
func (s *Searcher) QueryParameters(ctx context.Context) (string, error) {
	s.once.Do(func() {
		structure, err := s.backend.structure(ctx, specLocation(s.spec, s.record), false)
		s.settings, s.err = structure.GetSearchSettings(), err
	})
	return s.settings, s.err
}

// Search runs one case through the index and without it. A search the
// provider could not run is recorded against the case, as a failed query is
// on every engine.
func (s *Searcher) Search(ctx context.Context, testCase embedeval.Case, vector []float32, depth int) (embedeval.Result, error) {
	result := embedeval.Result{CaseID: testCase.ID}
	if depth <= 0 || depth > provider.MaxEmbeddingSearchDepth {
		result.Err = fmt.Sprintf("a search depth of %d is outside 1 to %d", depth, provider.MaxEmbeddingSearchDepth)
		return result, nil
	}
	found, err := s.backend.session.Store.SearchEmbeddings(ctx, &protocol.SearchEmbeddingsRequest{
		SessionId: s.backend.session.ID, Spec: s.record, Vector: vector, Depth: uint32(depth), // #nosec G115 -- bounded above
	})
	switch {
	case err != nil:
		result.Err = fmt.Sprintf("search %s: %v", s.spec.Target.Table, err)
	case found.GetOutcome() == protocol.EmbeddingSearchResult_FAILED:
		result.Err = fmt.Sprintf("search %s: %s", s.spec.Target.Table, found.GetMessage())
	default:
		result.Keys, result.ExactKeys, result.ExactRun = joinedKeys(found.GetIndexed()), joinedKeys(found.GetExact()), true
	}
	return result, nil
}

// joinedKeys renders each key with the separator a corpus file writes, so a
// result matches what a person wrote there.
func joinedKeys(keys []*protocol.EmbeddingKey) []string {
	joined := make([]string, len(keys))
	for i, found := range keys {
		joined[i] = strings.Join(found.GetComponents(), embedverify.KeyFieldSeparator)
	}
	return joined
}
