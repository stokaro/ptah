package provider_test

import (
	"math"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func embeddingKey(components ...string) *protocol.EmbeddingKey {
	return &protocol.EmbeddingKey{Components: components}
}

func sourceRow(tenant, id, version string) *protocol.EmbeddingSourceRow {
	return &protocol.EmbeddingSourceRow{Key: embeddingKey(tenant, id), Fields: []*protocol.DataValue{text("Title"), null()}, Version: version}
}

func scanExchange() (*protocol.ScanEmbeddingSourceRequest, *protocol.EmbeddingSourcePage) {
	return &protocol.ScanEmbeddingSourceRequest{SessionId: "session", Source: embeddingSpec().Source, After: embeddingKey("acme", "40"), MaxRows: 2},
		&protocol.EmbeddingSourcePage{Rows: []*protocol.EmbeddingSourceRow{sourceRow("acme", "41", "7"), sourceRow("acme", "42", "3")},
			Cursor: embeddingKey("acme", "42")}
}

func TestValidateEmbeddingSourcePage_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ScanEmbeddingSourceRequest, *protocol.EmbeddingSourcePage)
	}{
		{"a full page", func(*protocol.ScanEmbeddingSourceRequest, *protocol.EmbeddingSourcePage) {}},
		{"the first page", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) { q.After = nil }},
		{"the last page with no row", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows, r.Cursor, r.Done = nil, nil, true
		}},
		{"a stretch the filter matched nothing in", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows, r.Cursor = nil, embeddingKey("acme", "90")
		}},
		{"a source without a version field", func(q *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			q.Source.VersionField = ""
			r.Rows[0].Version, r.Rows[1].Version = "", ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := scanExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingSourcePage(request, result), qt.IsNil)
		})
	}
}

func TestValidateScanEmbeddingSourceRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ScanEmbeddingSourceRequest)
	}{
		{"no session", func(r *protocol.ScanEmbeddingSourceRequest) { r.SessionId = "" }},
		{"no source", func(r *protocol.ScanEmbeddingSourceRequest) { r.Source, r.After = nil, nil }},
		{"a source without a key", func(r *protocol.ScanEmbeddingSourceRequest) { r.Source.KeyFields, r.After = nil, nil }},
		{"a resume key of another width", func(r *protocol.ScanEmbeddingSourceRequest) { r.After = embeddingKey("40") }},
		{"no bound", func(r *protocol.ScanEmbeddingSourceRequest) { r.MaxRows = 0 }},
		{"a bound above the limit", func(r *protocol.ScanEmbeddingSourceRequest) { r.MaxRows = provider.MaxEmbeddingRows + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := scanExchange()
			test.mutate(request)
			c.Assert(provider.ValidateScanEmbeddingSourceRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingSourcePage_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ScanEmbeddingSourceRequest, *protocol.EmbeddingSourcePage)
	}{
		{"no session", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) { q.SessionId = "" }},
		{"no source", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) { q.Source = nil }},
		{"a resume key of another width", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) {
			q.After = embeddingKey("40")
		}},
		{"no bound", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) { q.MaxRows = 0 }},
		{"a bound above the limit", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) {
			q.MaxRows = provider.MaxEmbeddingRows + 1
		}},
		{"more rows than the bound", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) { q.MaxRows = 1 }},
		{"a missing row", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) { r.Rows[1] = nil }},
		{"a key of another width", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Key = embeddingKey("41")
		}},
		{"a key component that is not text", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Key = embeddingKey("acme", "4\x001")
		}},
		{"one field fewer", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Fields = r.Rows[0].Fields[:1]
		}},
		{"a field that is a number", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Fields[0] = &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: 1}}
		}},
		{"an unset field", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Fields[0] = &protocol.DataValue{}
		}},
		{"a field that is not UTF-8", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Fields[0] = text("\xff")
		}},
		{"a version from a source without a version field", func(q *protocol.ScanEmbeddingSourceRequest, _ *protocol.EmbeddingSourcePage) {
			q.Source.VersionField = ""
		}},
		{"a version that is not text", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[0].Version = "\xff"
		}},
		{"a key listed twice", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Rows[1].Key = embeddingKey("acme", "41")
		}},
		{"a page that is not done and names no cursor", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Cursor = nil
		}},
		{"a cursor of another width", func(_ *protocol.ScanEmbeddingSourceRequest, r *protocol.EmbeddingSourcePage) {
			r.Cursor, r.Done = embeddingKey("42"), true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := scanExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingSourcePage(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := scanExchange()
	c.Assert(provider.ValidateEmbeddingSourcePage(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func rereadExchange() (*protocol.ReadEmbeddingSourceRowsRequest, *protocol.EmbeddingSourceRows) {
	return &protocol.ReadEmbeddingSourceRowsRequest{SessionId: "session", Source: embeddingSpec().Source,
			Keys: []*protocol.EmbeddingKey{embeddingKey("acme", "41"), embeddingKey("acme", "42"), embeddingKey("acme", "43")}},
		&protocol.EmbeddingSourceRows{Rows: []*protocol.EmbeddingSourceRow{sourceRow("acme", "43", "1"), sourceRow("acme", "41", "8")}}
}

func TestValidateEmbeddingSourceRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := rereadExchange()
	c.Assert(provider.ValidateEmbeddingSourceRows(request, result), qt.IsNil)
	result.Rows = nil
	c.Assert(provider.ValidateEmbeddingSourceRows(request, result), qt.IsNil)
}

func TestValidateReadEmbeddingSourceRowsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingSourceRowsRequest)
	}{
		{"no session", func(r *protocol.ReadEmbeddingSourceRowsRequest) { r.SessionId = "" }},
		{"no source", func(r *protocol.ReadEmbeddingSourceRowsRequest) {
			r.Source, r.Keys = nil, []*protocol.EmbeddingKey{embeddingKey()}
		}},
		{"no key", func(r *protocol.ReadEmbeddingSourceRowsRequest) { r.Keys = nil }},
		{"a repeated key", func(r *protocol.ReadEmbeddingSourceRowsRequest) { r.Keys[2] = embeddingKey("acme", "41") }},
		{"more keys than the limit", func(r *protocol.ReadEmbeddingSourceRowsRequest) {
			r.Keys = make([]*protocol.EmbeddingKey, provider.MaxEmbeddingRows+1)
			for i := range r.Keys {
				r.Keys[i] = embeddingKey("acme", strings.Repeat("9", i+1))
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := rereadExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadEmbeddingSourceRowsRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingSourceRows_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingSourceRowsRequest, *protocol.EmbeddingSourceRows)
	}{
		{"no session", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) { q.SessionId = "" }},
		{"no source", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) { q.Source = nil }},
		{"no key", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) { q.Keys = nil }},
		{"a repeated key", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) {
			q.Keys[2] = embeddingKey("acme", "41")
		}},
		{"a key of another width", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) {
			q.Keys[0] = embeddingKey("41")
		}},
		{"more keys than the limit", func(q *protocol.ReadEmbeddingSourceRowsRequest, _ *protocol.EmbeddingSourceRows) {
			q.Keys = make([]*protocol.EmbeddingKey, provider.MaxEmbeddingRows+1)
			for i := range q.Keys {
				q.Keys[i] = embeddingKey("acme", strings.Repeat("9", i+1))
			}
		}},
		{"a row the request did not name", func(_ *protocol.ReadEmbeddingSourceRowsRequest, r *protocol.EmbeddingSourceRows) {
			r.Rows[0] = sourceRow("acme", "44", "1")
		}},
		{"a key answered twice", func(_ *protocol.ReadEmbeddingSourceRowsRequest, r *protocol.EmbeddingSourceRows) {
			r.Rows[1] = sourceRow("acme", "43", "1")
		}},
		{"a row that does not fit its source", func(_ *protocol.ReadEmbeddingSourceRowsRequest, r *protocol.EmbeddingSourceRows) {
			r.Rows[0].Fields = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := rereadExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingSourceRows(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := rereadExchange()
	c.Assert(provider.ValidateEmbeddingSourceRows(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func batchKeys() []*protocol.EmbeddingKey {
	return []*protocol.EmbeddingKey{embeddingKey("acme", "41"), embeddingKey("acme", "42")}
}

func batchStateExchange() (*protocol.ReadEmbeddingBatchStateRequest, *protocol.EmbeddingBatchState) {
	return &protocol.ReadEmbeddingBatchStateRequest{SessionId: "session", Spec: embeddingSpec(), RunId: "run-1", Keys: batchKeys()},
		&protocol.EmbeddingBatchState{RunId: "run-1", Run: runRecord("run-1", "gen-1"), Token: "token-7", Held: []*protocol.EmbeddingHeldTarget{
			{Key: embeddingKey("acme", "42"), Generation: "gen-1", InputHash: "hash-42", Version: "3", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT},
		}}
}

func TestValidateEmbeddingBatchState_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingBatchStateRequest, *protocol.EmbeddingBatchState)
	}{
		{"a run and a held key", func(*protocol.ReadEmbeddingBatchStateRequest, *protocol.EmbeddingBatchState) {}},
		{"a run that is not there", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) { r.Run = nil }},
		{"a batch of no key", func(q *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			q.Keys, r.Held = nil, nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := batchStateExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingBatchState(request, result), qt.IsNil)
		})
	}
}

func TestValidateReadEmbeddingBatchStateRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingBatchStateRequest)
	}{
		{"no session", func(r *protocol.ReadEmbeddingBatchStateRequest) { r.SessionId = "" }},
		{"no specification", func(r *protocol.ReadEmbeddingBatchStateRequest) { r.Spec, r.Keys = nil, nil }},
		{"no run", func(r *protocol.ReadEmbeddingBatchStateRequest) { r.RunId = "" }},
		{"a repeated key", func(r *protocol.ReadEmbeddingBatchStateRequest) { r.Keys[1] = embeddingKey("acme", "41") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := batchStateExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadEmbeddingBatchStateRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingBatchState_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingBatchStateRequest, *protocol.EmbeddingBatchState)
	}{
		{"no session", func(q *protocol.ReadEmbeddingBatchStateRequest, _ *protocol.EmbeddingBatchState) { q.SessionId = "" }},
		{"no session with nothing held", func(q *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			q.SessionId, r.Held = "", nil
		}},
		{"no specification", func(q *protocol.ReadEmbeddingBatchStateRequest, _ *protocol.EmbeddingBatchState) { q.Spec = nil }},
		{"no run", func(q *protocol.ReadEmbeddingBatchStateRequest, _ *protocol.EmbeddingBatchState) { q.RunId = "" }},
		{"a repeated key", func(q *protocol.ReadEmbeddingBatchStateRequest, _ *protocol.EmbeddingBatchState) {
			q.Keys[1] = embeddingKey("acme", "41")
		}},
		{"another run ID", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) { r.RunId = "run-2" }},
		{"another run", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Run = runRecord("run-2", "gen-1")
		}},
		{"an invalid run", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Run.FencingToken = -1
		}},
		{"a held key of another width", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Key = embeddingKey("42")
		}},
		{"a held key of one component that spells a requested one", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Key = embeddingKey("acme\x0042")
		}},
		{"a held key the request did not name", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Key = embeddingKey("acme", "43")
		}},
		{"a key held twice", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held = append(r.Held, r.Held[0])
		}},
		{"a held state without a generation", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Generation = ""
		}},
		{"a held input hash that is not text", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].InputHash = "\xff"
		}},
		{"a held state without a kind", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Kind = protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UNSPECIFIED
		}},
		{"a held state of an unknown kind", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) {
			r.Held[0].Kind = 9
		}},
		{"no token", func(_ *protocol.ReadEmbeddingBatchStateRequest, r *protocol.EmbeddingBatchState) { r.Token = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := batchStateExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingBatchState(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := batchStateExchange()
	c.Assert(provider.ValidateEmbeddingBatchState(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func upsert(key *protocol.EmbeddingKey, ordinal uint32) *protocol.EmbeddingTargetWrite {
	return &protocol.EmbeddingTargetWrite{Key: key, Ordinal: ordinal, Generation: "gen-1", InputHash: "hash", Version: "7",
		Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT, Vector: []float32{0.5, -1, 2}}
}

func tombstone(key *protocol.EmbeddingKey) *protocol.EmbeddingTargetWrite {
	return &protocol.EmbeddingTargetWrite{Key: key, Generation: "gen-1", Version: "8", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE}
}

func commitExchange() (*protocol.CommitEmbeddingBatchRequest, *protocol.EmbeddingWriteResult) {
	run := runRecord("run-1", "gen-1")
	return &protocol.CommitEmbeddingBatchRequest{SessionId: "session", RequestId: "commit-1", Spec: embeddingSpec(), RunId: "run-1",
			Keys: batchKeys(), ExpectedToken: "token-7", Run: run,
			Writes: []*protocol.EmbeddingTargetWrite{upsert(embeddingKey("acme", "41"), 0), tombstone(embeddingKey("acme", "42"))}},
		&protocol.EmbeddingWriteResult{RequestId: "commit-1", Outcome: protocol.EmbeddingWriteResult_APPLIED,
			Written: &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1")}}}
}

// chunkedCommit writes a set of three chunks for one key and a skip for
// another, into a table of the generation's own.
func chunkedCommit(request *protocol.CommitEmbeddingBatchRequest) {
	request.Spec = chunkedSpec()
	first := embeddingKey("acme", "41")
	skip := &protocol.EmbeddingTargetWrite{Key: embeddingKey("acme", "42"), Generation: "gen-1", InputHash: "hash",
		Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP}
	request.Writes = []*protocol.EmbeddingTargetWrite{upsert(first, 0), upsert(first, 1), upsert(first, 2), skip}
	for _, write := range request.Writes {
		write.Version = ""
	}
}

func TestValidateCommittedEmbeddingBatch_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.CommitEmbeddingBatchRequest, *protocol.EmbeddingWriteResult)
	}{
		{"an upsert and a tombstone", func(*protocol.CommitEmbeddingBatchRequest, *protocol.EmbeddingWriteResult) {}},
		{"a chunked set and a skip", func(q *protocol.CommitEmbeddingBatchRequest, _ *protocol.EmbeddingWriteResult) { chunkedCommit(q) }},
		{"a checkpoint with no write", func(q *protocol.CommitEmbeddingBatchRequest, _ *protocol.EmbeddingWriteResult) {
			q.Keys, q.Writes = nil, nil
		}},
		{"a conflict", func(_ *protocol.CommitEmbeddingBatchRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written = protocol.EmbeddingWriteResult_CONFLICT, nil
		}},
		{"a write the index would not take", func(_ *protocol.CommitEmbeddingBatchRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, nil, refusal(protocol.EmbeddingRefusal_UNSUPPORTED)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := commitExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateCommittedEmbeddingBatch(request, result), qt.IsNil)
		})
	}
}

func TestValidateCommitEmbeddingBatchRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.CommitEmbeddingBatchRequest)
	}{
		{"no session", func(r *protocol.CommitEmbeddingBatchRequest) { r.SessionId = "" }},
		{"no request ID", func(r *protocol.CommitEmbeddingBatchRequest) { r.RequestId = "" }},
		{"no specification", func(r *protocol.CommitEmbeddingBatchRequest) { r.Spec = nil }},
		{"no run ID", func(r *protocol.CommitEmbeddingBatchRequest) { r.RunId = "" }},
		{"a repeated key", func(r *protocol.CommitEmbeddingBatchRequest) { r.Keys[1] = embeddingKey("acme", "41") }},
		{"no expected token", func(r *protocol.CommitEmbeddingBatchRequest) { r.ExpectedToken = "" }},
		{"no run", func(r *protocol.CommitEmbeddingBatchRequest) { r.Run = nil }},
		{"an invalid run", func(r *protocol.CommitEmbeddingBatchRequest) { r.Run.FencingToken = -1 }},
		{"another run", func(r *protocol.CommitEmbeddingBatchRequest) { r.Run.Id = "run-2" }},
		{"a run of another generation", func(r *protocol.CommitEmbeddingBatchRequest) { r.Run.GenerationIdentity = "gen-2" }},
		{"a missing write", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[1] = nil }},
		{"a write key of another width", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Key = embeddingKey("41") }},
		{"no session for a checkpoint with no write", func(r *protocol.CommitEmbeddingBatchRequest) { r.SessionId, r.Keys, r.Writes = "", nil, nil }},
		{"a write key of one component that spells a read one", func(r *protocol.CommitEmbeddingBatchRequest) {
			r.Writes[0].Key = embeddingKey("acme\x0041")
		}},
		{"a write to a key the state was not read over", func(r *protocol.CommitEmbeddingBatchRequest) {
			r.Writes[1].Key = embeddingKey("acme", "43")
		}},
		{"a write of another generation", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Generation = "gen-2" }},
		{"a write without a kind", func(r *protocol.CommitEmbeddingBatchRequest) {
			r.Writes[0].Kind = protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UNSPECIFIED
		}},
		{"a write of an unknown kind", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[1].Kind = 9 }},
		{"a write input hash that is not text", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].InputHash = "\xff" }},
		{"an upsert of another width", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Vector = []float32{1, 2} }},
		{"an upsert without a vector", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Vector = nil }},
		{"an upsert holding NaN", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Vector[1] = float32(math.NaN()) }},
		{"an upsert holding an infinity", func(r *protocol.CommitEmbeddingBatchRequest) {
			r.Writes[0].Vector[2] = float32(math.Inf(-1))
		}},
		{"a tombstone with a vector", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[1].Vector = []float32{1, 2, 3} }},
		{"a set that starts at ordinal 1", func(r *protocol.CommitEmbeddingBatchRequest) { r.Writes[0].Ordinal = 1 }},
		{"a set of two where the specification does not chunk", func(r *protocol.CommitEmbeddingBatchRequest) {
			r.Writes[1] = upsert(embeddingKey("acme", "41"), 1)
		}},
		{"a chunked set with a gap", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[2].Ordinal = 3
		}},
		{"a chunked set out of order", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[1].Ordinal, r.Writes[2].Ordinal = 2, 1
		}},
		{"a chunked set whose members differ in input hash", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[2].InputHash = "other"
		}},
		{"a chunked set whose members differ in version", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[1].Version = "9"
		}},
		{"a chunked set whose members differ in kind", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[2].Kind, r.Writes[2].Vector = protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP, nil
		}},
		{"a set of two tombstones", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			second := tombstone(embeddingKey("acme", "42"))
			second.Ordinal, second.InputHash = 1, "hash"
			r.Writes[3].Kind = protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE
			r.Writes[3].Version = second.Version
			r.Writes = append(r.Writes, second)
		}},
		{"a key whose writes are not adjacent", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes = append(r.Writes, upsert(embeddingKey("acme", "41"), 0))
			r.Writes[4].Version = ""
		}},
		{"a chunk past the first holding NaN", func(r *protocol.CommitEmbeddingBatchRequest) {
			chunkedCommit(r)
			r.Writes[1].Vector[0] = float32(math.NaN())
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := commitExchange()
			test.mutate(request)
			c.Assert(provider.ValidateCommitEmbeddingBatchRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateCommittedEmbeddingBatch_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingWriteResult)
	}{
		{"another request", func(r *protocol.EmbeddingWriteResult) { r.RequestId = "commit-2" }},
		{"applied without the run", func(r *protocol.EmbeddingWriteResult) { r.Written = nil }},
		{"another run written", func(r *protocol.EmbeddingWriteResult) { r.Written.Runs[0] = runRecord("run-2", "gen-1") }},
		{"a generation written", func(r *protocol.EmbeddingWriteResult) {
			r.Written.Generations = []*protocol.EmbeddingGeneration{generationRecord("gen-1")}
		}},
		{"no record written", func(r *protocol.EmbeddingWriteResult) { r.Written.Runs = nil }},
		{"an invalid written run", func(r *protocol.EmbeddingWriteResult) { r.Written.Runs[0].FencingToken = -1 }},
		{"a sequence", func(r *protocol.EmbeddingWriteResult) { r.Sequence = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := commitExchange()
			test.mutate(result)
			c.Assert(provider.ValidateCommittedEmbeddingBatch(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func verificationExchange() (*protocol.ReadEmbeddingVerificationRequest, *protocol.EmbeddingVerificationPage) {
	source := &protocol.EmbeddingVerificationSource{Fields: []*protocol.DataValue{text("Title"), text("Body")}, Version: "7"}
	stored := func(ordinal uint32) *protocol.EmbeddingStoredRow {
		return &protocol.EmbeddingStoredRow{Generation: "gen-1", InputHash: "hash", Version: "7",
			Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT, Dimension: 3, Ordinal: ordinal}
	}
	return &protocol.ReadEmbeddingVerificationRequest{SessionId: "session", Spec: embeddingSpec(), PageToken: "after:acme/40", MaxRows: 4},
		&protocol.EmbeddingVerificationPage{NextPageToken: "after:acme/43", Rows: []*protocol.EmbeddingVerificationRow{
			{Key: embeddingKey("acme", "41"), Source: source, Stored: stored(0)},
			{Key: embeddingKey("acme", "41"), Source: source, Stored: stored(1)},
			{Key: embeddingKey("acme", "42"), Source: source},
			{Key: embeddingKey("acme", "43"), Stored: &protocol.EmbeddingStoredRow{Generation: "gen-1", Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE}},
		}}
}

func TestValidateEmbeddingVerificationPage_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingVerificationPage)
	}{
		{"a page with more to follow", func(*protocol.EmbeddingVerificationPage) {}},
		{"the last page", func(r *protocol.EmbeddingVerificationPage) { r.NextPageToken, r.Complete = "", true }},
		{"a row nothing wrote", func(r *protocol.EmbeddingVerificationPage) {
			r.Rows[2].Stored = &protocol.EmbeddingStoredRow{}
		}},
		{"a tombstone that still holds a vector", func(r *protocol.EmbeddingVerificationPage) { r.Rows[3].Stored.Dimension = 3 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := verificationExchange()
			test.mutate(result)
			c.Assert(provider.ValidateEmbeddingVerificationPage(request, result), qt.IsNil)
		})
	}
}

func TestValidateReadEmbeddingVerificationRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingVerificationRequest)
	}{
		{"no session", func(r *protocol.ReadEmbeddingVerificationRequest) { r.SessionId = "" }},
		{"no specification", func(r *protocol.ReadEmbeddingVerificationRequest) { r.Spec = nil }},
		{"a page token that is not UTF-8", func(r *protocol.ReadEmbeddingVerificationRequest) { r.PageToken = "\xff" }},
		{"no bound", func(r *protocol.ReadEmbeddingVerificationRequest) { r.MaxRows = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := verificationExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadEmbeddingVerificationRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingVerificationPage_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingVerificationRequest, *protocol.EmbeddingVerificationPage)
	}{
		{"no session", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.SessionId = ""
		}},
		{"no specification", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.Spec = nil
		}},
		{"a page token above the limit", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.PageToken = strings.Repeat("t", 4097)
		}},
		{"a page token that is not UTF-8", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.PageToken = "\xff"
		}},
		{"no bound", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.MaxRows = 0
		}},
		{"more rows than the bound", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.MaxRows = 3
		}},
		{"complete with a next page token", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Complete = true
		}},
		{"incomplete without a next page token", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.NextPageToken = ""
		}},
		{"a next page token above the limit", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.NextPageToken = strings.Repeat("t", 4097)
		}},
		{"a next page token that is not UTF-8", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.NextPageToken = "\xff"
		}},
		{"a missing row", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[2] = nil
		}},
		{"a row with neither side", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[2].Source = nil
		}},
		{"a key of another width", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[3].Key = embeddingKey("43")
		}},
		{"a source side with one field fewer", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[2].Source = &protocol.EmbeddingVerificationSource{Fields: []*protocol.DataValue{text("Title")}}
		}},
		{"a source version from a source without a version field", func(q *protocol.ReadEmbeddingVerificationRequest, _ *protocol.EmbeddingVerificationPage) {
			q.Spec.Source.VersionField = ""
		}},
		{"a stored kind that is unknown", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[3].Stored.Kind = 9
		}},
		{"a stored field that is not text", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[3].Stored.Generation = "\x00"
		}},
		{"a key whose rows are not adjacent", func(_ *protocol.ReadEmbeddingVerificationRequest, r *protocol.EmbeddingVerificationPage) {
			r.Rows[3].Key = embeddingKey("acme", "41")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := verificationExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingVerificationPage(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := verificationExchange()
	c.Assert(provider.ValidateEmbeddingVerificationPage(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func searchExchange() (*protocol.SearchEmbeddingsRequest, *protocol.EmbeddingSearchResult) {
	return &protocol.SearchEmbeddingsRequest{SessionId: "session", Spec: embeddingSpec(), Vector: []float32{0.1, 0.2, 0.3}, Depth: 2},
		&protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_COMPLETED, IndexUsed: true,
			Indexed: []*protocol.EmbeddingKey{embeddingKey("acme", "41"), embeddingKey("acme", "42")},
			Exact:   []*protocol.EmbeddingKey{embeddingKey("acme", "42"), embeddingKey("acme", "41")}}
}

func TestValidateEmbeddingSearchResult_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SearchEmbeddingsRequest, *protocol.EmbeddingSearchResult)
	}{
		{"through the index", func(*protocol.SearchEmbeddingsRequest, *protocol.EmbeddingSearchResult) {}},
		{"without an index", func(q *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			q.Spec.Target.IndexMethod, q.Spec.Target.IndexOptions, r.IndexUsed = "", nil, false
		}},
		{"a failed search", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome, r.Message, r.Indexed, r.Exact, r.IndexUsed = protocol.EmbeddingSearchResult_FAILED, "the index is not ready", nil, nil, false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := searchExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingSearchResult(request, result), qt.IsNil)
		})
	}
}

func TestValidateSearchEmbeddingsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SearchEmbeddingsRequest)
	}{
		{"no session", func(r *protocol.SearchEmbeddingsRequest) { r.SessionId = "" }},
		{"no specification", func(r *protocol.SearchEmbeddingsRequest) { r.Spec, r.Vector = nil, nil }},
		{"a vector of another width", func(r *protocol.SearchEmbeddingsRequest) { r.Vector = []float32{1} }},
		{"no depth", func(r *protocol.SearchEmbeddingsRequest) { r.Depth = 0 }},
		{"a depth above the limit", func(r *protocol.SearchEmbeddingsRequest) { r.Depth = provider.MaxEmbeddingSearchDepth + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := searchExchange()
			test.mutate(request)
			c.Assert(provider.ValidateSearchEmbeddingsRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingSearchResult_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.SearchEmbeddingsRequest, *protocol.EmbeddingSearchResult)
	}{
		{"no session", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) { q.SessionId = "" }},
		{"no specification", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) { q.Spec = nil }},
		{"a vector of another width", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) {
			q.Vector = []float32{1}
		}},
		{"a vector holding NaN", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) {
			q.Vector[0] = float32(math.NaN())
		}},
		{"no depth", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) { q.Depth = 0 }},
		{"a depth above the limit", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) {
			q.Depth = provider.MaxEmbeddingSearchDepth + 1
		}},
		{"no outcome", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome = protocol.EmbeddingSearchResult_OUTCOME_UNSPECIFIED
		}},
		{"a completed search with a reason", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Message = "slow"
		}},
		{"an index the specification does not declare", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) {
			q.Spec.Target.IndexMethod, q.Spec.Target.IndexOptions = "", nil
		}},
		{"more indexed keys than the depth", func(q *protocol.SearchEmbeddingsRequest, _ *protocol.EmbeddingSearchResult) { q.Depth = 1 }},
		{"an exact key of another width", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Exact[1] = embeddingKey("41")
		}},
		{"an indexed key listed twice", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Indexed[1] = embeddingKey("acme", "41")
		}},
		{"more exact keys than the depth", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Exact = append(r.Exact, embeddingKey("acme", "43"))
		}},
		{"a failed search without a reason", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome, r.Indexed, r.Exact, r.IndexUsed = protocol.EmbeddingSearchResult_FAILED, nil, nil, false
		}},
		{"a failed search with keys", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome, r.Message, r.Exact, r.IndexUsed = protocol.EmbeddingSearchResult_FAILED, "timed out", nil, false
		}},
		{"a failed search with exact keys", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome, r.Message, r.Indexed, r.IndexUsed = protocol.EmbeddingSearchResult_FAILED, "timed out", nil, false
		}},
		{"a failed search through an index", func(_ *protocol.SearchEmbeddingsRequest, r *protocol.EmbeddingSearchResult) {
			r.Outcome, r.Message, r.Indexed, r.Exact = protocol.EmbeddingSearchResult_FAILED, "timed out", nil, nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := searchExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingSearchResult(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := searchExchange()
	c.Assert(provider.ValidateEmbeddingSearchResult(request, nil), qt.ErrorIs, provider.ErrProtocol)
}
