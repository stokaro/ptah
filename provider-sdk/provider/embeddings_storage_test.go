package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func embeddingTable(name string) *protocol.Reference {
	return &protocol.Reference{Engine: "vectors", Schema: "app", Kind: "table", Name: name}
}

// embeddingSpec is a specification over a source with a composite key, a
// version field and a filter, stored beside the source rows and indexed.
func embeddingSpec() *protocol.EmbeddingSpec {
	return &protocol.EmbeddingSpec{
		Generation: "gen-1",
		Source: &protocol.EmbeddingSource{Table: embeddingTable("documents"), KeyFields: []string{"tenant", "id"},
			InputFields: []string{"title", "body"}, VersionField: "version", Filter: "deleted = false"},
		Target: &protocol.EmbeddingTarget{Table: embeddingTable("documents"), Column: "embedding",
			Layout: protocol.EmbeddingTarget_SOURCE_COLUMNS, Representation: "float", Metric: protocol.EmbeddingTarget_COSINE, Dimension: 3,
			IndexMethod: "vector_kmeans_tree", IndexOptions: map[string]string{"levels": "1", "clusters": "4"}},
		Names: &protocol.EmbeddingNames{Index: "documents_embedding_gen1_idx", ColumnComment: "embedding generation gen-1"},
	}
}

// chunkedSpec is a specification that keeps a set of rows per source key in a
// table of its own, with no index and no version field.
func chunkedSpec() *protocol.EmbeddingSpec {
	spec := embeddingSpec()
	spec.Chunked = true
	spec.Source.VersionField = ""
	spec.Target.Table = embeddingTable("document_chunks")
	spec.Target.Layout = protocol.EmbeddingTarget_OWN_TABLE
	spec.Target.IndexMethod, spec.Target.IndexOptions = "", nil
	spec.Names = &protocol.EmbeddingNames{OwnershipMark: "ptah embedding target for generation gen-1"}
	return spec
}

// onlyRefusal clears every object a target description can carry.
func onlyRefusal(objects *protocol.EmbeddingTargetObjects) {
	objects.ColumnType, objects.OwnsTable, objects.ReferencesSource, objects.HasIndex = "", false, false, false
	objects.IndexMethod, objects.IndexOperator, objects.RequiredExtensions = "", "", nil
	objects.Refusal = refusal(protocol.EmbeddingRefusal_UNSUPPORTED)
}

func describeExchange() (*protocol.DescribeEmbeddingTargetRequest, *protocol.EmbeddingTargetObjects) {
	return &protocol.DescribeEmbeddingTargetRequest{Engine: "vectors", Spec: embeddingSpec()},
		&protocol.EmbeddingTargetObjects{ColumnType: "String", HasIndex: true, IndexMethod: "vector_kmeans_tree", IndexOperator: "distance=cosine"}
}

func TestValidateEmbeddingTargetObjects_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.DescribeEmbeddingTargetRequest, *protocol.EmbeddingTargetObjects)
	}{
		{"an indexed column beside the source", func(*protocol.DescribeEmbeddingTargetRequest, *protocol.EmbeddingTargetObjects) {}},
		{"an owned table that references its source", func(q *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			q.Spec = chunkedSpec()
			r.OwnsTable, r.ReferencesSource, r.HasIndex, r.IndexMethod, r.IndexOperator = true, true, false, "", ""
			r.RequiredExtensions = []string{"vector"}
		}},
		{"a refusal", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) { onlyRefusal(r) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := describeExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingTargetObjects(embeddingDescription(), request, result), qt.IsNil)
		})
	}
}

// Every request that carries a specification refuses the same malformed
// specifications; the offline target description stands in for them here.
func TestValidateDescribeEmbeddingTargetRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.DescribeEmbeddingTargetRequest)
	}{
		{"an engine the provider does not own", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Engine = "rows" }},
		{"no specification", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec = nil }},
		{"no generation", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Generation = "" }},
		{"no source", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source = nil }},
		{"no source table", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.Table = nil }},
		{"a source that is not a table", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.Table.Kind = "view" }},
		{"a source schema that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.Table.Schema = "a\x00" }},
		{"a source name that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.Table.Name = "\xff" }},
		{"no key field", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.KeyFields = nil }},
		{"a repeated key field", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.KeyFields = []string{"id", "id"} }},
		{"an empty key field", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.KeyFields = []string{""} }},
		{"no input field", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.InputFields = nil }},
		{"a version field that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.VersionField = "v\x00" }},
		{"a filter that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Source.Filter = "\xff" }},
		{"no target", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target = nil }},
		{"no target table", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Table = nil }},
		{"no target column", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Column = "" }},
		{"no representation", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Representation = "" }},
		{"no layout", func(r *protocol.DescribeEmbeddingTargetRequest) {
			r.Spec.Target.Layout = protocol.EmbeddingTarget_LAYOUT_UNSPECIFIED
		}},
		{"an unknown layout", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Layout = 9 }},
		{"no metric", func(r *protocol.DescribeEmbeddingTargetRequest) {
			r.Spec.Target.Metric = protocol.EmbeddingTarget_METRIC_UNSPECIFIED
		}},
		{"an unknown metric", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Metric = 9 }},
		{"no dimension", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.Dimension = 0 }},
		{"a dimension above the limit", func(r *protocol.DescribeEmbeddingTargetRequest) {
			r.Spec.Target.Dimension = provider.MaxEmbeddingDimension + 1
		}},
		{"an index method that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.IndexMethod = "a\x00" }},
		{"an unnamed index option", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Target.IndexOptions[""] = "1" }},
		{"an index option value that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) {
			r.Spec.Target.IndexOptions["levels"] = "\xff"
		}},
		{"a chunked generation beside the source", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Chunked = true }},
		{"an index without a name", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Names.Index = "" }},
		{"an owned table without its mark", func(r *protocol.DescribeEmbeddingTargetRequest) {
			r.Spec = chunkedSpec()
			r.Spec.Names.OwnershipMark = ""
		}},
		{"a column comment that is not text", func(r *protocol.DescribeEmbeddingTargetRequest) { r.Spec.Names.ColumnComment = "\x00" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := describeExchange()
			test.mutate(request)
			c.Assert(provider.ValidateDescribeEmbeddingTargetRequest(embeddingDescription(), request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that did not negotiate the store, and a description whose engine
// keeps none, refuse the call before it is sent.
func TestValidateDescribeEmbeddingTargetRequest_RefusesAnUnnegotiatedStore(t *testing.T) {
	c := qt.New(t)
	request, _ := describeExchange()
	c.Assert(provider.ValidateDescribeEmbeddingTargetRequest(embeddingDescription(), nil), qt.ErrorIs, provider.ErrProtocol)
	unlisted := provider.NegotiatedDescription(embeddingDescription(), nil)
	c.Assert(provider.ValidateDescribeEmbeddingTargetRequest(unlisted, request), qt.ErrorMatches, `.*embeddings\.store was not negotiated`)
	storeless := embeddingDescription()
	storeless.Engines[0].Features, storeless.Engines[0].Embeddings = nil, nil
	c.Assert(provider.ValidateDescribeEmbeddingTargetRequest(storeless, request), qt.ErrorMatches, `.*engine "vectors" keeps no embedding store`)
}

func TestValidateEmbeddingTargetObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.DescribeEmbeddingTargetRequest, *protocol.EmbeddingTargetObjects)
	}{
		{"a refusal that carries a column type", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.ColumnType = "String"
		}},
		{"a refusal that carries an owned table", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.OwnsTable = true
		}},
		{"a refusal that carries a source reference", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.ReferencesSource = true
		}},
		{"a refusal that carries an index", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.HasIndex = true
		}},
		{"a refusal that carries an index method", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.IndexMethod = "hnsw"
		}},
		{"a refusal that carries an operator", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.IndexOperator = "vector_cosine_ops"
		}},
		{"a refusal that carries an extension", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.RequiredExtensions = []string{"vector"}
		}},
		{"a refusal without a reason", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			onlyRefusal(r)
			r.Refusal.Reason = protocol.EmbeddingRefusal_REASON_UNSPECIFIED
		}},
		{"no column type", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.ColumnType = ""
		}},
		{"an owned table beside the source", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.OwnsTable = true
		}},
		{"a source reference from a table the generation does not own", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.ReferencesSource = true
		}},
		{"no owned table for a layout that owns one", func(q *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			q.Spec = chunkedSpec()
			r.HasIndex, r.IndexMethod, r.IndexOperator = false, "", ""
		}},
		{"no index for a declared method", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.HasIndex = false
		}},
		{"no index method for a declared index", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.IndexMethod = ""
		}},
		{"an index the specification does not declare", func(q *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			q.Spec = chunkedSpec()
			r.OwnsTable = true
		}},
		{"an operator without an index", func(q *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			q.Spec = chunkedSpec()
			r.OwnsTable, r.HasIndex, r.IndexMethod = true, false, ""
		}},
		{"an operator that is not text", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.IndexOperator = "\xff"
		}},
		{"an empty extension", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.RequiredExtensions = []string{""}
		}},
		{"a repeated extension", func(_ *protocol.DescribeEmbeddingTargetRequest, r *protocol.EmbeddingTargetObjects) {
			r.RequiredExtensions = []string{"vector", "vector"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := describeExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingTargetObjects(embeddingDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, result := describeExchange()
	c.Assert(provider.ValidateEmbeddingTargetObjects(embeddingDescription(), request, nil), qt.ErrorIs, provider.ErrProtocol)
	request.Engine = "rows"
	c.Assert(provider.ValidateEmbeddingTargetObjects(embeddingDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
}

func relationsExchange() (*protocol.ResolveEmbeddingRelationsRequest, *protocol.EmbeddingRelations) {
	return &protocol.ResolveEmbeddingRelationsRequest{SessionId: "session", Relations: []*protocol.Reference{
			{Engine: "vectors", Kind: "table", Name: "documents"}, embeddingTable("notes")}},
		&protocol.EmbeddingRelations{Relations: []*protocol.Reference{embeddingTable("documents"), embeddingTable("notes")}}
}

func TestValidateEmbeddingRelations_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := relationsExchange()
	c.Assert(provider.ValidateEmbeddingRelations(request, result), qt.IsNil)
}

func TestValidateResolveEmbeddingRelationsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ResolveEmbeddingRelationsRequest)
	}{
		{"no session", func(r *protocol.ResolveEmbeddingRelationsRequest) { r.SessionId = "" }},
		{"no relation", func(r *protocol.ResolveEmbeddingRelationsRequest) { r.Relations = nil }},
		{"more relations than the limit", func(r *protocol.ResolveEmbeddingRelationsRequest) {
			r.Relations = make([]*protocol.Reference, provider.MaxEmbeddingRows+1)
			for i := range r.Relations {
				r.Relations[i] = embeddingTable("documents")
			}
		}},
		{"a relation that is not a table", func(r *protocol.ResolveEmbeddingRelationsRequest) { r.Relations[1].Kind = "view" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := relationsExchange()
			test.mutate(request)
			c.Assert(provider.ValidateResolveEmbeddingRelationsRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingRelations_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ResolveEmbeddingRelationsRequest, *protocol.EmbeddingRelations)
	}{
		{"no session", func(q *protocol.ResolveEmbeddingRelationsRequest, _ *protocol.EmbeddingRelations) { q.SessionId = "" }},
		{"no relation", func(q *protocol.ResolveEmbeddingRelationsRequest, _ *protocol.EmbeddingRelations) { q.Relations = nil }},
		{"more relations than the limit", func(q *protocol.ResolveEmbeddingRelationsRequest, _ *protocol.EmbeddingRelations) {
			q.Relations = make([]*protocol.Reference, provider.MaxEmbeddingRows+1)
			for i := range q.Relations {
				q.Relations[i] = embeddingTable("documents")
			}
		}},
		{"a relation that is not a table", func(q *protocol.ResolveEmbeddingRelationsRequest, _ *protocol.EmbeddingRelations) {
			q.Relations[1].Kind = "view"
		}},
		{"one relation fewer", func(_ *protocol.ResolveEmbeddingRelationsRequest, r *protocol.EmbeddingRelations) {
			r.Relations = r.Relations[:1]
		}},
		{"an answer without a name", func(_ *protocol.ResolveEmbeddingRelationsRequest, r *protocol.EmbeddingRelations) {
			r.Relations[0].Name = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := relationsExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingRelations(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := relationsExchange()
	c.Assert(provider.ValidateEmbeddingRelations(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func measureExchange() (*protocol.MeasureEmbeddingRequest, *protocol.EmbeddingMeasurement) {
	return &protocol.MeasureEmbeddingRequest{SessionId: "session", Spec: embeddingSpec()},
		&protocol.EmbeddingMeasurement{SourceExists: true, TargetTableExists: true, TargetColumnExists: true, RowsCounted: true, Rows: 1200,
			Capabilities: map[string]bool{"vector_type": true, "vector_index": false}, IndexBuildable: false,
			Refusals: []string{"key_fields are not the primary key"}}
}

func TestValidateEmbeddingMeasurement_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingMeasurement)
	}{
		{"a counted source", func(*protocol.EmbeddingMeasurement) {}},
		{"a source that is not there", func(r *protocol.EmbeddingMeasurement) {
			r.SourceExists, r.RowsCounted, r.Rows, r.Refusals = false, false, 0, nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := measureExchange()
			test.mutate(result)
			c.Assert(provider.ValidateEmbeddingMeasurement(request, result), qt.IsNil)
		})
	}
}

func TestValidateEmbeddingMeasurement_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.MeasureEmbeddingRequest, *protocol.EmbeddingMeasurement)
	}{
		{"no session", func(q *protocol.MeasureEmbeddingRequest, _ *protocol.EmbeddingMeasurement) { q.SessionId = "" }},
		{"no specification", func(q *protocol.MeasureEmbeddingRequest, _ *protocol.EmbeddingMeasurement) { q.Spec = nil }},
		{"a column without its table", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) {
			r.TargetTableExists = false
		}},
		{"a count of a source that is not there", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) {
			r.SourceExists = false
		}},
		{"a negative count", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) { r.Rows = -1 }},
		{"rows that were not counted", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) { r.RowsCounted = false }},
		{"a capability that is not a name", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) {
			r.Capabilities["Vector Index"] = true
		}},
		{"a refusal of two lines", func(_ *protocol.MeasureEmbeddingRequest, r *protocol.EmbeddingMeasurement) {
			r.Refusals = []string{"one\ntwo"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := measureExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingMeasurement(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := measureExchange()
	c.Assert(provider.ValidateEmbeddingMeasurement(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func prepareTargetExchange() (*protocol.PrepareEmbeddingTargetRequest, *protocol.EmbeddingWriteResult) {
	return &protocol.PrepareEmbeddingTargetRequest{SessionId: "session", RequestId: "target-1", Spec: embeddingSpec(),
			Mode: protocol.PrepareEmbeddingTargetRequest_CREATE},
		&protocol.EmbeddingWriteResult{RequestId: "target-1", Outcome: protocol.EmbeddingWriteResult_APPLIED}
}

func TestValidatePreparedEmbeddingTarget_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.PrepareEmbeddingTargetRequest, *protocol.EmbeddingWriteResult)
	}{
		{"created", func(*protocol.PrepareEmbeddingTargetRequest, *protocol.EmbeddingWriteResult) {}},
		{"a verified target", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) {
			q.Mode = protocol.PrepareEmbeddingTargetRequest_VERIFY
		}},
		{"a column of another generation", func(_ *protocol.PrepareEmbeddingTargetRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, refusal(protocol.EmbeddingRefusal_NOT_OWNED)
		}},
		{"stopped part way", func(_ *protocol.PrepareEmbeddingTargetRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome = protocol.EmbeddingWriteResult_UNKNOWN
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := prepareTargetExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidatePreparedEmbeddingTarget(request, result), qt.IsNil)
		})
	}
}

func TestValidatePreparedEmbeddingTarget_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.PrepareEmbeddingTargetRequest, *protocol.EmbeddingWriteResult)
	}{
		{"no session", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) { q.SessionId = "" }},
		{"no request ID", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) { q.RequestId = "" }},
		{"no mode", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) {
			q.Mode = protocol.PrepareEmbeddingTargetRequest_MODE_UNSPECIFIED
		}},
		{"an unknown mode", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) { q.Mode = 9 }},
		{"no specification", func(q *protocol.PrepareEmbeddingTargetRequest, _ *protocol.EmbeddingWriteResult) { q.Spec = nil }},
		{"a conflict", func(_ *protocol.PrepareEmbeddingTargetRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome = protocol.EmbeddingWriteResult_CONFLICT
		}},
		{"another request", func(_ *protocol.PrepareEmbeddingTargetRequest, r *protocol.EmbeddingWriteResult) {
			r.RequestId = "target-2"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := prepareTargetExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidatePreparedEmbeddingTarget(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func indexExchange() (*protocol.BuildEmbeddingIndexRequest, *protocol.EmbeddingWriteResult) {
	return &protocol.BuildEmbeddingIndexRequest{SessionId: "session", RequestId: "index-1", Spec: embeddingSpec()},
		&protocol.EmbeddingWriteResult{RequestId: "index-1", Outcome: protocol.EmbeddingWriteResult_APPLIED, IndexOutcome: protocol.EmbeddingWriteResult_BUILT}
}

func TestValidateBuiltEmbeddingIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.BuildEmbeddingIndexRequest, *protocol.EmbeddingWriteResult)
	}{
		{"built", func(*protocol.BuildEmbeddingIndexRequest, *protocol.EmbeddingWriteResult) {}},
		{"rebuilt", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.IndexOutcome = protocol.EmbeddingWriteResult_REBUILT
		}},
		{"already valid", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.IndexOutcome = protocol.EmbeddingWriteResult_ALREADY_VALID
		}},
		{"not declared", func(q *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			q.Spec, r.IndexOutcome = chunkedSpec(), protocol.EmbeddingWriteResult_NOT_DECLARED
		}},
		{"a refused build of an undeclared index", func(q *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			q.Spec = chunkedSpec()
			r.Outcome, r.IndexOutcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, 0, refusal(protocol.EmbeddingRefusal_UNSUPPORTED)
		}},
		{"a server line without vector indexes", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.IndexOutcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, 0, refusal(protocol.EmbeddingRefusal_UNSUPPORTED)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := indexExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateBuiltEmbeddingIndex(request, result), qt.IsNil)
		})
	}
}

func TestValidateBuiltEmbeddingIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.BuildEmbeddingIndexRequest, *protocol.EmbeddingWriteResult)
	}{
		{"no session", func(q *protocol.BuildEmbeddingIndexRequest, _ *protocol.EmbeddingWriteResult) { q.SessionId = "" }},
		{"no request ID", func(q *protocol.BuildEmbeddingIndexRequest, _ *protocol.EmbeddingWriteResult) { q.RequestId = "" }},
		{"no specification", func(q *protocol.BuildEmbeddingIndexRequest, _ *protocol.EmbeddingWriteResult) { q.Spec = nil }},
		{"applied without saying what the build did", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.IndexOutcome = protocol.EmbeddingWriteResult_INDEX_OUTCOME_UNSPECIFIED
		}},
		{"an unknown index outcome", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) { r.IndexOutcome = 9 }},
		{"not declared for a declared method", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.IndexOutcome = protocol.EmbeddingWriteResult_NOT_DECLARED
		}},
		{"built for no declared method", func(q *protocol.BuildEmbeddingIndexRequest, _ *protocol.EmbeddingWriteResult) {
			q.Spec = chunkedSpec()
		}},
		{"an index outcome with a refusal", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, refusal(protocol.EmbeddingRefusal_UNSUPPORTED)
		}},
		{"a conflict", func(_ *protocol.BuildEmbeddingIndexRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.IndexOutcome = protocol.EmbeddingWriteResult_CONFLICT, 0
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := indexExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateBuiltEmbeddingIndex(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func embeddingLocation() *protocol.EmbeddingLocation {
	return &protocol.EmbeddingLocation{Generation: "gen-1", Table: embeddingTable("documents"), Column: "embedding", Dimension: 3,
		Names: &protocol.EmbeddingNames{Index: "documents_embedding_gen1_idx", OwnershipMark: "ptah embedding target for generation gen-1"}}
}

func structureExchange() (*protocol.ReadEmbeddingStructureRequest, *protocol.EmbeddingStructure) {
	return &protocol.ReadEmbeddingStructureRequest{SessionId: "session", Location: embeddingLocation(), CountRows: true},
		&protocol.EmbeddingStructure{TableExists: true, ColumnExists: true, ColumnType: "String", Dimension: 3, IndexExists: true, IndexValid: true,
			IndexMethod: "vector_kmeans_tree", IndexOperator: "distance=cosine", VectorSupport: true, RowsCounted: true, Rows: 1200,
			SearchSettings: "ydb.KMeansTreeSearchTopSize=(server default)"}
}

func TestValidateEmbeddingStructure_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingStructureRequest, *protocol.EmbeddingStructure)
	}{
		{"an indexed generation", func(*protocol.ReadEmbeddingStructureRequest, *protocol.EmbeddingStructure) {}},
		{"a table that is not there, uncounted", func(q *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			q.CountRows = false
			r.TableExists, r.ColumnExists, r.ColumnType, r.Dimension, r.IndexExists, r.IndexValid = false, false, "", 0, false, false
			r.IndexMethod, r.IndexOperator, r.VectorSupport, r.RowsCounted, r.Rows, r.SearchSettings = "", "", false, false, 0, ""
		}},
		{"an index whose build did not finish", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.IndexValid = false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := structureExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingStructure(request, result), qt.IsNil)
		})
	}
}

func TestValidateEmbeddingStructure_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingStructureRequest, *protocol.EmbeddingStructure)
	}{
		{"no session", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) { q.SessionId = "" }},
		{"no location", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) { q.Location = nil }},
		{"a location without a generation", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.Location.Generation = ""
		}},
		{"a location without a column", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.Location.Column = ""
		}},
		{"a location without a table", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.Location.Table = nil
		}},
		{"a location dimension above the limit", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.Location.Dimension = provider.MaxEmbeddingDimension + 1
		}},
		{"a location without an index name", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.Location.Names.Index = ""
		}},
		{"a column without its table", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.TableExists, r.IndexExists, r.IndexValid, r.IndexMethod, r.IndexOperator = false, false, false, "", ""
		}},
		{"an index without its table", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.TableExists, r.ColumnExists, r.ColumnType = false, false, ""
		}},
		{"a valid index that is not there", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.IndexExists, r.IndexMethod, r.IndexOperator = false, "", ""
		}},
		{"a column type without a column", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.ColumnExists = false
		}},
		{"a column without a type", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) { r.ColumnType = "" }},
		{"an index without a method", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) { r.IndexMethod = "" }},
		{"an operator without an index", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.IndexExists, r.IndexValid, r.IndexMethod = false, false, ""
		}},
		{"a column type that is not text", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.ColumnType = "\xff"
		}},
		{"a count the request did not ask for", func(q *protocol.ReadEmbeddingStructureRequest, _ *protocol.EmbeddingStructure) {
			q.CountRows = false
		}},
		{"no count where the request asked for one", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.RowsCounted, r.Rows = false, 0
		}},
		{"a negative count", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) { r.Rows = -1 }},
		{"rows that were not counted", func(q *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			q.CountRows, r.RowsCounted = false, false
		}},
		{"search settings of two lines", func(_ *protocol.ReadEmbeddingStructureRequest, r *protocol.EmbeddingStructure) {
			r.SearchSettings = "a=1\nb=2"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := structureExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingStructure(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := structureExchange()
	request.CountRows = false
	c.Assert(provider.ValidateEmbeddingStructure(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func dropExchange() (*protocol.DropEmbeddingGenerationRequest, *protocol.EmbeddingWriteResult) {
	return &protocol.DropEmbeddingGenerationRequest{SessionId: "session", RequestId: "drop-1", Location: embeddingLocation(),
			Storage: protocol.DropEmbeddingGenerationRequest_TABLE},
		&protocol.EmbeddingWriteResult{RequestId: "drop-1", Outcome: protocol.EmbeddingWriteResult_APPLIED}
}

func TestValidateDroppedEmbeddingGeneration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.DropEmbeddingGenerationRequest, *protocol.EmbeddingWriteResult)
	}{
		{"a dropped table", func(*protocol.DropEmbeddingGenerationRequest, *protocol.EmbeddingWriteResult) {}},
		{"columns without an ownership mark", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Storage, q.Location.Names.OwnershipMark = protocol.DropEmbeddingGenerationRequest_COLUMNS, ""
		}},
		{"the index alone", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Storage = protocol.DropEmbeddingGenerationRequest_INDEX_ONLY
		}},
		{"a table that is not the generation's", func(_ *protocol.DropEmbeddingGenerationRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, refusal(protocol.EmbeddingRefusal_NOT_OWNED)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := dropExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateDroppedEmbeddingGeneration(request, result), qt.IsNil)
		})
	}
}

func TestValidateDroppedEmbeddingGeneration_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.DropEmbeddingGenerationRequest, *protocol.EmbeddingWriteResult)
	}{
		{"no session", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) { q.SessionId = "" }},
		{"no request ID", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) { q.RequestId = "" }},
		{"no location", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Location, q.Storage = nil, protocol.DropEmbeddingGenerationRequest_COLUMNS
		}},
		{"a location without a generation", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Location.Generation = ""
		}},
		{"no storage", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Storage = protocol.DropEmbeddingGenerationRequest_STORAGE_UNSPECIFIED
		}},
		{"an unknown storage", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) { q.Storage = 9 }},
		{"a table without its ownership mark", func(q *protocol.DropEmbeddingGenerationRequest, _ *protocol.EmbeddingWriteResult) {
			q.Location.Names.OwnershipMark = ""
		}},
		{"a conflict", func(_ *protocol.DropEmbeddingGenerationRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome = protocol.EmbeddingWriteResult_CONFLICT
		}},
		{"a payload", func(_ *protocol.DropEmbeddingGenerationRequest, r *protocol.EmbeddingWriteResult) {
			r.Message = strings.Repeat("x", 600)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := dropExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateDroppedEmbeddingGeneration(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
