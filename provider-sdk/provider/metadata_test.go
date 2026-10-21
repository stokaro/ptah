package provider_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func metadataScope(kind protocol.MetadataScope_Kind) *protocol.MetadataScope {
	return &protocol.MetadataScope{SessionId: "session", Schema: "main", Table: "metadata", Kind: kind}
}
func metadataRead(kind protocol.MetadataScope_Kind) *protocol.MetadataReadRequest {
	return &protocol.MetadataReadRequest{Scope: metadataScope(kind), ReaderId: "reader", RequestId: "request"}
}
func metadataReceipt(request *protocol.MetadataReadRequest) *protocol.MetadataReadResult {
	return &protocol.MetadataReadResult{Scope: proto.CloneOf(request.Scope), ReaderId: request.ReaderId, RequestId: request.RequestId}
}
func metadataTag() *protocol.MigrationTagRecord {
	return &protocol.MigrationTagRecord{Name: "release", Version: 0, RecordedAtSeconds: 1790000000}
}
func TestMetadataRefusesTransactionBoundLog(t *testing.T) {
	c := qt.New(t)
	scope := metadataScope(protocol.MetadataScope_LOG)
	scope.TransactionId = "transaction"
	c.Assert(provider.ValidateMetadataScope(scope), qt.ErrorIs, provider.ErrProtocol)
	scope.Kind = protocol.MetadataScope_TAGS
	c.Assert(provider.ValidateMetadataScope(scope), qt.IsNil)
}
func TestMetadataMutationsBindRecordKind(t *testing.T) {
	c := qt.New(t)
	request := &protocol.MutateMetadataRequest{Scope: metadataScope(protocol.MetadataScope_TAGS), RequestId: "request", Mutation: &protocol.MutateMetadataRequest_PutTag{PutTag: metadataTag()}}
	c.Assert(provider.ValidateMutateMetadataRequest(request), qt.IsNil)
	request.Scope.Kind = protocol.MetadataScope_LOG
	c.Assert(provider.ValidateMutateMetadataRequest(request), qt.ErrorIs, provider.ErrProtocol)
	request.Scope.Kind = protocol.MetadataScope_TAGS
	request.Mutation = &protocol.MutateMetadataRequest_DeleteTag{DeleteTag: ""}
	c.Assert(provider.ValidateMutateMetadataRequest(request), qt.ErrorIs, provider.ErrProtocol)
	request.Mutation = nil
	c.Assert(provider.ValidateMutateMetadataRequest(request), qt.ErrorIs, provider.ErrProtocol)
}
func TestMetadataLookupDistinguishesMissingAndZero(t *testing.T) {
	c := qt.New(t)
	request := &protocol.LookupMetadataTagRequest{Scope: metadataScope(protocol.MetadataScope_TAGS), RequestId: "request", Name: "release"}
	result := &protocol.MetadataTagResult{Scope: request.Scope, RequestId: request.RequestId, Found: true, Record: metadataTag()}
	c.Assert(provider.ValidateMetadataTagResult(request, result), qt.IsNil)
	result.Found = false
	c.Assert(provider.ValidateMetadataTagResult(request, result), qt.ErrorIs, provider.ErrProtocol)
	result.Record = nil
	c.Assert(provider.ValidateMetadataTagResult(request, result), qt.IsNil)
	result.RequestId = "stale"
	c.Assert(provider.ValidateMetadataTagResult(request, result), qt.ErrorIs, provider.ErrProtocol)
}
func TestMetadataMutationReceiptsCannotInventDeletion(t *testing.T) {
	c := qt.New(t)
	scope := metadataScope(protocol.MetadataScope_TAGS)
	result := &protocol.MetadataResult{Scope: scope, RequestId: "request", Outcome: protocol.MetadataResult_APPLIED, Deleted: true}
	deletion := &protocol.MutateMetadataRequest{Scope: scope, RequestId: "request", Mutation: &protocol.MutateMetadataRequest_DeleteTag{DeleteTag: "release"}}
	c.Assert(provider.ValidateMutateMetadataResult(deletion, result), qt.IsNil)
	c.Assert(provider.ValidatePrepareMetadataResult(&protocol.PrepareMetadataRequest{Scope: scope, RequestId: "request"}, result), qt.ErrorIs, provider.ErrProtocol)
	result.Outcome = protocol.MetadataResult_UNKNOWN
	c.Assert(provider.ValidateMutateMetadataResult(deletion, result), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.CheckMetadataResult(result), qt.ErrorIs, provider.ErrOutcomeUnknown)
	result.Deleted = false
	result.Outcome = protocol.MetadataResult_REJECTED
	c.Assert(provider.CheckMetadataResult(result), qt.ErrorIs, provider.ErrExecution)
}
func TestMetadataPagesRefuseIncompleteAndForeignRecords(t *testing.T) {
	request := &protocol.ReadMetadataPageRequest{Reader: metadataRead(protocol.MetadataScope_TAGS), MaxRecords: 1}
	for _, test := range []struct {
		name string
		page *protocol.MetadataPage
	}{
		{"missing", nil},
		{"no progress", &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Exists: true}},
		{"absent with records", &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Complete: true, Tags: []*protocol.MigrationTagRecord{metadataTag()}}},
		{"too many", &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Exists: true, Complete: true, Tags: []*protocol.MigrationTagRecord{metadataTag(), metadataTag()}}},
		{"wrong page", &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Page: 1, Exists: true, Complete: true}},
		{"wrong kind", &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Exists: true, Complete: true, Logs: []*protocol.MigrationLogRecord{{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateMetadataPage(request, test.page), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
func TestMetadataPagesEnforceBytesAndDuplicateIdentities(t *testing.T) {
	c := qt.New(t)
	request := &protocol.ReadMetadataPageRequest{Reader: metadataRead(protocol.MetadataScope_TAGS), MaxRecords: 2}
	page := &protocol.MetadataPage{Reader: metadataReceipt(request.Reader), Exists: true, Complete: true, Tags: []*protocol.MigrationTagRecord{metadataTag(), metadataTag()}}
	c.Assert(provider.ValidateMetadataPage(request, page), qt.ErrorIs, provider.ErrProtocol)
	page.Tags = page.Tags[:1]
	page.Tags[0].Name = strings.Repeat("a", provider.MaxMetadataBytes+1)
	c.Assert(provider.ValidateMetadataPage(request, page), qt.ErrorIs, provider.ErrProtocol)
	page.Tags[0] = metadataTag()
	c.Assert(provider.ValidateMetadataPage(request, page), qt.IsNil)
	request.MaxRecords = provider.MaxMetadataRecords + 1
	c.Assert(provider.ValidateReadMetadataPageRequest(request), qt.ErrorIs, provider.ErrProtocol)
}
