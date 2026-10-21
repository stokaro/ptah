package runtime_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func TestServerRequiresAdvertisedServices(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := transport.Server{Description: description, Sessions: &sessions{}}
	c.Assert(server.Validate(), qt.IsNil)
	server.Sessions = (*sessions)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Sessions = &sessions{}
	description.Features = []string{provider.InspectFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Revision++
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type sessions struct{}

func (*sessions) Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error) {
	return nil, nil
}
func (*sessions) Close(context.Context, *protocol.CloseRequest) error { return nil }

func TestCanceledStartupDoesNotNeedAnExecutable(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	client, err := transport.Start(ctx, transport.Options{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(client, qt.IsNil)
}

func TestServerRequiresAdvertisedValueNormalizer(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.NormalizeValuesFeature}
	c.Assert(server.Validate(), qt.IsNil)
	server.ValueNormalizer = (*normalizer)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.ValueNormalizer = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type normalizer struct{}

func (*normalizer) NormalizeValues(context.Context, *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	return &protocol.NormalizedValues{Complete: true}, nil
}

func TestServerRequiresAdvertisedSourceDecoder(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.NormalizeValuesFeature, provider.DecodeSourceFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.SourceDecoder = (*sourceDecoder)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.SourceDecoder = &sourceDecoder{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Features = []string{provider.NormalizeValuesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.DecodeSourceFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type sourceDecoder struct{}

func (*sourceDecoder) DecodeSource(context.Context, *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	return &protocol.DecodedSource{Complete: true}, nil
}

func TestServerRequiresAdvertisedValueConverter(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.NormalizeValuesFeature, provider.ConvertValuesFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.ValueConverter = (*converter)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.ValueConverter = &converter{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Features = []string{provider.NormalizeValuesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.ConvertValuesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type converter struct{}

func (*converter) ConvertValues(context.Context, *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	return &protocol.NormalizedValues{Complete: true}, nil
}

func TestServerRequiresAdvertisedTransactionImplementation(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.TransactionsFeature}, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.ExecuteFeature, provider.TransactionsFeature}}
	owner := &transactionServices{}
	server := transport.Server{Description: description, Sessions: &sessions{}, Executor: owner}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Transactions = (*transactionServices)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Transactions = owner
	c.Assert(server.Validate(), qt.IsNil)
	description.Features = []string{provider.ExecuteFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type transactionServices struct{}

func (*transactionServices) BeginTransaction(context.Context, *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	return nil, nil
}
func (*transactionServices) FinishTransaction(context.Context, *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	return nil, nil
}
func (*transactionServices) Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error) {
	return nil, nil
}

func TestServerRequiresAdvertisedHistoryImplementation(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.HistoryFeature}, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.HistoryFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.History = (*historyServices)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.History = &historyServices{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type historyServices struct{}

func (*historyServices) ReadHistory(context.Context, *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	return nil, nil
}
func (*historyServices) PrepareHistory(context.Context, *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	return nil, nil
}
func (*historyServices) MutateHistory(context.Context, *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	return nil, nil
}
