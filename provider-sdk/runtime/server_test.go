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

type metadataOwner struct{ provider.Metadata }

func TestServerRequiresAdvertisedMigrationMetadata(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "metadata-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := transport.Server{Description: description, Sessions: &sessions{}, Metadata: &metadataOwner{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.MetadataFeature}
	description.Engines[0].Features = []string{provider.MetadataFeature}
	c.Assert(server.Validate(), qt.IsNil)
	server.Metadata = (*metadataOwner)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Metadata = &metadataOwner{}
	description.Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

func TestServerRequiresAdvertisedTablePreparer(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.PrepareTablesFeature}, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.NormalizeValuesFeature, provider.PrepareTablesFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.TablePreparer = (*tablePreparer)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.TablePreparer = &tablePreparer{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.CommonTableIdentityFeature)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Engines[0].Features = []string{provider.PrepareTablesFeature}
	description.Features = []string{provider.NormalizeValuesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Engines[0].Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.PrepareTablesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type tablePreparer struct{}

func (*tablePreparer) PrepareTables(context.Context, *protocol.PrepareTablesRequest) (*protocol.PreparedTables, error) {
	return &protocol.PreparedTables{Complete: true}, nil
}

func TestServerRequiresAdvertisedTableCreations(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.ProjectTableCreationsFeature}, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.NormalizeValuesFeature, provider.ProjectTableCreationsFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.TableCreations = (*tableProjector)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.TableCreations = &tableProjector{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.CommonTableIdentityFeature)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Engines[0].Features = []string{provider.ProjectTableCreationsFeature}
	description.Features = []string{provider.NormalizeValuesFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Engines[0].Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = []string{provider.ProjectTableCreationsFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type tableProjector struct{}

func (*tableProjector) ProjectTableCreations(context.Context, *protocol.ProjectTableCreationsRequest) (*protocol.ProjectedTableCreations, error) {
	return &protocol.ProjectedTableCreations{Complete: true}, nil
}

func TestServerRequiresAdvertisedSchemaValidator(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "validation-1", Features: []string{provider.NormalizeValuesFeature}, Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := transport.Server{Description: description, Sessions: &sessions{}, ValueNormalizer: &normalizer{}, SchemaValidator: &schemaValidator{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Features = append(description.Features, provider.ValidateSchemaFeature)
	c.Assert(server.Validate(), qt.IsNil)
	server.SchemaValidator = (*schemaValidator)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.SchemaValidator = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type schemaValidator struct{}

func (*schemaValidator) ValidateSchema(context.Context, *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	return &protocol.SchemaValidation{Complete: true}, nil
}

func TestServerRequiresAdvertisedLinter(t *testing.T) {
	c := qt.New(t)
	rules := []*protocol.LintRule{{Code: "XY101", Title: "Drop loses data", Severity: protocol.LintRule_ERROR}}
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "lint-1", Engines: []*protocol.Engine{{Id: "test", Features: []string{provider.LintFeature}, LintRules: rules, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}, Features: []string{provider.LintFeature}}
	server := transport.Server{Description: description, Sessions: &sessions{}}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Linter = (*linter)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Linter = &linter{}
	c.Assert(server.Validate(), qt.IsNil)
	description.Features = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type linter struct{}

func (*linter) SplitLintStatements(context.Context, *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	return &protocol.SplitLint{}, nil
}

func (*linter) AnnotateLintStatements(context.Context, *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
	return &protocol.AnnotatedLint{}, nil
}

func (*linter) CheckLintMigrations(context.Context, *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	return &protocol.CheckedLint{Complete: true}, nil
}
