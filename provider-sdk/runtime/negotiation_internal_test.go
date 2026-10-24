package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a host or a provider of
// another SDK release can only be stood in for below the transport.

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// negotiatedServer is a server adapter after a host listed features in
// Describe, for tests whose description is not a complete one.
func negotiatedServer(server Server, features ...string) *serverAdapter {
	adapter := newServerAdapter(server)
	adapter.negotiation.features = features
	adapter.negotiation.description = provider.NegotiatedDescription(server.Description, features)
	return adapter
}

const contractSQL = "CREATE TABLE a (id Int32);"

// contractDescription offers every two-way feature on one lint engine.
func contractDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/contract", Version: "1", SemanticIdentity: "contract-1",
		Features: []string{provider.LintFeature, provider.ExecuteFeature, provider.StatementTimeoutFeature},
		Engines: []*protocol.Engine{{
			Id: "test", Features: []string{provider.LintFeature, provider.LintStatementFactsFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
			LintRules:    []*protocol.LintRule{{Code: "XY101", Title: "Table without key", Severity: protocol.LintRule_WARNING}},
			QueryDialect: &protocol.QueryDialect{Placeholder: "question"},
		}},
	}
}

// factsLinter splits one statement. A careful one reports the statement
// facts only to a host that lists them; a careless one always does.
type factsLinter struct {
	careful bool
	seen    []string
}

func (l *factsLinter) SplitLintStatements(ctx context.Context, request *protocol.SplitLintRequest) (*protocol.SplitLint, error) {
	l.seen = provider.HostFeatures(ctx)
	statement := &protocol.LintStatement{Start: 0, End: uint32(len(contractSQL)), Line: 1, Sql: contractSQL}
	result := &protocol.SplitLint{Files: []*protocol.LintFile{{Name: request.Sources[0].Name, Statements: []*protocol.LintStatement{statement}}}}
	if l.careful && !slices.Contains(l.seen, provider.LintStatementFactsFeature) {
		return result, nil
	}
	result.Judged = []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEMENT_KIND, protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY}
	statement.Effects = []*protocol.LintEffect{
		{Fact: protocol.LintFact_LINT_FACT_STATEMENT_KIND, StatementClass: protocol.LintStatementClass_LINT_STATEMENT_CLASS_CREATE_TABLE, Kind: "CREATE TABLE", Object: "a"},
		{Fact: protocol.LintFact_LINT_FACT_MISSING_PRIMARY_KEY, Table: "a"},
	}
	return result, nil
}

func (*factsLinter) AnnotateLintStatements(context.Context, *protocol.AnnotateLintRequest) (*protocol.AnnotatedLint, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (*factsLinter) CheckLintMigrations(context.Context, *protocol.CheckLintRequest) (*protocol.CheckedLint, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func splitRequest() *protocol.SplitLintRequest {
	return &protocol.SplitLintRequest{Engine: "test", Sources: []*protocol.LintSource{{Name: "1.sql", Sql: contractSQL, Up: true}}}
}

// A host of contract 25.0 announces no version and lists no feature. It gets
// the description of contract 25.0, the services it can call, and replies
// that use no addition: a careful provider leaves the statement facts out,
// and the SDK server refuses a careless one rather than send them.
func TestContract25HostGetsContract25Messages(t *testing.T) {
	c := qt.New(t)
	careful := &factsLinter{careful: true}
	server := newServerAdapter(Server{Description: contractDescription(), Linter: careful})
	described, err := server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision})
	c.Assert(err, qt.IsNil)
	c.Assert(described.Features, qt.DeepEquals, []string{provider.LintFeature, provider.ExecuteFeature})
	c.Assert(described.Engines[0].Features, qt.DeepEquals, []string{provider.LintFeature})
	c.Assert(described.Engines[0].QueryDialect, qt.IsNil)
	c.Assert(described.MinContract, qt.IsNil)
	split, err := server.SplitLintStatements(c.Context(), splitRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(careful.seen, qt.HasLen, 0)
	c.Assert(split.Judged, qt.HasLen, 0)
	c.Assert(split.Files[0].Statements[0].Effects, qt.HasLen, 0)
	_, err = server.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: "s", RequestId: "r", StatementTimeoutNanoseconds: 1,
		Units: []*protocol.Unit{{ChangeId: "a", Body: &protocol.Unit_Sql{Sql: contractSQL}}}})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	server.server.Executor = executorOwner{}
	_, err = server.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: "s", RequestId: "r", StatementTimeoutNanoseconds: 1,
		Units: []*protocol.Unit{{ChangeId: "a", Body: &protocol.Unit_Sql{Sql: contractSQL}}}})
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	careless := newServerAdapter(Server{Description: contractDescription(), Linter: &factsLinter{}})
	_, err = careless.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision})
	c.Assert(err, qt.IsNil)
	refused, err := careless.SplitLintStatements(c.Context(), splitRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(refused, qt.IsNil)
}

// A host of this SDK announces its version and the features it acts on, and
// gets exactly those: the statement facts and the query dialect here, and no
// statement timeouts, which it did not list.
func TestNewHostGetsWhatItLists(t *testing.T) {
	c := qt.New(t)
	linter := &factsLinter{careful: true}
	server := newServerAdapter(Server{Description: contractDescription(), Linter: linter})
	listed := []string{provider.LintStatementFactsFeature, provider.QueryDialectFeature, "a.later-feature"}
	described, err := server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(), HostFeatures: listed})
	c.Assert(err, qt.IsNil)
	c.Assert(described.Features, qt.DeepEquals, []string{provider.LintFeature, provider.ExecuteFeature})
	c.Assert(described.Engines[0].Features, qt.DeepEquals, []string{provider.LintFeature, provider.LintStatementFactsFeature})
	c.Assert(described.Engines[0].QueryDialect.GetPlaceholder(), qt.Equals, "question")
	split, err := server.SplitLintStatements(c.Context(), splitRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(linter.seen, qt.DeepEquals, listed)
	c.Assert(split.Judged, qt.HasLen, 2)
	c.Assert(split.Files[0].Statements[0].Effects[1].Table, qt.Equals, "a")
}

// A provider that needs contract 25.1 refuses a host that announces no
// version, since such a host cannot compare versions, and names both. A host
// that announces a version gets the description and decides itself; the SDK
// client refuses one below the minimum, naming both.
func TestMinimumContractDecidesWhoStarts(t *testing.T) {
	c := qt.New(t)
	description := contractDescription()
	description.MinContract = &protocol.ContractVersion{Major: provider.Revision, Minor: 1}
	server := newServerAdapter(Server{Description: description})
	_, err := server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision})
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(status.Convert(err).Message(), qt.Equals, "provider requires contract 25.1 or a later minor of it; this host speaks 25.0")
	described, err := server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract()})
	c.Assert(err, qt.IsNil)
	got, err := negotiate(described, Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ContractString(provider.MinimumContract(got)), qt.Equals, "25.1")
	older := &protocol.ContractVersion{Major: provider.Revision}
	described, err = server.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: older})
	c.Assert(err, qt.IsNil)
	c.Assert(described, qt.IsNotNil)
	described.MinContract.Minor = provider.ContractMinor + 1
	got, err = negotiate(described, Options{})
	c.Assert(err, qt.ErrorMatches, `provider requires contract 25\.2 or a later minor of it; this host speaks 25\.1`)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(got, qt.IsNil)
}

// A description from a provider of minor 0 declares no minimum: it needs
// 25.0, which every host of this major speaks. The client keeps from it only
// what the host listed.
func TestProviderWithoutMinimumIsContract25(t *testing.T) {
	c := qt.New(t)
	description := contractDescription()
	got, err := negotiate(description, Options{HostFeatures: []string{provider.StatementTimeoutFeature}})
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ContractString(provider.MinimumContract(got)), qt.Equals, "25.0")
	c.Assert(got.Features, qt.DeepEquals, []string{provider.LintFeature, provider.ExecuteFeature, provider.StatementTimeoutFeature})
	c.Assert(got.Engines[0].Features, qt.DeepEquals, []string{provider.LintFeature})
	c.Assert(got.Engines[0].QueryDialect, qt.IsNil)
}

func TestDescribeRefusesAnInvalidHandshake(t *testing.T) {
	c := qt.New(t)
	server := newServerAdapter(Server{Description: contractDescription()})
	tests := []struct {
		name    string
		request *protocol.DescribeRequest
		want    codes.Code
	}{
		{"another major", &protocol.DescribeRequest{Revision: provider.Revision + 1}, codes.FailedPrecondition},
		{"a version of another major than its revision", &protocol.DescribeRequest{Revision: provider.Revision,
			HostContract: &protocol.ContractVersion{Major: provider.Revision + 1, Minor: 1}}, codes.InvalidArgument},
		{"a repeated host feature", &protocol.DescribeRequest{Revision: provider.Revision,
			HostFeatures: []string{provider.QueryDialectFeature, provider.QueryDialectFeature}}, codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := server.Describe(c.Context(), test.request)
			c.Assert(status.Code(err), qt.Equals, test.want)
		})
	}
	// An executable path absolute on every platform: "/bin/provider" is not
	// absolute on Windows, where the check refused it before the features.
	executable := filepath.Join(c.TempDir(), "provider")
	_, err := validateOptions(Options{Executable: executable, ProviderID: "x", SHA256: strings.Repeat("0", 64), HostFeatures: []string{""}})
	c.Assert(err, qt.ErrorMatches, `.*invalid or repeated host feature`)
}

// A provider cannot require a minor its own SDK does not speak.
func TestServerRefusesAMinimumAboveItsSDK(t *testing.T) {
	c := qt.New(t)
	description := contractDescription()
	description.Features = []string{provider.LintFeature}
	description.MinContract = &protocol.ContractVersion{Major: provider.Revision, Minor: provider.ContractMinor + 1}
	server := Server{Description: description, Sessions: &sessionOwner{}, Linter: &factsLinter{}}
	c.Assert(server.Validate(), qt.ErrorMatches, `.*minimum contract 25\.2 is outside contract 25\.1`)
	description.MinContract.Minor = provider.ContractMinor
	c.Assert(server.Validate(), qt.IsNil)
}
