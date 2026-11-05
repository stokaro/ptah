// Package main serves the fixture with transactions, history and locks, and
// says why it refuses a read, a unit or a commit. It lists errors.reasons
// unless PTAH_FIXTURE_OMITTED names it, so a test can run the same refusal
// with the feature and without it. An inspection of a database opened with
// the fault "refuse" fails with a reason this program wrote, while the status
// message carries text no host may print. A unit that names the table
// "refused" applies nothing, and a transaction that ran a unit naming the
// table "uncommitted" rolls back when asked to commit; each result has a
// reason, and a message that carries text no host may print.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// omitted names the variable holding the comma-separated features the
// fixture leaves out of its description.
const omitted = "PTAH_FIXTURE_OMITTED"

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host explicitly selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := backend.Description()
	description.SemanticIdentity += "-error-reasons"
	services := []string{provider.TransactionsFeature, provider.HistoryFeature, provider.LocksFeature, provider.MetadataFeature}
	description.Features = append(description.Features, services...)
	description.Engines[0].Features = append(description.Engines[0].Features, services...)
	if !slices.Contains(strings.Split(os.Getenv(omitted), ","), provider.ErrorReasonsFeature) {
		description.Features = append(description.Features, provider.ErrorReasonsFeature)
	}
	owner := &reasonsOwner{TransactionalProvider: &backend.TransactionalProvider{}, uncommitted: make(map[string]bool)}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		Transactions: owner, History: owner, Locks: owner, Metadata: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner,
		ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// fixtureCredential stands for a driver's text, which can quote an address
// or a credential, in a result's message.
const fixtureCredential = "fixture://admin:hunter2@db" // #nosec G101 -- a fake credential no host may print

type reasonsOwner struct {
	*backend.TransactionalProvider

	mu sync.Mutex
	// uncommitted holds the transactions that ran a unit naming the table
	// "uncommitted".
	uncommitted map[string]bool
}

// Inspect refuses a session opened with the fault "refuse". The reason is the
// program's own sentence. The status message stands for a driver's error,
// which can quote an address or a credential.
func (o *reasonsOwner) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	var fault string
	err := o.WithSession(ctx, request.GetSessionId(), "", func(access backend.Access) error {
		fault = access.Fault
		return nil
	})
	if err != nil {
		return nil, err
	}
	if fault != "refuse" {
		return o.TransactionalProvider.Inspect(ctx, request)
	}
	reported, err := status.New(codes.FailedPrecondition, "dial fixture://admin:hunter2@db: the driver's text, which no host prints").
		WithDetails(&protocol.ProviderDiagnostic{Reason: "the fixture refuses to read this database"})
	if err != nil {
		return nil, err
	}
	return nil, reported.Err()
}

// Execute applies nothing of a request whose unit names the table "refused",
// and says why. A unit that names "uncommitted" runs, and marks its
// transaction for FinishTransaction to roll back.
func (o *reasonsOwner) Execute(ctx context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	if slices.ContainsFunc(request.GetUnits(), names("refused")) {
		return &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, DiagnosticCode: "fixture.refused",
			Message:    "cannot create the table on " + fixtureCredential,
			Diagnostic: &protocol.ProviderDiagnostic{Reason: "the fixture refuses to create this table"}}, nil
	}
	result, err := o.TransactionalProvider.Execute(ctx, request)
	if err == nil && request.GetTransactionId() != "" && slices.ContainsFunc(request.GetUnits(), names("uncommitted")) {
		o.mu.Lock()
		o.uncommitted[request.GetTransactionId()] = true
		o.mu.Unlock()
	}
	return result, err
}

// FinishTransaction rolls back a commit of a transaction Execute marked, and
// says why.
func (o *reasonsOwner) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	o.mu.Lock()
	marked := o.uncommitted[request.GetTransactionId()]
	o.mu.Unlock()
	if !marked || request.GetAction() != protocol.FinishTransactionRequest_COMMIT {
		return o.TransactionalProvider.FinishTransaction(ctx, request)
	}
	rollback := proto.CloneOf(request)
	rollback.Action = protocol.FinishTransactionRequest_ROLLBACK
	result, err := o.TransactionalProvider.FinishTransaction(ctx, rollback)
	if err != nil || result.GetOutcome() != protocol.TransactionResult_ROLLED_BACK {
		return result, err
	}
	result = proto.CloneOf(result)
	result.Message = "cannot commit on " + fixtureCredential
	result.Diagnostic = &protocol.ProviderDiagnostic{Reason: "the fixture refuses to commit this transaction"}
	return result, nil
}

// names reports a SQL unit that quotes table.
func names(table string) func(*protocol.Unit) bool {
	return func(unit *protocol.Unit) bool {
		return strings.Contains(unit.GetSql(), `"`+table+`"`)
	}
}
