package conformance_test

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/grpc/codes"
)

// The memory provider is the suite's own subject: an engine whose databases
// live in the test process, named memory:///<name>. It serves sessions,
// reads, rendering, execution, locks and transactions the way the contract
// asks, and each defect switches one of those ways off, so that a test can
// show the check that catches it.
const (
	memoryProviderID = "example.com/conformance-memory"
	memoryEngine     = "memory"
	unreadTable      = "ptah_unread"
)

// memoryDefects are the ways the memory provider can break the contract.
type memoryDefects struct {
	// renderDropsUnit returns one unit less than the request has changes.
	renderDropsUnit bool
	// renderCounts writes how many renders the process made into each unit.
	renderCounts bool
	// leakAddress quotes the address in the reason Open refuses it with.
	leakAddress bool
	// unreadRegardless reports an unread table whatever the host listed.
	unreadRegardless bool
	// sharedLocks hands a held lock to every session that asks.
	sharedLocks bool
}

type memoryProvider struct {
	defects memoryDefects

	mu          sync.Mutex
	databases   map[string]map[string][]*protocol.Column
	sessions    map[string]*memorySession
	held        map[string]memoryHolder
	renders     int
	nextSession int
}

type memorySession struct {
	database     string
	used         map[string]bool
	released     map[string]bool
	transactions map[string]*protocol.TransactionResult
	open         map[string]bool
}

type memoryHolder struct {
	session, lockID string
}

func newMemoryProvider(defects memoryDefects) *memoryProvider {
	return &memoryProvider{defects: defects, databases: make(map[string]map[string][]*protocol.Column), sessions: make(map[string]*memorySession),
		held: make(map[string]memoryHolder)}
}

func memoryRules() *protocol.IdentifierRules {
	return &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
		IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_SCHEMA}
}

func memoryDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: memoryProviderID, Version: "1.0.0", SemanticIdentity: "memory-1",
		Engines: []*protocol.Engine{{Id: memoryEngine, DefaultSchema: "main", Identifiers: memoryRules(),
			Features: []string{provider.CommonTableIdentityFeature, provider.LocksFeature, provider.TransactionsFeature, provider.UnreadTablesFeature}}},
		Features: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.LocksFeature, provider.TransactionsFeature,
			provider.ErrorReasonsFeature, provider.UnreadTablesFeature},
	}
}

// server joins the memory provider with its description.
func (m *memoryProvider) server() runtime.Server {
	return runtime.Server{Description: memoryDescription(), Sessions: m, Inspector: m, Renderer: m, Executor: m, Locks: m, Transactions: m}
}

func refuse(code codes.Code, reason string) error { return runtime.ErrorWithReason(code, reason) }

func (m *memoryProvider) Open(_ context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	parsed, err := url.Parse(request.GetAddress())
	if err != nil || parsed.Scheme != memoryEngine || parsed.Host != "" || parsed.User != nil || strings.Trim(parsed.Path, "/") == "" {
		if m.defects.leakAddress {
			return nil, refuse(codes.InvalidArgument, "cannot open "+request.GetAddress())
		}
		return nil, refuse(codes.InvalidArgument, "a memory address names a database and nothing else: memory:///name")
	}
	name := strings.Trim(parsed.Path, "/")
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.databases[name] == nil {
		m.databases[name] = make(map[string][]*protocol.Column)
	}
	m.nextSession++
	id := fmt.Sprintf("memory-session-%d", m.nextSession)
	m.sessions[id] = &memorySession{database: name, used: make(map[string]bool), released: make(map[string]bool),
		transactions: make(map[string]*protocol.TransactionResult), open: make(map[string]bool)}
	return &protocol.Session{Id: id, Engine: memoryEngine, Database: name, DefaultSchema: "main", ServerVersion: "1", Identifiers: memoryRules()}, nil
}

func (m *memoryProvider) Close(_ context.Context, request *protocol.CloseRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	session := m.sessions[request.GetSessionId()]
	if session == nil {
		return nil
	}
	for key, holder := range m.held {
		if holder.session == request.GetSessionId() {
			delete(m.held, key)
		}
	}
	delete(m.sessions, request.GetSessionId())
	return nil
}

// lookup returns an open session; the caller holds m.mu.
func (m *memoryProvider) lookup(id string) (*memorySession, error) {
	session := m.sessions[id]
	if session == nil {
		return nil, refuse(codes.NotFound, "the session is closed or was never opened")
	}
	return session, nil
}

func (m *memoryProvider) Inspect(ctx context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, err := m.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if len(request.GetSchemas()) != 0 {
		return nil, refuse(codes.InvalidArgument, "a memory database reads whole")
	}
	snapshot := &protocol.Schema{Complete: true, Engine: memoryEngine, Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED}}
	tables := m.databases[session.database]
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		identity := &protocol.Reference{Engine: memoryEngine, Database: session.database, Kind: "table", Name: name, DisplayName: name}
		snapshot.Tables = append(snapshot.Tables, &protocol.Table{Identity: identity, NormalizedIdentity: identity, Columns: tables[name]})
	}
	if m.defects.unreadRegardless || slices.Contains(provider.HostFeatures(ctx), provider.UnreadTablesFeature) {
		snapshot.TargetFeatures = []string{provider.UnreadTablesFeature}
		snapshot.UnreadTables = []*protocol.UnreadTable{{
			Table:      &protocol.Reference{Engine: memoryEngine, Database: session.database, Kind: "table", Name: unreadTable, DisplayName: unreadTable},
			Diagnostic: &protocol.ProviderDiagnostic{Reason: "the memory engine keeps this table for itself"},
		}}
	}
	return snapshot, nil
}

func (m *memoryProvider) Render(_ context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	m.mu.Lock()
	m.renders++
	count := m.renders
	m.mu.Unlock()
	result := &protocol.RenderResult{Complete: true}
	for _, change := range request.GetChanges() {
		statement, err := renderChange(change)
		if err != nil {
			return nil, err
		}
		if m.defects.renderCounts {
			statement += fmt.Sprintf(" -- render %d", count)
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: change.GetId(), Body: &protocol.Unit_Sql{Sql: statement}})
	}
	if m.defects.renderDropsUnit && len(result.Units) > 0 {
		result.Units = result.Units[:len(result.Units)-1]
	}
	return result, nil
}

func renderChange(change *protocol.Change) (string, error) {
	name := change.GetObject().GetName()
	switch operation := change.GetOperation().(type) {
	case *protocol.Change_CreateTable:
		columns := make([]string, 0, len(operation.CreateTable.GetColumns()))
		for _, column := range operation.CreateTable.GetColumns() {
			columns = append(columns, column.GetName()+" "+column.GetType())
		}
		return "CREATE TABLE " + name + " (" + strings.Join(columns, ", ") + ")", nil
	case *protocol.Change_DropTable:
		return "DROP TABLE " + name, nil
	default:
		return "", refuse(codes.InvalidArgument, "the memory engine creates and drops tables, nothing else")
	}
}

func (m *memoryProvider) Execute(_ context.Context, request *protocol.ExecuteRequest) (*protocol.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, err := m.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if id := request.GetTransactionId(); id != "" && !session.open[id] {
		return nil, refuse(codes.FailedPrecondition, "the transaction is not open")
	}
	tables := m.databases[session.database]
	for i, unit := range request.GetUnits() {
		if !applyUnit(tables, unit.GetSql()) {
			outcome := protocol.Execution_NOT_APPLIED
			if i > 0 {
				outcome = protocol.Execution_PARTIAL
			}
			return &protocol.Execution{Outcome: outcome, AppliedCount: uint32(i), Message: "a unit was refused", // #nosec G115 -- a request holds far fewer units than uint32 counts
				Diagnostic: &protocol.ProviderDiagnostic{Reason: "the memory engine refused a unit"}}, nil
		}
	}
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED, AppliedCount: uint32(len(request.GetUnits()))}, nil // #nosec G115 -- as above
}

// applyUnit applies one statement Render writes, and reports false for one
// the engine refuses. A trailing comment is not part of the statement.
func applyUnit(tables map[string][]*protocol.Column, statement string) bool {
	statement, _, _ = strings.Cut(statement, " -- ")
	if name, found := strings.CutPrefix(statement, "DROP TABLE "); found {
		if _, exists := tables[name]; !exists {
			return false
		}
		delete(tables, name)
		return true
	}
	rest, found := strings.CutPrefix(statement, "CREATE TABLE ")
	name, definition, parsed := strings.Cut(rest, " (")
	if !found || !parsed || tables[name] != nil {
		return false
	}
	var columns []*protocol.Column
	for column := range strings.SplitSeq(strings.TrimSuffix(definition, ")"), ", ") {
		columnName, kind, _ := strings.Cut(column, " ")
		columns = append(columns, &protocol.Column{Name: columnName, Type: kind, Nullable: true})
	}
	tables[name] = columns
	return true
}

func lockResult(request *protocol.LockRequest, outcome protocol.LockResult_Outcome, reason string) *protocol.LockResult {
	result := &protocol.LockResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: outcome}
	if reason != "" {
		result.Diagnostic = &protocol.ProviderDiagnostic{Reason: reason}
	}
	return result
}

func (m *memoryProvider) TryLock(_ context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	scope := request.GetScope()
	session, err := m.lookup(scope.GetSessionId())
	if err != nil {
		return nil, err
	}
	if session.used[scope.GetLockId()] {
		return lockResult(request, protocol.LockResult_NOT_ACQUIRED, "the lock ID was used"), nil
	}
	session.used[scope.GetLockId()] = true
	key := session.database + "\x00" + scope.GetName()
	if _, busy := m.held[key]; busy && !m.defects.sharedLocks {
		return lockResult(request, protocol.LockResult_BUSY, "another session holds the lock"), nil
	}
	m.held[key] = memoryHolder{session: scope.GetSessionId(), lockID: scope.GetLockId()}
	return lockResult(request, protocol.LockResult_ACQUIRED, ""), nil
}

func (m *memoryProvider) CheckLock(_ context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	scope := request.GetScope()
	session, err := m.lookup(scope.GetSessionId())
	if err != nil {
		return nil, err
	}
	if m.held[session.database+"\x00"+scope.GetName()] == (memoryHolder{session: scope.GetSessionId(), lockID: scope.GetLockId()}) {
		return lockResult(request, protocol.LockResult_ACQUIRED, ""), nil
	}
	return lockResult(request, protocol.LockResult_NOT_ACQUIRED, "the lock ID holds no lock"), nil
}

func (m *memoryProvider) ReleaseLock(_ context.Context, request *protocol.LockRequest) (*protocol.LockResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	scope := request.GetScope()
	session, err := m.lookup(scope.GetSessionId())
	if err != nil {
		return nil, err
	}
	key := session.database + "\x00" + scope.GetName()
	switch {
	case m.held[key] == (memoryHolder{session: scope.GetSessionId(), lockID: scope.GetLockId()}):
		delete(m.held, key)
		session.released[scope.GetLockId()] = true
		return lockResult(request, protocol.LockResult_RELEASED, ""), nil
	case session.released[scope.GetLockId()]:
		return lockResult(request, protocol.LockResult_RELEASED, ""), nil
	default:
		session.used[scope.GetLockId()] = true
		return lockResult(request, protocol.LockResult_NOT_ACQUIRED, ""), nil
	}
}

func (m *memoryProvider) BeginTransaction(_ context.Context, request *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, err := m.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	if session.open[request.GetTransactionId()] || session.transactions[request.GetTransactionId()] != nil {
		return nil, refuse(codes.FailedPrecondition, "the transaction ID was used")
	}
	session.open[request.GetTransactionId()] = true
	return &protocol.Transaction{SessionId: request.GetSessionId(), Id: request.GetTransactionId(), Isolation: request.GetIsolation()}, nil
}

func (m *memoryProvider) FinishTransaction(_ context.Context, request *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session, err := m.lookup(request.GetSessionId())
	if err != nil {
		return nil, err
	}
	id := request.GetTransactionId()
	if stored := session.transactions[id]; stored != nil {
		return stored, nil
	}
	result := &protocol.TransactionResult{Outcome: protocol.TransactionResult_NOT_STARTED}
	switch {
	case !session.open[id] && request.GetAction() == protocol.FinishTransactionRequest_COMMIT:
		result.Diagnostic = &protocol.ProviderDiagnostic{Reason: "the transaction never began"}
	case session.open[id] && request.GetAction() == protocol.FinishTransactionRequest_COMMIT:
		result.Outcome = protocol.TransactionResult_COMMITTED
	case session.open[id]:
		result.Outcome = protocol.TransactionResult_ROLLED_BACK
	}
	delete(session.open, id)
	session.transactions[id] = result
	return result, nil
}
