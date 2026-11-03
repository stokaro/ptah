package providerhost

import (
	"context"
	"fmt"
	"slices"
	"sync"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/migrationsplit"
)

var (
	_ migrationsplit.Splitter       = (*session)(nil)
	_ catalog.SchemaTransaction     = (*unitTransaction)(nil)
	_ dbschema.HistoryStoreProvider = (*unitTransaction)(nil)
)

// splitsMigrations reports whether the engine splits migration bodies. The
// negotiated description keeps units.migration-split only when the host lists
// it too, and an engine that declares it declares its own transactions and
// history, which the migrator runs units and checkpoints with.
func (t *target) splitsMigrations() bool { return t.offers(provider.MigrationSplitFeature) }

// SplitsMigrations reports whether the session's provider splits migration
// bodies into the units its engine runs.
func (s *session) SplitsMigrations() bool { return s.target.splitsMigrations() }

// SplitMigration asks the provider for the units of body and the verdicts on
// assertions. The answer reads the text alone, so the host keeps it for the
// provider's lifetime and asks once per body and once per assertion.
func (s *session) SplitMigration(ctx context.Context, body string, assertions []string) (migrationsplit.Split, error) {
	if err := ctx.Err(); err != nil {
		return migrationsplit.Split{}, err
	}
	if !s.target.splitsMigrations() {
		return migrationsplit.Split{}, unsupported("migration splits for the selected engine")
	}
	engine := s.target.engine.GetId()
	splits := &s.target.host.splits
	if result, found := splits.lookup(engine, body, assertions); found {
		return result, nil
	}
	if body == "" {
		// The contract carries no request without a body, and an empty body
		// has no unit; only its assertions would need an answer.
		return migrationsplit.Split{}, fmt.Errorf("a migration with an empty body cannot have its check assertions judged by the provider")
	}
	reply, err := s.target.host.client.SplitMigration(ctx, &protocol.SplitMigrationRequest{Engine: engine, Body: body, Assertions: slices.Clone(assertions)})
	if err != nil {
		return migrationsplit.Split{}, err
	}
	result := migrationSplit(reply)
	splits.store(engine, body, assertions, result)
	return result, nil
}

// migrationSplit translates a reply the client validated against its request.
func migrationSplit(reply *protocol.MigrationSplit) migrationsplit.Split {
	var result migrationsplit.Split
	for _, unit := range reply.GetUnits() {
		placement := migrationsplit.Outside
		if unit.GetPlacement() == protocol.MigrationUnit_WITH_CHECKPOINT {
			placement = migrationsplit.WithCheckpoint
		}
		result.Units = append(result.Units, migrationsplit.Unit{SQL: unit.GetSql(), Source: unit.GetSource(), Placement: placement})
	}
	if refusal := reply.GetRefusal(); refusal != nil {
		result.Refusal = &migrationsplit.Refusal{Start: int(refusal.GetStart()), End: int(refusal.GetEnd()), Reason: refusal.GetReason()}
	}
	for _, verdict := range reply.GetAssertions() {
		result.Verdicts = append(result.Verdicts, migrationsplit.Verdict{ReadOnly: verdict.GetReadOnly(), Reason: verdict.GetReason()})
	}
	return result
}

// migrationSplits keeps the answers of one provider: the split of each body
// and the verdict on each assertion, by engine.
type migrationSplits struct {
	mu       sync.Mutex
	bodies   map[splitKey]bodySplit
	verdicts map[splitKey]migrationsplit.Verdict
}

type splitKey struct{ engine, text string }

type bodySplit struct {
	units   []migrationsplit.Unit
	refusal *migrationsplit.Refusal
}

func (m *migrationSplits) lookup(engine, body string, assertions []string) (migrationsplit.Split, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	split, found := m.bodies[splitKey{engine, body}]
	if !found && body != "" {
		return migrationsplit.Split{}, false
	}
	result := migrationsplit.Split{Units: slices.Clone(split.units), Refusal: cloneRefusal(split.refusal)}
	for _, assertion := range assertions {
		verdict, found := m.verdicts[splitKey{engine, assertion}]
		if !found {
			return migrationsplit.Split{}, false
		}
		result.Verdicts = append(result.Verdicts, verdict)
	}
	return result, true
}

func (m *migrationSplits) store(engine, body string, assertions []string, result migrationsplit.Split) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bodies == nil {
		m.bodies = make(map[splitKey]bodySplit)
		m.verdicts = make(map[splitKey]migrationsplit.Verdict)
	}
	m.bodies[splitKey{engine, body}] = bodySplit{units: slices.Clone(result.Units), refusal: cloneRefusal(result.Refusal)}
	for i, assertion := range assertions {
		m.verdicts[splitKey{engine, assertion}] = result.Verdicts[i]
	}
}

func cloneRefusal(refusal *migrationsplit.Refusal) *migrationsplit.Refusal {
	if refusal == nil {
		return nil
	}
	copied := *refusal
	return &copied
}

// BeginUnitTransaction opens the serializable provider transaction one
// WithCheckpoint unit and its checkpoint run in. Unlike BeginTransaction it
// does not require transactional DDL: a split places a unit there only when
// the unit changes data alone.
func (s *session) BeginUnitTransaction(ctx context.Context) (catalog.SchemaTransaction, error) {
	if !s.target.splitsMigrations() {
		return nil, unsupported("unit transactions for the selected engine")
	}
	tx, err := s.beginProviderTransaction(ctx, protocol.TransactionIsolation_SERIALIZABLE)
	if err != nil {
		return nil, err
	}
	return &unitTransaction{tx: tx}, nil
}

// unitTransaction runs one unit and the history write that records it. A
// second execution is refused, so the transaction cannot carry more than the
// unit its checkpoint names.
type unitTransaction struct {
	mu       sync.Mutex
	tx       *transaction
	executed bool
}

func (*unitTransaction) IsDryRun() bool { return false }

func (u *unitTransaction) ExecuteSQL(ctx context.Context, sql string, args ...any) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.executed {
		return fmt.Errorf("a unit transaction runs one unit")
	}
	u.executed = true
	return u.tx.ExecuteSQL(ctx, sql, args...)
}

func (u *unitTransaction) HistoryStore() (dbschema.HistoryStore, error) { return u.tx.HistoryStore() }
func (u *unitTransaction) Commit() error                                { return u.tx.Commit() }
func (u *unitTransaction) Rollback() error                              { return u.tx.Rollback() }

// abortedError marks a failure the provider reported as an abort, so the
// migrator may run the unit again. Only units.migration-split carries the
// flags, and the client returns no reply that misuses them.
func abortedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", migrationsplit.ErrTransactionAborted, err)
}
