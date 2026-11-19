package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// repeatableStore keeps the record of repeatable migrations through the
// provider's sessions.repeatable-record calls. A store a transaction returns
// writes only completions, in that transaction; every other operation runs on
// the session.
type repeatableStore struct{ historyStore }

// RepeatableStore returns the record of repeatable migrations, or refuses an
// engine whose provider does not keep one; see [target.unrecordedRepeatables].
func (s *session) RepeatableStore() (dbschema.RepeatableStore, error) {
	if !s.target.negotiated(provider.RepeatableRecordFeature) {
		return nil, s.target.unrecordedRepeatables()
	}
	return repeatableStore{historyStore{session: s}}, nil
}

// RepeatableStore returns the record of repeatable migrations bound to the
// transaction, which writes a completion with the migration body.
func (t *transaction) RepeatableStore() (dbschema.RepeatableStore, error) {
	if _, err := t.session.RepeatableStore(); err != nil {
		return nil, err
	}
	return repeatableStore{historyStore{session: t.session, transaction: t}}, nil
}

// unrecordedRepeatables is the refusal of the record by a provider or an
// engine that does not list [provider.RepeatableRecordFeature]. It names what
// the operator can change: the provider.
func (t *target) unrecordedRepeatables() error {
	return fmt.Errorf("%w: provider %s keeps no record of repeatable migrations for engine %s, because it does not list %s "+
		"(contract 25.44); install a provider release that lists it",
		ptaherr.ErrUnsupportedFeature, t.host.description.GetProviderId(), t.engine.GetId(), provider.RepeatableRecordFeature)
}

// MaxBodyBytes is the longest content a completion carries.
func (r repeatableStore) MaxBodyBytes() int { return provider.MaxRepeatableBodyBytes }

// scope binds a call to the session. A store bound to a transaction refuses
// it: only a completion runs there, under [repeatableStore.completionScope].
func (r repeatableStore) scope(table dbschema.MigrationMetadataTable) (*protocol.RepeatableScope, error) {
	if r.transaction != nil {
		return nil, unsupported("a repeatable migration record operation other than a completion inside a transaction")
	}
	return r.bind(table, "")
}

// completionScope binds a completion to the session and to the store's
// transaction, so the record and the migration body commit together.
func (r repeatableStore) completionScope(table dbschema.MigrationMetadataTable) (*protocol.RepeatableScope, error) {
	transaction := ""
	if r.transaction != nil {
		transaction = r.transaction.id
	}
	return r.bind(table, transaction)
}

func (r repeatableStore) bind(table dbschema.MigrationMetadataTable, transaction string) (*protocol.RepeatableScope, error) {
	scope := &protocol.RepeatableScope{SessionId: r.session.remote.GetId(), Schema: table.Schema, Table: table.Name, TransactionId: transaction}
	return scope, provider.ValidateRepeatableScope(scope)
}

func (r repeatableStore) Read(ctx context.Context, table dbschema.MigrationMetadataTable) (dbschema.RepeatableSnapshot, error) {
	scope, err := r.scope(table)
	if err != nil {
		return dbschema.RepeatableSnapshot{}, err
	}
	var result *protocol.RepeatableSnapshot
	err = r.use(ctx, func() error {
		var err error
		result, err = r.session.target.host.client.ReadRepeatables(ctx, &protocol.ReadRepeatablesRequest{Scope: scope, MaxRows: provider.MaxRepeatableRows})
		return err
	})
	if err != nil {
		return dbschema.RepeatableSnapshot{}, err
	}
	return repeatableSnapshot(result)
}

// repeatableSnapshot is a snapshot the SDK client accepted, as dbschema holds
// it.
func repeatableSnapshot(result *protocol.RepeatableSnapshot) (dbschema.RepeatableSnapshot, error) {
	snapshot := dbschema.RepeatableSnapshot{Exists: result.GetExists(), Rows: make([]dbschema.RepeatableRow, 0, len(result.GetRows()))}
	for _, row := range result.GetRows() {
		record, err := repeatableRecordRow(row)
		if err != nil {
			return dbschema.RepeatableSnapshot{}, err
		}
		snapshot.Rows = append(snapshot.Rows, record)
	}
	return snapshot, nil
}

func (r repeatableStore) Prepare(ctx context.Context, table dbschema.MigrationMetadataTable) error {
	if r.session.IsDryRun() {
		return unsupported("repeatable migration record preparation during a dry run")
	}
	scope, err := r.scope(table)
	if err != nil {
		return err
	}
	request := &protocol.PrepareRepeatablesRequest{Scope: scope, RequestId: rand.Text()}
	return r.use(ctx, func() error {
		_, err := r.session.target.host.client.PrepareRepeatables(ctx, request)
		return repeatableWriteError(err)
	})
}

func (r repeatableStore) ReadBody(ctx context.Context, table dbschema.MigrationMetadataTable, identity string) (dbschema.RepeatableBody, error) {
	scope, err := r.scope(table)
	if err != nil {
		return dbschema.RepeatableBody{}, err
	}
	var body dbschema.RepeatableBody
	err = r.use(ctx, func() error {
		result, err := r.session.target.host.client.ReadRepeatableBody(ctx, &protocol.ReadRepeatableBodyRequest{Scope: scope, Identity: identity})
		if err != nil {
			return err
		}
		body = dbschema.RepeatableBody{Found: result.GetFound(), Kept: result.GetKept(), Body: result.GetBody()}
		return nil
	})
	if err != nil {
		return dbschema.RepeatableBody{}, err
	}
	return body, nil
}

func (r repeatableStore) Claim(ctx context.Context, table dbschema.MigrationMetadataTable, claim dbschema.RepeatableClaim) error {
	if claim.AttemptOrder <= 0 || claim.AttemptOrder > math.MaxUint32 {
		return fmt.Errorf("invalid repeatable migration attempt order %d", claim.AttemptOrder)
	}
	seconds, nanos, err := metadataTimestamp(claim.AttemptedAt)
	if err != nil {
		return err
	}
	scope, err := r.scope(table)
	if err != nil {
		return err
	}
	_, err = r.mutate(ctx, scope, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Claim{Claim: &protocol.RepeatableClaim{
		Identity: claim.Identity, Insert: claim.Insert, ExpectedRunId: claim.ExpectedRunID, RunId: claim.RunID,
		AttemptChecksum: claim.AttemptChecksum, AttemptOrder: uint32(claim.AttemptOrder), Transactional: claim.Transactional,
		AttemptedAtSeconds: seconds, AttemptedAtNanos: nanos,
	}}})
	return err
}

func (r repeatableStore) Complete(ctx context.Context, table dbschema.MigrationMetadataTable, completion dbschema.RepeatableCompletion) error {
	seconds, nanos, err := metadataTimestamp(completion.AppliedAt)
	if err != nil {
		return err
	}
	scope, err := r.completionScope(table)
	if err != nil {
		return err
	}
	_, err = r.mutate(ctx, scope, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Complete{Complete: &protocol.RepeatableCompletion{
		Identity: completion.Identity, RunId: completion.RunID, Checksum: completion.Checksum, Body: completion.Body,
		AppliedAtSeconds: seconds, AppliedAtNanos: nanos,
	}}})
	return err
}

func (r repeatableStore) Fail(ctx context.Context, table dbschema.MigrationMetadataTable, failure dbschema.RepeatableFailure) error {
	scope, err := r.scope(table)
	if err != nil {
		return err
	}
	_, err = r.mutate(ctx, scope, &protocol.MutateRepeatableRequest{Mutation: &protocol.MutateRepeatableRequest_Fail{Fail: &protocol.RepeatableFailure{
		Identity: failure.Identity, RunId: failure.RunID, Error: repeatableErrorText(failure.Error),
	}}})
	return err
}

func (r repeatableStore) Remove(ctx context.Context, table dbschema.MigrationMetadataTable, identity string) (bool, error) {
	scope, err := r.scope(table)
	if err != nil {
		return false, err
	}
	result, err := r.mutate(ctx, scope, &protocol.MutateRepeatableRequest{
		Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: identity}},
	})
	if err != nil {
		return false, err
	}
	return result.GetRemoved(), nil
}

// mutate sends request, one write that changes one row of the record or
// nothing, under scope and a new request ID. It returns the receipt of a write
// that applied.
func (r repeatableStore) mutate(
	ctx context.Context,
	scope *protocol.RepeatableScope,
	request *protocol.MutateRepeatableRequest,
) (*protocol.RepeatableResult, error) {
	if r.session.IsDryRun() {
		return nil, unsupported("repeatable migration record writes during a dry run")
	}
	request.Scope, request.RequestId = scope, rand.Text()
	if err := provider.ValidateMutateRepeatableRequest(request); err != nil {
		return nil, err
	}
	var receipt *protocol.RepeatableResult
	err := r.use(ctx, func() error {
		var err error
		receipt, err = r.session.target.host.client.MutateRepeatable(ctx, request)
		return repeatableWriteError(err)
	})
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

// repeatableWriteError turns a CONFLICT into the conflict dbschema names, and
// leaves an uncertain outcome to [historyStore.use], which marks it unknown.
func repeatableWriteError(err error) error {
	if refused, ok := errors.AsType[*provider.RepeatableError](err); ok && refused.Outcome == protocol.RepeatableResult_CONFLICT {
		return fmt.Errorf("%w: %w", &dbschema.RepeatableConflictError{Holder: refused.Holder}, err)
	}
	return err
}

// repeatableErrorText is a failure's error as the record keeps it: valid
// UTF-8 without NUL bytes, at most [provider.MaxRepeatableErrorBytes] long,
// cut at a character boundary, and never empty.
func repeatableErrorText(text string) string {
	text = strings.ReplaceAll(strings.ToValidUTF8(text, "�"), "\x00", "�")
	if text == "" {
		return "the repeatable migration failed"
	}
	if len(text) <= provider.MaxRepeatableErrorBytes {
		return text
	}
	cut := provider.MaxRepeatableErrorBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}

// repeatableRecordRow is a row the SDK client accepted, as dbschema holds it.
func repeatableRecordRow(row *protocol.RepeatableRow) (dbschema.RepeatableRow, error) {
	states := map[protocol.RepeatableRow_State]string{
		protocol.RepeatableRow_RUNNING: dbschema.RepeatableRunning,
		protocol.RepeatableRow_APPLIED: dbschema.RepeatableApplied,
		protocol.RepeatableRow_FAILED:  dbschema.RepeatableFailed,
	}
	state, ok := states[row.GetState()]
	if !ok || uint64(row.GetAttemptOrder()) > math.MaxInt {
		return dbschema.RepeatableRow{}, fmt.Errorf("%w: invalid repeatable migration row", provider.ErrProtocol)
	}
	result := dbschema.RepeatableRow{
		Identity: row.GetIdentity(), State: state, Checksum: row.GetChecksum(), AttemptChecksum: row.GetAttemptChecksum(),
		AttemptOrder: int(row.GetAttemptOrder()), Transactional: row.GetTransactional(), RunID: row.GetRunId(),
		AttemptedAt: time.Unix(row.GetAttemptedAtSeconds(), int64(row.GetAttemptedAtNanos())).UTC(),
		Error:       row.GetError(), BodyKept: row.GetBodyKept(),
	}
	if row.GetApplied() {
		result.AppliedAt = time.Unix(row.GetAppliedAtSeconds(), int64(row.GetAppliedAtNanos())).UTC()
	}
	return result, nil
}
