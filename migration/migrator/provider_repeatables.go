package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"ptah.run/dbschema"
)

// This file is the record of repeatable migrations on a connection that
// reaches its engine through a provider. The provider stores the rows through
// a [dbschema.RepeatableStore]; what a row means, which file runs, a claim's
// settlement and a retry, stays in the functions of repeatable_store.go that
// call these, as it does on the SQL path.

// providerRepeatableTable names the record for the provider, beside its
// revision table and under the same suffix the SQL path uses.
func (m *Migrator) providerRepeatableTable() dbschema.MigrationMetadataTable {
	return dbschema.MigrationMetadataTable{Schema: m.metadataTableSchemaName(), Name: m.repeatableTableName()}
}

// readProviderRepeatables reads the record the provider keeps, keyed by
// identity. A missing record has no rows; reading it creates nothing.
func (m *Migrator) readProviderRepeatables(ctx context.Context, store dbschema.RepeatableStore) (map[string]repeatableRow, error) {
	snapshot, err := store.Read(ctx, m.providerRepeatableTable())
	if err != nil {
		return nil, fmt.Errorf("failed to read the repeatable migration record: %w", err)
	}
	rows := make(map[string]repeatableRow, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		if _, repeated := rows[row.Identity]; repeated {
			return nil, fmt.Errorf("failed to read the repeatable migration record: identity %q appears twice", row.Identity)
		}
		rows[row.Identity] = repeatableRow{
			identity: row.Identity, checksum: row.Checksum, state: row.State, attemptChecksum: row.AttemptChecksum,
			attemptOrder: row.AttemptOrder, transactional: row.Transactional, runID: row.RunID,
			attemptedAt: row.AttemptedAt, appliedAt: row.AppliedAt, errorText: row.Error, hasBody: row.BodyKept,
		}
	}
	return rows, nil
}

// readProviderRepeatableBody reads the content the provider keeps for
// identity, nil when the row keeps none. A row that is gone is an error, as
// the SQL path's read of it is.
func (m *Migrator) readProviderRepeatableBody(ctx context.Context, store dbschema.RepeatableStore, identity string) ([]byte, error) {
	body, err := store.ReadBody(ctx, m.providerRepeatableTable(), identity)
	if err != nil {
		return nil, fmt.Errorf("failed to read the body of repeatable migration %s: %w", identity, err)
	}
	if !body.Found {
		return nil, fmt.Errorf("failed to read the body of repeatable migration %s: the record holds no row for it", identity)
	}
	return body.Body, nil
}

// claimProviderRepeatable writes the attempt's claim: an insert where the run
// read no row, and otherwise a claim that applies only while the row still
// names the run this run read. A claim that met another row returns
// errRepeatableRowUnmatched, which [Migrator.settleRepeatableClaim] decides
// as it decides an UPDATE that matched nothing.
func (m *Migrator) claimProviderRepeatable(ctx context.Context, store dbschema.RepeatableStore, attempt repeatableAttempt) error {
	claim := dbschema.RepeatableClaim{
		Identity: attempt.repeatable.identity, Insert: !attempt.recorded, RunID: m.migrationLogRunID,
		AttemptChecksum: attempt.repeatable.checksum, AttemptOrder: attempt.order, Transactional: attempt.transactional,
		AttemptedAt: time.Now().UTC(),
	}
	if attempt.recorded {
		claim.ExpectedRunID = attempt.read.runID
	}
	err := store.Claim(ctx, m.providerRepeatableTable(), claim)
	if _, conflict := errors.AsType[*dbschema.RepeatableConflictError](err); conflict {
		return fmt.Errorf("%w: %w", errRepeatableRowUnmatched, err)
	}
	return err
}

// providerRepeatableRunID reads the run ID of the provider's row for
// identity, and whether there is one.
func (m *Migrator) providerRepeatableRunID(ctx context.Context, store dbschema.RepeatableStore, identity string) (string, bool, error) {
	rows, err := m.readProviderRepeatables(ctx, store)
	if err != nil {
		return "", false, err
	}
	row, found := rows[identity]
	return row.runID, found, nil
}

// completeProviderRepeatable records the attempt's content as applied in the
// store conn returns: bound to the body's transaction when the body ran in
// one, so the record and the work commit together.
func (m *Migrator) completeProviderRepeatable(ctx context.Context, store dbschema.RepeatableStore, repeatable *repeatableMigration) error {
	return store.Complete(ctx, m.providerRepeatableTable(), dbschema.RepeatableCompletion{
		Identity: repeatable.identity, RunID: m.migrationLogRunID, Checksum: repeatable.checksum,
		Body: repeatable.body, AppliedAt: time.Now().UTC(),
	})
}

// failProviderRepeatable records that the attempt's body failed. The provider
// keeps the error up to its own bound.
func (m *Migrator) failProviderRepeatable(
	ctx context.Context,
	store dbschema.RepeatableStore,
	repeatable *repeatableMigration,
	failure error,
) error {
	return store.Fail(ctx, m.providerRepeatableTable(), dbschema.RepeatableFailure{
		Identity: repeatable.identity, RunID: m.migrationLogRunID, Error: failure.Error(),
	})
}

// refuseRepeatablesOverBodyLimit refuses, before anything runs, a repeatable
// migration whose content the provider's record cannot keep. Without the
// refusal the body runs and its completion is refused, which leaves the row
// running; a retry runs the body again and fails the same way.
func refuseRepeatablesOverBodyLimit(store dbschema.RepeatableStore, state *repeatableState, work []RepeatableWork) error {
	limit := store.MaxBodyBytes()
	for _, item := range work {
		repeatable := state.repeatable(item.Identity)
		if len(repeatable.body) > limit {
			return &RepeatableBodyTooLongError{Path: repeatable.path, Length: len(repeatable.body), Limit: limit}
		}
	}
	return nil
}

// RepeatableBodyTooLongError reports a repeatable migration whose content is
// longer than the record of repeatable migrations keeps on an engine reached
// through a provider. It is returned before anything runs.
type RepeatableBodyTooLongError struct {
	// Path is the file.
	Path string
	// Length is the content's length in bytes.
	Length int
	// Limit is the longest content the record keeps, in bytes.
	Limit int
}

// Error names the file, its length and the limit.
func (e *RepeatableBodyTooLongError) Error() string {
	return fmt.Sprintf("repeatable migration %s is %d bytes, and the record of repeatable migrations on this engine keeps "+
		"at most %d; split the file into smaller repeatable migrations", e.Path, e.Length, e.Limit)
}
