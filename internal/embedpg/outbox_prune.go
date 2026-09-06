package embedpg

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedrun"
)

// rowsQueryer is the common query surface used by an ordinary floor lookup and
// the transaction that binds that lookup to pruning.
type rowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// PruneOutbox reads the live-run floor and removes the safe prefix in one
// source-scoped lifecycle transaction.
//
// The lock is the safety boundary, not merely an optimization. A prepare can
// create a new run whose snapshot boundary is earlier than the current floor.
// Without serializing that membership change with the floor query and DELETE,
// the new run can appear between them and lose events it still owes.
//
//revive:disable-next-line:function-result-limit The command needs the floor, its presence, the deletion count, and an error separately.
func (s *Store) PruneOutbox(
	ctx context.Context, outbox *Outbox,
) (OutboxFloorResult, bool, int64, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return OutboxFloorResult{}, false, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	source := outbox.spec.Source
	if err := lifecycleLock(
		ctx, tx, "source", SourceIdentity(source.Schema, source.Table)); err != nil {
		return OutboxFloorResult{}, false, 0, err
	}
	floor, found, err := readOutboxFloor(ctx, tx, source.Schema, source.Table)
	if err != nil {
		return OutboxFloorResult{}, false, 0, err
	}
	if !found {
		if err := tx.Commit(); err != nil {
			return OutboxFloorResult{}, false, 0, fmt.Errorf(
				"finish outbox pruning for %s: %w", source.Table, err)
		}
		return OutboxFloorResult{}, false, 0, nil
	}

	// #nosec G201 -- the table name comes from Outbox.TableName and is quoted.
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE xact < $1`, quoteIdentifier(outbox.TableName()))
	result, err := tx.ExecContext(ctx, query, floor.Position.Transaction)
	if err != nil {
		return OutboxFloorResult{}, false, 0, fmt.Errorf(
			"prune outbox for %s: %w", source.Table, err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return OutboxFloorResult{}, false, 0, fmt.Errorf(
			"prune outbox for %s: %w", source.Table, err)
	}
	if err := tx.Commit(); err != nil {
		return OutboxFloorResult{}, false, 0, fmt.Errorf(
			"finish outbox pruning for %s: %w", source.Table, err)
	}
	return floor, true, removed, nil
}

func readOutboxFloor(
	ctx context.Context,
	source rowsQueryer,
	sourceSchema, sourceTable string,
) (OutboxFloorResult, bool, error) {
	rows, err := source.QueryContext(ctx, outboxFloorSQL,
		SourceIdentity(sourceSchema, sourceTable), sourceTable,
		string(embedrun.StatusAbandoned), string(embedrun.StatusComplete))
	if err != nil {
		return OutboxFloorResult{}, false, fmt.Errorf(
			"read the outbox floor for %s: %w", sourceTable, err)
	}
	defer rows.Close()

	var floor OutboxFloorResult
	var found bool
	for rows.Next() {
		var runID, generation, catchUp, snapshot string
		if err := rows.Scan(&runID, &generation, &catchUp, &snapshot); err != nil {
			return OutboxFloorResult{}, false, fmt.Errorf(
				"read the outbox floor for %s: %w", sourceTable, err)
		}
		cursor, ok, err := embedcatchup.ResumeFrom(catchUp, snapshot)
		if err != nil {
			return OutboxFloorResult{}, false, fmt.Errorf(
				"read the outbox floor for %s: %w", sourceTable, err)
		}
		if !ok {
			continue
		}
		holder := OutboxFloorHolder{RunID: runID, Generation: generation}
		if !found || cursor.Before(floor.Position) {
			floor = OutboxFloorResult{
				Position: cursor,
				Holders:  []OutboxFloorHolder{holder},
			}
			found = true
			continue
		}
		if cursor == floor.Position {
			floor.Holders = append(floor.Holders, holder)
		}
	}
	if err := rows.Err(); err != nil {
		return OutboxFloorResult{}, false, fmt.Errorf(
			"read the outbox floor for %s: %w", sourceTable, err)
	}
	return floor, found, nil
}
