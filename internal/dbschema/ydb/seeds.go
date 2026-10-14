package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/dataext"
	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasretry"
	"ptah.run/internal/yqlquery"
)

// Seeds runs YDB seed files and lists the tables of a YDB directory. It is
// the seed service a YDB connection hands migration/seeder
// (dbschema.DatabaseConnection.Seeds): a seed runs through the driver, so the
// service lives with the connection rather than with the rendering runtime,
// which links no driver.
type Seeds struct{}

var _ dataext.Seeds = Seeds{}

// TableNames lists the tables in the directory the connection reads as its
// schema, as the other engines list the tables of their current schema.
func (Seeds) TableNames(ctx context.Context, db dataext.Database, schema string) ([]string, error) {
	session, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("query target tables: %w", err)
	}
	defer func() { _ = session.Close() }()
	tables, err := TableNames(ctx, session, schema)
	if err != nil {
		return nil, fmt.Errorf("query target tables: %w", err)
	}
	return tables, nil
}

// Apply runs one seed file on YDB.
//
// The file runs as the queries YQL reads it into (internal/yqlquery), so a
// named expression or an action reaches the statements that use it, and all
// of them run in one serializable transaction with the row that records the
// seed: the file and its record commit together or not at all. YDB aborts the
// transaction of two conflicting ones that commits second, and nothing of it
// is applied, so the whole transaction runs again (see [seedTransaction]).
//
// A scheme query, or a BATCH UPDATE or BATCH DELETE, runs on YDB only outside
// a transaction, so a file holding one is refused before anything runs.
//
// YDB has no savepoint, and a conflict ends the transaction it ran in -- the
// next statement in it answers `Transaction not found` -- so a file that hits
// a duplicate key fails whole, and nothing of it is applied. An idempotent run
// records it with [Seeds.Record].
func (Seeds) Apply(ctx context.Context, db dataext.Database, seed dataext.Seed) error {
	queries, err := yqlquery.Split(seed.Text)
	if err != nil {
		return err
	}
	for _, query := range queries {
		if query.Kind != yqlquery.Data {
			return fmt.Errorf("YDB runs a %s query only outside a transaction, and a seed runs "+
				"in one; move this statement to a migration: %s", query.Kind, query.Source)
		}
	}
	return seedTransaction(ctx, db, func(tx *sql.Tx) error {
		for _, query := range queries {
			if _, err := tx.ExecContext(ctx, query.Text); err != nil {
				return err
			}
		}
		if err := recordSeed(ctx, tx, seed); err != nil {
			return fmt.Errorf("record the seed: %w", err)
		}
		return nil
	})
}

// Record writes the row that records a seed, in a transaction of its own.
func (Seeds) Record(ctx context.Context, db dataext.Database, seed dataext.Seed) error {
	return seedTransaction(ctx, db, func(tx *sql.Tx) error {
		return recordSeed(ctx, tx, seed)
	})
}

// seedTransactionAttempts bounds how often a seed transaction YDB aborted for
// a conflicting one is run again.
const seedTransactionAttempts = 5

// seedTransaction runs body in a serializable transaction and commits it, and
// runs the whole transaction again when YDB aborts it: `Transaction locks
// invalidated` is the answer for the transaction of two conflicting ones that
// commits second, and nothing of it was applied. Any other failure is
// returned, with the transaction rolled back.
func seedTransaction(ctx context.Context, db dataext.Database, body func(*sql.Tx) error) error {
	var err error
	for attempt := range seedTransactionAttempts {
		if err = seedAttempt(ctx, db, body); err == nil {
			return nil
		}
		if !atlasretry.IsRetryable(err) || attempt == seedTransactionAttempts-1 {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
	return err
}

// seedAttempt is one attempt of [seedTransaction].
func seedAttempt(ctx context.Context, db dataext.Database, body func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	if err := body(tx); err != nil {
		// A failed statement may have ended the transaction on the server
		// already, and then the rollback answers `Transaction not found`;
		// the statement's failure is the one that matters.
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// recordSeed writes the row that records a seed. UPSERT replaces the row a
// forced re-run finds, which is what the DELETE and INSERT pair does on the
// other engines, in one statement.
func recordSeed(ctx context.Context, tx *sql.Tx, seed dataext.Seed) error {
	query := sqlutil.Rebind(platform.YDB,
		"UPSERT INTO schema_seeds (seed_path, env, checksum, applied_at) VALUES (?, ?, ?, ?)")
	_, err := tx.ExecContext(ctx, query, seed.Path, seed.Env, seed.Checksum, time.Now().UTC())
	return err
}
