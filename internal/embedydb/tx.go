package embedydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// maxAttempts bounds how often a transaction YDB aborted for a conflict runs
// again. Each attempt rereads what it decides on, so running again is correct;
// the bound is what turns a conflict that never clears into an error rather
// than a command that never returns.
const maxAttempts = 12

// errAborted is the last conflict of a transaction that ran out of attempts.
var errAborted = errors.New("another transaction kept changing the rows this one read")

// inTransaction runs fn in a serializable read-write transaction and commits
// it, running it again when YDB aborts it for a conflict.
//
// YDB's serializable transactions are optimistic: a transaction whose reads
// another transaction changed before it committed is aborted with `Transaction
// locks invalidated`, at the statement that noticed or at the commit. That is
// the conflict the PostgreSQL store prevents with locks, and the answer is the
// same either way -- decide again on what is there now. fn therefore has to
// read everything it decides on, in the transaction, on every attempt.
//
// Only a conflict and a transient refusal run again. A refusal fn returns for
// a rule is returned as it is.
func (b *Backend) inTransaction(ctx context.Context, what string, fn func(*sql.Tx) error) error {
	var last error
	for attempt := range maxAttempts {
		if attempt > 0 {
			if err := pause(ctx, attempt); err != nil {
				return fmt.Errorf("%s: %w", what, errors.Join(err, last))
			}
		}
		err := b.attempt(ctx, fn)
		if err == nil {
			return nil
		}
		if !retryable(err) {
			return err
		}
		last = err
	}
	return fmt.Errorf("%s: %w after %d attempts: %w", what, errAborted, maxAttempts, last)
}

// attempt runs one transaction to its commit or its rollback.
func (b *Backend) attempt(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin a YDB transaction: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit a YDB transaction: %w", err)
	}
	return nil
}

// retryable reports a refusal that running the transaction again can clear:
// a conflict with another transaction, or a server that is briefly
// overloaded or unavailable.
func retryable(err error) bool {
	return ydbsdk.IsOperationErrorTransactionLocksInvalidated(err) ||
		ydbsdk.IsOperationError(err, Ydb.StatusIds_OVERLOADED, Ydb.StatusIds_UNAVAILABLE)
}

// pause waits before attempt, longer each time and with jitter, so two
// transactions that conflicted do not meet again at the same instant.
func pause(ctx context.Context, attempt int) error {
	base := min(10*time.Millisecond<<attempt, time.Second)
	// #nosec G404 -- jitter, not a secret.
	wait := base/2 + time.Duration(rand.Int64N(int64(base)))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
