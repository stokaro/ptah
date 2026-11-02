// Package sessiontx begins a transaction on a pinned database/sql session
// whose end the caller owns, so that canceling the caller's context does not
// end it from another goroutine.
//
// database/sql ends a transaction whose context is canceled from a goroutine of
// its own, and discards the session when it does. A caller that later discards
// the same session with Conn.Raw races that discard: Conn.Raw checks that the
// session is open, the discard closes it and clears its driver connection, and
// Conn.Raw dereferences the cleared connection and panics. The callers here
// defer their own rollback and run every statement with the caller's context,
// so a canceled run still stops at its next statement and is rolled back, by the
// caller and before the session is discarded.
package sessiontx

import (
	"context"
	"database/sql"
)

// Begin starts a transaction on session that the caller ends. The transaction
// carries ctx's values but not its cancellation or deadline; a context that is
// already done refuses the transaction with ctx's error.
func Begin(ctx context.Context, session *sql.Conn, opts *sql.TxOptions) (*sql.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return session.BeginTx(context.WithoutCancel(ctx), opts)
}
