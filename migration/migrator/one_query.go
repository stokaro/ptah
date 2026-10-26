package migrator

import (
	"context"

	"ptah.run/dbschema"
	"ptah.run/internal/filetx"
)

// ownTransactionSession runs a body that opens and closes its own
// transactions on one session, the way a runner that sends the body as it is
// runs it, while the migrator still sends it one statement at a time, so
// progress is recorded where each transaction commits.
//
// With oneQuery set the body runs in the transactions PostgreSQL runs it in
// when it is sent as one query, which is how golang-migrate sends a file by
// default ([filetx.Blocks.AsOneQuery]). Before a statement that no transaction
// holds, the session opens one, so the statements before a BEGIN run in the
// transaction the BEGIN goes on to name, and those after a COMMIT run in the
// next one. The end of the body commits the last one
// ([ownTransactionSession.finish]). Without oneQuery, as under tx-mode none, a
// statement outside the body's own blocks commits on its own.
//
// Either way, the body's BEGIN opens a transaction itself when none is open.
// One that comes while a transaction is open, the session's own or a block of
// the body's, is not sent, because PostgreSQL would only warn that a
// transaction is in progress; its modes are applied with SET TRANSACTION,
// which is what PostgreSQL does with the modes of such a BEGIN. An isolation
// level is refused there after the transaction's first query, as
// golang-migrate's single query refuses it. The body's COMMIT ends the
// transaction, and its savepoint statements run as written inside it.
//
// A nil session runs nothing.
type ownTransactionSession struct {
	conn *dbschema.DatabaseConnection
	// transactions are the transactions the body's statements run in.
	transactions filetx.Blocks
	// oneQuery opens a transaction for a statement no transaction holds.
	oneQuery bool
	// open reports a transaction the next statement joins.
	open bool
}

// withSessionHooks installs the session's hooks alone, for a caller that
// records no progress, such as the dev database replay.
func (s *ownTransactionSession) withSessionHooks(ctx context.Context) context.Context {
	ctx = withStatementProgressRecorder(
		ctx,
		s.before,
		func(ctx context.Context, event StatementEvent) error {
			s.after(ctx, event)
			return nil
		},
	)
	return s.withBeginCommitter(ctx)
}

// withBeginCommitter runs the body's BEGIN statements through the session in
// place of the migrator; see [ownTransactionSession.begin].
func (s *ownTransactionSession) withBeginCommitter(ctx context.Context) context.Context {
	return withStatementCommitter(ctx, statementCommitter{
		claims: func(statement string) bool {
			_, begin := filetx.Begin(statement)
			return begin
		},
		run: s.begin,
	})
}

// before opens the transaction event's statement joins, when the body runs as
// one query and none is open.
func (s *ownTransactionSession) before(ctx context.Context, _ StatementEvent) error {
	if s == nil || !s.oneQuery || s.open {
		return nil
	}
	if err := executeSQLOn(ctx, s.conn, "BEGIN"); err != nil {
		return err
	}
	s.open = true
	return nil
}

// after notes that the statement that closes a transaction ended it.
func (s *ownTransactionSession) after(_ context.Context, event StatementEvent) {
	if s != nil && s.transactions.Closes(event.Index-1) {
		s.open = false
	}
}

// begin runs the body's BEGIN: as written when it opens a transaction, and as
// the modes it names, if any, of the transaction already open otherwise.
func (s *ownTransactionSession) begin(ctx context.Context, event StatementEvent) error {
	if !s.open {
		if err := executeSQLOn(ctx, s.conn, event.Statement); err != nil {
			return err
		}
		s.open = true
		return nil
	}
	modes, _ := filetx.Begin(event.Statement)
	if modes == "" {
		return nil
	}
	return executeSQLOn(ctx, s.conn, "SET TRANSACTION "+modes)
}

// finish commits the transaction the body's last statements ran in, the way
// the end of a query commits it.
func (s *ownTransactionSession) finish(ctx context.Context) error {
	if s == nil || !s.open {
		return nil
	}
	s.open = false
	return executeSQLOn(ctx, s.conn, "COMMIT")
}
