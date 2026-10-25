package migrator

import (
	"context"

	"ptah.run/dbschema"
	"ptah.run/internal/filetx"
)

// oneQuerySession runs a body that opens and closes its own transactions in
// the transactions PostgreSQL runs it in when it is sent as one query, which
// is how golang-migrate sends a file by default ([filetx.Blocks.AsOneQuery]),
// while the migrator still sends it one statement at a time, so progress is
// recorded where each transaction commits.
//
// Before a statement that no transaction holds, the session opens one, so
// the statements before a BEGIN run in the transaction the BEGIN goes on to
// name, and those after a COMMIT run in the next one. The body's BEGIN opens
// the transaction itself when it is the first statement of one; one that
// follows statements of its transaction is not sent, because PostgreSQL would
// only warn that a transaction is in progress, and its modes are applied with
// SET TRANSACTION, which is what PostgreSQL does with the modes of a BEGIN
// inside a query. An isolation level is refused there after the
// transaction's first query, as golang-migrate's single query refuses it.
// The body's COMMIT ends the transaction, and the end of the body commits the
// last one ([oneQuerySession.finish]).
//
// A nil session runs nothing: the body's statements outside its own
// transactions then commit on their own.
type oneQuerySession struct {
	conn *dbschema.DatabaseConnection
	// transactions are the transactions the body's statements run in.
	transactions filetx.Blocks
	// open reports a transaction of the session's the next statement joins.
	open bool
}

// withOneQueryHooks installs the session's hooks alone, for a caller that
// records no progress, such as the dev database replay.
func (s *oneQuerySession) withOneQueryHooks(ctx context.Context) context.Context {
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
// place of the migrator; see [oneQuerySession.begin].
func (s *oneQuerySession) withBeginCommitter(ctx context.Context) context.Context {
	return withStatementCommitter(ctx, statementCommitter{
		claims: func(statement string) bool {
			_, begin := filetx.Begin(statement)
			return begin
		},
		run: s.begin,
	})
}

// before opens the transaction event's statement joins, when none is open.
func (s *oneQuerySession) before(ctx context.Context, _ StatementEvent) error {
	if s == nil || s.open {
		return nil
	}
	if err := executeSQLOn(ctx, s.conn, "BEGIN"); err != nil {
		return err
	}
	s.open = true
	return nil
}

// after notes that the statement that closes a transaction ended it.
func (s *oneQuerySession) after(_ context.Context, event StatementEvent) {
	if s != nil && s.transactions.Closes(event.Index-1) {
		s.open = false
	}
}

// begin runs the body's BEGIN: as written when it opens a transaction, and as
// the modes it names, if any, of the transaction already open otherwise.
func (s *oneQuerySession) begin(ctx context.Context, event StatementEvent) error {
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
func (s *oneQuerySession) finish(ctx context.Context) error {
	if !s.open {
		return nil
	}
	s.open = false
	return executeSQLOn(ctx, s.conn, "COMMIT")
}
