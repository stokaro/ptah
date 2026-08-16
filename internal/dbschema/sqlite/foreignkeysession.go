package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
)

// foreignKeySession is foreign-key enforcement turned off on one connection for
// the duration of one transaction, and turned back on afterwards.
//
// It exists because PRAGMA foreign_keys is silently ignored inside a
// transaction. A rebuild plan carries the pragma as a statement -- the pinned
// community binary emits it, and outside a transaction it works -- but ptah
// applies plans in a transaction by default, where executing it would do
// nothing and the DROP would then fail against a referencing row.
//
// The connection is the load-bearing part. A pragma issued on one pooled
// connection and a BEGIN issued on another never meet, so the pragma and the
// transaction are pinned to the same *sql.Conn for as long as both are alive.
type foreignKeySession struct {
	conn     *sql.Conn
	release  func() error
	restored bool
	// enabled is the value found before the session took over, which is what
	// gets put back. A caller that had enforcement off keeps it off.
	enabled bool
	// ctx belongs to the call that opened the session. Commit and Rollback
	// carry no context of their own, and the verification and the restore both
	// have to run somewhere.
	ctx context.Context
}

// verify reports the inbound references the rebuild left unresolved.
//
// The pinned community binary emits no such check: its plan disables
// enforcement, rebuilds, and re-enables, and a rebuild that dropped a
// referenced row leaves the database quietly inconsistent. Running
// PRAGMA foreign_key_check while the transaction can still be rolled back is
// the difference between "the rebuild ran" and "the rebuild ran and every
// reference still resolves". AGENTS.md: matching is the floor.
func (s *foreignKeySession) verify(tx *sql.Tx) error {
	if s == nil {
		return nil
	}
	// Asked through the transaction, not through its connection: the rebuild
	// is not committed yet, and only the transaction that wrote it can see it.
	rows, err := tx.QueryContext(s.ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("sqlite: check foreign keys after rebuild: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			slog.Warn("sqlite: close foreign-key check", "error", closeErr)
		}
	}()

	violations, err := collectForeignKeyViolations(rows)
	if err != nil {
		return err
	}
	if len(violations) == 0 {
		return nil
	}
	return fmt.Errorf(
		"sqlite: rebuild left %d unresolved foreign-key reference(s): %s",
		len(violations),
		strings.Join(violations, ", "),
	)
}

// restore puts enforcement back the way it was found. It runs on a context
// detached from the caller's, for the same reason the drop-all cleanup does:
// an interrupted apply still has to give the connection back usable.
func (s *foreignKeySession) restore() {
	if s == nil || s.restored {
		return
	}
	s.restored = true

	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), sessionRestoreTimeout)
	defer cancel()

	statement := "PRAGMA foreign_keys = 0"
	if s.enabled {
		statement = "PRAGMA foreign_keys = 1"
	}
	if _, err := s.conn.ExecContext(restoreCtx, statement); err != nil {
		// The connection is now in a state the pool must not hand out: it
		// silently skips foreign-key enforcement.
		slog.Error("sqlite: restore foreign keys after rebuild", "error", err)
		discardConn(s.conn)
	}
	if s.release != nil {
		if err := s.release(); err != nil {
			slog.Warn("sqlite: release rebuild connection", "error", err)
		}
	}
}

// collectForeignKeyViolations renders what PRAGMA foreign_key_check reported.
// Its columns are the referencing table, the referencing rowid, the referenced
// table, and the index of the failing foreign key on the referencing table.
func collectForeignKeyViolations(rows *sql.Rows) ([]string, error) {
	var violations []string
	for rows.Next() {
		var (
			child     sql.NullString
			rowID     sql.NullInt64
			parent    sql.NullString
			keyIndex  sql.NullInt64
			renderRow string
		)
		if err := rows.Scan(&child, &rowID, &parent, &keyIndex); err != nil {
			return nil, fmt.Errorf("sqlite: read foreign-key check row: %w", err)
		}
		renderRow = fmt.Sprintf("%s references missing %s", child.String, parent.String)
		violations = append(violations, renderRow)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: read foreign-key check: %w", err)
	}
	return violations, nil
}

// BeginTransactionWithoutForeignKeys begins a transaction with foreign-key
// enforcement disabled on its connection, for a plan that rebuilds a table.
//
// SQLite cannot alter a table in place beyond a narrow grammar, so a rebuild
// creates a copy, drops the original and renames the copy over it. The DROP is
// a foreign-key violation the moment another table references the original --
// measured on SQLite 3.51, `FOREIGN KEY constraint failed` -- so SQLite's own
// documented procedure disables enforcement around the sequence.
//
// Before the commit, PRAGMA foreign_key_check has to pass. Enforcement is put
// back the way it was found whether the transaction commits or rolls back.
func (w *Writer) BeginTransactionWithoutForeignKeys(ctx context.Context) (types.SchemaTransaction, error) {
	if w.dryRun {
		slog.Info("[DRY RUN] Would begin transaction with foreign keys disabled")
		return &transactionWriter{schema: w.schema, dryRun: true}, nil
	}
	if w.db == nil && w.conn == nil {
		return nil, fmt.Errorf("no database connection")
	}

	conn, release, err := w.acquireCleanupConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlite: acquire rebuild connection: %w", err)
	}

	enabled, err := readForeignKeys(ctx, conn)
	if err != nil {
		return nil, errors.Join(err, releaseError(release))
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return nil, errors.Join(
			fmt.Errorf("sqlite: disable foreign keys for rebuild: %w", err),
			releaseError(release),
		)
	}

	session := &foreignKeySession{conn: conn, release: release, enabled: enabled, ctx: ctx}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		session.restore()
		return nil, fmt.Errorf("sqlite: begin rebuild transaction: %w", err)
	}
	return &transactionWriter{tx: tx, schema: w.schema, session: session}, nil
}

func releaseError(release func() error) error {
	if release == nil {
		return nil
	}
	if err := release(); err != nil {
		return fmt.Errorf("sqlite: release rebuild connection: %w", err)
	}
	return nil
}
