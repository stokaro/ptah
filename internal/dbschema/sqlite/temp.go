package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// DropSessionObjects drops every object in the pinned session's temp schema:
// triggers first, then views, then tables, which also take their indexes with
// them. A TEMP object lives as long as its connection, and an in-memory
// database keeps its only connection for the life of the database, so a replay
// that created one has to give it up explicitly, or the next replay collides
// with it and the realm cleanup refuses the connection.
//
// It drops everything it can and returns the joined errors of what it could
// not. The temp schema is empty when the call returns without an error. A
// writer that is not bound to a pinned session has no session to clean and
// returns an error.
func (w *Writer) DropSessionObjects(ctx context.Context) (resultErr error) {
	if w.conn == nil {
		return errors.New("sqlite: dropping session objects requires a pinned connection")
	}
	session := w.conn
	objects, err := listTempObjects(ctx, session)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return nil
	}
	// DROP TABLE runs an implicit DELETE FROM when foreign keys are on, and
	// a parent dropped before its child fails on the child's rows. The drop
	// does not depend on the order of the names, so enforcement is off for it
	// and restored after. The pragma is a no-op inside a transaction, so the
	// value is read back to prove it took effect.
	foreignKeys, err := readForeignKeys(ctx, session)
	if err != nil {
		return err
	}
	if foreignKeys {
		if _, err := session.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
			return fmt.Errorf("sqlite: disable foreign keys for the TEMP drop: %w", err)
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
			defer cancel()
			if _, restoreErr := session.ExecContext(restoreCtx, "PRAGMA foreign_keys = ON"); restoreErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: restore foreign keys after the TEMP drop: %w", restoreErr))
			}
		}()
		if still, err := readForeignKeys(ctx, session); err != nil || still {
			return errors.Join(err, errors.New("sqlite: foreign keys stay on inside a transaction, so TEMP objects cannot be dropped"))
		}
	}
	for _, object := range objects {
		// #nosec G202 -- the kind comes from a fixed set and the name is an identifier quoted by sqlident.
		statement := "DROP " + object.kind + " IF EXISTS " + quoteQualifiedIdent("temp", object.name)
		if _, err := session.ExecContext(ctx, statement); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("sqlite: drop TEMP %s %q: %w", object.kind, object.name, err))
		}
	}
	return resultErr
}

type tempObject struct {
	kind string
	name string
}

func listTempObjects(ctx context.Context, session *sql.Conn) ([]tempObject, error) {
	// Escaped: LIKE reads a bare _ as a wildcard. The sqlite_ prefix is the
	// engine's own (sqlite_sequence, sqlite_stat1) and cannot be dropped.
	const query = `
		SELECT upper(type), name FROM temp.sqlite_schema
		WHERE type IN ('trigger', 'view', 'table')
		  AND name NOT LIKE 'sqlite\_%' ESCAPE '\'
		ORDER BY CASE type WHEN 'trigger' THEN 0 WHEN 'view' THEN 1 ELSE 2 END, name
	`
	rows, err := session.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list TEMP objects: %w", err)
	}
	defer rows.Close()

	var objects []tempObject
	for rows.Next() {
		var object tempObject
		if err := rows.Scan(&object.kind, &object.name); err != nil {
			return nil, fmt.Errorf("sqlite: scan TEMP object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate TEMP objects: %w", err)
	}
	return objects, nil
}
