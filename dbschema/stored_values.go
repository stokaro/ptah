package dbschema

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrStoredValuesRewritten is the error a statement run by
// [DatabaseConnection.WithStoredValuesKept] fails with when the server refuses
// it because it would change values a table stores to fit a new definition.
// The error that wraps it also wraps the server's own error.
var ErrStoredValuesRewritten = errors.New("the change would rewrite stored values")

// storedValuesRestoreTimeout bounds giving a session back the settings
// [DatabaseConnection.WithStoredValuesKept] changed, which runs after the
// caller's context may have ended.
const storedValuesRestoreTimeout = 5 * time.Second

// WithStoredValuesKept calls use with one physical database session on which
// the server refuses a statement that would change a value a table stores to
// fit a new definition, where a setting of the session would let the server
// make that change and warn. On MySQL and MariaDB that is a session whose
// sql_mode names no strict mode: there an ALTER TABLE that drops an ENUM or SET
// member some row holds, or narrows a column below a value some row holds,
// succeeds and rewrites the rows. Other engines refuse such a statement
// whatever the session says, and use runs on the session unchanged.
//
// Run Ptah's own plan through it, never SQL the user wrote: a migration file
// may depend on the server's settings, and runs as written.
//
// A connection that is not pinned pins a session for use and discards it
// afterward, where the engine decides by a setting of the session, and a
// pinned one runs use on its own session. Either way the session has the
// settings it started with again when WithStoredValuesKept returns, and an
// error setting them back is joined to the result. An error from use that the
// server gives for a statement it refuses this way wraps
// [ErrStoredValuesRewritten] and names the column the server names.
func (dc *DatabaseConnection) WithStoredValuesKept(
	ctx context.Context,
	use func(*DatabaseConnection) error,
) error {
	if dc == nil {
		return fmt.Errorf("stored values session requires an open database connection")
	}
	if use == nil {
		return fmt.Errorf("database session callback is nil")
	}
	if dc.native != nil || !dc.engine().StoredValuesBySession() {
		// A native engine runs no SQL session whose settings could let a
		// statement rewrite stored values, and another refuses one whatever
		// the session says.
		return use(dc)
	}
	return dc.WithSessionOrCurrent(ctx, func(session *DatabaseConnection) (resultErr error) {
		if session.session == nil {
			return fmt.Errorf("stored values session requires a pinned SQL session")
		}
		engine := session.engine()
		restore, err := engine.KeepStoredValues(ctx, session.session)
		if err != nil {
			return fmt.Errorf("keep stored values on the session: %w", err)
		}
		if restore != nil {
			defer func() {
				restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storedValuesRestoreTimeout)
				defer cancel()
				resultErr = errors.Join(resultErr, restore(restoreCtx))
			}()
		}
		err = use(session)
		if column, refused := engine.StoredValueRefusal(err); refused {
			return storedValuesError(column, err)
		}
		return err
	})
}

// storedValuesError is err, refused by the server because it would rewrite
// the values column stores, with the column in front of the reason.
func storedValuesError(column string, err error) error {
	if column == "" {
		return fmt.Errorf("%w: %w", ErrStoredValuesRewritten, err)
	}
	return fmt.Errorf("column %q: %w: %w", column, ErrStoredValuesRewritten, err)
}
