package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// probeSchemaName is the alias used by the self-check ATTACH. It must not
// collide with a schema a caller might legitimately attach.
const probeSchemaName = "ptah_restriction_probe"

// RestrictSession applies engine-level restrictions to one pinned SQLite
// session and then proves they are live.
//
// It sets SQLITE_LIMIT_ATTACHED to 0, which makes the engine itself refuse
// ATTACH and DETACH — and, because SQLite implements it by attaching,
// VACUUM INTO as well. That closes the paths by which SQL executed on a
// throwaway dev database could reach another database file or write to an
// arbitrary filesystem path. Loading native extensions is already refused by
// the driver's default configuration.
//
// The limit is a property of the physical connection, so the caller must pass
// a session that is pinned for the whole unit of work; a pooled connection
// would leave sibling connections unrestricted. Because that is easy to get
// wrong and the failure would be silent, this function verifies the
// restriction by attempting an ATTACH and requiring the engine to refuse it.
// A restriction that did not take effect is an error, never a warning.
func RestrictSession(ctx context.Context, session *sql.Conn) error {
	if session == nil {
		return errors.New("sqlite session restriction requires a pinned session")
	}
	if _, err := sqlitedriver.Limit(session, sqlite3.SQLITE_LIMIT_ATTACHED, 0); err != nil {
		return fmt.Errorf("restrict sqlite session: set attached-database limit: %w", err)
	}
	return verifyAttachRefused(ctx, session)
}

// verifyAttachRefused fails unless the engine refuses an ATTACH on this
// session. A successful ATTACH means the restriction is not in force, so the
// probe database is detached again and the caller is told to stop.
func verifyAttachRefused(ctx context.Context, session *sql.Conn) error {
	_, err := session.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE ':memory:' AS %s", probeSchemaName))
	if err != nil {
		return nil
	}
	if _, detachErr := session.ExecContext(ctx, "DETACH DATABASE "+probeSchemaName); detachErr != nil {
		return fmt.Errorf(
			"sqlite session restriction did not take effect: ATTACH still succeeds, and detaching the probe database failed: %w",
			detachErr)
	}
	return errors.New("sqlite session restriction did not take effect: ATTACH still succeeds on the restricted session")
}
