package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// probeSchemaName is the alias used by the self-check ATTACH. It must not
// collide with a schema a caller might legitimately attach.
const probeSchemaName = "ptah_restriction_probe"

// attachRefusalMarker is the text SQLite returns when SQLITE_LIMIT_ATTACHED
// forbids an ATTACH. Matching it is what distinguishes "the restriction is
// working" from "the ATTACH failed for some other reason".
const attachRefusalMarker = "too many attached databases"

// RestrictSession applies engine-level restrictions to one pinned SQLite
// session and then proves they are live.
//
// It sets SQLITE_LIMIT_ATTACHED to 0, which makes the engine itself refuse
// ATTACH and DETACH — and, because SQLite implements it by attaching,
// VACUUM INTO as well. So SQL executed on the session cannot reach another
// database file, and cannot write a database copy to an arbitrary path.
// Loading native extensions is already refused by the driver's default
// configuration.
//
// What it does NOT cover: the storage-directory pragmas
// (temp_store_directory, data_store_directory) still execute, and
// writable_schema is still settable. Those remain the lint's problem, not the
// engine's.
//
// The limit is a property of the physical connection, so the caller must pass
// a session that is pinned for the whole unit of work; a pooled connection
// would leave sibling connections unrestricted. Because that is easy to get
// wrong and the failure would be silent, this function verifies the
// restriction by attempting an ATTACH and requiring the engine to refuse it
// with the specific limit error. Any other outcome — the ATTACH succeeding,
// or failing for an unrelated reason that would equally hide a missing
// restriction — is an error, never a warning.
func RestrictSession(ctx context.Context, session *sql.Conn) error {
	if session == nil {
		return errors.New("sqlite session restriction requires a pinned session")
	}
	if _, err := sqlitedriver.Limit(session, sqlite3.SQLITE_LIMIT_ATTACHED, 0); err != nil {
		return fmt.Errorf("restrict sqlite session: set attached-database limit: %w", err)
	}
	return verifyRestriction(ctx, session)
}

// verifyRestriction is the confirmation step, kept as a variable so a test can
// prove RestrictSession actually consults it. Without that, dropping the
// verification would be invisible: on a session where the limit did take
// effect, verifying and not verifying look identical.
var verifyRestriction = verifyAttachRefused

// verifyAttachRefused fails unless the engine refuses an ATTACH on this
// session with the attached-database limit error. An ATTACH that succeeds
// means the restriction is not in force, so the probe database is detached
// again and the caller is told to stop; an ATTACH that fails for some other
// reason proves nothing about the restriction and is treated the same way.
func verifyAttachRefused(ctx context.Context, session *sql.Conn) error {
	_, err := session.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE ':memory:' AS %s", probeSchemaName))
	switch {
	case err == nil:
		if _, detachErr := session.ExecContext(ctx, "DETACH DATABASE "+probeSchemaName); detachErr != nil {
			return fmt.Errorf(
				"sqlite session restriction did not take effect: ATTACH still succeeds, and detaching the probe database failed: %w",
				detachErr)
		}
		return errors.New("sqlite session restriction did not take effect: ATTACH still succeeds on the restricted session")
	case strings.Contains(err.Error(), attachRefusalMarker):
		return nil
	default:
		return fmt.Errorf(
			"sqlite session restriction could not be confirmed: the verification ATTACH failed without the expected %q error, "+
				"so the restriction cannot be assumed to be in force: %w",
			attachRefusalMarker, err)
	}
}
