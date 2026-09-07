//go:build !js

package sqlite

import (
	"context"
	"database/sql"

	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// applyAttachedDatabaseLimit sets SQLITE_LIMIT_ATTACHED to 0 on the pinned
// session, which is what makes the engine itself refuse ATTACH. The driver
// reaches the physical connection through session.Raw, so the limit lands on
// the connection the caller pinned rather than on a pooled sibling.
//
// The context is unused here: the driver's own call is synchronous and takes
// none. RestrictSession's verification step is where cancelation is observed.
func applyAttachedDatabaseLimit(_ context.Context, session *sql.Conn) error {
	_, err := sqlitedriver.Limit(session, sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	return err
}
