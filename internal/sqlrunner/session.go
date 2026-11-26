package sqlrunner

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// Release gives conn back to its pool, which keeps it open for the next
// caller while the pool has room for idle sessions. label names the session in
// the error. A session the driver already closed has nothing left to give
// back, and releasing it is not an error.
func Release(conn *sql.Conn, label string) error {
	if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("close %s: %w", label, err)
	}
	return nil
}

// Discard closes conn and the physical session under it, so its pool does not
// keep it open as an idle session. A caller discards a session whose state it
// changed, or one it opened beyond what the pool held, which an idle session
// would otherwise keep counted against the server's connection limits. label
// names the session in the errors. A session the server or the driver already
// closed has nothing left to discard, and discarding it is not an error.
func Discard(conn *sql.Conn, label string) error {
	var errs []error
	err := conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
	if err != nil && !errors.Is(err, driver.ErrBadConn) && !errors.Is(err, sql.ErrConnDone) {
		errs = append(errs, fmt.Errorf("discard %s: %w", label, err))
	}
	errs = append(errs, Release(conn, label))
	return errors.Join(errs...)
}
