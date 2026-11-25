package mysqlconnect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/connector"
	"ptah.run/internal/mysqlbody"
	"ptah.run/internal/sqlmode"
)

// A MySQL-family session whose sql_mode names no strict mode lets an ALTER
// TABLE change the values a table stores to fit the column's new definition,
// and warn: an ENUM or SET member the definition drops becomes '' or leaves
// the set, a VARCHAR that narrows cuts its strings, an integer that narrows
// is clamped, and a NULL in a column that turns NOT NULL becomes ''. The
// statement succeeds, so a plan that changes such a column on a loose session
// loses data and exits 0. Under a strict mode the same statement fails with
// one of storedValueErrors and the table is unchanged.

// StoredValuesBySession answers true: the session's sql_mode decides.
func (Service) StoredValuesBySession() bool { return true }

// KeepStoredValues makes a loose session strict, as [sqlmode.WithStrict]
// writes the mode, and returns the window that sets the mode it had again. A
// strict session needs nothing, and the window is nil.
//
// The server keeps the sql_mode of the session that creates a trigger, a
// stored routine or an event, and runs the body under it every time. The
// window gives such a statement the session's own mode, as mysqldump does
// around each body it writes, so the body does not keep the strict one and
// fail a later statement of the application's loose session.
func (Service) KeepStoredValues(ctx context.Context, session *sql.Conn) (connector.StoredValuesWindow, error) {
	var current string
	if err := session.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&current); err != nil {
		return nil, fmt.Errorf("query sql_mode: %w", err)
	}
	if sqlmode.Strict(current) {
		return nil, nil
	}
	strict := sqlmode.WithStrict(current)
	if _, err := session.ExecContext(ctx, setSQLMode, strict); err != nil {
		return nil, fmt.Errorf("make sql_mode strict: %w", err)
	}
	return sqlModeWindow{session: session, own: current, strict: strict}, nil
}

// setSQLMode sets the session's sql_mode to the one argument it binds.
const setSQLMode = "SET SESSION sql_mode = ?"

// sqlModeWindow is a session KeepStoredValues made strict: own is the mode
// the session had, and strict the one it runs a plan under.
type sqlModeWindow struct {
	session     *sql.Conn
	own, strict string
}

// Restore sets the mode the session had again.
func (w sqlModeWindow) Restore(ctx context.Context) error {
	if _, err := w.session.ExecContext(ctx, setSQLMode, w.own); err != nil {
		return fmt.Errorf("set sql_mode back to %q: %w", w.own, err)
	}
	return nil
}

// StoredWith reports a statement that defines a trigger, a stored routine or
// an event, as [mysqlbody.StatementKind] reads it, and returns the statements
// that set the session's own mode for it and the strict one after it.
func (w sqlModeWindow) StoredWith(statement string) (own, window connector.SessionStatement, stored bool) {
	if mysqlbody.StatementKind(statement) == "" {
		return connector.SessionStatement{}, connector.SessionStatement{}, false
	}
	return connector.SessionStatement{SQL: setSQLMode, Args: []any{w.own}},
		connector.SessionStatement{SQL: setSQLMode, Args: []any{w.strict}},
		true
}

// storedValueErrors are the errors a strict session fails a statement with
// where a loose one would change a stored value: a value out of the column's
// range (1264), cut (1265), of the wrong kind for it (1292, 1366), too long
// for it (1406), or a NULL in a column that no longer takes one (1138).
var storedValueErrors = []uint16{1138, 1264, 1265, 1292, 1366, 1406}

// StoredValueRefusal reports whether err is the server failing a statement
// because it would change a stored value, and the column its message names.
func (Service) StoredValueRefusal(err error) (string, bool) {
	var server *mysqldriver.MySQLError
	if !errors.As(err, &server) || !slices.Contains(storedValueErrors, server.Number) {
		return "", false
	}
	return columnInMessage(server.Message), true
}

// columnInMessage returns the column a stored-value error names, and "" where
// it names none. MySQL writes the name in single quotes, and MariaDB writes
// some errors with the database and the table in front, each part in
// backticks. Neither escapes a quote inside a name, so the name is the text
// between the last "for column " and the last " at row ", without its quotes.
// The value the error quotes comes before it, and may say anything.
func columnInMessage(message string) string {
	start := strings.LastIndex(message, "for column ")
	end := strings.LastIndex(message, " at row ")
	if start < 0 || end < start+len("for column ") {
		return ""
	}
	quoted := message[start+len("for column ") : end]
	if len(quoted) < 2 {
		return ""
	}
	inner := quoted[1 : len(quoted)-1]
	switch {
	case quoted[0] == '\'' && quoted[len(quoted)-1] == '\'':
		return inner
	case quoted[0] == '`' && quoted[len(quoted)-1] == '`':
		parts := strings.SplitN(inner, "`.`", 3)
		return parts[len(parts)-1]
	default:
		return ""
	}
}
