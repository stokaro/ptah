package mysqlconnect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/core/platform"
	"ptah.run/internal/connector"
	"ptah.run/internal/mysqlstored"
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
// stored routine, an event or a MariaDB package, and runs the body under it
// every time. The window gives such a statement the session's own mode, as
// mysqldump does around each body it writes, so the body does not keep the
// strict one and fail a later statement of the application's loose session.
// It reads each statement with [mysqlstored.Statement] for the server the
// session reports, so it reads what the server reads, an executable comment
// the server runs included. That reader takes a statement's bytes as UTF-8,
// so a loose session whose client character set is outside the UTF-8 family,
// as a URL's charset or collation parameter or the server's default can make
// it, is refused before the window opens: the server would read a statement
// by other rules, and the window could miss the object it defines.
func (Service) KeepStoredValues(ctx context.Context, session *sql.Conn) (connector.StoredValuesWindow, error) {
	var current, version, charset string
	if err := session.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode, VERSION(), @@character_set_client").
		Scan(&current, &version, &charset); err != nil {
		return nil, fmt.Errorf("query sql_mode: %w", err)
	}
	if sqlmode.Strict(current) {
		return nil, nil
	}
	if !mysqlstored.UTF8CharacterSet(charset) {
		return nil, fmt.Errorf("the session's client character set is %q, and the statements are read as UTF-8 "+
			"to find each one that defines an object keeping the session's sql_mode: connect with a utf8mb4 client "+
			"character set, such as charset=utf8mb4 in the URL, or give the session a strict sql_mode, which needs "+
			"no such reading", charset)
	}
	strict := sqlmode.WithStrict(current)
	if _, err := session.ExecContext(ctx, setSQLMode, strict); err != nil {
		return nil, fmt.Errorf("make sql_mode strict: %w", err)
	}
	return sqlModeWindow{
		session: session,
		own:     current,
		strict:  strict,
		server:  readFor(current, version),
	}, nil
}

// readFor is the server and session the window reads statements for: the
// dialect and version VERSION() reports, and the modes of sql_mode, the
// session's own, that change where a string, a quoted name or a comment
// ends. The window sets only strict modes, so it keeps those.
func readFor(sqlMode, version string) mysqlstored.Server {
	dialect := detectMySQLWireDialect(version)
	return mysqlstored.Server{
		Dialect:            dialect,
		VersionID:          mysqlstored.VersionID(version),
		NoBackslashEscapes: sqlmode.Has(sqlMode, sqlmode.NoBackslashEscapes),
		AnsiQuotes:         sqlmode.Has(sqlMode, sqlmode.AnsiQuotes),
		BracketNames:       dialect == platform.MariaDB && sqlmode.Has(sqlMode, sqlmode.MSSQL),
	}
}

// setSQLMode sets the session's sql_mode to the one argument it binds.
const setSQLMode = "SET SESSION sql_mode = ?"

// sqlModeWindow is a session KeepStoredValues made strict: own is the mode
// the session had, strict the one it runs a plan under, and server the
// server and session its statements are read for.
type sqlModeWindow struct {
	session     *sql.Conn
	own, strict string
	server      mysqlstored.Server
}

// Restore sets the mode the session had again.
func (w sqlModeWindow) Restore(ctx context.Context) error {
	if _, err := w.session.ExecContext(ctx, setSQLMode, w.own); err != nil {
		return fmt.Errorf("set sql_mode back to %q: %w", w.own, err)
	}
	return nil
}

// StoredWith reports a statement after which the server keeps the session's
// mode with the object, as [mysqlstored.Definition.KeepsSessionMode] reads
// it, and returns the statements that set the session's own mode for it and
// the strict one after it. A statement holding an executable comment the
// reader cannot read, and a CREATE or ALTER whose kind of object it cannot
// read, is refused: either may define such an object.
func (w sqlModeWindow) StoredWith(statement string) (*connector.StoredSettings, error) {
	definition, ok, err := mysqlstored.Statement(w.server, statement)
	if err != nil {
		return nil, fmt.Errorf("the strict session cannot tell whether the statement defines a stored body: %w", err)
	}
	if !ok || !definition.KeepsSessionMode() {
		return nil, nil
	}
	return &connector.StoredSettings{
		Own:    connector.SessionStatement{SQL: setSQLMode, Args: []any{w.own}},
		Window: connector.SessionStatement{SQL: setSQLMode, Args: []any{w.strict}},
	}, nil
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
