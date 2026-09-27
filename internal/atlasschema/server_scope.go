package atlasschema

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/atlasurl"
)

// ErrServerDevDatabase refuses a dev database beside a whole MySQL or MariaDB
// server. A dev database for a server replays the desired state as a server,
// creating and dropping databases, and a dev server is not taken yet
// (stokaro/ptah#3789).
var ErrServerDevDatabase = errors.New(
	"a dev database for a whole MySQL or MariaDB server is not supported yet; " +
		"apply an HCL desired state without --dev-url, or name a database in --url")

// RefuseServerDevDatabase answers [ErrServerDevDatabase] when info is a
// connection to a whole MySQL or MariaDB server and devURL names a dev
// database, before the dev database is contacted.
func RefuseServerDevDatabase(info catalog.ServerInfo, devURL string) error {
	if strings.TrimSpace(devURL) == "" || !info.WholeServer {
		return nil
	}
	return ErrServerDevDatabase
}

// ErrDevServerUnsupported refuses a dev server, a --dev-url that names no
// MySQL or MariaDB database, on the verbs that do not take one yet: schema
// diff and schema apply rehearse and materialize on one dev database
// (stokaro/ptah#3789).
var ErrDevServerUnsupported = errors.New(
	"a --dev-url naming no MySQL or MariaDB database is a whole dev server, which schema diff " +
		"and schema apply do not take yet; name a database in --dev-url")

// RefuseDevServer answers [ErrDevServerUnsupported] for a dev URL that names
// no MySQL-family database, before anything is contacted.
func RefuseDevServer(devURL string) error {
	parsed, err := atlasurl.ParseMySQLURL(strings.TrimSpace(devURL))
	if err != nil || parsed.Database() != "" {
		return nil
	}
	return ErrDevServerUnsupported
}

// RefuseServerScopeMismatch answers a [ServerScopeMismatchError] when the
// connection info reads and the desired state disagree about whether they are
// a whole server; see refuseServerScopeMismatch.
func RefuseServerScopeMismatch(current catalog.ServerInfo, desired atlassource.State) error {
	return refuseServerScopeMismatch(connectionSide(current), stateSide(desired))
}

// ServerScopeMismatchError refuses a comparison of a whole MySQL or MariaDB
// server with one database. Read as a server, the database would be the only
// one the desired state declares, and every other database would be dropped;
// the pinned community binary v1.3.0 refuses the pair on `schema diff` and
// `schema apply`, in either order, measured on MySQL 8.4.11. The message is
// the binary's.
type ServerScopeMismatchError struct {
	// Database is the database the other side names.
	Database string
	// DatabaseIsCurrent reports that the current side, --from or --url, names
	// the database and the desired side is the server.
	DatabaseIsCurrent bool
}

// Error implements error.
func (e *ServerScopeMismatchError) Error() string {
	if e.DatabaseIsCurrent {
		return fmt.Sprintf("cannot diff a database connection with a schema %q", e.Database)
	}
	return fmt.Sprintf("cannot diff a schema %q with a database connection", e.Database)
}

// refuseServerScopeMismatch answers a [ServerScopeMismatchError] when one of
// the two database reads is a whole server and the other is one database. A
// side that is not a database read, such as a schema file, is not compared by
// scope.
func refuseServerScopeMismatch(current, desired serverScopeSide) error {
	if !current.database || !desired.database || current.wholeServer == desired.wholeServer {
		return nil
	}
	if current.wholeServer {
		return &ServerScopeMismatchError{Database: desired.name}
	}
	return &ServerScopeMismatchError{Database: current.name, DatabaseIsCurrent: true}
}

// serverScopeSide is one side of a comparison, as the scope rule reads it.
type serverScopeSide struct {
	// database reports a side read from a database.
	database bool
	// wholeServer reports a whole MySQL or MariaDB server.
	wholeServer bool
	// name is the database a read of one database selected.
	name string
}

// connectionSide is the side a connection reads.
func connectionSide(info catalog.ServerInfo) serverScopeSide {
	return serverScopeSide{database: true, wholeServer: info.WholeServer, name: info.Schema}
}

// stateSide is the side a resolved desired or current state reads.
func stateSide(state atlassource.State) serverScopeSide {
	return serverScopeSide{
		database:    state.Kind == atlassource.KindDatabase,
		wholeServer: state.WholeServer,
		name:        state.DefaultSchema,
	}
}
