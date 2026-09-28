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
// MySQL or MariaDB database, on the verb that does not take one yet: schema
// apply rehearses its plan on one dev database (stokaro/ptah#3885).
var ErrDevServerUnsupported = errors.New(
	"a --dev-url naming no MySQL or MariaDB database is a whole dev server, which schema apply " +
		"does not take yet; name a database in --dev-url")

// RefuseDevServer answers [ErrDevServerUnsupported] for a dev URL that names
// no MySQL-family database, before anything is contacted.
func RefuseDevServer(devURL string) error {
	if !isDevServer(devURL) {
		return nil
	}
	return ErrDevServerUnsupported
}

// isDevServer reports whether devURL is a whole dev server: a MySQL-family URL
// naming no database.
func isDevServer(devURL string) bool {
	parsed, err := atlasurl.ParseMySQLURL(strings.TrimSpace(devURL))
	return err == nil && parsed.Database() == ""
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

// devServerSides is what [scopeOnDevServer] needs to know about a comparison
// beyond its two resolved states.
type devServerSides struct {
	// server reports that --dev-url is a whole dev server.
	server bool
	// fromRunsSQL and toRunsSQL report a side that is SQL the dev server has
	// to run: a SQL schema file or a migration directory.
	fromRunsSQL, toRunsSQL bool
}

// devServerSidesOf classifies a comparison's sources for [scopeOnDevServer].
func devServerSidesOf(devURL string, from, to atlassource.Set) devServerSides {
	return devServerSides{server: isDevServer(devURL), fromRunsSQL: runsSQL(from), toRunsSQL: runsSQL(to)}
}

// runsSQL reports a source that is SQL a dev database runs: a migration
// directory, or schema files that are not all declarative documents.
func runsSQL(set atlassource.Set) bool {
	return set.Kind == atlassource.KindMigrationDir || (set.Kind == atlassource.KindLocalFile && !set.DeclarativeLocalFiles())
}

// scopeOnDevServer reads the sides of a comparison beside a whole dev server.
//
// Beside a dev server, a schema file declares databases, as `schema` blocks
// and CREATE DATABASE, and a migration directory replays as a server: every
// side that is not a database is a whole server. A database side keeps the
// scope its URL gives it. The pinned community binary v1.3.0 compares them
// that way, measured on MySQL 8.4.11 and MariaDB 11.8.9: `schema diff --from
// <server> --to realm.hcl --dev-url <dev server>` plans CREATE DATABASE and
// the tables (stokaro/ptah#3885).
//
// A database URL naming one database beside such a side is refused unless the
// side is a declarative document declaring at most one database, which the
// binary compares with the one database. Measured, the binary refuses a SQL
// file or a migration directory there with the sentence
// [ServerScopeMismatchError] gives, whether it declares one database or
// several. It diffs a document declaring several, and its plan reaches
// databases the one-database side never read: with `more` holding a table,
// `--from mysql://…/app --to realm.hcl` plans CREATE DATABASE more, which the
// server answers with ERROR 1007, and the reverse plans DROP DATABASE more.
// That pair is refused with the binary's `schema apply` sentence for it.
func scopeOnDevServer(from, to atlassource.State, sides devServerSides) (scopedFrom, scopedTo atlassource.State, err error) {
	if !sides.server {
		return from, to, nil
	}
	from.WholeServer = from.WholeServer || from.Kind != atlassource.KindDatabase
	to.WholeServer = to.WholeServer || to.Kind != atlassource.KindDatabase
	switch {
	case oneDatabase(from) && to.Kind != atlassource.KindDatabase:
		if sides.toRunsSQL {
			return atlassource.State{}, atlassource.State{}, &ServerScopeMismatchError{
				Database: from.DefaultSchema, DatabaseIsCurrent: true,
			}
		}
		to, err = documentBesideDatabase(to, from, "--from")
	case oneDatabase(to) && from.Kind != atlassource.KindDatabase:
		if sides.fromRunsSQL {
			return atlassource.State{}, atlassource.State{}, &ServerScopeMismatchError{Database: to.DefaultSchema}
		}
		from, err = documentBesideDatabase(from, to, "--to")
	}
	if err != nil {
		return atlassource.State{}, atlassource.State{}, err
	}
	return from, to, nil
}

// oneDatabase reports a database side limited to one database.
func oneDatabase(state atlassource.State) bool {
	return state.Kind == atlassource.KindDatabase && !state.WholeServer
}

// documentBesideDatabase narrows a declarative document compared with one
// database to that database's scope, or refuses it when it declares more than
// one.
func documentBesideDatabase(document, database atlassource.State, flag string) (atlassource.State, error) {
	if document.Schema != nil && len(document.Schema.Schemas) > 1 {
		return atlassource.State{}, &OneDatabaseBesideDocumentError{Flag: flag, Database: database.DefaultSchema}
	}
	document.WholeServer = false
	return document, nil
}

// OneDatabaseBesideDocumentError refuses a database URL naming one database
// beside a declarative document that declares several, on a dev server; see
// scopeOnDevServer.
type OneDatabaseBesideDocumentError struct {
	// Flag is the flag whose URL names the one database.
	Flag string
	// Database is the database that URL names.
	Database string
}

// Error implements error.
func (e *OneDatabaseBesideDocumentError) Error() string {
	return fmt.Sprintf("cannot use HCL with more than 1 schema when %s is limited to schema %q", e.Flag, e.Database)
}
