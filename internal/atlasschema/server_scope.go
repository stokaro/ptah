package atlasschema

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/schemafile"
)

// ErrServerDevDatabase refuses a dev database beside a whole MySQL or MariaDB
// server. A plan for a server creates and drops databases, so its rehearsal
// runs on a whole dev server, which is claimed empty and emptied again; a dev
// database is claimed alone, and a server plan rehearsed there would reach the
// other databases of its server (stokaro/ptah#3789, stokaro/ptah#3885). Where
// the pinned community binary v1.3.0 refuses the pair it is refused in its
// words; see [RefuseServerDevDatabase].
var ErrServerDevDatabase = errors.New(
	"a dev database beside a whole MySQL or MariaDB server is refused: the plan creates and drops databases, " +
		"and its rehearsal needs a whole dev server; name no database in --dev-url")

// RefuseServerDevDatabase refuses a dev database beside a whole MySQL or
// MariaDB server, before the dev database is contacted.
//
// The sentence is the pinned community binary v1.3.0's where it refuses the
// same pair, measured on MySQL 8.4.11 and MariaDB 11.8.9 with `schema apply -u
// <server> --dev-url mysql://…/devdb`. A SQL file or a migration directory is
// `cannot diff a schema "devdb" with a database connection`, which the binary
// prints after it replayed the SQL and left its databases on the dev server. A
// document declaring several databases is `cannot use HCL with more than 1
// schema when dev-url is limited to schema "devdb"`. A document declaring one
// database the binary plans, and Ptah refuses with [ErrServerDevDatabase],
// because its plan is rehearsed and the rehearsal needs a whole dev server.
func RefuseServerDevDatabase(info catalog.ServerInfo, devURL string, desired atlassource.Set) error {
	devDatabase, beside := devDatabaseBesideServer(info, devURL)
	switch {
	case !beside:
		return nil
	case devDatabase == "":
		return ErrServerDevDatabase
	case runsSQL(desired):
		return &ServerScopeMismatchError{Database: devDatabase}
	case declaresSeveralDatabases(info.Dialect, desired):
		return &OneDatabaseBesideDocumentError{Flag: "dev-url", Database: devDatabase}
	default:
		return ErrServerDevDatabase
	}
}

// declaresSeveralDatabases reports a declarative document that declares more
// than one database. It reads the files, which needs no dev database. A
// document that does not read answers false, and the refusal is
// [ErrServerDevDatabase] either way.
func declaresSeveralDatabases(dialect string, desired atlassource.Set) bool {
	if !desired.DeclarativeLocalFiles() {
		return false
	}
	schema, err := schemafile.LoadSources(desired.SchemaFileSources(), schemafile.Options{Dialect: dialect})
	return err == nil && len(schema.Schemas) > 1
}

// devDatabaseBesideServer reports a dev URL that names a database beside a
// connection to a whole MySQL or MariaDB server, and the database it names,
// empty when the URL is not a MySQL-family one.
func devDatabaseBesideServer(info catalog.ServerInfo, devURL string) (string, bool) {
	if strings.TrimSpace(devURL) == "" || !info.WholeServer || isDevServer(devURL) {
		return "", false
	}
	parsed, err := atlasurl.ParseMySQLURL(strings.TrimSpace(devURL))
	if err != nil {
		return "", true
	}
	return parsed.Database(), true
}

// refuseSQLBesideOneDatabaseOnDevServer refuses SQL beside a target naming one
// database, on a whole dev server. The pinned community binary v1.3.0 runs the
// SQL on the dev server as a server and refuses the pair with `cannot diff a
// database connection with a schema "app"`, measured on MySQL 8.4.11 and
// MariaDB 11.8.9 with `schema apply -u mysql://…/app --to realm.sql --dev-url
// <dev server>`; a document declaring one database is taken and rehearsed in a
// database of the target's name on the dev server.
func refuseSQLBesideOneDatabaseOnDevServer(info catalog.ServerInfo, devURL string, desired atlassource.Set) error {
	if !isDevServer(devURL) || info.WholeServer || !runsSQL(desired) {
		return nil
	}
	return &ServerScopeMismatchError{Database: info.Schema, DatabaseIsCurrent: true}
}

// isDevServer reports whether devURL is a whole dev server: a MySQL-family URL
// naming no database, written out or as a docker URL whose server the command
// starts.
//
// A `docker+mysql://_/mysql:8.4.11` URL names no database, and the pinned
// community binary v1.3.0 plans a whole server beside it: measured, `schema
// apply -u <server> --dev-url docker+mysql://_/mysql:8.4.11 --dry-run` prints
// the realm's plan and exits 0. Read as a database, it was refused as a dev
// database beside a whole server. A `docker://mysql/<tag>` URL always names a
// database, `dev` when it names none; see [devdocker.DefaultDatabase].
//
// The docker URL is read as written, as [devdocker.IsURL] reads it.
func isDevServer(devURL string) bool {
	if devdocker.IsURL(devURL) {
		spec, err := devdocker.Parse(devURL)
		return err == nil && spec.Database == "" &&
			(spec.Dialect == platform.MySQL || spec.Dialect == platform.MariaDB)
	}
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
