// Package dbreset names what a reset of a dev database drops.
//
// Each dialect's schema writer resets a dev database with a catalog query of
// its own, and the check that refuses a dev database before that reset has to
// judge the same objects the reset removes. The writers list them through the
// query the reset runs, and internal/migrateclean reads the list; the type
// lives here so that neither side imports the other.
package dbreset

import (
	"errors"

	"ptah.run/internal/pgdefaultacl"
	"ptah.run/internal/pgsnapshot"
)

// Object is something a reset of a dev database drops: an object in one of
// the schemas the reset empties, or one that belongs to no schema, such as a
// PostgreSQL large object, whose Name is its oid, or a SQL Server schema,
// whose Name is the schema.
type Object struct {
	// Kind is the object's kind in the words its drop statement uses, in
	// lower case: "view", "function", "text search configuration".
	Kind string
	// Schema names the schema the object is in, and is empty for one that
	// belongs to no schema and on SQLite, which has one.
	Schema string
	Name   string
}

// Scope is the part of a PostgreSQL-family database a reset empties: the
// schemas it drops objects from, the extensions it keeps with everything they
// own, and the default privileges it returns to what they were rather than
// revoking. The other writers reset the database they are connected to, and
// ignore it.
type Scope struct {
	Schemas               []string
	KeptExtensions        []string
	KeptDefaultPrivileges DefaultPrivilegeScope
}

// DefaultPrivilegeScope names the PostgreSQL default privileges a reset keeps:
// the rows set IN SCHEMA one of Schemas, and with Global the rows set without
// IN SCHEMA, which apply in every schema of the database. A writer whose
// catalog cannot read them back keeps none, and its reset revokes them as it
// revokes the rest.
type DefaultPrivilegeScope struct {
	Schemas []string
	Global  bool
}

// IsZero reports whether the scope keeps no default privilege.
func (s DefaultPrivilegeScope) IsZero() bool {
	return len(s.Schemas) == 0 && !s.Global
}

// DefaultPrivileges is what the default privileges in Scope were when a dev
// database was claimed. A reset returns them to Rows: a row the run added is
// revoked, one it changed or removed is set back, and one it left alone is
// not touched. The zero value keeps nothing.
type DefaultPrivileges struct {
	Scope DefaultPrivilegeScope
	Rows  []pgdefaultacl.Row
}

// Kept is the environment a reset of a dev database leaves in place: what the
// database held when it was claimed and does not belong to the run, and whose
// server it is on.
//
// Extensions stay installed with everything they own. Schemas, which only a
// realm reset reads, are left as they are, contents and all. DefaultPrivileges
// are returned to what they were. Artifacts, which only a realm reset reads,
// are the database-scoped objects the database held, such as an event trigger
// or a publication: the reset cannot remove one, and leaves these alone where
// it refuses any other. Server, which only a realm reset reads, says whether
// the server's default user database may be reset. The zero value keeps
// nothing and is on a [NamedServer].
//
// StartingPoint, when set, replaces the rest with the whole state a dev
// database an atlas.hcl docker block provisioned started from: a reset of
// either kind returns the database to it, removing what the run added in
// every schema and putting back each privilege the run changed. Artifacts
// still applies. Server does not: such a reset removes only what the run
// added, so it cannot empty a database someone else filled.
type Kept struct {
	Extensions        []string
	Schemas           []string
	DefaultPrivileges DefaultPrivileges
	Artifacts         []Object
	Server            Server
	StartingPoint     *pgsnapshot.Snapshot
}

// Server says whose server a reset of a PostgreSQL-family database realm runs
// on, which decides whether the server's default user database may be reset.
// The other writers ignore it.
type Server int

const (
	// NamedServer is a server the operator named. Its default user database,
	// such as PostgreSQL's postgres, is where work that is not the run's tends
	// to live, so a reset refuses it.
	NamedServer Server = iota
	// OwnedServer is a server the run owns as a whole: one Ptah started for the
	// run, or one the operator declared disposable. Its default user database
	// holds nothing that is not the run's, and a reset empties it as it empties
	// any other.
	OwnedServer
)

// ErrServerDefaultDatabase is wrapped by a reset that refused a database only
// because it is the server's default user database on a [NamedServer]. The
// same reset on an [OwnedServer] would have run.
var ErrServerDefaultDatabase = errors.New("it is the server's default database, which is reset only on a server the run owns")
