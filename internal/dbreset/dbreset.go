// Package dbreset names what a reset of a dev database drops.
//
// Each dialect's schema writer resets a dev database with a catalog query of
// its own, and the check that refuses a dev database before that reset has to
// judge the same objects the reset removes. The writers list them through the
// query the reset runs, and internal/migrateclean reads the list; the type
// lives here so that neither side imports the other.
package dbreset

import "errors"

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
// schemas it drops objects from, and the extensions it keeps with everything
// they own. The other writers reset the database they are connected to, and
// ignore it.
type Scope struct {
	Schemas        []string
	KeptExtensions []string
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
