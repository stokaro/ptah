// Package dbreset names what a reset of a dev database drops.
//
// Each dialect's schema writer resets a dev database with a catalog query of
// its own, and the check that refuses a dev database before that reset has to
// judge the same objects the reset removes. The writers list them through the
// query the reset runs, and internal/migrateclean reads the list. A writer
// also answers, through [CleanupCatalog], what a cleanup plan of
// internal/schemaclean needs from its catalog, and, through [ScopeCatalog],
// which schemas and tables a connection reaches, which the clean check before
// a reset judges. The types live here so that neither side imports the other.
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
	// Statement is the statement the reset drops the object with, where the
	// writer drops each object with one of its own, as the Oracle writer
	// does. It is empty where the writer drops objects some other way, such
	// as with the schema that holds them.
	Statement string
	// Refusal says in one line why the reset cannot remove the object, which
	// stops the reset before it removes anything. It is empty for an object
	// the reset removes. Only an executable provider's listing sets it.
	Refusal string
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
//
// SchemaStates, which only a PostgreSQL-family realm reset reads, are the
// owner, access list and comment the schemas such a reset creates again or
// empties where they stand held at the claim. The reset returns each schema to
// its state rather than to what the run left on it.
//
// ArtifactsRecorded reports that the claim listed the database-scoped objects,
// so that one Artifacts does not name is the run's. Only then may a cleanup
// drop a publication: a zero Kept says nothing about who created one, so a
// realm cleanup refuses every publication it finds, and a cleanup of a pinned
// schema drops none.
type Kept struct {
	Extensions        []string
	Schemas           []string
	DefaultPrivileges DefaultPrivileges
	Artifacts         []Object
	ArtifactsRecorded bool
	Server            Server
	StartingPoint     *pgsnapshot.Snapshot
	SchemaStates      []SchemaState
	// DatabaseRoles are the SQL Server database roles the database held at
	// the claim, beyond the fixed roles and public, and DatabaseRolesRecorded
	// reports that the claim listed them. Only then does a realm reset drop
	// a database role, each one DatabaseRoles does not name: a zero Kept says
	// nothing about who created one, so the reset refuses every one it finds.
	DatabaseRoles         []string
	DatabaseRolesRecorded bool
}

// SchemaState is one PostgreSQL-family schema as a dev database's claim found
// it: who owns it, who may use it, and its comment.
type SchemaState struct {
	// Name is the schema.
	Name string
	// Owner is the role that owns the schema.
	Owner string
	// DefaultAccess reports that the schema held no access list of its own,
	// which gives its owner every privilege on it and no other role any.
	// Privileges is empty then.
	DefaultAccess bool
	// Privileges is the schema's access list, one entry per privilege of one
	// grantee, when DefaultAccess is false.
	Privileges []SchemaPrivilege
	// Comment is the schema's comment, empty for none.
	Comment string
}

// SchemaPrivilege is one entry of a schema's access list.
type SchemaPrivilege struct {
	// Grantee is the role the privilege is granted to, PUBLIC for every role.
	Grantee string
	// Privilege is USAGE or CREATE.
	Privilege string
	// GrantOption reports that the grantee may grant the privilege on.
	GrantOption bool
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

// Survivors are the names a PostgreSQL-family realm reset of a database leaves
// in place whether or not a run created them, as the claim found them: every
// schema the database held, the server's own and an extension's included,
// with the root schema and "public", which the reset creates again when it
// drops them; and every extension installed, with the server's own, which the
// reset never drops. An extension or a schema a run creates under any other
// name is dropped by every realm reset, and the comment on it with it.
type Survivors struct {
	// Schemas are the schema names, sorted.
	Schemas []string
	// Extensions are the extension names, sorted.
	Extensions []string
}
