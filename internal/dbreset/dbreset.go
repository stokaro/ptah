// Package dbreset names what a reset of a dev database drops.
//
// Each dialect's schema writer resets a dev database with a catalog query of
// its own, and the check that refuses a dev database before that reset has to
// judge the same objects the reset removes. The writers list them through the
// query the reset runs, and internal/migrateclean reads the list; the type
// lives here so that neither side imports the other.
package dbreset

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
