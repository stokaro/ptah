// Package sqlite implements SQLite schema introspection and DDL execution for
// the dbschema connection layer.
//
// The engine itself is linked by a sibling file rather than by this one,
// because whether there is an engine to link depends on the platform. On every
// platform modernc.org/libc supports, that engine is the modernc.org/sqlite
// amalgamation, registered by an import with no other purpose. A js/wasm build
// links none, because modernc.org/libc does not build there: a host that runs
// this package in a browser registers its own driver under the same name and
// installs the one engine call this package then cannot make for itself,
// through SetAttachedDatabaseLimiter.
package sqlite
