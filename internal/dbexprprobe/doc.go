// Package dbexprprobe asks a live server to re-spell declared expressions, so
// a comparison holds the same spelling on both sides.
//
// Several engines store a rewrite of an expression rather than the text they
// were given: PostgreSQL prints a CHECK back from its parse tree, TimescaleDB
// rewrites a continuous aggregate's SELECT, Oracle upper-cases and re-quotes a
// generated column's expression. Comparing a declaration against such a
// read-back is comparing two languages, and acting on the difference plans
// work for objects nobody changed. Each resolver here puts the declaration
// through the same server-side rewrite the catalog form went through --
// creating a throwaway object, reading its stored form back, and undoing the
// creation -- and answers with the server's own spelling, keyed the way the
// caller keys it.
//
// The probes are a comparator implementation detail, not connection API: the
// comparison in migration/schemadiff and the `ptah compare` command are their
// consumers. What they need from a [dbschema.DatabaseConnection] is exactly
// [dbschema.DatabaseConnection.WithRolledBackTransaction] -- one throwaway
// session, one transaction that never commits -- plus plain statement
// execution for the Oracle probe, whose DDL no transaction can take back.
//
// # Pinned connections
//
// Comparisons run on connections pinned to a session
// ([dbschema.DatabaseConnection.WithSession] and its wrappers): `schema apply`
// compares on the session that holds its apply lock, `migrate diff` on the
// session its migration replay ran on, and a plan rehearsal on its dev
// session. On such a connection the probes run on the pinned session itself,
// inside a transaction they roll back, when the driver reports the session
// outside any transaction.
//
// When it does not -- the owner has a transaction open, or the driver cannot
// say -- every resolver that probes inside a transaction returns a nil map and
// a nil error without asking the server anything: the rollback the probes need
// would discard the owner's work along with the probe's.
// [ResolveGeneratedExpressions] is the one exception, for the reason its own
// documentation gives: Oracle commits its DDL itself, so no transaction could
// take the probe back on any connection, pinned or not. The nil is deliberate
// and not an error, because refusing there would fail a comparison to protect
// it. What it costs is the server's spelling: the comparison proceeds with the
// declared text, exactly as it does for a dialect that rewrites nothing, so an
// expression the server rewrites plans a change it does not need.
package dbexprprobe
