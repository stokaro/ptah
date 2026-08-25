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
// # Pinned connections resolve nothing
//
// On a connection pinned to a session ([dbschema.DatabaseConnection.WithSession]
// and its wrappers), every resolver that probes inside a transaction returns a
// nil map and a nil error without asking the server anything: the rolled-back
// transaction the probes need would discard the session owner's work along
// with the probe's. [ResolveGeneratedExpressions] is the one exception, for
// the reason its own documentation gives: Oracle commits its DDL itself, so no
// transaction could take the probe back on any connection, pinned or not.
// The nil is deliberate and not an error, because a pinned connection
// legitimately reaches a comparison -- `schema apply` rehearses its plan on a
// dev database inside [dbschema.DatabaseConnection.WithUntrustedSQLSession],
// and the rehearsal compares schemas on that pinned session. Refusing there
// would fail the rehearsal to protect it. The cost of the nil is precision,
// not correctness: the comparison proceeds with the declared spelling
// uncompared, exactly as it does for a dialect that rewrites nothing.
package dbexprprobe
