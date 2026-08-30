// Package platform declares the canonical dialect names the rest of Ptah
// compares against, and answers the two questions asked about them:
// NormalizeDialect folds every accepted spelling onto one canonical constant,
// and IsPostgresFamily reports whether a dialect speaks the PostgreSQL wire
// protocol and catalog.
//
// The canonical dialects are the constants this package declares.
// NormalizeDialect is the one place an alias such as "pgx", "crdb", or
// "libsql" becomes a dialect, and it returns "" for a name it does not know;
// callers check that answer rather than pass it on. What a dialect being
// accepted means for a concrete construct is a capability question, answered
// by core/platform/capability rather than here.
//
// # Stability
//
// Platform constant names are the public identifiers callers should use. Ptah
// is pre-GA, so constant values can still change before a stable release if
// the platform model needs a cleaner shape.
package platform
