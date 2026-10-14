// Package platform serves the process dialect registry and answers the
// questions asked about a dialect name: NormalizeDialect folds every accepted
// spelling onto one canonical name, Lookup returns what the dialect states
// about itself, and IsPostgresFamily reports whether it speaks the PostgreSQL
// wire protocol and catalog.
//
// What a dialect is, and which spellings select it, is stated by its
// descriptor (see core/platform/descriptor), not by this package. The process
// registry starts with the descriptors of the engines Ptah ships, ready
// before any package-level variable reads it, and Register adds an engine
// Ptah does not ship. The constants this package declares are the canonical
// names of the built-in engines, for code that has to name one.
//
// NormalizeDialect is the one place an alias such as "pgx", "crdb", or
// "libsql" becomes a dialect, and it returns "" for a name no descriptor
// claims; callers check that answer rather than pass it on. What a dialect
// being accepted means for a concrete construct is a capability question,
// answered by core/platform/capability rather than here.
//
// # Stability
//
// Platform constant names are the public identifiers callers should use. Ptah
// is pre-GA, so constant values can still change before a stable release if
// the platform model needs a cleaner shape.
package platform
