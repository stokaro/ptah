// Package sqlsyntax is the module that holds the lexical syntax of SQL
// families that Ptah and its providers read alike. It is released apart from
// Ptah, as provider-sdk is, so a provider depends on the syntax without
// depending on the host. It knows no engine's catalog, types or DDL: reading,
// rendering and planning stay with the engine that owns them.
//
// Package pgsyntax reads the PostgreSQL family.
package sqlsyntax
