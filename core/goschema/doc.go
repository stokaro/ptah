// Package goschema parses Ptah annotation comments in Go source files into the
// database-agnostic schema model used across the Ptah toolchain.
//
// The package walks directories or file systems (ParseDir, ParseFS, ParseDirs),
// extracts tables, fields, indexes, constraints, enums, extensions, roles, and
// RLS declarations into a Database, merges embedded fields and multi-file
// declarations, and reports conflicting definitions. The resulting model feeds
// the renderer, planner, and schema-diff layers.
package goschema
