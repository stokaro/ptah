// Package goschema parses Ptah annotation comments in Go source files into the
// schema model.
//
// Every entry point returns a
// [ptah.run/core/schemamodel.Database] holding the tables, fields,
// indexes, constraints, enums, extensions, roles, grants, and RLS declarations
// the annotations declare. They differ in what they read and in how far the
// result is taken:
//
//   - ParseFile and ParseSource read one file, resolve its table-scoped names,
//     and build its dependency graph. Embedded fields are not expanded and
//     nothing is deduplicated: one file is not a schema.
//   - ParseDir, ParseFS, and ParseDirs walk one or more roots and finalize the
//     result, which expands embedded fields, folds repeated declarations,
//     reports conflicting ones as an error, and orders tables and functions by
//     their dependencies.
//   - ParseDirRaw walks one root and stops before that pipeline, so
//     [ptah.run/core/schemamodel.Merge] can compose it with schemas from
//     other authoring sources under one collision policy.
//
// The model itself, and everything done to it after parsing, lives in
// schemamodel. This package only turns Go source into one.
package goschema
