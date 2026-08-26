// Package schemamodel is the database-agnostic model of a schema as a SOURCE
// declared it: tables, fields, indexes, constraints, enums, extensions, roles,
// grants, and RLS declarations, together with the derived dependency maps that
// decide creation order.
//
// It is the desired side of every comparison. The current side is
// [go.5x5.cz/ptah/catalog], the model of a schema as a live server reports it,
// and the two are kept apart on purpose: catalog.Table is the live table and
// schemamodel.Table is the declared one.
//
// Nothing here reads Go source. The annotation parser is
// [go.5x5.cz/ptah/core/goschema], the YAML front end is
// [go.5x5.cz/ptah/core/yamlschema], and a caller that builds a Database itself
// is a source too -- each produces this model, and the renderer, planner, and
// schema-diff layers consume it without knowing which one did.
//
// A Database assembled by hand is not finished until [Finalize] has run: the
// dependency maps, the expanded embedded fields, and the qualified table-scoped
// names are derived rather than declared, and every consumer reads them.
package schemamodel
