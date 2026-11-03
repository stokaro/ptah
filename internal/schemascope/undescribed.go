package schemascope

import (
	"cmp"
	"fmt"
	"io"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaselection"
)

// Undescribed is a schema a comparison read only because its URL pins no
// search_path, that the desired schema declares nothing in, and that the
// comparison removes tables from. See [UndescribedRemovals].
type Undescribed struct {
	// Schema is the schema's name as the database reports it.
	Schema string
	// Tables is how many of its tables the comparison removes. It is at
	// least one.
	Tables int
}

// Warning is the line a verb prints for u, without the "warning: " prefix.
// It names the two ways to keep the tables, both of which name the scope: a
// URL that pins search_path, and an explicit --schemas selection.
func (u Undescribed) Warning() string {
	if u.Tables == 1 {
		return fmt.Sprintf("the desired schema declares nothing in schema %q, so its 1 table is planned for removal; "+
			"to keep it, pin search_path in the URL or pass --schemas", u.Schema)
	}
	return fmt.Sprintf("the desired schema declares nothing in schema %q, so its %d tables are planned for removal; "+
		"to keep them, pin search_path in the URL or pass --schemas", u.Schema, u.Tables)
}

// UndescribedRemovals reports each schema whose tables diff removes only
// because the read reached a schema nobody named, sorted by name. info is the
// connection the live side was read on, requested is the explicit selection
// the verb read with, and desired and diff are the two sides' declared state
// and the difference the verb plans from.
//
// [ComparisonScope] reads every schema of the database when the URL pins no
// search_path and the operator selected none. A schema the desired schema
// declares nothing in is then authoritatively absent, and the comparison
// removes its tables, as `schema apply` does and as the pinned Atlas
// community binary v1.3.0 plans `DROP SCHEMA ... CASCADE`. That stays the
// answer; this names it, so a removal the operator did not ask for in words
// is not silent. `migrations generate` writes it into a file.
//
// A schema is reported when all of these hold:
//
//   - the URL pins no search_path on a dialect that selects schemas with
//     one, so the read covers the whole database;
//   - requested is empty, because a selection is the operator naming the
//     scope;
//   - the schema is not the connection's own, which every scope reads and
//     where an unqualified declaration lives;
//   - the desired schema names nothing in it, in the sense of
//     [DeclaredSchemaNames];
//   - diff removes at least one of its tables.
//
// Tables are counted because removing one removes its rows. Another object
// the comparison removes from the same schema goes with it in the plan and is
// not counted. diff is read as the plan will run it, so a caller that deletes
// table drops before planning passes the diff without them. A removal whose
// captured table names no schema is the connection's default and is skipped.
//
// The error is [DeclaredSchemaNames]'s.
func UndescribedRemovals(
	info catalog.ServerInfo,
	requested []string,
	desired *schemamodel.Database,
	diff *difftypes.SchemaDiff,
) ([]Undescribed, error) {
	if diff == nil || len(diff.TablesRemoved) == 0 || len(SplitNames(requested)) > 0 || !searchPathRealm(info) {
		return nil, nil
	}
	declared, err := DeclaredSchemaNames(desired)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, removal := range diff.TablesRemoved {
		schema := removal.Current.Table.Schema
		if schema == "" || schema == info.Schema || slices.Contains(declared, schema) {
			continue
		}
		counts[schema]++
	}
	if len(counts) == 0 {
		return nil, nil
	}
	found := make([]Undescribed, 0, len(counts))
	for schema, tables := range counts {
		found = append(found, Undescribed{Schema: schema, Tables: tables})
	}
	slices.SortFunc(found, func(a, b Undescribed) int { return cmp.Compare(a.Schema, b.Schema) })
	return found, nil
}

// WarnUndescribed writes one warning line for each schema in found to out, in
// order. A nil out writes nothing, for a caller that runs a comparison without
// a diagnostics stream.
//
// It writes at once rather than through the run's aggregated report, which
// flushes when the command ends: `schema apply` asks for confirmation before
// it ends, and a warning about the plan has to be on the screen when the
// question is. A database holds few schemas, so one line each stays short.
func WarnUndescribed(out io.Writer, found []Undescribed) {
	if out == nil {
		return
	}
	for _, schema := range found {
		fmt.Fprintf(out, "warning: %s\n", schema.Warning())
	}
}

// searchPathRealm reports whether info is a connection of a dialect that
// selects its schema with search_path, whose URL pins none, so a read with no
// selection covers every schema of the database.
func searchPathRealm(info catalog.ServerInfo) bool {
	described, _ := platform.Lookup(info.Dialect)
	return described.Schemas.Selection == descriptor.SchemaSelectionSearchPath &&
		schemaselection.Realm(info.Dialect, info.URL, info.Schema)
}
