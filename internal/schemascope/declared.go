package schemascope

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// DeclaredSchemaNames is every schema a desired state names, over the
// declarations that carry one. A document may name a schema by declaring a
// block for it or by qualifying an object with it, and both have to count: an
// inspected document does the first, a hand-written one often only the second.
//
// A default privilege's schema is its home the same way a table's is: `IN
// SCHEMA app` is where the object lives, and it can be the document's only
// mention of `app`. Leave it out and the current side never reads that schema,
// so a default privilege the database already has reads as absent and every run
// plans it again.
//
// Grants are not walked here. A grant is written against a target object, which
// a document usually declares in its own right, so the schema is already on
// this list by the time the grant is read.
func DeclaredSchemaNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	var names []string
	add := func(name string) {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	for _, schema := range desired.Schemas {
		add(schema.Name)
	}
	for _, table := range desired.Tables {
		add(table.Schema)
	}
	for _, sequence := range desired.Sequences {
		add(sequence.Schema)
	}
	for _, domain := range desired.Domains {
		add(domain.Schema)
	}
	for _, composite := range desired.CompositeTypes {
		add(composite.Schema)
	}
	for _, rangeType := range desired.Ranges {
		add(rangeType.Schema)
	}
	for _, privilege := range desired.DefaultPrivileges {
		add(privilege.Schema)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
