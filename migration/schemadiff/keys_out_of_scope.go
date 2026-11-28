package schemadiff

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/foreignkeyscope"
	"ptah.run/internal/schemaprep"
)

// withoutKeysIntoTablesOutOfScope is desired without the foreign keys that
// reference a table an exclude selector took out of database, for the
// validation of the desired schema alone.
//
// That validation refuses a key into a table the description does not
// declare, since the plan would create a key into nothing. A table out of
// scope is on the server and the plan leaves it there, so a key into it is
// checked as a key into a schema the description holds nothing of: not at
// all, because what it references is not described. The comparison still
// compares and plans the key. desired is returned unchanged when nothing is
// out of scope, and is never modified.
func withoutKeysIntoTablesOutOfScope(desired *schemamodel.Database, database *catalog.Database, info catalog.ServerInfo) *schemamodel.Database {
	if desired == nil || database == nil || len(database.TablesOutOfScope) == 0 {
		return desired
	}
	into := func(reference string) bool {
		return reference != "" && foreignkeyscope.IntoTableOf(info.Dialect, reference, info.Schema, database.TablesOutOfScope)
	}
	validated := *desired
	validated.Fields = slices.Clone(desired.Fields)
	for i, field := range validated.Fields {
		if field.Foreign != "" && into(schemaprep.ParseForeignKeyReference(field.Foreign).Table) {
			validated.Fields[i].Foreign, validated.Fields[i].ForeignKeyName = "", ""
		}
	}
	validated.Constraints = slices.DeleteFunc(slices.Clone(desired.Constraints), func(constraint schemamodel.Constraint) bool {
		return strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") && into(constraint.ForeignTable)
	})
	return &validated
}
