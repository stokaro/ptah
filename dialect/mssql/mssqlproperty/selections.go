package mssqlproperty

import "ptah.run/core/selectext"

// Selections is how a command's scope names an extended property: by its
// name, `*[type=extended_property]`, in the schema that holds it, and by its
// name alone for a property on the database itself, which no schema selects
// or removes. A property on a table, or on a column of one, also follows its
// table, which [RelationService] reports it attached to: a scope keeps or
// leaves it with the table, and an exclusion of the table excludes it.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: Kind, Type: "extended_property", Naming: selectext.SchemaOrDatabase},
	}}
}
