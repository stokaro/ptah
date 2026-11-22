package schemaprep

import (
	"slices"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
)

// AssignDefaultCheckNames returns database with a name on every table CHECK it
// leaves unnamed, on a dialect whose server refuses a CHECK without one
// ([descriptor.CheckNamingRequired]). On any other dialect it returns database
// as it is, since that server names the CHECK itself and the comparison looks
// for the name it gives.
//
// The name is the one an entry of the table's checks list takes:
// `<table>_check`, then `<table>_check1` and on, past the names the table's
// constraints declare and the names its columns' CHECKs take. A render, a plan
// and a comparison of the same schema all read the CHECK under that name, so the
// constraint the first apply creates is the one the next comparison finds.
// Left unnamed, the comparison keys the declared CHECK by its definition and
// the one the server holds by the name the renderer gave it, and the two never
// pair.
//
// The input is not modified; a database that needs no name is returned as it
// is, and one that does is a copy with its own constraint list.
func AssignDefaultCheckNames(database *schemamodel.Database, dialect string) *schemamodel.Database {
	if database == nil || describe(dialect).checkNaming != descriptor.CheckNamingRequired {
		return database
	}
	if !slices.ContainsFunc(database.Constraints, unnamedCheck) {
		return database
	}
	assigned := *database
	assigned.Constraints = slices.Clone(database.Constraints)
	for _, table := range assigned.Tables {
		taken := declaredConstraintNames(table, assigned.Constraints)
		for _, name := range ColumnCheckNames(table, assigned.Fields, assigned.Constraints, dialect) {
			taken[name] = struct{}{}
		}
		ordinal := 0
		for i, constraint := range assigned.Constraints {
			if !unnamedCheck(constraint) || !ConstraintBelongsToTable(constraint, table) {
				continue
			}
			name := tableCheckConstraintName(table.Name, ordinal)
			for {
				if _, used := taken[name]; !used {
					break
				}
				ordinal++
				name = tableCheckConstraintName(table.Name, ordinal)
			}
			taken[name] = struct{}{}
			ordinal++
			assigned.Constraints[i].Name = name
		}
	}
	return &assigned
}

// AssignDefaultConstraintNames is [AssignDefaultForeignKeyNames] followed by
// [AssignDefaultCheckNames]: the names every path that renders, plans or
// compares a schema for dialect gives the constraints the schema leaves
// unnamed. The input is not modified.
func AssignDefaultConstraintNames(database *schemamodel.Database, dialect string) *schemamodel.Database {
	return AssignDefaultCheckNames(AssignDefaultForeignKeyNames(database, dialect), dialect)
}

// unnamedCheck reports a CHECK constraint declared without a name.
func unnamedCheck(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(strings.TrimSpace(constraint.Type), "CHECK") && strings.TrimSpace(constraint.Name) == ""
}
