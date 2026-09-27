package sqlschema

import (
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// foldCreatedIndexConstraints takes out of the model every UNIQUE and EXCLUDE
// one CREATE TABLE declared that PostgreSQL does not build, and gives a dropped
// constraint's name to the one the server keeps when that one has none; see
// [schemaprep.FoldedIndexConstraints] for the rule.
//
// Kept in the model, a dropped constraint is one the database the file built
// does not hold, so a comparison of the two plans it on every run.
//
// It runs before [nameCreatedConstraints], because the server folds before it
// names: a dropped constraint claims no name, and a kept one that takes a
// dropped one's name derives none. table is the last table of database, and
// fieldsStart and constraintsStart are where this statement's columns and
// constraints begin.
func foldCreatedIndexConstraints(
	database *schemamodel.Database,
	table *schemamodel.Table,
	fieldsStart, constraintsStart int,
	sourcePlatform string,
) {
	if !namesConstraintsLikePostgres(sourcePlatform) {
		return
	}
	created := database.Constraints[constraintsStart:]
	folds := schemaprep.FoldedIndexConstraints(*table, database.Fields[fieldsStart:], created, sourcePlatform)
	if len(folds) == 0 {
		return
	}
	dropped := make(map[int]bool, len(folds))
	for _, fold := range folds {
		dropped[fold.Folded] = true
		name := created[fold.Folded].Name
		switch {
		case name == "":
		case fold.Into == schemaprep.PrimaryKeyOutsideList:
			namePrimaryKey(table, database.Fields[fieldsStart:], name)
		case created[fold.Into].Name == "":
			created[fold.Into].Name = name
		}
	}
	kept := make([]schemamodel.Constraint, 0, len(created)-len(dropped))
	for position, constraint := range created {
		if !dropped[position] {
			kept = append(kept, constraint)
		}
	}
	database.Constraints = append(database.Constraints[:constraintsStart], kept...)
}

// namePrimaryKey gives table's primary key name when the key has none. fields
// are the columns the statement declared. A key a column declares is moved
// onto the table, which is where a named key is kept: `id int CONSTRAINT pk
// PRIMARY KEY` reads as the table's key over id.
func namePrimaryKey(table *schemamodel.Table, fields []schemamodel.Field, name string) {
	if table.PrimaryKeyName != "" {
		return
	}
	table.PrimaryKeyName = name
	if len(table.PrimaryKey) > 0 {
		return
	}
	for _, field := range fields {
		if field.Primary {
			table.PrimaryKey = append(table.PrimaryKey, field.Name)
		}
	}
}
