package sqlschema

import (
	"slices"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// foldCreatedIndexConstraints takes out of the model every UNIQUE and EXCLUDE,
// and every column's own UNIQUE, one CREATE TABLE declared that PostgreSQL does
// not build, and gives a dropped constraint's name to the one the server keeps
// when that one has none; see [schemaprep.FoldedIndexConstraints] for the rule.
//
// Kept in the model, a dropped constraint is one the database the file built
// does not hold, so a comparison of the two plans it on every run.
//
// It runs before [nameCreatedConstraints], because the server folds before it
// names: a dropped constraint claims no name, and a kept one that takes a
// dropped one's name derives none. table is the last table of database, and
// fieldsStart and constraintsStart are where this statement's columns and
// constraints begin.
//
// places holds the place of each of this statement's constraints, in the
// order of the model; see [constraintPlaces]. The answer holds the places of
// the constraints the fold keeps. A kept constraint takes the earliest place
// of the ones folded into it, because the server builds the first of equal
// keys and folds the rest into it: `a int UNIQUE, CONSTRAINT w1_a_key UNIQUE
// (b), b int, UNIQUE (a)` names the key over a when it reaches the column, and
// PostgreSQL 18.6 then refuses the name w1_a_key the statement writes.
func foldCreatedIndexConstraints(
	database *schemamodel.Database,
	table *schemamodel.Table,
	fieldsStart, constraintsStart int,
	places []int,
	sourcePlatform string,
) []int {
	if !namesConstraintsLikePostgres(sourcePlatform) {
		return places
	}
	created := database.Constraints[constraintsStart:]
	fields := database.Fields[fieldsStart:]
	folds := schemaprep.FoldedIndexConstraints(*table, fields, created, sourcePlatform)
	if len(folds) == 0 {
		return places
	}
	dropped := make(map[int]bool, len(folds))
	for _, fold := range folds {
		if fold.Folded == schemaprep.ColumnKeyOutsideList {
			dropColumnKey(fields, fold.Column)
			moveToEarlierPlace(places, fold.Into, columnPlace(slices.IndexFunc(fields, func(field schemamodel.Field) bool {
				return field.Name == fold.Column
			})))
			continue
		}
		dropped[fold.Folded] = true
		moveToEarlierPlace(places, fold.Into, places[fold.Folded])
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
	keptPlaces := make([]int, 0, len(created)-len(dropped))
	for position, constraint := range created {
		if !dropped[position] {
			kept = append(kept, constraint)
			keptPlaces = append(keptPlaces, places[position])
		}
	}
	database.Constraints = append(database.Constraints[:constraintsStart], kept...)
	return keptPlaces
}

// moveToEarlierPlace gives the constraint at position into the earlier of its
// place and place. A key folded into the primary key moves nothing: the server
// builds the primary key first wherever the statement writes it.
func moveToEarlierPlace(places []int, into, place int) {
	if into < 0 {
		return
	}
	places[into] = min(places[into], place)
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

// dropColumnKey clears the own UNIQUE of the column called column. fields are
// the columns the statement declared.
func dropColumnKey(fields []schemamodel.Field, column string) {
	for i := range fields {
		if fields[i].Name == column {
			fields[i].Unique = false
		}
	}
}
