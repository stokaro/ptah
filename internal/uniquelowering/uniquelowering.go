// Package uniquelowering writes a desired schema's UNIQUE constraints as the
// global unique indexes an engine without UNIQUE constraints holds for them.
// The provider host reads a declaration through it, before a comparison, for
// a provider whose session states capability.UniqueConstraints false, so a
// schema applied once compares as the indexes the engine reports.
//
// The index names are a convention the host and the provider share: the
// constraint's own name, or `<table>_<column>[_<column>...]_key` for one that
// has none. A provider that creates the index for a declared constraint gives
// it the name [IndexName] returns, or every comparison after the first apply
// plans the index again.
package uniquelowering

import (
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// ConstraintsAsIndexes returns database with its UNIQUE constraints and UNIQUE
// columns written as global unique indexes, for an engine that has unique
// indexes and no UNIQUE constraint.
//
// Such an engine holds an index where the declaration says constraint, and
// its reader reports the index. The comparison reads the declaration through
// this function so both sides are indexes of the same name, and a schema
// applied once plans nothing the second time.
//
// Each index takes the name [IndexName] gives it, and a UNIQUE over the
// table's key folds into the key ([IsTheKey]). A constraint an index cannot
// hold -- deferrable, NOT ENFORCED, NOT VALID, carrying an index method or a
// facet -- is left a constraint, so the refusal that reaches the author is the
// owner's, which names what the engine cannot hold. A constraint's comment
// becomes the index's. database is not changed, and a nil database is
// returned as it is.
func ConstraintsAsIndexes(database *schemamodel.Database) *schemamodel.Database {
	if database == nil {
		return nil
	}
	lowered := *database
	lowered.Fields = slices.Clone(database.Fields)
	lowered.Constraints = nil
	lowered.Indexes = slices.Clone(database.Indexes)

	for _, constraint := range database.Constraints {
		table, owned := constraintTable(database.Tables, constraint)
		if !owned || !strings.EqualFold(strings.TrimSpace(constraint.Type), "UNIQUE") || !lowersToIndex(constraint) {
			lowered.Constraints = append(lowered.Constraints, constraint)
			continue
		}
		if IsTheKey(constraint.Columns, tableKey(table, database)) {
			continue
		}
		lowered.Indexes = append(lowered.Indexes, schemamodel.Index{
			StructName:     constraint.StructName,
			TableName:      constraint.Table,
			Name:           IndexName(table.Name, constraint.Name, constraint.Columns),
			Fields:         slices.Clone(constraint.Columns),
			Unique:         true,
			IncludeColumns: slices.Clone(constraint.IncludeColumns),
			NullsDistinct:  constraint.NullsDistinct,
			Comment:        constraint.Comment,
		})
	}

	for position, field := range lowered.Fields {
		if !field.Unique {
			continue
		}
		index := slices.IndexFunc(database.Tables, func(table schemamodel.Table) bool {
			return table.StructName == field.StructName
		})
		if index < 0 {
			continue
		}
		table := database.Tables[index]
		lowered.Fields[position].Unique = false
		if IsTheKey([]string{field.Name}, tableKey(table, database)) {
			continue
		}
		lowered.Indexes = append(lowered.Indexes, schemamodel.Index{
			StructName: field.StructName,
			Name:       IndexName(table.Name, "", []string{field.Name}),
			Fields:     []string{field.Name},
			Unique:     true,
		})
	}
	return &lowered
}

// IndexName is the name of the global unique index that holds a UNIQUE
// constraint: the constraint's own name, or for one that has none -- a
// column's UNIQUE, or a table-level UNIQUE left unnamed --
// `<table>_<column>[_<column>...]_key`, the name PostgreSQL gives the same
// key. table is the table's own name, without its schema or directory.
func IndexName(table, declared string, columns []string) string {
	if strings.TrimSpace(declared) != "" {
		return declared
	}
	return table + "_" + strings.Join(columns, "_") + "_key"
}

// IsTheKey reports whether a UNIQUE over columns names the table's key
// columns, in any order, which every row already holds unique. Such a UNIQUE
// needs no index of its own, so it folds into the key, as it does on
// PostgreSQL, where `id int PRIMARY KEY UNIQUE` builds one key.
func IsTheKey(columns, key []string) bool {
	if len(key) == 0 || len(columns) != len(key) {
		return false
	}
	return slices.Equal(slices.Sorted(slices.Values(columns)), slices.Sorted(slices.Values(key)))
}

// constraintTable is the table a constraint belongs to.
func constraintTable(tables []schemamodel.Table, constraint schemamodel.Constraint) (schemamodel.Table, bool) {
	index := slices.IndexFunc(tables, func(table schemamodel.Table) bool {
		return schemaprep.ConstraintBelongsToTable(constraint, table)
	})
	if index < 0 {
		return schemamodel.Table{}, false
	}
	return tables[index], true
}

// lowersToIndex reports whether a UNIQUE constraint says nothing a global
// unique index cannot hold.
func lowersToIndex(constraint schemamodel.Constraint) bool {
	return !constraint.Deferrable && constraint.Initially == "" && !constraint.NotEnforced && !constraint.NotValid &&
		constraint.UsingMethod == "" &&
		constraint.Facets.IsZero()
}

// tableKey is a table's key columns: its PRIMARY KEY, a PRIMARY KEY constraint
// that belongs to it, or the fields it marks primary, in declaration order.
func tableKey(table schemamodel.Table, database *schemamodel.Database) []string {
	if len(table.PrimaryKey) > 0 {
		return table.PrimaryKey
	}
	for _, constraint := range database.Constraints {
		if strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") && schemaprep.ConstraintBelongsToTable(constraint, table) {
			return constraint.Columns
		}
	}
	var key []string
	for _, field := range database.Fields {
		if field.StructName == table.StructName && field.Primary {
			key = append(key, field.Name)
		}
	}
	return key
}
