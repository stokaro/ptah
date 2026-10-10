package ydblowering

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/ydbindex"
)

// UniqueConstraintsAsIndexesFor returns database with its UNIQUE constraints and
// UNIQUE columns written as the global unique indexes the YDB renderer builds
// for them, on a YDB target without [capability.UniqueConstraints]. Every
// other target, and a YDB target that has the key, gets database itself.
//
// YDB has unique indexes and no UNIQUE constraint, so a database a plan
// applied holds an index where the declaration says constraint, and the reader
// reports the index. The comparison reads the declaration through this
// function so both sides are indexes of the same name, and a schema applied
// once plans nothing the second time. A file-to-file comparison reads its
// current side through it too, so a document compared with itself is clean.
//
// Each index takes the name [ydbindex.UniqueIndexName] gives it, the one the
// renderer writes, and a UNIQUE over the table's key folds into the key
// ([ydbindex.UniqueIsTheKey]) as the renderer folds it. A constraint the
// renderer refuses -- deferrable, NOT ENFORCED, NOT VALID, partial, or carrying
// an index method or a block size -- is left a constraint, so the refusal that
// reaches the author is the renderer's, which names what YDB cannot hold. A
// constraint's comment becomes the index's, as the renderer writes it.
// database is not changed.
func UniqueConstraintsAsIndexesFor(
	database *schemamodel.Database,
	dialect string,
	caps capability.Capabilities,
) *schemamodel.Database {
	if database == nil || platform.NormalizeDialect(dialect) != platform.YDB || caps.Has(capability.UniqueConstraints) {
		return database
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
		if ydbindex.UniqueIsTheKey(constraint.Columns, tableKey(table, database)) {
			continue
		}
		lowered.Indexes = append(lowered.Indexes, schemamodel.Index{
			StructName:     constraint.StructName,
			TableName:      constraint.Table,
			Name:           ydbindex.UniqueIndexName(table.Name, constraint.Name, constraint.Columns),
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
		if ydbindex.UniqueIsTheKey([]string{field.Name}, tableKey(table, database)) {
			continue
		}
		lowered.Indexes = append(lowered.Indexes, schemamodel.Index{
			StructName: field.StructName,
			Name:       ydbindex.UniqueIndexName(table.Name, "", []string{field.Name}),
			Fields:     []string{field.Name},
			Unique:     true,
		})
	}
	return &lowered
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
		strings.TrimSpace(constraint.WhereCondition) == "" && constraint.UsingMethod == "" &&
		constraint.KeyBlockSize == 0
}

// tableKey is a table's key columns, as the renderer reads them: its PRIMARY
// KEY, a PRIMARY KEY constraint that belongs to it, or the fields it marks
// primary, in declaration order.
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
