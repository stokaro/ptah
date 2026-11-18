// Package catalogspelling gives the names a live read reports the spelling
// that addresses them. It sits between a reader and every consumer that turns
// a read into statements or compares it with a schema: the comparison in
// migration/schemadiff, and the conversion to a schema model in
// internal/convert/dbschematogo.
//
// The stored and the addressing spelling differ only on an engine whose
// descriptor states an UnquotedFold. Oracle stores an unquoted name in upper
// case and a quoted one as written, so a table created as "orders" is stored
// as orders, which no unquoted name reaches. Its addressing spelling is `"orders"`, which
// identifier.Semantics keys apart from ORDERS and the Oracle renderer writes
// quoted (stokaro/ptah#6243).
package catalogspelling

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/sqlident"
)

// Spell returns read with every table, column, index, constraint, view,
// materialized view, sequence and trigger name, and every column a key or an
// index lists, written as [sqlident.CatalogName] writes it for dialect.
//
// It changes nothing unless read.StoredNames is set and dialect's identifier
// semantics state an UnquotedFold. The result has StoredNames cleared, since
// its names are not the stored spelling, so spelling it again changes
// nothing. Every slice it changes is a copy, so the caller's read is left as
// it was. Schema names keep their stored spelling: a session's default schema
// is compared against them unquoted.
func Spell(read catalog.Database, dialect string) catalog.Database {
	if !read.StoredNames || identifier.ForDialect(dialect).UnquotedFold == "" {
		return read
	}
	spell := func(name string) string { return sqlident.CatalogName(dialect, name) }
	spellAll := func(names []string) []string {
		if names == nil {
			return nil
		}
		spelled := make([]string, len(names))
		for index, name := range names {
			spelled[index] = spell(name)
		}
		return spelled
	}
	spellPointer := func(name *string) *string {
		if name == nil {
			return nil
		}
		return new(spell(*name))
	}

	read.StoredNames = false
	read.Tables = slices.Clone(read.Tables)
	for index := range read.Tables {
		table := &read.Tables[index]
		table.Name = spell(table.Name)
		table.Columns = slices.Clone(table.Columns)
		for column := range table.Columns {
			table.Columns[column].Name = spell(table.Columns[column].Name)
		}
	}
	read.Indexes = slices.Clone(read.Indexes)
	for index := range read.Indexes {
		held := &read.Indexes[index]
		held.Name = spell(held.Name)
		held.TableName = spell(held.TableName)
		held.Columns = spellAll(held.Columns)
		held.Parts = slices.Clone(held.Parts)
		for part := range held.Parts {
			if held.Parts[part].Name != "" {
				held.Parts[part].Name = spell(held.Parts[part].Name)
			}
		}
	}
	read.Constraints = slices.Clone(read.Constraints)
	for index := range read.Constraints {
		held := &read.Constraints[index]
		held.Name = spell(held.Name)
		held.TableName = spell(held.TableName)
		if held.ColumnName != "" {
			held.ColumnName = spell(held.ColumnName)
		}
		held.ColumnNames = spellAll(held.ColumnNames)
		held.ForeignTable = spellPointer(held.ForeignTable)
		held.ForeignColumn = spellPointer(held.ForeignColumn)
		held.ForeignColumns = spellAll(held.ForeignColumns)
		held.IncludeColumns = spellAll(held.IncludeColumns)
	}
	read.Views = slices.Clone(read.Views)
	for index := range read.Views {
		read.Views[index].Name = spell(read.Views[index].Name)
	}
	read.MatViews = slices.Clone(read.MatViews)
	for index := range read.MatViews {
		read.MatViews[index].Name = spell(read.MatViews[index].Name)
	}
	read.Sequences = slices.Clone(read.Sequences)
	for index := range read.Sequences {
		read.Sequences[index].Name = spell(read.Sequences[index].Name)
	}
	read.Triggers = slices.Clone(read.Triggers)
	for index := range read.Triggers {
		read.Triggers[index].Name = spell(read.Triggers[index].Name)
		read.Triggers[index].Table = spell(read.Triggers[index].Table)
	}
	return read
}
