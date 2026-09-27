package pgname

import (
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// Names is the set of names one namespace of one schema holds.
//
// Schema-wide rather than per table, because that is where PostgreSQL looks:
// it picks a name no constraint in the schema carries, and for the index
// behind a UNIQUE, no relation either. Exact rather than folded, because every
// name reaching here was already read the way the server stores it.
type Names map[string]struct{}

// Taken reports whether the namespace holds name.
func (n Names) Taken(name string) bool {
	_, found := n[name]
	return found
}

// Claim adds name to the namespace. An empty name is none.
func (n Names) Claim(name string) {
	if name != "" {
		n[name] = struct{}{}
	}
}

// ConstraintNames collects every constraint name the databases give the tables
// of one schema: table constraints, the names a column carries for its own
// constraints, and a table's named primary key. A column's own UNIQUE is not
// among them, because the model keeps no name for it.
//
// Explicit names and derived ones are the only names that can collide with a
// derived `_check`, `_fkey` or `_key`: every other name PostgreSQL derives
// ends in a label of its own, such as `_pkey` or `_not_null`.
func ConstraintNames(databases []*schemamodel.Database, schema string) Names {
	names := make(Names)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.Claim(table.PrimaryKeyName)
			}
		}
		for _, constraint := range database.Constraints {
			if tableSchema, _ := splitTable(constraint.Table); tableSchema == schema {
				names.Claim(constraint.Name)
			}
		}
		for _, field := range database.Fields {
			if !inSchema[field.StructName] {
				continue
			}
			names.Claim(field.ForeignKeyName)
			names.Claim(field.CheckName)
			names.Claim(field.NotNullConstraintName)
		}
	}
	return names
}

// RelationNames collects the names of the relations the databases declare in
// one schema -- tables, views, materialized views, sequences and indexes --
// which the index behind a UNIQUE may not take either. Measured, an index or a
// table already called `q_a_key` makes `UNIQUE (a)` on `q` become `q_a_key1`.
func RelationNames(databases []*schemamodel.Database, schema string) Names {
	names := make(Names)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.Claim(table.Name)
			}
		}
		for _, view := range database.Views {
			if viewSchema, name := splitTable(view.Name); viewSchema == schema {
				names.Claim(name)
			}
		}
		for _, view := range database.MaterializedViews {
			if viewSchema, name := splitTable(view.Name); viewSchema == schema {
				names.Claim(name)
			}
		}
		for _, sequence := range database.Sequences {
			if sequence.Schema == schema {
				names.Claim(sequence.Name)
			}
		}
		for _, index := range database.Indexes {
			if indexInSchema(index, inSchema, schema) {
				names.Claim(index.Name)
			}
		}
	}
	return names
}

// tablesInSchema answers, by struct name, which tables of database sit in
// schema.
func tablesInSchema(database *schemamodel.Database, schema string) map[string]bool {
	inSchema := make(map[string]bool, len(database.Tables))
	for _, table := range database.Tables {
		if table.Schema == schema {
			inSchema[table.StructName] = true
		}
	}
	return inSchema
}

// indexInSchema reports whether an index belongs to a table of schema, read
// from the table it names when it names one and from its owner otherwise.
func indexInSchema(index schemamodel.Index, inSchema map[string]bool, schema string) bool {
	if index.TableName != "" {
		tableSchema, _ := splitTable(index.TableName)
		return tableSchema == schema
	}
	return inSchema[index.StructName]
}

// splitTable returns the schema and bare name of a qualified table reference,
// and the reference itself with no schema when it does not parse.
func splitTable(qualified string) (schema, name string) {
	ref, ok := tableref.Parse(qualified)
	if !ok {
		return "", qualified
	}
	return ref.Schema, ref.Name
}
