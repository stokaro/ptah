package sqlschema

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
)

// The labels PostgreSQL ends a derived constraint name with.
const (
	foreignKeyLabel = "fkey"
	uniqueLabel     = "key"
)

// namesConstraintsLikePostgres reports whether an unnamed UNIQUE or FOREIGN KEY
// read from a source of this dialect takes the name PostgreSQL gives it.
//
// PostgreSQL alone: every rule here was measured on PostgreSQL 18.6. The other
// engines name an unnamed constraint differently or not in a way anyone
// measured here -- MySQL writes `<table>_ibfk_<n>` for a foreign key, SQLite
// keeps no name -- and they keep the name the rest of Ptah derives.
func namesConstraintsLikePostgres(sourcePlatform string) bool {
	return platform.NormalizeDialect(sourcePlatform) == platform.Postgres
}

// nameCreatedConstraints gives every unnamed table-level UNIQUE and every
// unnamed FOREIGN KEY one CREATE TABLE declared the name PostgreSQL gives it.
//
// The name has to be decided on the desired model, because the other side of a
// comparison is a catalog, and a catalog holds the name the server chose. A
// schema file compared with the database its own SQL created otherwise kept the
// server's `<table>_<column>_fkey` and added a second, identical key named
// Ptah's `fk_<table>_<column>` beside it, and dropped the server's
// `<table>_<columns>_key` to add the same UNIQUE back without a name, which the
// server named again (stokaro/ptah#3643).
//
// The order is the server's. PostgreSQL creates the table and its CHECK
// constraints, then the index behind each UNIQUE, then the foreign keys, and
// each derived name avoids every name that exists by then. So every name the
// schema already holds is claimed first -- in this file, in the earlier files
// the document read, and the table's own explicitly named constraints -- then
// the UNIQUE names, then the foreign key names. Measured, `REFERENCES
// parent(id)` beside `CONSTRAINT dup_parent_id_fkey CHECK (...)` becomes
// `dup_parent_id_fkey1`, and `UNIQUE (a)` beside `CONSTRAINT s_a_key CHECK
// (...)` becomes `s_a_key1`. Inline foreign keys are named before table-level
// ones. The server follows the document's order across the two, which the
// model does not hold; the order decides only which of two keys over the same
// columns takes the numbered name.
//
// A column-level UNIQUE is not named here: the model keeps it on the column,
// and the comparison matches it by its columns.
func nameCreatedConstraints(
	database, base *schemamodel.Database,
	table schemamodel.Table,
	fieldsStart, constraintsStart int,
	sourcePlatform string,
) {
	if !namesConstraintsLikePostgres(sourcePlatform) {
		return
	}
	databases := []*schemamodel.Database{database, base}
	constraints := constraintNamesInSchema(databases, table.Schema)
	relations := relationNamesInSchema(databases, table.Schema)
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !strings.EqualFold(constraint.Type, "UNIQUE") || constraint.Name != "" {
			continue
		}
		constraint.Name = claimDerivedName(table.Name, constraint.Columns, uniqueLabel, constraints, relations)
		relations.claim(constraint.Name)
	}
	for i := fieldsStart; i < len(database.Fields); i++ {
		field := &database.Fields[i]
		if field.Foreign == "" || field.ForeignKeyName != "" {
			continue
		}
		field.ForeignKeyName = claimDerivedName(table.Name, []string{field.Name}, foreignKeyLabel, constraints)
	}
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || constraint.Name != "" {
			continue
		}
		constraint.Name = claimDerivedName(table.Name, constraint.Columns, foreignKeyLabel, constraints)
	}
}

// nameAddedConstraint names an unnamed UNIQUE or FOREIGN KEY an ALTER TABLE
// adds, by the rule [nameCreatedConstraints] follows, against every name the
// schema already holds -- in this file and in the earlier ones the document
// read.
func nameAddedConstraint(constraint *schemamodel.Constraint, target alterTarget) {
	if !namesConstraintsLikePostgres(target.sourcePlatform) || constraint.Name != "" {
		return
	}
	schema, table := splitQualifiedTable(target.qualified)
	constraints := constraintNamesInSchema(target.databases, schema)
	switch {
	case strings.EqualFold(constraint.Type, "UNIQUE"):
		relations := relationNamesInSchema(target.databases, schema)
		constraint.Name = claimDerivedName(table, constraint.Columns, uniqueLabel, constraints, relations)
	case strings.EqualFold(constraint.Type, "FOREIGN KEY"):
		constraint.Name = claimDerivedName(table, constraint.Columns, foreignKeyLabel, constraints)
	}
}

// claimDerivedName derives the first name none of the sets holds and claims it
// in the first set, which is the constraint namespace.
func claimDerivedName(table string, columns []string, label string, sets ...namespaceNames) string {
	name := pgname.Constraint(table, columns, label, func(candidate string) bool {
		for _, set := range sets {
			if set.taken(candidate) {
				return true
			}
		}
		return false
	})
	sets[0].claim(name)
	return name
}

// namespaceNames is the set of names one namespace of one schema holds.
//
// Schema-wide rather than per table, because that is where PostgreSQL looks:
// it picks a name no constraint in the schema carries, and for the index
// behind a UNIQUE, no relation either. Exact rather than folded, because every
// name reaching here was already read the way the server stores it.
type namespaceNames map[string]struct{}

func (n namespaceNames) taken(name string) bool {
	_, found := n[name]
	return found
}

func (n namespaceNames) claim(name string) {
	if name != "" {
		n[name] = struct{}{}
	}
}

// constraintNamesInSchema collects every constraint name the databases give
// the tables of one schema: table constraints, the names a column carries for
// its own constraints, and a table's named primary key.
//
// Explicit names and the names derived here are the only ones that can
// collide with a derived `_fkey` or `_key`: every other name PostgreSQL
// derives ends in a label of its own, such as `_pkey`, `_check` or
// `_not_null`.
func constraintNamesInSchema(databases []*schemamodel.Database, schema string) namespaceNames {
	names := make(namespaceNames)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.claim(table.PrimaryKeyName)
			}
		}
		for _, constraint := range database.Constraints {
			if tableSchema, _ := splitQualifiedTable(constraint.Table); tableSchema == schema {
				names.claim(constraint.Name)
			}
		}
		for _, field := range database.Fields {
			if !inSchema[field.StructName] {
				continue
			}
			names.claim(field.ForeignKeyName)
			names.claim(field.CheckName)
			names.claim(field.NotNullConstraintName)
		}
	}
	return names
}

// relationNamesInSchema collects the names of the relations the databases
// declare in one schema -- tables, views, materialized views, sequences and
// indexes -- which the index behind a UNIQUE may not take either. Measured, an
// index or a table already called `q_a_key` makes `UNIQUE (a)` on `q` become
// `q_a_key1`.
func relationNamesInSchema(databases []*schemamodel.Database, schema string) namespaceNames {
	names := make(namespaceNames)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.claim(table.Name)
			}
		}
		for _, view := range database.Views {
			if viewSchema, name := splitQualifiedTable(view.Name); viewSchema == schema {
				names.claim(name)
			}
		}
		for _, view := range database.MaterializedViews {
			if viewSchema, name := splitQualifiedTable(view.Name); viewSchema == schema {
				names.claim(name)
			}
		}
		for _, sequence := range database.Sequences {
			if sequence.Schema == schema {
				names.claim(sequence.Name)
			}
		}
		for _, index := range database.Indexes {
			if indexInSchema(index, inSchema, schema) {
				names.claim(index.Name)
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
		tableSchema, _ := splitQualifiedTable(index.TableName)
		return tableSchema == schema
	}
	return inSchema[index.StructName]
}

// splitQualifiedTable returns the schema and bare name of a table reference the
// reader already qualified, and the reference itself with no schema when it
// does not parse.
func splitQualifiedTable(qualified string) (schema, name string) {
	ref, ok := tableref.Parse(qualified)
	if !ok {
		return "", qualified
	}
	return ref.Schema, ref.Name
}
