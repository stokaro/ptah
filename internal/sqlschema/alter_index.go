package sqlschema

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnsequence"
	"ptah.run/internal/pgname"
)

// applyAlterIndex renames an index, as PostgreSQL reads `ALTER INDEX [IF
// EXISTS] name RENAME TO new_name`.
//
// Measured on PostgreSQL 18.6 against Atlas CE v1.3.0, each rename applies and
// Atlas CE reports the database it builds synced with the file
// (stokaro/ptah#3879):
//
//   - a declared index takes the new name;
//   - the index behind a UNIQUE, an EXCLUDE or the primary key renames the
//     constraint with it;
//   - a column's own UNIQUE becomes a UNIQUE constraint of the new name, as
//     RENAME CONSTRAINT makes it; see [alterTarget.nameColumnKey].
//
// The index is looked up in the schema the name spells, a bare name in the
// default schema; see [sameSchema]. A name no index of that schema holds is refused unless the statement says IF EXISTS,
// as the server answers `relation "x" does not exist`. So is a new name another
// relation of the schema holds, which the server answers with `relation "x"
// already exists`, and, for the index behind a constraint, a name another
// constraint of the table holds.
func applyAlterIndex(database *schemamodel.Database, document *Document, node *ast.AlterIndexNode, sourcePlatform string) error {
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
	to := normalizeSQLIdentifier(sourcePlatform, node.NewName)
	databases := []*schemamodel.Database{database}
	if document.base != nil {
		databases = append(databases, document.base)
	}
	index, found := findNamedIndex(databases, document, schema, name, sourcePlatform)
	if !found {
		if node.IfExists {
			return nil
		}
		return fmt.Errorf("ALTER INDEX %s names an index this schema does not declare", node.Name)
	}
	if relationHeld(databases, document, schema, to, sourcePlatform) {
		return fmt.Errorf("ALTER INDEX %s RENAME TO %s: the schema already holds a relation named %s",
			node.Name, node.NewName, to)
	}
	if index.backsConstraint() && index.target.holdsConstraint(to) {
		return fmt.Errorf("ALTER INDEX %s RENAME TO %s: table %s already holds a constraint named %s",
			node.Name, node.NewName, index.target.table.Name, to)
	}
	index.rename(database, to)
	return nil
}

// holdsConstraint reports whether a constraint of the table holds name: one
// the table declares, or the name a column gives its CHECK, foreign key or NOT
// NULL. A constraint with an index of its own holds a relation name as well,
// which [relationHeld] refuses first, so a CHECK, a foreign key and a NOT NULL
// are what this adds. Measured on PostgreSQL 18.6, each refuses the index
// behind a constraint renamed onto its name with `constraint "x" for relation
// "c" already exists`, and leaves a plain index free to take it.
func (t alterTarget) holdsConstraint(name string) bool {
	for _, database := range t.databases {
		if slices.ContainsFunc(database.Constraints, func(constraint schemamodel.Constraint) bool {
			return constraint.StructName == t.structName && constraint.Name == name
		}) || slices.ContainsFunc(database.Fields, func(field schemamodel.Field) bool {
			return field.StructName == t.structName &&
				(field.CheckName == name || field.ForeignKeyName == name || field.NotNullConstraintName == name)
		}) {
			return true
		}
	}
	return false
}

// relationHeld reports whether a relation of schema holds name: a table, a
// view, a sequence, an index, the index behind a constraint, a composite type,
// or the sequence behind a serial or identity column. Measured on PostgreSQL
// 18.6, each answers `relation "x" already exists` to an index renamed onto
// its name; an enum, which is a type and not a relation, does not.
func relationHeld(databases []*schemamodel.Database, document *Document, schema, name, sourcePlatform string) bool {
	for _, spelling := range schemaSpellings(sourcePlatform, schema) {
		if pgname.RelationNames(databases, spelling).Taken(name) {
			return true
		}
	}
	if _, found := findNamedIndex(databases, document, schema, name, sourcePlatform); found {
		return true
	}
	for _, database := range databases {
		if slices.ContainsFunc(database.CompositeTypes, func(composite schemamodel.CompositeType) bool {
			return sameSchema(sourcePlatform, schema, composite.Schema) && composite.Name == name
		}) {
			return true
		}
	}
	return ownedSequenceNamed(databases, schema, name, sourcePlatform)
}

// schemaSpellings answers the schema names the model may record for the
// objects of schema. The default schema is recorded bare or as public, and
// [pgname.RelationNames] compares a schema name as written.
func schemaSpellings(sourcePlatform, schema string) []string {
	if sameSchema(sourcePlatform, schema, "") {
		return []string{"", "public"}
	}
	return []string{schema}
}

// ownedSequenceNamed reports whether a serial or identity column of a table in
// schema owns a sequence called name: `<table>_<column>_seq`, as PostgreSQL
// names it.
func ownedSequenceNamed(databases []*schemamodel.Database, schema, name, sourcePlatform string) bool {
	for _, holder := range databases {
		for _, table := range holder.Tables {
			if !sameSchema(sourcePlatform, schema, table.Schema) {
				continue
			}
			for _, database := range databases {
				if slices.ContainsFunc(database.Fields, func(field schemamodel.Field) bool {
					sequence, ok := columnsequence.Declared(table.Name, field)
					return field.StructName == table.StructName && ok && sequence == name
				}) {
					return true
				}
			}
		}
	}
	return false
}
