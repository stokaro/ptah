package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemamodel"
)

// namedIndex is what an index name reaches on one table: an index the schema
// declares, the constraint whose index it is, the primary key, or a column's
// own UNIQUE. Exactly one of index, constraint, primaryKey and column is set.
//
// DROP INDEX and ALTER INDEX read an index name through this one type, so the
// two statements cannot disagree about what a name holds: which one deletes
// or renames, and which one refuses because a constraint owns the index.
type namedIndex struct {
	target alterTarget
	// owner and position place a declared index, which DROP INDEX deletes.
	owner      *schemamodel.Database
	position   int
	index      *schemamodel.Index
	constraint *schemamodel.Constraint
	primaryKey bool
	column     *schemamodel.Field
}

// backsConstraint reports whether the index is the one a constraint is
// enforced by: renamed, it renames the constraint, and it cannot be dropped
// apart from it.
func (n namedIndex) backsConstraint() bool {
	return n.index == nil
}

// describe names the constraint the index backs, the way a refusal says it.
func (n namedIndex) describe(name string) string {
	switch {
	case n.constraint != nil:
		return fmt.Sprintf("%s constraint %s", strings.ToLower(n.constraint.Type), n.constraint.Name)
	case n.primaryKey:
		return "primary key constraint " + name
	default:
		return "unique constraint " + name
	}
}

// drop deletes the declared index. It is for an index that backs no
// constraint.
func (n namedIndex) drop() {
	n.owner.Indexes = slices.Delete(n.owner.Indexes, n.position, n.position+1)
}

// rename gives the index, and the constraint it backs, the name to.
func (n namedIndex) rename(database *schemamodel.Database, to string) {
	switch {
	case n.index != nil:
		n.index.Name = to
	case n.constraint != nil:
		n.constraint.Name = to
	case n.primaryKey:
		n.target.table.PrimaryKeyName = to
	default:
		n.target.keyAsConstraint(database, n.column, to)
	}
}

// findNamedIndex looks name up among the indexes of the tables in schema, a
// bare name among the tables of the default schema; see [sameSchema].
func findNamedIndex(
	databases []*schemamodel.Database, document *Document, schema, name, sourcePlatform string,
) (namedIndex, bool) {
	for _, holder := range databases {
		for i := range holder.Tables {
			if !sameSchema(sourcePlatform, schema, holder.Tables[i].Schema) {
				continue
			}
			target := tableTarget(&holder.Tables[i], databases, document, sourcePlatform)
			if index, found := target.namedIndex(name); found {
				return index, true
			}
		}
	}
	return namedIndex{}, false
}

// tableTarget answers the alterTarget of a table a statement reached by
// something other than the table's name, such as one of its indexes.
func tableTarget(
	table *schemamodel.Table, databases []*schemamodel.Database, document *Document, sourcePlatform string,
) alterTarget {
	return alterTarget{
		written:        table.QualifiedName(),
		structName:     table.StructName,
		qualified:      table.QualifiedName(),
		table:          table,
		databases:      databases,
		sourcePlatform: sourcePlatform,
		statement:      &alterStatement{},
		keys:           &document.keys,
	}
}

// namedIndex answers what the index name reaches on the table, if anything.
func (t alterTarget) namedIndex(name string) (namedIndex, bool) {
	for _, database := range t.databases {
		for i := range database.Indexes {
			if t.ownsIndex(database.Indexes[i]) && database.Indexes[i].Name == name {
				return namedIndex{target: t, owner: database, position: i, index: &database.Indexes[i]}, true
			}
		}
		for i := range database.Constraints {
			constraint := &database.Constraints[i]
			if constraint.StructName == t.structName && constraint.Name == name && isIndexBacked(*constraint) {
				return namedIndex{target: t, constraint: constraint}, true
			}
		}
	}
	if hasPrimaryKey(t.databases, *t.table) && primaryKeyIndexName(*t.table, t.sourcePlatform) == name {
		return namedIndex{target: t, primaryKey: true}, true
	}
	if field := t.columnWithKey(name); field != nil {
		return namedIndex{target: t, column: field}, true
	}
	return namedIndex{}, false
}
