package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// namedIndex is what an index name reaches on one table or materialized view:
// an index the schema declares, the constraint whose index it is, the primary
// key, or a column's own UNIQUE. Exactly one of index, constraint, primaryKey
// and column is set.
//
// DROP INDEX and ALTER INDEX read an index name through this one type, so the
// two statements cannot disagree about what a name holds: which one deletes
// or renames, and which one refuses because a constraint owns the index.
type namedIndex struct {
	// target is the table the index belongs to. It is the zero alterTarget
	// for an index of a materialized view, which only an index the schema
	// declares can be.
	target alterTarget
	// view is the materialized view the index belongs to, empty for an index
	// of a table.
	view string
	// owner and position place a declared index, which DROP INDEX deletes.
	owner      *schemamodel.Database
	position   int
	index      *schemamodel.Index
	constraint *schemamodel.Constraint
	owned      *objectidentity.ID
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
	case n.owned != nil:
		return "constraint " + n.owned.Name.Source
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
func (n namedIndex) rename(database *schemamodel.Database, to string) error {
	if n.owned != nil {
		return n.target.renameOwnedConstraint(n.owner, *n.owned, to)
	}
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
	return nil
}

// findNamedIndex looks name up among the indexes of the tables and the
// materialized views in schema, a bare name among those of the default schema;
// see [sameSchema]. PostgreSQL indexes a materialized view as it indexes a
// table, in one namespace with the tables' indexes: measured on 18.6, `DROP
// INDEX`, `ALTER INDEX ... RENAME TO` and `COMMENT ON INDEX` each reach an
// index of a materialized view, and `ALTER INDEX IF EXISTS ... RENAME TO`
// renames it rather than skipping.
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
	return viewIndex(databases, schema, name, sourcePlatform)
}

// viewIndex answers the declared index called name of a materialized view in
// schema, by the rule [indexHolder] gives. A materialized view has no
// constraint, so its index is always one the schema declares.
func viewIndex(databases []*schemamodel.Database, schema, name, sourcePlatform string) (namedIndex, bool) {
	for _, owner := range databases {
		for i := range owner.Indexes {
			if owner.Indexes[i].Name != name {
				continue
			}
			holder, found := indexHolder(databases, owner.Indexes[i], sourcePlatform)
			if found && holder.table == nil && sameSchema(sourcePlatform, schema, holder.schema) {
				return namedIndex{view: holder.qualified, owner: owner, position: i, index: &owner.Indexes[i]}, true
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
		document:       document,
	}
}

// namedIndex answers what the index name reaches on the table, if anything.
func (t alterTarget) namedIndex(name string) (namedIndex, bool) {
	for _, database := range t.databases {
		for _, definition := range schemaext.DefinedConstraints(database.FeatureObjects) {
			if definition.Indexed && t.ownsConstraint(definition.Ref) && normalizeSQLIdentifier("", definition.Ref.Name.Source) == name {
				ref := definition.Ref
				return namedIndex{target: t, owner: database, owned: &ref}, true
			}
		}
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
