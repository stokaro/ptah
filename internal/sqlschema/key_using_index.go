package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// applyKeyUsingIndex adds the key `ALTER TABLE ... ADD [CONSTRAINT name]
// UNIQUE | PRIMARY KEY USING INDEX index` makes of an index the schema
// declares on the table, as PostgreSQL 18.6 makes it: the key takes the
// index's columns, INCLUDE columns and NULLS treatment, the index is renamed
// to the constraint's name, and a key the statement does not name takes the
// index's. The index leaves the model, because the index the server holds
// is now the key's own, which the key declares.
//
// What the server refuses to make a key of is refused with the server's
// reason; see [refuseKeyIndex]. A CONCURRENTLY the index was built with is
// how it was built, not what it is, so the key does not carry it: building
// the index CONCURRENTLY and attaching it is the usual way to add a key
// without a long lock, and the schema it leaves is the key.
func applyKeyUsingIndex(database *schemamodel.Database, target alterTarget, constraint *ast.ConstraintNode) error {
	clause := fmt.Sprintf("ALTER TABLE %s ADD %s USING INDEX %s", target.written, keyClause(constraint), constraint.UsingIndex)
	name := normalizeSQLIdentifier(target.sourcePlatform, constraint.UsingIndex)
	holder, position := target.declaredIndex(name)
	if holder == nil {
		if target.renamesIndex(name) {
			// PostgreSQL 18.6: `index "i" is already associated with a constraint`.
			return fmt.Errorf("%s: the index is already the key of a constraint", clause)
		}
		return fmt.Errorf("%s names an index this schema does not declare on the table", clause)
	}
	index := holder.Indexes[position]
	primary := constraint.Type == ast.PrimaryKeyConstraint
	if err := refuseKeyIndex(index, primary); err != nil {
		return fmt.Errorf("%s: %w", clause, err)
	}
	keyName := index.Name
	if constraint.Name != "" {
		keyName = normalizeSQLIdentifier(target.sourcePlatform, constraint.Name)
	}
	if primary {
		if err := target.refuseSecondPrimaryKey(); err != nil {
			return err
		}
		holder.Indexes = slices.Delete(holder.Indexes, position, position+1)
		target.table.PrimaryKeyName = keyName
		target.table.PrimaryKey = slices.Clone(index.Fields)
		target.table.PrimaryKeyInclude = slices.Clone(index.IncludeColumns)
		target.table.PrimaryKeyDeferrable = constraint.Deferrable
		target.table.PrimaryKeyInitially = constraint.Initially
		for _, database := range target.databases {
			markPrimaryFields(database.Fields, target.structName, target.table.PrimaryKey)
		}
		return nil
	}
	holder.Indexes = slices.Delete(holder.Indexes, position, position+1)
	return addTableConstraint(database, target, schemamodel.Constraint{
		StructName:     target.structName,
		Name:           keyName,
		Type:           "UNIQUE",
		Table:          normalizeSQLTableReference("", target.qualified),
		Columns:        slices.Clone(index.Fields),
		IncludeColumns: slices.Clone(index.IncludeColumns),
		NullsDistinct:  cloneBoolPtr(index.NullsDistinct),
		Deferrable:     constraint.Deferrable,
		Initially:      constraint.Initially,
	})
}

// keyClause is the key a constraint adds, as the statement spells it.
func keyClause(constraint *ast.ConstraintNode) string {
	if constraint.Type == ast.PrimaryKeyConstraint {
		return "PRIMARY KEY"
	}
	return "UNIQUE"
}

// declaredIndex answers the model holding the index of the table named name,
// and the index's position there, or nil.
func (t alterTarget) declaredIndex(name string) (*schemamodel.Database, int) {
	for _, database := range t.databases {
		for i, index := range database.Indexes {
			if t.ownsIndex(index) && index.Name == name {
				return database, i
			}
		}
	}
	return nil, -1
}

// refuseKeyIndex refuses an index a key cannot be made of. The first reasons
// are PostgreSQL 18.6's own, in its words: a key is a unique B-tree index on
// columns, sorted as the columns sort by default, over every row, and a
// primary key's treats no two NULLs as equal. The rest are settings of the
// index a key keeps nowhere, refused as a key written with them is refused.
//
// An operator class written out is refused even where it is the column type's
// default, which the server accepts: telling the two apart needs the
// column's type and the server's catalog, and a key is built with the
// default class whatever the index names.
func refuseKeyIndex(index schemamodel.Index, primary bool) error {
	switch {
	case !index.Unique:
		return fmt.Errorf("%q is not a unique index", index.Name)
	case index.Condition != "":
		return fmt.Errorf("%q is a partial index", index.Name)
	case index.Type != "" && !strings.EqualFold(index.Type, "btree"):
		return fmt.Errorf("index %q is not a b-tree", index.Name)
	case primary && index.NullsDistinct != nil && !*index.NullsDistinct:
		return fmt.Errorf("primary keys cannot use NULLS NOT DISTINCT indexes")
	}
	for i, part := range index.Parts {
		switch {
		case part.Expr != "":
			return fmt.Errorf("index %q contains expressions", index.Name)
		case part.Desc || part.NullsOrder != "":
			return fmt.Errorf("index %q column number %d does not have default sorting behavior", index.Name, i+1)
		case part.Operator != "":
			return fmt.Errorf("index %q names operator class %s for column number %d, and a key is built with "+
				"its columns' default classes; write the index without one", index.Name, part.Operator, i+1)
		}
	}
	switch {
	case index.Operator != "":
		return fmt.Errorf("index %q names operator class %s, and a key is built with its columns' default classes; "+
			"write the index without one", index.Name, index.Operator)
	case len(index.StorageParams) > 0:
		return fmt.Errorf("index %q has storage parameters, which a key does not keep; set them with "+
			"ALTER INDEX ... SET in a migration", index.Name)
	case index.Comment != "":
		return fmt.Errorf("index %q has a comment, which a key does not keep; comment on the constraint instead", index.Name)
	default:
		return nil
	}
}
