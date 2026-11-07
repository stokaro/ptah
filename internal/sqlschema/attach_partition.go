package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// attachedIndexes follows what `ALTER INDEX parent ATTACH PARTITION copy`
// statements attached, and which parent indexes `CREATE INDEX ... ON ONLY`
// created without copies, so the end of a document can check that every
// partition of such a table received one. pg_dump writes both for every index
// of a partitioned table.
type attachedIndexes struct {
	// only are the indexes created ON ONLY, keyed by their qualified table.
	only []onlyIndex
	// copies holds, by parent index, the partitions whose copy was attached.
	copies map[string][]string
}

// onlyIndex is one index `CREATE INDEX ... ON ONLY table` created.
type onlyIndex struct {
	key, written, table string
}

// indexKey identifies an index by its schema and name, as a statement reaches it.
func indexKey(table *schemamodel.Table, name string) string {
	return table.Schema + "." + name
}

// noteOnlyIndex records an index created ON ONLY its table.
func (d *Document) noteOnlyIndex(index schemamodel.Index, node *ast.IndexNode, databases []*schemamodel.Database) {
	for _, holder := range databases {
		for i := range holder.Tables {
			if holder.Tables[i].QualifiedName() == index.TableName {
				d.attached.only = append(d.attached.only, onlyIndex{key: indexKey(&holder.Tables[i], index.Name), written: node.Name, table: index.TableName})
				return
			}
		}
	}
}

// attachIndexPartition reads `ALTER INDEX parent ATTACH PARTITION copy`. The copy
// is the index PostgreSQL gives a partition for an index of its parent, which a
// declared partition takes from its parent as it takes its columns; so the copy
// leaves the declarations, and the parent's index stays. The copy has to be an
// index of a partition of the parent index's table, as the server requires.
func attachIndexPartition(database *schemamodel.Database, document *Document, node *ast.AlterIndexNode, sourcePlatform string) error {
	databases := declaringDatabases(database, document.base)
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
	parent, found := findNamedIndex(databases, document, schema, name, sourcePlatform)
	if !found {
		return fmt.Errorf("ALTER INDEX %s names an index this schema does not declare", node.Name)
	}
	copySchema, copyName := normalizeSQLTableIdentifier(sourcePlatform, node.AttachPartition)
	attached, found := findNamedIndex(databases, document, copySchema, copyName, sourcePlatform)
	if !found {
		return fmt.Errorf("ALTER INDEX %s ATTACH PARTITION %s names an index this schema does not declare", node.Name, node.AttachPartition)
	}
	tables := declaredTables(databases)
	parentTable, partition := parent.target.table, attached.target.table
	if of, isPartition := schemamodel.FacetParentTable(tables, *partition); !isPartition || of != parentTable.QualifiedName() {
		return fmt.Errorf("ALTER INDEX %s ATTACH PARTITION %s: table %s is not a partition of %s",
			node.Name, node.AttachPartition, partition.QualifiedName(), parentTable.QualifiedName())
	}
	if err := attached.forget(); err != nil {
		return fmt.Errorf("ALTER INDEX %s ATTACH PARTITION %s: %w", node.Name, node.AttachPartition, err)
	}
	if document.attached.copies == nil {
		document.attached.copies = make(map[string][]string)
	}
	key := indexKey(parentTable, name)
	document.attached.copies[key] = append(document.attached.copies[key], partition.QualifiedName())
	return nil
}

// forget removes the index from the declarations: a declared index, the
// primary key or UNIQUE constraint it backs, or a column's own UNIQUE.
func (n namedIndex) forget() error {
	switch {
	case n.index != nil:
		n.owner.Indexes = slices.Delete(n.owner.Indexes, n.position, n.position+1)
	case n.primaryKey:
		n.target.clearPrimaryKey()
	case n.constraint != nil:
		position := slices.IndexFunc(n.owner.Constraints, func(constraint schemamodel.Constraint) bool {
			return constraint.StructName == n.constraint.StructName && constraint.Name == n.constraint.Name
		})
		n.owner.Constraints = slices.Delete(n.owner.Constraints, position, position+1)
	case n.column != nil:
		n.column.Unique = false
	default:
		return fmt.Errorf("the index backs a constraint of a feature owner, which a partition does not copy")
	}
	return nil
}

// declaredTables lists the tables of every model a statement resolves in.
func declaredTables(databases []*schemamodel.Database) []schemamodel.Table {
	var tables []schemamodel.Table
	for _, holder := range databases {
		tables = append(tables, holder.Tables...)
	}
	return tables
}

// settleAttachedPartitions finishes what `ALTER TABLE ... ATTACH PARTITION`
// declared once a document's statements are read. A partition takes its
// columns from its parent, so the columns its own CREATE TABLE wrote leave the
// declarations, which a declaration with PARTITION OF never states. They have
// to be the parent's, as the server requires to attach a table: a column the
// parent lacks, or one of another type, nullability or default, is refused,
// because the declarations could not hold it. Every index created ON ONLY a
// partitioned table has to receive a copy from each of the table's partitions,
// or the server leaves it invalid.
func (d *Document) settleAttachedPartitions(database *schemamodel.Database) error {
	databases := declaringDatabases(database, d.base)
	tables := declaredTables(databases)
	var fields []schemamodel.Field
	for _, holder := range databases {
		fields = append(fields, holder.Fields...)
	}
	for _, holder := range databases {
		settled := make(map[string]bool)
		for _, table := range holder.Tables {
			if _, defined := schemaext.DefiningKind(table.Facets); !defined {
				continue
			}
			own := slices.DeleteFunc(slices.Clone(holder.Fields), func(field schemamodel.Field) bool { return field.StructName != table.StructName })
			if len(own) == 0 {
				continue
			}
			parentName, found := schemamodel.FacetParentTable(tables, table)
			if !found {
				continue
			}
			parent := tables[slices.IndexFunc(tables, func(candidate schemamodel.Table) bool { return candidate.QualifiedName() == parentName })]
			inherited := slices.DeleteFunc(schemamodel.WithParentColumns(tables, fields), func(field schemamodel.Field) bool { return field.StructName != parent.StructName })
			if err := samePartitionColumns(table, parent, own, inherited); err != nil {
				return err
			}
			settled[table.StructName] = true
		}
		holder.Fields = slices.DeleteFunc(holder.Fields, func(field schemamodel.Field) bool { return settled[field.StructName] })
	}
	return d.attached.check(tables)
}

// samePartitionColumns refuses a partition whose own columns are not its
// parent's.
func samePartitionColumns(partition, parent schemamodel.Table, own, inherited []schemamodel.Field) error {
	refuse := func(reason string) error {
		return fmt.Errorf("%w: partition %s of %s %s; Ptah takes a partition's columns from its parent",
			ErrUnmodeledStatement, partition.QualifiedName(), parent.QualifiedName(), reason)
	}
	for _, field := range own {
		position := slices.IndexFunc(inherited, func(candidate schemamodel.Field) bool { return candidate.Name == field.Name })
		if position < 0 {
			return refuse(fmt.Sprintf("declares column %s, which its parent does not", field.Name))
		}
		from := inherited[position]
		switch {
		case !strings.EqualFold(strings.TrimSpace(field.Type), strings.TrimSpace(from.Type)):
			return refuse(fmt.Sprintf("declares column %s as %s, where its parent declares %s", field.Name, field.Type, from.Type))
		case field.Nullable != from.Nullable:
			return refuse(fmt.Sprintf("declares column %s with another nullability than its parent", field.Name))
		case field.Default != from.Default || field.DefaultSet != from.DefaultSet || field.DefaultExpr != from.DefaultExpr:
			return refuse(fmt.Sprintf("declares column %s with another default than its parent", field.Name))
		}
	}
	for _, from := range inherited {
		if !slices.ContainsFunc(own, func(field schemamodel.Field) bool { return field.Name == from.Name }) {
			return refuse(fmt.Sprintf("does not declare column %s of its parent", from.Name))
		}
	}
	return nil
}

// check refuses an index created ON ONLY a table one of whose partitions did
// not attach a copy of it.
func (a attachedIndexes) check(tables []schemamodel.Table) error {
	for _, index := range a.only {
		for _, table := range tables {
			if of, isPartition := schemamodel.FacetParentTable(tables, table); !isPartition || of != index.table {
				continue
			}
			if !slices.Contains(a.copies[index.key], table.QualifiedName()) {
				return fmt.Errorf("%w: CREATE INDEX %s ON ONLY %s: partition %s attaches no copy of the index, so the server leaves it invalid; "+
					"attach one with ALTER INDEX ... ATTACH PARTITION, or create the index without ONLY",
					ErrUnmodeledStatement, index.written, index.table, table.QualifiedName())
			}
		}
	}
	return nil
}
