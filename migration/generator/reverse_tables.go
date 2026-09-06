package generator

// Reversing table and column changes, and the lookups that recover a table's
// prior shape from the schema the diff was built against.

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/planner/objectlookup"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
	"ptah.run/migration/schemadiff/difftypes"
)

func generatedTableByStructName(tables []schemamodel.Table, structName string) *schemamodel.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []schemamodel.Table, structName, tableName string) *schemamodel.Table {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return generatedTableByStructName(tables, structName)
	}
	for i := range tables {
		if tables[i].QualifiedName() == tableName {
			return &tables[i]
		}
	}
	ref, ok := tableref.Parse(tableName)
	if !ok || ref.Qualified {
		return nil
	}
	for i := range tables {
		if tables[i].StructName == structName && tables[i].Name == ref.Name {
			return &tables[i]
		}
	}

	var match *schemamodel.Table
	for i := range tables {
		if tables[i].Name != ref.Name {
			continue
		}
		if match != nil {
			return nil
		}
		match = &tables[i]
	}
	return match
}

func canonicalTableMemberKey(
	semantics identifier.Semantics,
	table,
	member string,
) tableMemberKey {
	return tableMemberKey{
		table:  semantics.QualifiedTableIdentityKey(table),
		member: semantics.IndexIdentityKey(member),
	}
}

func tableDiffAddsColumn(tableDiffs []difftypes.TableDiff, table schemamodel.Table, column string) bool {
	for _, tableDiff := range tableDiffs {
		if (tableDiff.TableName == table.Name || tableDiff.TableName == table.QualifiedName() ||
			tableDiff.TableName == table.StructName) && slices.Contains(tableDiff.ColumnsAdded.Names(), column) {
			return true
		}
	}
	return false
}

func generatedTableInSet(table schemamodel.Table, tableNames map[string]struct{}) bool {
	_, byName := tableNames[table.Name]
	_, byQualifiedName := tableNames[table.QualifiedName()]
	return byName || byQualifiedName
}

type tableMemberKey struct {
	table  string
	member string
}

// reverseTableDiffs reverses table modifications for down migrations
func reverseTableDiffs(tableDiffs []difftypes.TableDiff, prior *schemamodel.Database) []difftypes.TableDiff {
	reversed := make([]difftypes.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = difftypes.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    tableDiff.ColumnsRemoved, // Columns to remove become columns to add
			ColumnsRemoved:  tableDiff.ColumnsAdded,   // Columns to add become columns to remove
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified, tableDiff.TableName, prior),
			// The table as the PRE-CHANGE database declared it. A rollback that
			// rebuilds is rebuilding what that database held, and the forward
			// declaration describes the state being rolled back from.
			Desired: priorTableDeclaration(prior, tableDiff.TableName),
			// The three Desired/Current pairs below carry BOTH sides for the
			// reason each of their doc comments gives, which is exactly so a
			// reversal can swap them. None of them was swapped, or carried at
			// all: a migration that changed a table's comment rolled back to
			// "No rollback operations needed" (stokaro/ptah#2418).
			CommentChange:           reverseCommentChange(tableDiff.CommentChange),
			RowTTLChange:            reverseRowTTLChange(tableDiff.RowTTLChange),
			RowDeletionPolicyChange: reverseRowDeletionPolicyChange(tableDiff.RowDeletionPolicyChange),
		}
	}
	return reversed
}

// reverseColumnDiffs reverses column modifications for down migrations
// reverseColumnDiffs turns the forward direction's column modifications into
// the rollback's, giving each the column the PRE-CHANGE database held.
//
// The operand is resolved against prior rather than carried across. A forward
// modification's Desired is the column the change moved TO, and re-rendering
// that on the way back would restore the state being rolled back -- the same
// direction-dependent operand every reversal in this file resolves rather than
// inherits.
//
// A column prior does not describe leaves the operand zero, which the planners
// report rather than render.
func reverseColumnDiffs(
	columnDiffs []difftypes.ColumnDiff,
	tableName string,
	prior *schemamodel.Database,
) []difftypes.ColumnDiff {
	reversed := make([]difftypes.ColumnDiff, len(columnDiffs))
	for i, columnDiff := range columnDiffs {
		// For column changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range columnDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = difftypes.ColumnDiff{
			ColumnName: columnDiff.ColumnName,
			Changes:    reversedChanges,
			Desired:    priorColumn(prior, tableName, columnDiff.ColumnName),
		}
	}
	return reversed
}

// priorTableSchema is the schema the pre-change database declares a table under.
// priorColumn answers with the named column of the named table as the
// pre-change database held it, folded the same way the comparison folds a
// declaration so an embedded column is found under the name it renders with.
// priorTableDeclaration answers with everything the pre-change database
// declared about the named table.
//
// It is the rollback's half of the declaration a modification carries: a
// dialect that rebuilds rather than alters recreates the table, and the one
// it has to recreate is the one that database held.
func priorTableDeclaration(prior *schemamodel.Database, tableName string) difftypes.TableDeclaration {
	if prior == nil {
		return difftypes.TableDeclaration{}
	}
	for _, table := range prior.Tables {
		if table.Name == tableName || table.QualifiedName() == tableName {
			return difftypes.TableDeclarationFor(prior, table)
		}
	}
	return difftypes.TableDeclaration{}
}

func priorColumn(prior *schemamodel.Database, tableName, columnName string) schemamodel.Field {
	if prior == nil {
		return schemamodel.Field{}
	}
	for _, table := range prior.Tables {
		if table.Name != tableName && table.QualifiedName() != tableName {
			continue
		}
		for _, field := range generatedschema.FieldsForTable(prior, table) {
			if field.Name == columnName {
				return field
			}
		}
	}
	return schemamodel.Field{}
}

// tableCreationsFromRemovals turns the forward direction's removals into the
// rollback's creations, giving each the declaration the pre-change database
// held.
//
// A creation carries the columns and the enums CREATE TABLE renders from, and a
// removal carries none of that -- so the bundle is rebuilt here, from the same
// schema the planner used to be handed for the down direction.
//
// A name the pre-change schema does not hold yields a creation with no table.
// That is the honest answer rather than a silent omission: the planner has
// nothing to render, and the entry still names the table so a report can say
// which one.
func tableCreationsFromRemovals(names []string, prior *schemamodel.Database) difftypes.TableChanges {
	if len(names) == 0 {
		return nil
	}
	creations := make(difftypes.TableChanges, 0, len(names))
	for _, name := range names {
		creations = append(creations, priorTableCreation(prior, name))
	}
	return creations
}

// priorTableCreation is the creation bundle for one table the pre-change
// database held.
func priorTableCreation(prior *schemamodel.Database, name string) difftypes.TableCreation {
	creation := difftypes.TableCreation{Name: name}
	if prior == nil {
		return creation
	}
	table := objectlookup.Qualified(prior.Tables, name, identifier.Semantics{})
	if table == nil {
		return creation
	}
	fields := schemamodel.ProcessEmbeddedFields(prior.EmbeddedFields, prior.Fields)
	owned := make([]schemamodel.Field, 0, len(fields))
	for _, field := range fields {
		if field.StructName == table.StructName {
			owned = append(owned, field)
		}
	}
	creation.Table = *table
	creation.Fields = owned
	creation.Enums = schemaprep.EnumsFor(owned, prior.Enums)
	// The constraints that database's table had. A target with no
	// ADD CONSTRAINT renders them inside the CREATE, so a rollback that
	// omitted them would put the table back without them and report success
	// (stokaro/ptah#2315).
	creation.Constraints = difftypes.TableCreationFor(prior, *table, name).Constraints
	// The edges between the tables being put back. Without them
	// TablesAdded.InDependencyOrder() has nothing to order by, so a rollback
	// recreates them in whatever order TablesRemoved held and can put a child
	// before the parent it references (stokaro/ptah#2541).
	//
	// Derived rather than read out of prior.Dependencies for the reason
	// TableCreationFor gives: that map is filled by [schemamodel.Finalize],
	// and this schema has not necessarily been through it.
	creation.DependsOn = deporder.GeneratedTableDependencies(prior)[table.QualifiedName()]
	// SelfReferencingForeignKeys stays unfilled: such a key is already emitted
	// twice on the FORWARD path when it is declared as a table-level
	// constraint, so a copy here is a third (stokaro/ptah#2583).
	// nestedCoverageExempt records that, so it is a decision and not a gap.
	return creation
}
