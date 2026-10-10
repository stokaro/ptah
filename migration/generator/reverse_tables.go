package generator

// Reversing table and column changes, and the lookups that recover a table's
// prior shape from the schema the diff was built against.

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/planner/objectlookup"
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
func reverseTableDiffs(tableDiffs []difftypes.TableDiff, prior *schemamodel.Database, semantics identifier.Semantics) []difftypes.TableDiff {
	reversed := make([]difftypes.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = difftypes.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    clonedColumnChanges(tableDiff.ColumnsRemoved),
			ColumnsRemoved:  clonedColumnChanges(tableDiff.ColumnsAdded),
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified, tableDiff.TableName, prior),
			// The table as the PRE-CHANGE database declared it. A rollback that
			// rebuilds is rebuilding what that database held, and the forward
			// declaration describes the state being rolled back from.
			Desired: priorTableDeclaration(prior, tableDiff.TableName, semantics),
			// The Desired/Current pairs below carry BOTH sides for the
			// reason each of their doc comments gives, which is exactly so a
			// reversal can swap them. None of them was swapped, or carried at
			// all: a migration that changed a table's comment rolled back to
			// "No rollback operations needed" (stokaro/ptah#2418).
			CommentChange:         reverseCommentChange(tableDiff.CommentChange),
			YDBPartitioningChange: reversePartitioningChange(tableDiff.YDBPartitioningChange),
			YDBColumnTableChange:  reverseColumnTableChange(tableDiff.YDBColumnTableChange),
		}
	}
	return reversed
}

func clonedColumnChanges(changes difftypes.ColumnChanges) difftypes.ColumnChanges {
	result := slices.Clone(changes)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
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
			// The rollback alters the same sequence of the same database,
			// whose restart is replayed by its ALTER as by the forward one.
			CurrentSequenceRestart: columnDiff.CurrentSequenceRestart,
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
func priorTableDeclaration(prior *schemamodel.Database, tableName string, semantics identifier.Semantics) schemacapture.TableDeclaration {
	if prior == nil {
		return schemacapture.TableDeclaration{}
	}
	for _, table := range prior.Tables {
		if table.Name == tableName || table.QualifiedName() == tableName {
			return schemacapture.DeclareTable(prior, table, semantics)
		}
	}
	return schemacapture.TableDeclaration{}
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
				return field.Clone()
			}
		}
	}
	return schemamodel.Field{}
}

// tableCreationsFromRemovals turns the forward direction's removals into the
// rollback's creations, giving each the declaration the pre-change database
// held.
//
// The caller assembles prior from the removal observations and the surrounding
// type vocabulary. Conversion supplies the declaration CREATE TABLE renders;
// it does not look for the removed table in the desired document.
//
// A name the pre-change schema does not hold yields a creation with no table.
// That is the honest answer rather than a silent omission: the planner has
// nothing to render, and the entry still names the table so a report can say
// which one.
func tableCreationsFromRemovals(names []string, prior *schemamodel.Database, semantics identifier.Semantics) difftypes.TableChanges {
	if len(names) == 0 {
		return nil
	}
	creations := make(difftypes.TableChanges, 0, len(names))
	for _, name := range names {
		creations = append(creations, priorTableCreation(prior, name, semantics))
	}
	return creations
}

// reverseTableRemovals orders the reverse removal intents. Their observations
// are projected from the accepted creations at the selected service boundary.
func reverseTableRemovals(creations difftypes.TableChanges) difftypes.TableRemovals {
	tables := make([]schemamodel.Table, 0, len(creations))
	dependencies := make(map[string][]string, len(creations))
	for _, creation := range creations {
		tables = append(tables, creation.Table)
		dependencies[creation.Table.QualifiedName()] = slices.Clone(creation.DependsOn)
	}
	names := deporder.TableDropOrderWithDependencies(creations.Names(), tables, dependencies)
	if len(names) == 0 {
		return nil
	}
	removals := make(difftypes.TableRemovals, len(names))
	for i, name := range names {
		removals[i].Name = name
	}
	return removals
}

// priorTableCreation is the creation bundle for one table the pre-change
// database held.
func priorTableCreation(prior *schemamodel.Database, name string, semantics identifier.Semantics) difftypes.TableCreation {
	creation := difftypes.TableCreation{Name: name}
	if prior == nil {
		return creation
	}
	table := objectlookup.Qualified(prior.Tables, name, identifier.Semantics{})
	if table == nil {
		return creation
	}
	creation = difftypes.TableCreationFor(prior, *table, name, semantics)

	// SelfReferencingForeignKeys stays unfilled: such a key is already emitted
	// twice on the FORWARD path when it is declared as a table-level
	// constraint, so a copy here is a third (stokaro/ptah#2583).
	// nestedCoverageExempt records that, so it is a decision and not a gap.
	creation.SelfReferencingForeignKeys = nil
	return creation
}

func reverseColumnTableChange(change *difftypes.YDBColumnTableChange) *difftypes.YDBColumnTableChange {
	if change == nil {
		return nil
	}
	return &difftypes.YDBColumnTableChange{Desired: change.Current.Clone(), Current: change.Desired.Clone()}
}
