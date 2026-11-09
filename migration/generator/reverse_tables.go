package generator

// Reversing table and column changes, and the lookups that recover a table's
// prior shape from the schema the diff was built against.

import (
	"regexp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
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
			ColumnsAdded:    restoredColumns(tableDiff.ColumnsRemoved, tableDiff.TableName, prior, semantics),
			ColumnsRemoved:  clonedColumnChanges(tableDiff.ColumnsAdded),
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified, tableDiff.TableName, prior, semantics),
			// The table as the PRE-CHANGE database declared it. A rollback that
			// rebuilds is rebuilding what that database held, and the forward
			// declaration describes the state being rolled back from.
			Desired: priorTableDeclaration(prior, tableDiff.TableName, semantics),
			// The Desired/Current pairs below carry BOTH sides for the
			// reason each of their doc comments gives, which is exactly so a
			// reversal can swap them. None of them was swapped, or carried at
			// all: a migration that changed a table's comment rolled back to
			// "No rollback operations needed" (stokaro/ptah#2418).
			CommentChange: reverseCommentChange(tableDiff.CommentChange),
		}
	}
	return reversed
}

// restoredColumns is the rollback's column additions: the columns the forward
// direction removes, each with the feature settings prior, the pre-change
// declaration, gives it. A removal is the column as the database reported it,
// so its settings are observations, and the planner of an added column reads
// them as a declaration: an owner that keeps the two representations apart
// refuses an observed value in an addition. prior holds the same settings
// converted to their declared form. A column prior does not describe keeps the
// settings the read reported, which such an owner refuses rather than reads.
func restoredColumns(
	removed difftypes.ColumnChanges,
	tableName string,
	prior *schemamodel.Database,
	semantics identifier.Semantics,
) difftypes.ColumnChanges {
	restored := clonedColumnChanges(removed)
	for i := range restored {
		if declared := priorColumn(prior, tableName, restored[i].Name, semantics); declared.Name != "" {
			restored[i].Facets = declared.Facets
		}
	}
	return restored
}

func clonedColumnChanges(changes difftypes.ColumnChanges) difftypes.ColumnChanges {
	result := slices.Clone(changes)
	for i := range result {
		result[i] = result[i].Clone()
	}
	return result
}

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
	semantics identifier.Semantics,
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

		desired := priorColumn(prior, tableName, columnDiff.ColumnName, semantics)
		// A serial column's prior field writes no default, since its type implies
		// one. The forward plan took that default away, and only the change says
		// what it was.
		if restored, found := strings.CutSuffix(columnDiff.Changes["default_expr"], " -> "); found && desired.Default == "" && desired.DefaultExpr == "" {
			desired.DefaultExpr = restored
		}
		reversed[i] = difftypes.ColumnDiff{
			ColumnName:     columnDiff.ColumnName,
			Changes:        reversedChanges,
			Desired:        desired,
			CommentChange:  reverseCommentChange(columnDiff.CommentChange),
			SerialSequence: restoredSerialSequence(columnDiff.Changes["default_expr"]),
			// The rollback alters the same sequence of the same database,
			// whose restart is replayed by its ALTER as by the forward one.
			CurrentSequenceRestart:        columnDiff.CurrentSequenceRestart,
			NotNullConstraintNameChange:   reverseNotNullConstraintName(tableName, columnDiff),
			ReleasedNotNullConstraintName: reverseAddedNotNullName(tableName, columnDiff),
		}
	}
	return reversed
}

// reverseNotNullConstraintName swaps a NOT NULL constraint's name transition, so a
// rollback gives the constraint back the name it held. A transition from no name has
// none to give back, and a target that reported none cannot be asked to hold one.
//
// A column the forward plan made nullable held a name too, and takes it back with the
// NOT NULL the rollback adds. The name the server gives such a NOT NULL needs no
// statement of its own, so the rollback keeps SET NOT NULL for it.
func reverseNotNullConstraintName(table string, columnDiff difftypes.ColumnDiff) *difftypes.NotNullConstraintNameChange {
	if released := columnDiff.ReleasedNotNullConstraintName; released != "" && released != defaultNotNullName(table, columnDiff.ColumnName) {
		return &difftypes.NotNullConstraintNameChange{Desired: released}
	}
	change := columnDiff.NotNullConstraintNameChange
	if change == nil || change.Current == "" {
		return nil
	}
	return &difftypes.NotNullConstraintNameChange{Current: change.Desired, Desired: change.Current}
}

// reverseAddedNotNullName is the name a column the forward plan made NOT NULL holds at
// the start of the rollback, which makes it nullable again, and is empty for any other
// column. It is the name the plan stated, the one the database the plan moves toward
// holds, or else the one the server gave.
func reverseAddedNotNullName(table string, columnDiff difftypes.ColumnDiff) string {
	if columnDiff.Changes["nullable"] != "true -> false" {
		return ""
	}
	if change := columnDiff.NotNullConstraintNameChange; change != nil {
		return change.Desired
	}
	if columnDiff.Desired.NotNullConstraintName != "" {
		return columnDiff.Desired.NotNullConstraintName
	}
	return defaultNotNullName(table, columnDiff.ColumnName)
}

// defaultNotNullName is the name PostgreSQL 18 gives a NOT NULL nobody named on column
// of table, as the table's diff names it. A target that keeps no such name gives this
// name no statement to act on.
func defaultNotNullName(table, column string) string {
	if ref, ok := tableref.Parse(table); ok {
		table = ref.Name
	}
	return pgname.NotNull(table, column)
}

// restoredSerialSequence names the sequence a rollback builds again for a column the
// forward plan took a serial default from, or "" for any other change. The new sequence
// starts at the beginning while the table holds rows, so the planner moves it past them.
func restoredSerialSequence(change string) string {
	matched := restoredSequencePattern.FindStringSubmatch(change)
	if matched == nil {
		return ""
	}
	return matched[1]
}

// restoredSequencePattern reads the bare sequence name out of the default a forward
// change took away.
var restoredSequencePattern = regexp.MustCompile(`^nextval\('(?:"?[^".']+"?\.)?"?([^".']+)"?'(?:::regclass)?\) -> $`)

// priorTable is the table of the pre-change database a forward diff entry
// names, or nil when that database holds no single table under the name.
//
// The entry is matched by identity rather than by name text, the way the
// comparison paired the two sides. The comparison keys `public.items` and
// `items` to one table on PostgreSQL, so a diff names the table as the
// declaration spells it while the database read names it bare; a text match
// finds nothing there, and the rollback loses the table's prior shape.
func priorTable(prior *schemamodel.Database, tableName string, semantics identifier.Semantics) *schemamodel.Table {
	if prior == nil {
		return nil
	}
	return objectlookup.Qualified(prior.Tables, tableName, semantics)
}

// priorTableDeclaration answers with everything the pre-change database
// declared about the named table.
//
// It is the rollback's half of the declaration a modification carries: a
// dialect that rebuilds rather than alters recreates the table, and the one
// it has to recreate is the one that database held.
func priorTableDeclaration(prior *schemamodel.Database, tableName string, semantics identifier.Semantics) schemacapture.TableDeclaration {
	table := priorTable(prior, tableName, semantics)
	if table == nil {
		return schemacapture.TableDeclaration{}
	}
	return schemacapture.DeclareTable(prior, *table, semantics)
}

// priorColumn answers with the named column of the named table as the
// pre-change database held it, folded the same way the comparison folds a
// declaration so an embedded column is found under the name it renders with.
// The column is matched by the dialect's column identity, as the comparison
// matched it, with an exact spelling preferred.
func priorColumn(prior *schemamodel.Database, tableName, columnName string, semantics identifier.Semantics) schemamodel.Field {
	table := priorTable(prior, tableName, semantics)
	if table == nil {
		return schemamodel.Field{}
	}
	fields := generatedschema.FieldsForTable(prior, *table)
	if i := slices.IndexFunc(fields, func(field schemamodel.Field) bool { return field.Name == columnName }); i >= 0 {
		return fields[i].Clone()
	}
	key := semantics.ColumnIdentityKey(columnName)
	if i := slices.IndexFunc(fields, func(field schemamodel.Field) bool {
		return semantics.ColumnIdentityKey(field.Name) == key
	}); i >= 0 {
		return fields[i].Clone()
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

// tableGrantsFromRemovals is the privileges the rollback gives back to the
// tables it creates again, as the pre-change database held them, column
// privileges included.
//
// The forward direction revokes only the privileges a comparison manages,
// which are those of a role the schema declares or grants to. Dropping the
// table takes the others with it, and the table the rollback creates holds
// none of them. The privileges the forward direction revokes are left out here:
// the rollback grants those back already. So is a privilege its grantor holds
// on the table, which is the owner's, and which the owner of the new table
// holds without a grant. A read that reported no grants restores none.
func tableGrantsFromRemovals(
	names []string,
	current *catalog.Database,
	revoked []difftypes.GrantRef,
	semantics identifier.Semantics,
) []difftypes.GrantRef {
	if len(names) == 0 || current == nil {
		return nil
	}
	var granted []difftypes.GrantRef
	for _, grant := range current.Grants {
		if !strings.EqualFold(strings.TrimSpace(grant.ObjectType), "TABLE") || grant.Implicit || grant.IsPartialRevoke ||
			strings.EqualFold(strings.TrimSpace(grant.Role), strings.TrimSpace(grant.GrantedBy)) ||
			!objectlookup.Contains(names, grant.QualifiedTarget(), semantics) {
			continue
		}
		ref := difftypes.GrantRef{
			Role:       strings.TrimSpace(grant.Role),
			GrantedBy:  strings.TrimSpace(grant.GrantedBy),
			Privilege:  strings.ToUpper(strings.TrimSpace(grant.Privilege)),
			ObjectType: "TABLE",
			ObjectName: grant.QualifiedTarget(),
			Column:     strings.TrimSpace(grant.Column),
			WithOption: grant.WithOption,
		}
		if !slices.ContainsFunc(revoked, func(planned difftypes.GrantRef) bool { return sameTableGrant(planned, ref, semantics) }) {
			granted = append(granted, ref)
		}
	}
	return granted
}

// sameTableGrant reports whether two table privileges are one: the same role,
// privilege and column on the same table.
func sameTableGrant(a, b difftypes.GrantRef, semantics identifier.Semantics) bool {
	return strings.EqualFold(a.ObjectType, b.ObjectType) && a.Role == b.Role && a.Privilege == b.Privilege &&
		a.Column == b.Column && objectlookup.Same(a.ObjectName, b.ObjectName, semantics)
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
