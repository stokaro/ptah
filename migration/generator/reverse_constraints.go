package generator

// Reversing constraints and indexes. Foreign keys carry the most: a removal
// has to be reconstructed with the columns it referenced, which no name holds.

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/schemaprep"
	"ptah.run/migration/schemadiff/difftypes"
)

// reverseIndexRemovals splits the up direction's index removals by what the
// object each one names actually is, because the down direction has to put that
// object back and only one of the two spellings does.
//
// An ordinary index removal reverses into an index addition, which the down
// path resolves from the introspected schema. A removal the comparator marked
// as constraint-backed (ConstraintBackedIndexRemovals) does not: the object is
// a UNIQUE constraint whose index carries the constraint's name, the up
// direction dropped it with ALTER TABLE ... DROP CONSTRAINT, and the statement
// that restores it is ALTER TABLE ... ADD CONSTRAINT ... UNIQUE. Reversing it
// into an index addition is wrong twice over: the down path builds its target
// from ConvertDBSchemaToGoSchema, which deliberately omits a constraint-backed
// index (it is the constraint's, not an index of its own), so the addition has
// no definition to resolve and down generation fails outright with
// `added index users.uq_users_email at position 0 is missing or ambiguous in
// the target schema` -- and where it did resolve it would rebuild a plain
// unique index in place of the constraint, leaving the rollback's catalog
// different from the one the migration started against.
//
// The prior body comes from the introspected constraint, the same source
// reverseConstraintAdditions restores a removed UNIQUE constraint from, so a
// covering INCLUDE list and NULLS [NOT] DISTINCT survive the round trip.
//
// A marked removal with no introspected constraint to rebuild from -- a nil
// dbSchema, or a hand-built diff -- stays an index addition rather than
// disappearing. That is the loud failure above, which is the right outcome: a
// down migration that silently omits the uniqueness protection it is supposed
// to restore is worse than one that refuses to be generated.
func reverseIndexRemovals(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
) (additions []difftypes.IndexRef, restored []difftypes.ConstraintAdditionInfo) {
	removals := diff.IndexRemovals()
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	if len(constraintBacked) == 0 {
		return removals, nil
	}
	uniqueConstraints := introspectedUniqueConstraintsByHost(dbSchema)
	for _, ref := range removals {
		if _, ownedByConstraint := constraintBacked[ref]; !ownedByConstraint {
			additions = append(additions, ref)
			continue
		}
		dbConstraint, hasBody := uniqueConstraints[tableMemberKey{table: ref.TableName, member: ref.Name}]
		columns := dbConstraint.ColumnNamesOrDefault()
		if !hasBody || len(columns) == 0 {
			additions = append(additions, ref)
			continue
		}
		restored = append(restored, difftypes.ConstraintAdditionInfo{
			Name:           ref.Name,
			TableName:      ref.TableName,
			Type:           "UNIQUE",
			Columns:        slices.Clone(columns),
			IncludeColumns: slices.Clone(dbConstraint.IncludeColumns),
			NullsDistinct:  cloneBoolPtr(dbConstraint.NullsDistinct),
		})
	}
	return additions, restored
}

// introspectedUniqueConstraintsByHost keys the pre-change UNIQUE constraints by
// the host table and name an IndexRef names, which is the identity the
// comparator marked the removal under.
func introspectedUniqueConstraintsByHost(
	dbSchema *catalog.Database,
) map[tableMemberKey]catalog.Constraint {
	constraints := make(map[tableMemberKey]catalog.Constraint)
	if dbSchema == nil {
		return constraints
	}
	for _, constraint := range dbSchema.Constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		constraints[tableMemberKey{table: constraint.QualifiedTableName(), member: constraint.Name}] = constraint
	}
	return constraints
}

// reverseConstraintAdditions builds the table-qualified additions for the down
// migration. In the down direction the constraints to add back are the ones the
// up migration REMOVED (diff.ConstraintsRemoved) — restoring their
// prior definition. The prior body is read from the introspected (pre-change)
// database schema, which is the authoritative source for what the down must
// restore.
//
// Carrying the full per-host body here lets both dialect planners' add-paths
// (which already prefer ConstraintsAdded) emit one correct ALTER TABLE
// per real host table. This is what makes the down of a multi-host mixin FK
// modify apply cleanly: a name-only down re-adds only one host (and drops only
// one host), so the others collide on re-add (issue #197 DOWN path). When
// dbSchema is nil, the names still flow through ConstraintsAdded and the
// planners fall back to the name-only field scan.
func reverseConstraintAdditions(
	diff *difftypes.SchemaDiff,
	dbSchema *catalog.Database,
	semantics identifier.Semantics,
) difftypes.ConstraintAdditions {
	if dbSchema == nil || len(diff.ConstraintsRemoved) == 0 {
		return nil
	}

	// Index the introspected constraints by (table, name) so each reversed
	// addition restores the body from the exact host it was removed from. A
	// mixin-shared FK name legitimately repeats across host tables, so a
	// name-only key would collapse them onto one host.
	dbConstraintByTableName := make(map[tableMemberKey]catalog.Constraint)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" && c.Type != "PRIMARY KEY" && c.Type != "CHECK" && c.Type != "UNIQUE" {
			continue
		}
		dbConstraintByTableName[tableMemberKey{table: c.QualifiedTableName(), member: c.Name}] = c
	}

	var infos difftypes.ConstraintAdditions
	for _, removed := range diff.ConstraintsRemoved {
		if removed.TableName == "" {
			continue
		}
		dbConstraint, ok := dbConstraintByTableName[tableMemberKey{table: removed.TableName, member: removed.Name}]
		if !ok {
			// No introspected body to restore (e.g. the constraint was a
			// pure-removal not present pre-change, or a type this helper does not
			// reconstruct). The name still rides in ConstraintsAdded for the
			// name-only fallback.
			continue
		}
		switch removed.Type {
		case "FOREIGN KEY":
			infos = append(infos, foreignKeyAdditionFromDBConstraint(removed.Name, removed.TableName, dbConstraint, semantics))
		case "PRIMARY KEY":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:      removed.Name,
					TableName: removed.TableName,
					Identity:  constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:      "PRIMARY KEY",
					Columns:   append([]string(nil), columns...),
				})
			}
		case "CHECK":
			if dbConstraint.CheckClause != nil && *dbConstraint.CheckClause != "" {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:            removed.Name,
					TableName:       removed.TableName,
					Identity:        constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:            "CHECK",
					CheckExpression: *dbConstraint.CheckClause,
				})
			}
		case "UNIQUE":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, difftypes.ConstraintAdditionInfo{
					Name:           removed.Name,
					TableName:      removed.TableName,
					Identity:       constraintscope.Identity(semantics, removed.TableName, removed.Name),
					Type:           "UNIQUE",
					Columns:        append([]string(nil), columns...),
					IncludeColumns: append([]string(nil), dbConstraint.IncludeColumns...),
					NullsDistinct:  cloneBoolPtr(dbConstraint.NullsDistinct),
				})
			}
		}
	}
	return infos
}

// foreignKeyAdditionFromDBConstraint builds a ConstraintAdditionInfo carrying the
// full FK body from an introspected database FOREIGN KEY constraint. The
// referential actions come straight from the pre-change DB, so the down
// migration restores exactly the prior ON DELETE / ON UPDATE behavior.
func foreignKeyAdditionFromDBConstraint(
	name, table string,
	dbFK catalog.Constraint,
	semantics identifier.Semantics,
) difftypes.ConstraintAdditionInfo {
	info := difftypes.ConstraintAdditionInfo{
		Name:      name,
		TableName: table,
		Identity:  constraintscope.Identity(semantics, table, name),
		Type:      "FOREIGN KEY",
		OnDelete:  derefString(dbFK.DeleteRule),
		OnUpdate:  derefString(dbFK.UpdateRule),
	}
	if columns := dbFK.ColumnNamesOrDefault(); len(columns) > 0 {
		info.Columns = uniqueStringsPreserveOrder(columns)
	}
	if dbFK.ForeignTable != nil {
		info.ForeignTable = *dbFK.ForeignTable
	}
	if foreignColumns := dbFK.ForeignColumnsOrDefault(); len(foreignColumns) > 0 {
		foreignColumns = uniqueStringsPreserveOrder(foreignColumns)
		info.ForeignColumn = foreignColumns[0]
		info.ForeignColumns = foreignColumns
	}
	return info
}

// reverseConstraintRemovals builds the table-qualified removal info for the
// down migration. In the down direction the constraints to remove are the ones
// the up migration ADDED (diff.ConstraintsAdded); their owning table and type
// are resolved from the generated schema, which is the source the up side
// synthesized them from. This lets dialect planners that need the table and a
// type-specific drop syntax (MySQL/MariaDB DROP FOREIGN KEY) emit a real drop in
// the down migration. When the schema is unavailable, the names still flow
// through ConstraintsRemoved; only the richer per-table info is omitted.
func reverseConstraintRemovals(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	semantics identifier.Semantics,
) difftypes.ConstraintRemovals {
	if schema == nil {
		return nil
	}

	// Index explicit table-level constraints by name.
	tableConstraints := make(map[string]schemamodel.Constraint, len(schema.Constraints))
	for _, c := range schema.Constraints {
		tableConstraints[c.Name] = c
	}

	// Prefer the table-qualified additions the comparator recorded. A
	// field-level FK contributed by an embedded inline-relation mixin shares one
	// name across every host table, so resolving the table from a field's Go
	// struct name collapses every host onto the same (often non-table) name —
	// the down migration would then drop the constraint from the wrong table or
	// from a struct name that does not exist (issue #197). ConstraintsAdded
	// carries the concrete table for each addition, so the down side drops the
	// FK from exactly the table the up side added it to. Names present here are
	// recorded so the field-scan fallback below does not double-emit them.
	var infos difftypes.ConstraintRemovals
	seen := make(map[tableMemberKey]struct{})
	handled := make(map[string]struct{})
	for _, add := range diff.ConstraintsAdded {
		if add.TableName == "" {
			continue
		}
		infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
			Name: add.Name, TableName: add.TableName, Type: add.Type,
			// Carried, not re-derived: this record IS the forward addition,
			// turned around. Deriving it again would be a second answer to a
			// question the comparator already answered.
			Identity: add.Identity,
		})
		handled[add.Name] = struct{}{}
	}
	infos = appendAddedTableForeignKeyRemovals(infos, seen, diff.TablesAdded.Names(), schema)
	infos = appendAddedColumnForeignKeyRemovals(infos, seen, diff.TablesModified, schema)

	// Index field-level constraint names to their owning table for the names
	// that did not arrive with table-qualified info.
	structToTable := make(map[string]string, len(schema.Tables))
	for _, t := range schema.Tables {
		structToTable[t.StructName] = t.Name
	}
	fkTables := make(map[string]string, len(schema.Fields))
	checkTables := make(map[string]string, len(schema.Fields))
	for _, f := range schema.Fields {
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}

		if f.Foreign != "" {
			name := f.ForeignKeyName
			if name == "" {
				name = schemaprep.GenerateForeignKeyName(tableName, f.Name)
			}
			fkTables[name] = tableName
		}

		if f.Check != "" {
			name := f.CheckName
			if name == "" {
				name = tableName + "_" + f.Name + "_check"
			}
			checkTables[name] = tableName
		}
	}

	for _, name := range diff.ConstraintsAdded.Names() {
		if _, done := handled[name]; done {
			continue
		}
		switch {
		case tableConstraints[name].Name != "":
			c := tableConstraints[name]
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{Name: name, TableName: c.Table, Type: c.Type, Identity: constraintscope.Identity(semantics, c.Table, name)})
		case fkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
				Name: name, TableName: fkTables[name], Type: "FOREIGN KEY",
			})
		case checkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{Name: name, TableName: checkTables[name], Type: "CHECK", Identity: constraintscope.Identity(semantics, checkTables[name], name)})
		}
	}
	return infos
}

func reverseForeignKeyRemovals(
	diff *difftypes.SchemaDiff,
	schema *schemamodel.Database,
	dialect string,
) []difftypes.ForeignKeyRemovalInfo {
	if diff == nil || schema == nil {
		return nil
	}
	collector := newReverseForeignKeyRemovalCollector(diff, dialect)
	collector.addQualifiedAdditions(diff.ConstraintsAdded)
	collector.addFieldForeignKeys(schema)
	collector.addTableForeignKeys(schema)
	return collector.result()
}

type reverseForeignKeyRemovalCollector struct {
	diff        *difftypes.SchemaDiff
	semantics   identifier.Semantics
	removals    map[tableMemberKey]difftypes.ForeignKeyRemovalInfo
	addedNames  map[string]struct{}
	addedHosts  map[tableMemberKey]struct{}
	addedTables map[string]struct{}
}

func newReverseForeignKeyRemovalCollector(
	diff *difftypes.SchemaDiff,
	dialect string,
) *reverseForeignKeyRemovalCollector {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	collector := &reverseForeignKeyRemovalCollector{
		diff:        diff,
		semantics:   semantics,
		removals:    make(map[tableMemberKey]difftypes.ForeignKeyRemovalInfo),
		addedNames:  make(map[string]struct{}, len(diff.ConstraintsAdded)),
		addedHosts:  make(map[tableMemberKey]struct{}, len(diff.ConstraintsAdded)),
		addedTables: stringSet(diff.TablesAdded.Names()),
	}
	for _, name := range diff.ConstraintsAdded.Names() {
		collector.addedNames[semantics.IndexIdentityKey(name)] = struct{}{}
	}
	return collector
}

func (c *reverseForeignKeyRemovalCollector) addQualifiedAdditions(
	constraints difftypes.ConstraintAdditions,
) {
	for _, constraint := range constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		c.addedHosts[canonicalTableMemberKey(c.semantics, constraint.TableName, constraint.Name)] = struct{}{}
		foreignColumns := slices.Clone(constraint.ForeignColumns)
		if len(foreignColumns) == 0 && constraint.ForeignColumn != "" {
			foreignColumns = []string{constraint.ForeignColumn}
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: constraint.Name, TableName: constraint.TableName,
			Columns: slices.Clone(constraint.Columns), ForeignTable: constraint.ForeignTable,
			ForeignColumns: foreignColumns,
			Identity:       constraintscope.Identity(c.semantics, constraint.TableName, constraint.Name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) addFieldForeignKeys(schema *schemamodel.Database) {
	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil {
			continue
		}
		name := field.ForeignKeyName
		if name == "" {
			name = schemaprep.GenerateForeignKeyName(table.Name, field.Name)
		}
		if !c.selectedFieldForeignKey(*table, name, field.Name) {
			continue
		}
		ref := schemaprep.ParseForeignKeyReference(field.Foreign)
		if ref == nil {
			continue
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: name, TableName: table.QualifiedName(), Columns: []string{field.Name},
			ForeignTable: ref.Table, ForeignColumns: slices.Clone(ref.ReferencedColumns()),
			Identity: constraintscope.Identity(c.semantics, table.QualifiedName(), name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) addTableForeignKeys(schema *schemamodel.Database) {
	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil {
			continue
		}
		name := constraint.Name
		if name == "" {
			name = defaultForeignKeyConstraintName(table.Name, constraint.Columns)
		}
		tableName := table.QualifiedName()
		if constraint.Table != "" {
			tableName = constraint.Table
		}
		if !c.selectedTableForeignKey(*table, tableName, name) {
			continue
		}
		c.add(difftypes.ForeignKeyRemovalInfo{
			Name: name, TableName: tableName, Columns: slices.Clone(constraint.Columns),
			ForeignTable:   constraint.ForeignTable,
			ForeignColumns: slices.Clone(constraint.ForeignColumnsOrDefault()),
			Identity:       constraintscope.Identity(c.semantics, tableName, name),
		})
	}
}

func (c *reverseForeignKeyRemovalCollector) selectedFieldForeignKey(
	table schemamodel.Table,
	name,
	column string,
) bool {
	return c.namedOrHostedAddition(table.QualifiedName(), name) ||
		generatedTableInSet(table, c.addedTables) ||
		tableDiffAddsColumn(c.diff.TablesModified, table, column)
}

func (c *reverseForeignKeyRemovalCollector) selectedTableForeignKey(
	table schemamodel.Table,
	tableName,
	name string,
) bool {
	return c.namedOrHostedAddition(tableName, name) || generatedTableInSet(table, c.addedTables)
}

func (c *reverseForeignKeyRemovalCollector) namedOrHostedAddition(table, name string) bool {
	_, named := c.addedNames[c.semantics.IndexIdentityKey(name)]
	_, hosted := c.addedHosts[canonicalTableMemberKey(c.semantics, table, name)]
	return named || hosted
}

func (c *reverseForeignKeyRemovalCollector) add(info difftypes.ForeignKeyRemovalInfo) {
	if !completeForeignKeyRemovalInfo(info) {
		return
	}
	key := canonicalTableMemberKey(c.semantics, info.TableName, info.Name)
	c.removals[key] = info
}

func (c *reverseForeignKeyRemovalCollector) result() []difftypes.ForeignKeyRemovalInfo {
	result := make([]difftypes.ForeignKeyRemovalInfo, 0, len(c.removals))
	for _, removal := range c.removals {
		result = append(result, removal)
	}
	slices.SortFunc(result, func(a, b difftypes.ForeignKeyRemovalInfo) int {
		if order := strings.Compare(a.TableName, b.TableName); order != 0 {
			return order
		}
		return strings.Compare(a.Name, b.Name)
	})
	return result
}

func completeForeignKeyRemovalInfo(info difftypes.ForeignKeyRemovalInfo) bool {
	return info.Name != "" && info.TableName != "" && len(info.Columns) > 0 &&
		info.ForeignTable != "" && len(info.ForeignColumns) > 0
}

func appendAddedColumnForeignKeyRemovals(
	infos difftypes.ConstraintRemovals,
	seen map[tableMemberKey]struct{},
	tableDiffs []difftypes.TableDiff,
	schema *schemamodel.Database,
) difftypes.ConstraintRemovals {
	for _, tableDiff := range tableDiffs {
		if len(tableDiff.ColumnsAdded) == 0 {
			continue
		}
		table := generatedTableReference(schema.Tables, "", tableDiff.TableName)
		if table == nil {
			continue
		}
		for _, field := range schema.Fields {
			if field.StructName != table.StructName ||
				field.Foreign == "" ||
				!slices.Contains(tableDiff.ColumnsAdded.Names(), field.Name) {
				continue
			}
			name := field.ForeignKeyName
			if name == "" {
				name = schemaprep.GenerateForeignKeyName(table.Name, field.Name)
			}
			info := difftypes.ConstraintRemovalInfo{
				Name: name, TableName: table.QualifiedName(), Type: "FOREIGN KEY",
			}
			infos = appendConstraintRemovalInfo(infos, seen, info)
		}
	}
	return infos
}

func appendAddedTableForeignKeyRemovals(
	infos difftypes.ConstraintRemovals,
	seen map[tableMemberKey]struct{},
	tableNames []string,
	schema *schemamodel.Database,
) difftypes.ConstraintRemovals {
	addedTables := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		addedTables[tableName] = struct{}{}
	}
	if len(addedTables) == 0 {
		return infos
	}

	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		name := field.ForeignKeyName
		if name == "" {
			name = schemaprep.GenerateForeignKeyName(table.Name, field.Name)
		}
		info := difftypes.ConstraintRemovalInfo{Name: name, TableName: tableName, Type: "FOREIGN KEY"}
		infos = appendConstraintRemovalInfo(infos, seen, info)
	}

	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		if constraint.Table != "" {
			tableName = constraint.Table
		}
		name := constraint.Name
		if name == "" {
			name = defaultForeignKeyConstraintName(table.Name, constraint.Columns)
		}
		infos = appendConstraintRemovalInfo(infos, seen, difftypes.ConstraintRemovalInfo{
			Name: name, TableName: tableName, Type: "FOREIGN KEY",
		})
	}

	return infos
}

func appendConstraintRemovalInfo(
	infos difftypes.ConstraintRemovals,
	seen map[tableMemberKey]struct{},
	info difftypes.ConstraintRemovalInfo,
) difftypes.ConstraintRemovals {
	if info.Name == "" || info.TableName == "" {
		return infos
	}
	key := tableMemberKey{table: info.TableName, member: info.Name}
	if _, ok := seen[key]; ok {
		return infos
	}
	seen[key] = struct{}{}
	return append(infos, info)
}

func defaultForeignKeyConstraintName(tableName string, columns []string) string {
	columnName := strings.Join(columns, "_")
	if columnName == "" {
		columnName = "foreign_key"
	}
	return schemaprep.GenerateForeignKeyName(tableName, columnName)
}
