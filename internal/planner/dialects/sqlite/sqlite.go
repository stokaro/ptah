// Package sqlite plans schema migrations for SQLite.
package sqlite

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

const DialectName = platform.SQLite

type Planner struct{}

func New() *Planner {
	return &Planner{}
}

func (p *Planner) GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
	if generated == nil {
		generated = &goschema.Database{}
	}
	indexes, err := indexscope.NewResolverWithSemantics(
		DialectName,
		diff.EffectiveIdentifierSemantics(DialectName),
		diff,
		generated,
	)
	if err != nil {
		return nil, err
	}
	if err := rejectUnsupportedChanges(diff); err != nil {
		return nil, err
	}
	rebuilds, err := planTableRebuilds(diff)
	if err != nil {
		return nil, err
	}
	if err := validateAddedColumns(diff, generated, rebuilds); err != nil {
		return nil, err
	}

	var result []ast.Node
	addedTables, err := p.addTables(diff, generated)
	if err != nil {
		return nil, err
	}
	result = append(result, addedTables...)
	modifiedTables, err := p.modifyTables(diff, generated, rebuilds)
	if err != nil {
		return nil, err
	}
	result = append(result, modifiedTables...)
	result = append(result, p.addViews(diff, generated)...)
	result = append(result, p.modifyViews(diff, generated)...)
	result = append(result, p.addTriggers(diff, generated, rebuilds)...)
	result = append(result, p.modifyTriggers(diff, generated, rebuilds)...)
	addedIndexes, err := p.addIndexes(diff, indexes, rebuilds)
	if err != nil {
		return nil, err
	}
	result = append(result, addedIndexes...)
	result = append(result, p.removeIndexes(diff, rebuilds)...)
	result = append(result, p.removeTriggers(diff)...)
	result = append(result, p.removeViews(diff)...)
	result = append(result, p.removeTables(diff)...)
	return result, nil
}

func rejectUnsupportedChanges(diff *types.SchemaDiff) error {
	if err := rejectUnsupportedSchemaObjects(diff); err != nil {
		return err
	}
	if err := rejectUnsupportedAccessControl(diff); err != nil {
		return err
	}
	return nil
}

// tableRebuilds is the set of existing tables whose diff cannot be expressed
// with SQLite's narrow ALTER TABLE grammar and is therefore executed as a
// create-new / copy-rows / drop-old / rename sequence.
//
// The order field keeps emission deterministic: tables reached through
// [types.SchemaDiff.TablesModified] keep that slice's order, and tables reached
// only through a constraint change follow, sorted by name.
type tableRebuilds struct {
	order   []string
	targets map[string]rebuildTarget
}

// rebuildTarget records what a single table's rebuild has to account for.
type rebuildTarget struct {
	// tableName is the table's qualified name, as the diff spells it.
	tableName string
	// addedColumns are columns that exist only in the desired schema, so the
	// copy step must leave them out of the INSERT ... SELECT.
	addedColumns []string
}

func (r tableRebuilds) target(tableName string) (rebuildTarget, bool) {
	target, ok := r.targets[tableName]
	return target, ok
}

func (r tableRebuilds) contains(tableName string) bool {
	_, ok := r.targets[tableName]
	return ok
}

// planTableRebuilds decides which existing tables need a rebuild.
//
// SQLite's ALTER TABLE can only rename a table, rename a column, add a column,
// or drop a column. Every other shape — a column's type, nullability, default
// or generated expression, and any table constraint — has to be rewritten
// through a new table. A constraint change that cannot be attributed to a table
// is still refused, because there is nothing to rebuild.
func planTableRebuilds(diff *types.SchemaDiff) (tableRebuilds, error) {
	rebuilds := tableRebuilds{targets: map[string]rebuildTarget{}}
	add := func(tableName string, addedColumns []string) {
		target, seen := rebuilds.targets[tableName]
		if !seen {
			target = rebuildTarget{tableName: tableName}
			rebuilds.order = append(rebuilds.order, tableName)
		}
		target.addedColumns = append(target.addedColumns, addedColumns...)
		rebuilds.targets[tableName] = target
	}

	for _, table := range diff.TablesModified {
		if !tableDiffNeedsRebuild(table) {
			continue
		}
		add(table.TableName, table.ColumnsAdded)
	}

	constrained, err := existingTablesWithConstraintChanges(diff)
	if err != nil {
		return tableRebuilds{}, err
	}
	for _, tableName := range constrained {
		add(tableName, nil)
	}
	return rebuilds, nil
}

func tableDiffNeedsRebuild(table types.TableDiff) bool {
	return len(table.ColumnsModified) > 0 ||
		len(table.ColumnsRemoved) > 0 ||
		len(table.ConstraintsAdded) > 0 ||
		len(table.ConstraintsRemoved) > 0
}

// existingTablesWithConstraintChanges returns the sorted names of tables that
// keep existing across the diff while gaining or losing a constraint. Adding
// and dropping a table already carries its constraints inline, so those are
// skipped.
func existingTablesWithConstraintChanges(diff *types.SchemaDiff) ([]string, error) {
	tables := map[string]bool{}
	named := make(map[string]bool, len(diff.ConstraintsAddedWithTables)+len(diff.ConstraintsRemovedWithTables))
	for _, constraint := range diff.ConstraintsAddedWithTables {
		named[constraint.Name] = true
		if !slices.Contains(diff.TablesAdded, constraint.TableName) {
			tables[constraint.TableName] = true
		}
	}
	for _, constraint := range diff.ConstraintsRemovedWithTables {
		named[constraint.Name] = true
		if !slices.Contains(diff.TablesRemoved, constraint.TableName) {
			tables[constraint.TableName] = true
		}
	}
	unattributed := slices.ContainsFunc(diff.ConstraintsAdded, func(name string) bool { return !named[name] }) ||
		slices.ContainsFunc(diff.ConstraintsRemoved, func(name string) bool { return !named[name] })
	if unattributed {
		return nil, unsupportedFeaturef("changing constraints on existing tables requires a table rebuild plan")
	}
	names := slices.Collect(maps.Keys(tables))
	slices.Sort(names)
	return names, nil
}

func rejectUnsupportedSchemaObjects(diff *types.SchemaDiff) error {
	if len(diff.MaterializedViewsAdded) > 0 || len(diff.MaterializedViewsModified) > 0 || len(diff.MaterializedViewsRemoved) > 0 {
		return unsupportedFeaturef("materialized views are not supported")
	}
	if len(diff.ExtensionsAdded) > 0 || len(diff.ExtensionsRemoved) > 0 {
		return unsupportedFeaturef("extensions are not supported")
	}
	if len(diff.FunctionsAdded) > 0 || len(diff.FunctionsModified) > 0 || len(diff.FunctionsRemoved) > 0 {
		return unsupportedFeaturef("functions are not supported")
	}
	if len(diff.SequencesAdded) > 0 || len(diff.SequencesModified) > 0 || len(diff.SequencesRemoved) > 0 {
		return unsupportedFeaturef("sequences are not supported")
	}
	if len(diff.DomainsAdded) > 0 || len(diff.DomainsRemoved) > 0 || len(diff.DomainsModified) > 0 ||
		len(diff.CompositeTypesAdded) > 0 || len(diff.CompositeTypesRemoved) > 0 || len(diff.CompositeTypesModified) > 0 ||
		len(diff.RangesAdded) > 0 || len(diff.RangesRemoved) > 0 {
		return unsupportedFeaturef("user-defined types are not supported")
	}
	return nil
}

func rejectUnsupportedAccessControl(diff *types.SchemaDiff) error {
	if len(diff.RLSPoliciesAdded) > 0 || len(diff.RLSPoliciesModified) > 0 || len(diff.RLSPoliciesRemoved) > 0 ||
		len(diff.RLSEnabledTablesAdded) > 0 || len(diff.RLSEnabledTablesRemoved) > 0 {
		return unsupportedFeaturef("row-level security is not supported")
	}
	if len(diff.RolesAdded) > 0 || len(diff.RolesModified) > 0 || len(diff.RolesRemoved) > 0 ||
		len(diff.GrantsAdded) > 0 || len(diff.GrantsRemoved) > 0 ||
		len(diff.GrantOptionsAdded) > 0 || len(diff.GrantOptionsRevoked) > 0 {
		return unsupportedFeaturef("roles and grants are not supported")
	}
	return nil
}

func (p *Planner) addTables(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
	added := make(map[string]bool, len(diff.TablesAdded))
	for _, name := range diff.TablesAdded {
		added[name] = true
	}

	var result []ast.Node
	for _, table := range generated.Tables {
		if !added[table.QualifiedName()] {
			continue
		}
		node := fromschema.FromTable(table, generated.Fields, generated.Enums, DialectName)
		if err := addInlineConstraints(node, table, generated.Constraints); err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	return result, nil
}

func addInlineConstraints(node *ast.CreateTableNode, table goschema.Table, constraints []goschema.Constraint) error {
	for _, constraint := range constraints {
		if !constraintBelongsToTable(constraint, table) {
			continue
		}
		if strings.EqualFold(constraint.Type, "EXCLUDE") {
			return unsupportedFeaturef("EXCLUDE constraints are not supported")
		}
		if slices.ContainsFunc(node.Constraints, func(existing *ast.ConstraintNode) bool {
			return existing.Name != "" && existing.Name == constraint.Name
		}) {
			continue
		}
		if constraint.Name == "" && constraint.Type == "FOREIGN KEY" {
			constraint = withDefaultForeignKeyName(table.Name, constraint)
		}
		constraintNode := fromschema.FromConstraint(constraint)
		if constraintNode != nil {
			node.AddConstraint(constraintNode)
		}
	}
	return nil
}

func constraintBelongsToTable(constraint goschema.Constraint, table goschema.Table) bool {
	if constraint.Table != "" {
		return constraint.Table == table.QualifiedName()
	}
	return constraint.StructName == table.StructName
}

func withDefaultForeignKeyName(tableName string, constraint goschema.Constraint) goschema.Constraint {
	columnName := "foreign_key"
	if len(constraint.Columns) > 0 {
		columnName = constraint.Columns[0]
	}
	constraint.Name = fromschema.GenerateForeignKeyName(tableName, columnName)
	return constraint
}

func (p *Planner) modifyTables(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	rebuilds tableRebuilds,
) ([]ast.Node, error) {
	var result []ast.Node
	emitted := make(map[string]bool, len(rebuilds.order))
	for _, tableDiff := range diff.TablesModified {
		if target, ok := rebuilds.target(tableDiff.TableName); ok {
			nodes, err := p.rebuildTable(target, diff, generated)
			if err != nil {
				return nil, err
			}
			result = append(result, nodes...)
			emitted[tableDiff.TableName] = true
			continue
		}
		for _, columnName := range tableDiff.ColumnsAdded {
			if column := findColumn(generated, tableDiff.TableName, columnName); column != nil {
				result = append(result, &ast.AlterTableNode{
					Name:       tableDiff.TableName,
					Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
				})
			}
		}
	}
	// Tables reached only through a constraint change never appear in
	// TablesModified, so they are emitted after the column-driven ones.
	for _, tableName := range rebuilds.order {
		if emitted[tableName] {
			continue
		}
		nodes, err := p.rebuildTable(rebuilds.targets[tableName], diff, generated)
		if err != nil {
			return nil, err
		}
		result = append(result, nodes...)
	}
	return result, nil
}

// rebuildTable emits the create-new / copy-rows / drop-old / rename sequence
// that stands in for the ALTER TABLE forms SQLite does not have. The new table
// is rendered from the desired definition, so one rebuild covers column type,
// nullability, default, generated-expression and constraint changes at once,
// as well as dropped and added columns.
func (p *Planner) rebuildTable(
	target rebuildTarget,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) ([]ast.Node, error) {
	table := findTable(generated.Tables, target.tableName)
	if table == nil {
		return nil, unsupportedFeaturef("rebuilding table %s requires the retained table definition", target.tableName)
	}
	if err := validateRebuildTablePreconditions(*table, diff, generated); err != nil {
		return nil, err
	}

	tempName := rebuildTableName(*table)

	createNode := fromschema.FromTable(*table, generated.Fields, generated.Enums, DialectName)
	if err := addInlineConstraints(createNode, *table, generated.Constraints); err != nil {
		return nil, err
	}
	createNode.Name = qualifyLikeTable(*table, tempName)

	columns, err := rebuildCopiedColumns(*table, generated.Fields, target.addedColumns)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, unsupportedFeaturef("rebuilding table %s without retained columns is not supported", table.QualifiedName())
	}

	nodes := []ast.Node{
		ast.NewComment("SQLite table rebuild for changes ALTER TABLE cannot express on " + table.QualifiedName()),
		createNode,
		ast.NewRawSQL("INSERT INTO " + quoteQualifiedIdentifier(createNode.Name) +
			" (" + quoteIdentifierList(copiedColumnNames(columns)) + ") SELECT " + copiedColumnSelectList(columns) +
			" FROM " + quoteQualifiedIdentifier(table.QualifiedName()) + ";"),
		ast.NewDropTable(table.QualifiedName()),
		ast.NewRawSQL("ALTER TABLE " + quoteQualifiedIdentifier(createNode.Name) +
			" RENAME TO " + quoteIdentifier(table.Name) + ";"),
	}
	nodes = append(nodes, p.recreateTableIndexes(*table, generated)...)
	triggers, err := p.recreateTableTriggers(*table, generated)
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, triggers...)
	return nodes, nil
}

func validateRebuildTablePreconditions(table goschema.Table, diff *types.SchemaDiff, generated *goschema.Database) error {
	tempName := rebuildTableName(table)
	if tableNameCollides(generated.Tables, table, tempName) || removedTableNameCollides(diff.TablesRemoved, table, tempName) {
		return unsupportedFeaturef("rebuilding table %s would collide with existing table %s", table.QualifiedName(), tempName)
	}
	if hasInboundForeignKey(table, generated) {
		return unsupportedFeaturef("rebuilding table %s with inbound foreign keys requires a manual rebuild plan", table.QualifiedName())
	}
	return nil
}

func findTable(tables []goschema.Table, name string) *goschema.Table {
	for i := range tables {
		if tables[i].QualifiedName() == name {
			return &tables[i]
		}
	}
	return nil
}

func rebuildTableName(table goschema.Table) string {
	return "__ptah_rebuild_" + table.Name
}

func tableNameCollides(tables []goschema.Table, target goschema.Table, name string) bool {
	for _, table := range tables {
		if table.Schema == target.Schema && table.Name == name {
			return true
		}
	}
	return false
}

func removedTableNameCollides(removed []string, target goschema.Table, name string) bool {
	qualified := qualifyLikeTable(target, name)
	for _, tableName := range removed {
		if tableName == name || tableName == qualified {
			return true
		}
	}
	return false
}

func qualifyLikeTable(table goschema.Table, name string) string {
	return goschema.QualifyTableName(table.Schema, name)
}

// copiedColumn is one column of the rebuilt table that carries data over from
// the old table.
type copiedColumn struct {
	// name is the column name on both the old and the new table.
	name string
	// selectExpr is the expression read from the old table. It is the bare
	// quoted column name unless the desired definition needs a backfill.
	selectExpr string
}

func copiedColumnNames(columns []copiedColumn) []string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.name
	}
	return names
}

func copiedColumnSelectList(columns []copiedColumn) string {
	parts := make([]string, len(columns))
	for i, column := range columns {
		parts[i] = column.selectExpr
	}
	return strings.Join(parts, ", ")
}

// rebuildCopiedColumns lists the columns the rebuild copies over. Columns the
// diff adds exist only in the desired schema, so they are left out of the copy
// and take their declared default in the new table; a NOT NULL addition with no
// default would violate the new table on the very first row, so it is refused
// rather than emitted.
func rebuildCopiedColumns(
	table goschema.Table,
	fields []goschema.Field,
	addedColumns []string,
) ([]copiedColumn, error) {
	added := make(map[string]bool, len(addedColumns))
	for _, name := range addedColumns {
		added[name] = true
	}

	var columns []copiedColumn
	for _, field := range fields {
		if field.StructName != table.StructName {
			continue
		}
		if added[field.Name] {
			if err := validateRebuiltAddedColumn(table, field); err != nil {
				return nil, err
			}
			continue
		}
		columns = append(columns, copiedColumn{
			name:       field.Name,
			selectExpr: rebuildSelectExpression(field),
		})
	}
	return columns, nil
}

// rebuildSelectExpression backfills a column that the desired schema makes NOT
// NULL while giving it a default. Rows already holding NULL would otherwise
// abort the copy, so the default is substituted in flight.
func rebuildSelectExpression(field goschema.Field) string {
	quoted := quoteIdentifier(field.Name)
	if field.Nullable || field.Primary {
		return quoted
	}
	backfill := rebuildBackfillValue(field)
	if backfill == "" {
		return quoted
	}
	return "IFNULL(" + quoted + ", " + backfill + ") AS " + quoted
}

func rebuildBackfillValue(field goschema.Field) string {
	if value := strings.TrimSpace(field.Default); value != "" && !isNullLiteral(value) {
		return value
	}
	if value := strings.TrimSpace(field.DefaultExpr); value != "" && !isNullLiteral(value) {
		return value
	}
	return ""
}

func validateRebuiltAddedColumn(table goschema.Table, field goschema.Field) error {
	if field.Nullable || field.Primary || field.AutoInc {
		return nil
	}
	if strings.TrimSpace(field.GeneratedExpression) != "" {
		return nil
	}
	if rebuildBackfillValue(field) != "" {
		return nil
	}
	return unsupportedFeaturef(
		"rebuilding table %s cannot add NOT NULL column %s without a default",
		table.QualifiedName(),
		field.Name,
	)
}

func (p *Planner) recreateTableIndexes(table goschema.Table, generated *goschema.Database) []ast.Node {
	tableMap := structToTableMap(generated.Tables)
	var nodes []ast.Node
	for _, index := range generated.Indexes {
		tableName := generatedIndexTableName(index, tableMap)
		if tableName == table.QualifiedName() {
			nodes = append(nodes, fromschema.FromIndexWithTableMapping(index, tableMap))
		}
	}
	return nodes
}

func generatedIndexTableName(index goschema.Index, tableMap map[string]string) string {
	if strings.TrimSpace(index.TableName) != "" {
		return index.TableName
	}
	return tableMap[index.StructName]
}

func structToTableMap(tables []goschema.Table) map[string]string {
	out := make(map[string]string, len(tables))
	for _, table := range tables {
		out[table.StructName] = table.QualifiedName()
	}
	return out
}

func hasInboundForeignKey(table goschema.Table, generated *goschema.Database) bool {
	for _, field := range generated.Fields {
		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef != nil && tableMatchesName(table, fkRef.Table) {
			return true
		}
	}
	for _, constraint := range generated.Constraints {
		if strings.EqualFold(constraint.Type, "FOREIGN KEY") && tableMatchesName(table, constraint.ForeignTable) {
			return true
		}
	}
	return false
}

func tableMatchesName(table goschema.Table, name string) bool {
	return name == table.QualifiedName()
}

func (p *Planner) recreateTableTriggers(table goschema.Table, generated *goschema.Database) ([]ast.Node, error) {
	var nodes []ast.Node
	for _, trigger := range generated.Triggers {
		if trigger.Table == table.QualifiedName() {
			if triggerBodyContainsCreateTrigger(trigger.Body) {
				return nil, unsupportedFeaturef(
					"rebuilding table %s with trigger %s requires a manual rebuild plan",
					table.QualifiedName(),
					trigger.Name,
				)
			}
			nodes = append(nodes, fromschema.FromTrigger(trigger))
		}
	}
	return nodes, nil
}

func triggerBodyContainsCreateTrigger(body string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(body)), "CREATE TRIGGER")
}

func quoteIdentifierList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

func quoteQualifiedIdentifier(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return quoteIdentifier(name)
	}
	if !ref.Qualified {
		return quoteIdentifier(ref.Name)
	}
	return quoteIdentifier(ref.Schema) + "." + quoteIdentifier(ref.Name)
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// validateAddedColumns gates the ALTER TABLE ... ADD COLUMN path, whose
// accepted shapes SQLite restricts far below what CREATE TABLE accepts. Tables
// that are being rebuilt go through CREATE TABLE instead, so they are excluded
// here and checked by [validateRebuiltAddedColumn].
func validateAddedColumns(diff *types.SchemaDiff, generated *goschema.Database, rebuilds tableRebuilds) error {
	for _, tableDiff := range diff.TablesModified {
		if rebuilds.contains(tableDiff.TableName) {
			continue
		}
		for _, columnName := range tableDiff.ColumnsAdded {
			column := findColumn(generated, tableDiff.TableName, columnName)
			if column == nil {
				continue
			}
			if err := validateAddedColumn(tableDiff.TableName, column); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAddedColumn(tableName string, column *ast.ColumnNode) error {
	if column.Primary || column.Unique || column.AutoInc {
		return sqliteColumnRebuildError(tableName, column.Name)
	}
	if !column.Nullable && !hasNonNullLiteralDefault(column.Default) {
		return sqliteColumnRebuildError(tableName, column.Name)
	}
	if column.ForeignKey != nil && !hasNullDefault(column.Default) {
		return sqliteColumnRebuildError(tableName, column.Name)
	}
	if !isAllowedAddedColumnDefault(column.Default) {
		return sqliteColumnRebuildError(tableName, column.Name)
	}
	if strings.EqualFold(strings.TrimSpace(column.GeneratedKind), "STORED") {
		return sqliteColumnRebuildError(tableName, column.Name)
	}
	return nil
}

func sqliteColumnRebuildError(tableName, columnName string) error {
	return unsupportedFeaturef("adding column %s to table %s requires a table rebuild plan", columnName, tableName)
}

func hasNonNullLiteralDefault(defaultValue *ast.DefaultValue) bool {
	return defaultValue != nil && defaultValue.HasLiteral() && !isNullLiteral(defaultValue.Value)
}

func hasNullDefault(defaultValue *ast.DefaultValue) bool {
	return defaultValue == nil || defaultValue.HasLiteral() && isNullLiteral(defaultValue.Value)
}

func isAllowedAddedColumnDefault(defaultValue *ast.DefaultValue) bool {
	if defaultValue == nil {
		return true
	}
	if !defaultValue.HasLiteral() {
		return false
	}
	value := strings.TrimSpace(defaultValue.Value)
	if strings.HasPrefix(value, "(") {
		return false
	}
	switch strings.ToUpper(value) {
	case "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
		return false
	default:
		return true
	}
}

func isNullLiteral(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "NULL")
}

func findColumn(generated *goschema.Database, tableName, columnName string) *ast.ColumnNode {
	for _, table := range generated.Tables {
		if table.Name != tableName && table.QualifiedName() != tableName {
			continue
		}
		for _, field := range generated.Fields {
			if field.StructName == table.StructName && field.Name == columnName {
				return fromschema.FromField(field, generated.Enums, DialectName)
			}
		}
	}
	return nil
}

func (p *Planner) addIndexes(
	diff *types.SchemaDiff,
	indexes *indexscope.Resolver,
	rebuilds tableRebuilds,
) ([]ast.Node, error) {
	var result []ast.Node
	indexRemovals := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.SQLite),
		diff.IndexRemovals(),
	)
	for _, ref := range diff.IndexAdditions() {
		// A rebuilt table drops every index with the old table and recreates
		// the desired set from scratch, so repeating them here would emit the
		// same CREATE INDEX twice.
		if rebuilds.contains(ref.TableName) {
			continue
		}
		index, err := indexes.Resolve(ref)
		if err != nil {
			return nil, err
		}
		for removal := range indexRemovals.Matches(ref) {
			result = append(result, ast.NewDropIndex(removal.Name).SetTable(removal.TableName).SetIfExists())
		}
		index.TableName = ref.TableName
		result = append(result, fromschema.FromIndex(index))
	}
	return result, nil
}

func (p *Planner) removeIndexes(diff *types.SchemaDiff, rebuilds tableRebuilds) []ast.Node {
	var result []ast.Node
	indexAdditions := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(platform.SQLite),
		diff.IndexAdditions(),
	)
	for _, ref := range diff.IndexRemovals() {
		if indexAdditions.Contains(ref) {
			continue
		}
		// DROP TABLE already took this index with it.
		if rebuilds.contains(ref.TableName) {
			continue
		}
		result = append(result, ast.NewDropIndex(ref.Name).SetTable(ref.TableName).SetIfExists())
	}
	return result
}

func (p *Planner) removeTables(diff *types.SchemaDiff) []ast.Node {
	result := make([]ast.Node, 0, len(diff.TablesRemoved))
	for _, tableName := range diff.TablesRemoved {
		result = append(result, ast.NewDropTable(tableName).SetIfExists().SetComment("WARNING: This will delete all data!"))
	}
	return result
}

func (p *Planner) addViews(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node
	for _, name := range diff.ViewsAdded {
		if view := findView(generated.Views, name); view != nil {
			result = append(result, fromschema.FromView(*view))
		}
	}
	return result
}

func (p *Planner) modifyViews(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node
	for _, viewDiff := range diff.ViewsModified {
		if view := findView(generated.Views, viewDiff.ViewName); view != nil {
			result = append(result, fromschema.FromView(*view).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeViews(diff *types.SchemaDiff) []ast.Node {
	result := make([]ast.Node, 0, len(diff.ViewsRemoved))
	for _, name := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(name).SetIfExists())
	}
	return result
}

func findView(views []goschema.View, name string) *goschema.View {
	for i := range views {
		if views[i].Name == name {
			return &views[i]
		}
	}
	return nil
}

func (p *Planner) addTriggers(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	rebuilds tableRebuilds,
) []ast.Node {
	var result []ast.Node
	for _, ref := range diff.TriggersAdded {
		// A rebuilt table recreates every desired trigger already; CREATE
		// TRIGGER carries no IF NOT EXISTS here, so a second copy would fail.
		if rebuilds.contains(ref.TableName) {
			continue
		}
		if trigger := findTrigger(generated.Triggers, ref.TableName, ref.TriggerName); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger))
		}
	}
	return result
}

func (p *Planner) modifyTriggers(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	rebuilds tableRebuilds,
) []ast.Node {
	var result []ast.Node
	for _, triggerDiff := range diff.TriggersModified {
		if rebuilds.contains(triggerDiff.TableName) {
			continue
		}
		if trigger := findTrigger(generated.Triggers, triggerDiff.TableName, triggerDiff.TriggerName); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeTriggers(diff *types.SchemaDiff) []ast.Node {
	result := make([]ast.Node, 0, len(diff.TriggersRemoved))
	for _, ref := range diff.TriggersRemoved {
		result = append(result, ast.NewDropTrigger(ref.TriggerName, ref.TableName).SetIfExists())
	}
	return result
}

func findTrigger(triggers []goschema.Trigger, tableName, triggerName string) *goschema.Trigger {
	for i := range triggers {
		if triggers[i].Table == tableName && triggers[i].Name == triggerName {
			return &triggers[i]
		}
	}
	return nil
}

func unsupportedFeaturef(format string, args ...any) error {
	message := fmt.Sprintf("sqlite: "+format, args...)
	return &ptaherr.CapabilityError{
		Dialect: DialectName,
		Feature: message,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: message,
	}
}
