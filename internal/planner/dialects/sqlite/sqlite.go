// Package sqlite plans schema migrations for SQLite.
package sqlite

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

const DialectName = platform.SQLite

type Planner struct{}

func New() *Planner {
	return &Planner{}
}

func (p *Planner) GenerateMigrationAST(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	nodes, _ := p.GenerateMigrationASTChecked(diff, generated)
	return nodes
}

func (p *Planner) GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
	if diff == nil {
		return nil, nil
	}
	if generated == nil {
		generated = &goschema.Database{}
	}
	if err := rejectUnsupportedChanges(diff); err != nil {
		return nil, err
	}
	if err := validateAddedColumns(diff, generated); err != nil {
		return nil, err
	}

	var result []ast.Node
	addedTables, err := p.addTables(diff, generated)
	if err != nil {
		return nil, err
	}
	result = append(result, addedTables...)
	result = append(result, p.modifyTables(diff, generated)...)
	result = append(result, p.addViews(diff, generated)...)
	result = append(result, p.modifyViews(diff, generated)...)
	result = append(result, p.addTriggers(diff, generated)...)
	result = append(result, p.modifyTriggers(diff, generated)...)
	result = append(result, p.addIndexes(diff, generated)...)
	result = append(result, p.removeIndexes(diff)...)
	result = append(result, p.removeTriggers(diff)...)
	result = append(result, p.removeViews(diff)...)
	result = append(result, p.removeTables(diff)...)
	return result, nil
}

func rejectUnsupportedChanges(diff *types.SchemaDiff) error {
	if err := rejectUnsupportedTableChanges(diff); err != nil {
		return err
	}
	if err := rejectUnsupportedSchemaObjects(diff); err != nil {
		return err
	}
	if err := rejectUnsupportedAccessControl(diff); err != nil {
		return err
	}
	return nil
}

func rejectUnsupportedTableChanges(diff *types.SchemaDiff) error {
	for _, table := range diff.TablesModified {
		switch {
		case len(table.ColumnsRemoved) > 0:
			return unsupportedFeaturef("dropping columns from table %s requires a table rebuild plan", table.TableName)
		case len(table.ColumnsModified) > 0:
			return unsupportedFeaturef("modifying columns on table %s requires a table rebuild plan", table.TableName)
		case len(table.ConstraintsAdded) > 0 || len(table.ConstraintsRemoved) > 0:
			return unsupportedFeaturef("changing constraints on table %s requires a table rebuild plan", table.TableName)
		}
	}
	if len(diff.ConstraintsAdded) > 0 || len(diff.ConstraintsRemoved) > 0 {
		return unsupportedFeaturef("changing constraints on existing tables requires a table rebuild plan")
	}
	if len(diff.EnumsModified) > 0 || len(diff.EnumsRemoved) > 0 {
		return unsupportedFeaturef("changing enum-backed CHECK constraints requires a table rebuild plan")
	}
	return nil
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
		if !added[table.Name] && !added[table.QualifiedName()] {
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
		return constraint.Table == table.Name || constraint.Table == table.QualifiedName()
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

func (p *Planner) modifyTables(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node
	for _, tableDiff := range diff.TablesModified {
		for _, columnName := range tableDiff.ColumnsAdded {
			if column := findColumn(generated, tableDiff.TableName, columnName); column != nil {
				result = append(result, &ast.AlterTableNode{
					Name:       tableDiff.TableName,
					Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: column}},
				})
			}
		}
	}
	return result
}

func validateAddedColumns(diff *types.SchemaDiff, generated *goschema.Database) error {
	for _, tableDiff := range diff.TablesModified {
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

func (p *Planner) addIndexes(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	tableMap := structToTableMap(generated.Tables)
	var result []ast.Node
	for _, indexName := range diff.IndexesAdded {
		for _, index := range generated.Indexes {
			if index.Name == indexName {
				result = append(result, fromschema.FromIndexWithTableMapping(index, tableMap))
				break
			}
		}
	}
	return result
}

func structToTableMap(tables []goschema.Table) map[string]string {
	out := make(map[string]string, len(tables))
	for _, table := range tables {
		out[table.StructName] = table.Name
	}
	return out
}

func (p *Planner) removeIndexes(diff *types.SchemaDiff) []ast.Node {
	var result []ast.Node
	for _, info := range diff.IndexesRemovedWithTables {
		result = append(result, ast.NewDropIndex(info.Name).SetIfExists())
	}
	if len(result) > 0 {
		return result
	}
	for _, name := range diff.IndexesRemoved {
		result = append(result, ast.NewDropIndex(name).SetIfExists())
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

func (p *Planner) addTriggers(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node
	for _, ref := range diff.TriggersAdded {
		if trigger := findTrigger(generated.Triggers, ref.TableName, ref.TriggerName); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger))
		}
	}
	return result
}

func (p *Planner) modifyTriggers(diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	var result []ast.Node
	for _, triggerDiff := range diff.TriggersModified {
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
