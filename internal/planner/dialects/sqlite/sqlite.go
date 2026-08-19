// Package sqlite plans schema migrations for SQLite.
package sqlite

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/internal/planner/sqliterebuild"
	"go.5x5.cz/ptah/internal/sqliteforeignkeys"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

const DialectName = platform.SQLite

type Planner struct {
	caps capability.Capabilities
}

func New() *Planner {
	return NewWithCapabilities(capability.SQLite3())
}

// NewWithCapabilities constructs a SQLite planner for a concrete server
// capability set. The set is cloned so later caller mutations cannot change
// planning behavior (stokaro/ptah#916).
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
}

// capabilities answers for a zero-value Planner too: the type is constructed
// directly in a few call sites, and a nil set there would refuse every view
// and trigger rather than plan the dialect default.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps == nil {
		return capability.SQLite3()
	}
	return p.caps
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
	if err := p.rejectObjectsTheTargetDeclines(diff); err != nil {
		return nil, err
	}
	semantics := diff.EffectiveIdentifierSemantics(DialectName)
	rebuilds, err := planTableRebuilds(diff, generated, semantics)
	if err != nil {
		return nil, err
	}

	var result []ast.Node
	addedTables, err := p.addTables(diff, generated, semantics)
	if err != nil {
		return nil, err
	}
	result = append(result, addedTables...)
	modifiedTables, err := p.modifyTables(diff, generated, rebuilds)
	if err != nil {
		return nil, err
	}
	result = append(result, modifiedTables...)
	result = append(result, p.addViews(diff, generated, semantics)...)
	result = append(result, p.modifyViews(diff, generated, semantics)...)
	result = append(result, p.addTriggers(diff, generated, rebuilds, semantics)...)
	result = append(result, p.modifyTriggers(diff, generated, rebuilds, semantics)...)
	addedIndexes, err := p.addIndexes(diff, indexes, rebuilds)
	if err != nil {
		return nil, err
	}
	result = append(result, addedIndexes...)
	result = append(result, p.removeIndexes(diff, rebuilds)...)
	result = append(result, p.removeTriggers(diff)...)
	result = append(result, p.removeViews(diff)...)
	result = append(result, p.removeTables(diff)...)
	return withRebuildForeignKeySession(result, rebuilds), nil
}

// withRebuildForeignKeySession wraps a plan that rebuilds a table in the
// foreign-key pragmas SQLite's own ALTER TABLE procedure prescribes.
//
// A rebuild drops the old table and renames a copy over it. If another table
// references the rebuilt one, the DROP is a foreign-key violation and the
// statement fails outright -- measured on SQLite 3.51: `FOREIGN KEY constraint
// failed`. Disabling enforcement for the duration is what makes the sequence
// legal, and it is why a rebuild with inbound references used to be refused
// here instead of planned.
//
// The pair wraps the whole plan rather than each rebuild, which is also where
// the pinned community binary puts it. One rebuild can reference a table a
// later statement rebuilds in turn, so per-rebuild pairs would re-enable
// enforcement between two halves of the same reshuffle.
//
// Applying this SQL needs more than printing it: PRAGMA foreign_keys is a
// no-op inside a transaction, so ptah's apply path lifts these two statements
// to the connection. See internal/atlasschema.applyStatements. The statements
// stay in the plan because the plan is also a file a person runs, and outside
// a transaction they do exactly what they say.
func withRebuildForeignKeySession(plan []ast.Node, rebuilds tableRebuilds) []ast.Node {
	if len(rebuilds.order) == 0 || len(plan) == 0 {
		return plan
	}
	wrapped := make([]ast.Node, 0, len(plan)+4)
	wrapped = append(wrapped,
		ast.NewComment("Disable foreign-key enforcement for the table rebuild below"),
		ast.NewRawSQL(sqliteforeignkeys.DisableStatement),
	)
	wrapped = append(wrapped, plan...)
	return append(wrapped,
		ast.NewComment("Restore foreign-key enforcement after the table rebuild"),
		ast.NewRawSQL(sqliteforeignkeys.EnableStatement),
	)
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
	// semantics answers "is this reference the table this rebuild covers". A
	// trigger's owning table and a TableDiff's name are two different sources
	// and do not have to spell the schema the same way.
	semantics identifier.Semantics
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
	key := objectlookup.Find(r.order, tableName, r.semantics, identity)
	if key == nil {
		return rebuildTarget{}, false
	}
	target, ok := r.targets[*key]
	return target, ok
}

func (r tableRebuilds) contains(tableName string) bool {
	_, ok := r.target(tableName)
	return ok
}

// identity is the name-of accessor for a slice that already holds names.
func identity(name string) string { return name }

// planTableRebuilds decides which existing tables need a rebuild.
//
// SQLite's ALTER TABLE can only rename a table, rename a column, add a column,
// or drop a column. Every other shape — a column's type, nullability, default
// or generated expression, and any table constraint — has to be rewritten
// through a new table. A constraint change that cannot be attributed to a table
// is still refused, because there is nothing to rebuild.
func planTableRebuilds(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) (tableRebuilds, error) {
	rebuilds := tableRebuilds{targets: make(map[string]rebuildTarget), semantics: semantics}
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
		if !sqliterebuild.NeedsTableRebuild(table) {
			continue
		}
		add(table.TableName, table.ColumnsAdded)
	}

	// A column shape `ALTER TABLE ... ADD COLUMN` cannot express is a reason to
	// rebuild, exactly like a type or nullability change. It reads as a separate
	// case only because the diff calls it an addition; the table that results is
	// the same one CREATE TABLE would have written, and a table already being
	// rebuilt for another reason has always taken such a column without comment.
	// Refusing here instead told the operator a rebuild plan was required by a
	// tool that writes rebuild plans (stokaro/ptah#1707).
	for _, table := range diff.TablesModified {
		for _, columnName := range table.ColumnsAdded {
			column := findColumn(generated, table.TableName, columnName)
			if column == nil || !addedColumnNeedsRebuild(column) {
				continue
			}
			add(table.TableName, table.ColumnsAdded)
			break
		}
	}

	constrained, err := existingTablesWithConstraintChanges(diff, semantics)
	if err != nil {
		return tableRebuilds{}, err
	}
	for _, tableName := range constrained {
		// The columns this table gains in the SAME diff have to travel with it.
		// A table reaches this loop when its constraint change arrived at schema
		// level (ConstraintsAddedWithTables) rather than on the TableDiff, so
		// [sqliterebuild.NeedsTableRebuild] answered false and the loop above
		// skipped it -- even though the diff also adds columns to it.
		//
		// Passing nil here made rebuildCopiedColumns copy the new column out of
		// the old table, where it does not exist. SQLite reads an unknown
		// double-quoted identifier as a STRING LITERAL, so every row received
		// the column's own name instead of NULL and `schema apply` exited 0
		// saying it succeeded (#930).
		//
		// A table the loop above already added arrives here carrying the same
		// names a second time. That is inert, not a bug worth a branch:
		// rebuildCopiedColumns turns addedColumns into a set before using it, so
		// a repeat cannot change the plan, and a guard against it would be a
		// branch no fixture could redden.
		add(tableName, addedColumnsFor(diff, tableName, semantics))
	}
	return rebuilds, nil
}

// existingTablesWithConstraintChanges returns the sorted names of tables that
// keep existing across the diff while gaining or losing a constraint. Adding
// and dropping a table already carries its constraints inline, so those are
// skipped.
// The two `Contains` questions below cross two different sources: a constraint's
// owning table comes from the declaration or from the catalog, while
// TablesAdded/TablesRemoved carry the comparator's spelling. Asking them as raw
// string membership answered "not being created" for a table the diff creates as
// `main.t` while the constraint names it `t`, and the table was then rebuilt as
// well as created. SQLite folds ASCII, so the same split opens on case alone.
func existingTablesWithConstraintChanges(
	diff *types.SchemaDiff,
	semantics identifier.Semantics,
) ([]string, error) {
	tables := make(map[string]bool)
	named := make(map[string]bool, len(diff.ConstraintsAddedWithTables)+len(diff.ConstraintsRemovedWithTables))
	for _, constraint := range diff.ConstraintsAddedWithTables {
		named[constraint.Name] = true
		if !objectlookup.Contains(diff.TablesAdded, constraint.TableName, semantics) {
			tables[constraint.TableName] = true
		}
	}
	for _, constraint := range diff.ConstraintsRemovedWithTables {
		named[constraint.Name] = true
		if !objectlookup.Contains(diff.TablesRemoved, constraint.TableName, semantics) {
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

// rejectObjectsTheTargetDeclines refuses the two object kinds this planner does
// have a code path for when the target's own capability set declines them.
//
// It is separate from rejectUnsupportedChanges because the two answer different
// questions. That one is about SQLite's grammar -- there is no CREATE SEQUENCE
// to emit at any version, so the refusal cannot be lifted by a capability set.
// This one is about the server in front of us, and a set that declines views or
// triggers must not be handed statements creating them (stokaro/ptah#916).
func (p *Planner) rejectObjectsTheTargetDeclines(diff *types.SchemaDiff) error {
	caps := p.capabilities()
	if !caps.Has(capability.Views) && touchesViews(diff) {
		return unsupportedFeaturef("views are not supported by the target capability set")
	}
	if !caps.Has(capability.Triggers) && touchesTriggers(diff) {
		return unsupportedFeaturef("triggers are not supported by the target capability set")
	}
	return nil
}

func touchesViews(diff *types.SchemaDiff) bool {
	return len(diff.ViewsAdded) > 0 || len(diff.ViewsModified) > 0 || len(diff.ViewsRemoved) > 0
}

func touchesTriggers(diff *types.SchemaDiff) bool {
	return len(diff.TriggersAdded) > 0 || len(diff.TriggersModified) > 0 || len(diff.TriggersRemoved) > 0
}

// rejectUnsupportedSchemaObjects refuses the object kinds SQLite has no grammar
// for, naming the objects it refused.
//
// The names are the point. Refusing with "functions are not supported" tells an
// operator that something in their schema is a function, which they knew; it
// does not tell them WHICH function to remove or move, and a schema with forty
// objects is then a search (stokaro/ptah#1628).
func rejectUnsupportedSchemaObjects(diff *types.SchemaDiff) error {
	if names := changedNames(diff.MaterializedViewsAdded, diff.MaterializedViewsRemoved, materializedViewNames(diff)); len(names) > 0 {
		return unsupportedFeaturef("materialized views are not supported: %s", strings.Join(names, ", "))
	}
	if names := changedNames(diff.ExtensionsAdded, diff.ExtensionsRemoved, extensionNames(diff)); len(names) > 0 {
		return unsupportedFeaturef("extensions are not supported: %s", strings.Join(names, ", "))
	}
	if names := changedNames(diff.FunctionsAdded, diff.FunctionsRemoved, functionNames(diff)); len(names) > 0 {
		return unsupportedFeaturef("functions are not supported: %s", strings.Join(names, ", "))
	}
	if names := changedNames(diff.SequencesAdded, diff.SequencesRemoved, sequenceNames(diff)); len(names) > 0 {
		return unsupportedFeaturef("sequences are not supported: %s", strings.Join(names, ", "))
	}
	if names := userDefinedTypeNames(diff); len(names) > 0 {
		return unsupportedFeaturef("user-defined types are not supported: %s", strings.Join(names, ", "))
	}
	return nil
}

// changedNames merges the added, removed and modified name lists of one object
// kind into one sorted, deduplicated list for the refusal to print.
func changedNames(added, removed, modified []string) []string {
	names := slices.Concat(added, removed, modified)
	slices.Sort(names)
	return slices.Compact(names)
}

// rejectUnsupportedAccessControl refuses roles, grants and row-level security,
// naming the objects for the reason rejectUnsupportedSchemaObjects does.
func rejectUnsupportedAccessControl(diff *types.SchemaDiff) error {
	if names := rowLevelSecurityNames(diff); len(names) > 0 {
		return unsupportedFeaturef("row-level security is not supported: %s", strings.Join(names, ", "))
	}
	if names := roleAndGrantNames(diff); len(names) > 0 {
		return unsupportedFeaturef("roles and grants are not supported: %s", strings.Join(names, ", "))
	}
	return nil
}

// rowLevelSecurityNames lists every policy and every table whose row-level
// security the diff changes.
func rowLevelSecurityNames(diff *types.SchemaDiff) []string {
	names := slices.Concat(diff.RLSEnabledTablesAdded, diff.RLSEnabledTablesRemoved)
	for _, policy := range slices.Concat(diff.RLSPoliciesAdded, diff.RLSPoliciesRemoved) {
		names = append(names, policy.PolicyName)
	}
	for _, policy := range diff.RLSPoliciesModified {
		names = append(names, policy.PolicyName)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// roleAndGrantNames lists every role the diff changes and every role a changed
// grant names, which is what an operator has to find in their schema.
func roleAndGrantNames(diff *types.SchemaDiff) []string {
	names := slices.Concat(diff.RolesAdded, diff.RolesRemoved)
	for _, role := range diff.RolesModified {
		names = append(names, role.RoleName)
	}
	for _, grant := range slices.Concat(diff.GrantsAdded, diff.GrantsRemoved) {
		names = append(names, grant.Role)
	}
	for _, grant := range slices.Concat(diff.GrantOptionsAdded, diff.GrantOptionsRevoked) {
		names = append(names, grant.Role)
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func (p *Planner) addTables(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) ([]ast.Node, error) {
	var result []ast.Node
	for _, table := range generated.Tables {
		if !objectlookup.Contains(diff.TablesAdded, table.QualifiedName(), semantics) {
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
			return unsupportedFeaturef(
				"table %s declares EXCLUDE constraint %s: SQLite has no EXCLUDE constraint. "+
					"Express the same rule as a UNIQUE index when it compares whole values for equality, "+
					"or as a CHECK constraint or trigger when it does not",
				table.QualifiedName(), constraint.Name)
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
	table := findTable(generated.Tables, target.tableName, diff.EffectiveIdentifierSemantics(DialectName))
	if table == nil {
		return nil, unsupportedFeaturef(
			"rebuilding table %s requires its desired definition, and the declaration does not contain it. "+
				"Declare the table, or drop it instead of changing it",
			target.tableName)
	}
	tempName, err := availableRebuildTableName(*table, diff, generated)
	if err != nil {
		return nil, err
	}

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
		return nil, unsupportedFeaturef(
			"rebuilding table %s would retain none of its columns, so the rebuilt table has nothing to copy and "+
				"every existing row would be lost. Drop and recreate the table instead, which says the same thing "+
				"and says it out loud",
			table.QualifiedName())
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

// rebuildTableNameAttempts bounds the search below. A schema holding
// __ptah_rebuild_t and every numbered variant up to this many is not a schema
// this planner should keep guessing at, and an unbounded loop over a name it
// controls is a hang rather than a diagnostic.
const rebuildTableNameAttempts = 100

// availableRebuildTableName picks the scratch table the rebuild moves through.
//
// The obvious name is __ptah_rebuild_<table>, and a schema is allowed to
// contain a table by that name -- it is an ordinary identifier, and Ptah does
// not own the namespace. This used to be refused, which asked the operator to
// rename their own table so that a name Ptah chose was free. The collision is
// Ptah's to resolve, so the search continues into __ptah_rebuild_<table>_1 and
// upward until a name nothing declares and nothing is dropping is found
// (stokaro/ptah#1707).
//
// Both sources have to be asked. A name the diff DROPS is unusable even though
// the declaration no longer holds it, because the drop and the rebuild are in
// one plan and their order is not this function's to assume.
func availableRebuildTableName(
	table goschema.Table,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) (string, error) {
	base := "__ptah_rebuild_" + table.Name
	for attempt := range rebuildTableNameAttempts {
		candidate := base
		if attempt > 0 {
			candidate = base + "_" + strconv.Itoa(attempt)
		}
		if tableNameCollides(generated.Tables, table, candidate) {
			continue
		}
		if removedTableNameCollides(diff.TablesRemoved, table, candidate) {
			continue
		}
		return candidate, nil
	}
	return "", unsupportedFeaturef(
		"rebuilding table %s found no free scratch table name: %s and %d numbered variants are all taken. "+
			"Rename or drop one of them in a separate migration first",
		table.QualifiedName(), base, rebuildTableNameAttempts-1)
}

func findTable(tables []goschema.Table, name string, semantics identifier.Semantics) *goschema.Table {
	return objectlookup.Qualified(tables, name, semantics)
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
		"rebuilding table %s cannot add NOT NULL column %s without a default: the rows copied from the old "+
			"table have no value for it, so the new table's NOT NULL is violated as the copy runs. "+
			"Give the column a default, or declare it nullable",
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

func (p *Planner) recreateTableTriggers(table goschema.Table, generated *goschema.Database) ([]ast.Node, error) {
	var nodes []ast.Node
	for _, trigger := range generated.Triggers {
		if trigger.Table == table.QualifiedName() {
			if triggerBodyContainsCreateTrigger(trigger.Body) {
				return nil, unsupportedFeaturef(
					"rebuilding table %s cannot recreate trigger %s: its body is itself a CREATE TRIGGER "+
						"statement, so recreating it would nest one trigger inside another. Declare the body as "+
						"the statements the trigger runs, without the CREATE TRIGGER header",
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

// addedColumnNeedsRebuild reports whether a column can only arrive through a
// rebuild, because `ALTER TABLE ... ADD COLUMN` accepts far less than
// CREATE TABLE does.
//
// Every shape here is one SQLite documents as rejected for ADD COLUMN and
// accepts inside CREATE TABLE, so each is a reason to enter the rebuild path
// rather than a reason to refuse. What the rebuild itself still cannot do is a
// separate question, asked by [validateRebuiltAddedColumn] once the table is
// being rebuilt.
func addedColumnNeedsRebuild(column *ast.ColumnNode) bool {
	switch {
	case column.Primary, column.Unique, column.AutoInc:
		return true
	case !column.Nullable && !hasNonNullLiteralDefault(column.Default):
		return true
	case column.ForeignKey != nil && !hasNullDefault(column.Default):
		return true
	case !isAllowedAddedColumnDefault(column.Default):
		return true
	case strings.EqualFold(strings.TrimSpace(column.GeneratedKind), "STORED"):
		return true
	default:
		return false
	}
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

func (p *Planner) addViews(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	var result []ast.Node
	for _, name := range diff.ViewsAdded {
		if view := findView(generated.Views, name, semantics); view != nil {
			result = append(result, fromschema.FromView(*view))
		}
	}
	return result
}

func (p *Planner) modifyViews(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	var result []ast.Node
	for _, viewDiff := range diff.ViewsModified {
		if view := findView(generated.Views, viewDiff.ViewName, semantics); view != nil {
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

func findView(views []goschema.View, name string, semantics identifier.Semantics) *goschema.View {
	return objectlookup.View(views, name, semantics)
}

func (p *Planner) addTriggers(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	rebuilds tableRebuilds,
	semantics identifier.Semantics,
) []ast.Node {
	var result []ast.Node
	for _, ref := range diff.TriggersAdded {
		// A rebuilt table recreates every desired trigger already; CREATE
		// TRIGGER carries no IF NOT EXISTS here, so a second copy would fail.
		if rebuilds.contains(ref.TableName) {
			continue
		}
		if trigger := findTrigger(generated.Triggers, ref.TableName, ref.TriggerName, semantics); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger))
		}
	}
	return result
}

func (p *Planner) modifyTriggers(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	rebuilds tableRebuilds,
	semantics identifier.Semantics,
) []ast.Node {
	var result []ast.Node
	for _, triggerDiff := range diff.TriggersModified {
		if rebuilds.contains(triggerDiff.TableName) {
			continue
		}
		if trigger := findTrigger(generated.Triggers, triggerDiff.TableName, triggerDiff.TriggerName, semantics); trigger != nil {
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

func findTrigger(
	triggers []goschema.Trigger,
	tableName, triggerName string,
	semantics identifier.Semantics,
) *goschema.Trigger {
	return objectlookup.Trigger(triggers, tableName, triggerName, semantics)
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

// addedColumnsFor returns the columns tableName gains in this diff, or nil.
//
// planTableRebuilds needs it for a table whose constraint change is recorded at
// schema level: such a table is in TablesModified with its ColumnsAdded, but
// [sqliterebuild.NeedsTableRebuild] does not select it, so the rebuild would
// otherwise be planned without knowing which columns are new.
//
// tableName arrives from a constraint's owning table and the TableDiff names
// come from the comparator, so the two are matched by identity rather than as
// text. A raw `==` here answered "no added columns" whenever the two spelled the
// schema differently, and that answer is the #930 corruption itself: the rebuild
// then copies the new column out of the old table, where SQLite reads the
// unknown double-quoted identifier as a STRING LITERAL and writes the column's
// own name into every row.
func addedColumnsFor(diff *types.SchemaDiff, tableName string, semantics identifier.Semantics) []string {
	table := objectlookup.Find(
		diff.TablesModified,
		tableName,
		semantics,
		func(candidate types.TableDiff) string { return candidate.TableName },
	)
	if table == nil {
		return nil
	}
	return table.ColumnsAdded
}

// The four helpers below turn one object kind's "modified" list into names.
// They exist because each modified-diff type spells its name field
// differently -- ViewName, Name, FunctionName, SequenceName -- so one generic
// helper would take a closure per call site and read worse than four lines.

func materializedViewNames(diff *types.SchemaDiff) []string {
	names := make([]string, 0, len(diff.MaterializedViewsModified))
	for _, view := range diff.MaterializedViewsModified {
		names = append(names, view.ViewName)
	}
	return names
}

func extensionNames(diff *types.SchemaDiff) []string {
	names := make([]string, 0, len(diff.ExtensionsModified))
	for _, extension := range diff.ExtensionsModified {
		names = append(names, extension.Name)
	}
	return names
}

func functionNames(diff *types.SchemaDiff) []string {
	names := make([]string, 0, len(diff.FunctionsModified))
	for _, function := range diff.FunctionsModified {
		names = append(names, function.FunctionName)
	}
	return names
}

func sequenceNames(diff *types.SchemaDiff) []string {
	names := make([]string, 0, len(diff.SequencesModified))
	for _, sequence := range diff.SequencesModified {
		names = append(names, sequence.SequenceName)
	}
	return names
}

// userDefinedTypeNames lists every domain, composite type and range the diff
// touches. hasUserDefinedTypeChanges answered the same question as a bool; this
// answers it with the names the refusal prints.
func userDefinedTypeNames(diff *types.SchemaDiff) []string {
	names := slices.Concat(
		diff.DomainsAdded, diff.DomainsRemoved,
		diff.CompositeTypesAdded, diff.CompositeTypesRemoved,
		diff.RangesAdded, diff.RangesRemoved,
	)
	for _, domain := range diff.DomainsModified {
		names = append(names, domain.DomainName)
	}
	for _, composite := range diff.CompositeTypesModified {
		names = append(names, composite.TypeName)
	}
	for _, rangeType := range diff.RangesModified {
		names = append(names, rangeType.RangeName)
	}
	slices.Sort(names)
	return slices.Compact(names)
}
