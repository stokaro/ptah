package ydb

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan rebuilds a table.
//
// YDB cannot change a table's key, a column's type or a column's nullability
// toward NOT NULL in place, nor give a table the partitions it starts with.
// When the caller asks for it (planner option AllowTableRebuild, the native
// --allow-table-rebuild flag), the planner makes such a change by recreating
// the table:
//
//  1. CREATE TABLE a scratch table from the declaration, its indexes and its
//     column families inside it, each family with the settings the old table
//     holds and the declaration does not state, and every partitioning setting
//     of the table and its indexes named: the declared ones, and the value the
//     old table or index holds for every other one;
//  2. INSERT INTO the scratch table SELECT the rows of the old one, converting
//     each changed column;
//  3. ALTER TABLE the old table DROP CHANGEFEED, for each changefeed it holds,
//     since YDB moves no table that carries one (`Cannot move table with cdc
//     streams`, measured on 25.1.4.7 and 26.2.1.14);
//  4. ALTER TABLE the old table RENAME TO a second scratch name;
//  5. ALTER TABLE the scratch table RENAME TO the table's name;
//  6. ALTER TABLE the table ADD CHANGEFEED, for each changefeed the
//     declaration names, with the consumers of its topic;
//  7. DROP TABLE the renamed old table.
//
// A changefeed's stream restarts across steps 3 to 6: the records nobody read
// go with the old topic, and each consumer of the new one starts from its
// beginning. The plan says so above its steps.
//
// YDB has no transactional DDL, and YQL has no statement that swaps two tables
// at once, so the steps are not atomic. Each runs as a query of its own and the
// migrator records progress after each, so an interrupted rebuild resumes at
// the step that did not run. The old rows survive until step 7, and the table's
// name is free only between steps 4 and 5.
//
// The copy is one data query, which commits whole or not at all. A failed
// conversion fails it with a message that names the column, and a NULL bound
// for a NOT NULL column fails it the same way: `Unwrap` stops the query where a
// plain CAST would write NULL in silence. YDB refuses a copy that carries more
// than a limit: measured with rows of about 80 bytes, 400000 rows copy on
// 26.2.1.14 and 25.1.4.7, and 600000 rows are refused (`Out of buffer memory.
// Used 74395928 bytes of 67108864 bytes` on 26.2, `Datashard program size
// limit exceeded (56281409 > 50331648)` on 25.1). Nothing is then copied, and
// the old table keeps serving.
//
// Rows written to the old table between the copy and the swap are not in the
// new one, and YDB has no lock that keeps writers out, so the plan says so
// above its steps.

// TableRebuildFlag is the native command-line flag that asks for a rebuild. A
// refusal names it so the operator learns how to ask, unless the caller named
// its own request with [Planner.WithTableRebuildRequest]; the command packages
// declare their flag with this spelling.
const TableRebuildFlag = "--allow-table-rebuild"

// tableRebuild is one table the plan recreates.
type tableRebuild struct {
	// name is the table as the diff spells it.
	name string
	// declaration is the desired table, which the new one is written from.
	declaration difftypes.TableDeclaration
	// tableDiff is the table's modification, or nil when the change is its
	// key alone, which the diff carries as a constraint change.
	tableDiff *difftypes.TableDiff
	// scratch is the name the new table is created under, and replaced the
	// name the old one is moved to before it is dropped.
	scratch, replaced string
	// held is what the old table holds of its partitioning, read replicas and
	// key bloom filter, and heldIndexes the partitioning each of its indexes
	// holds, by the name the declaration gives the index. The new table and
	// its indexes take these for every setting the declaration leaves out.
	held        *ast.YDBTablePartitioningSpec
	heldIndexes map[string]*ast.IndexPartitioningSpec
}

// rebuildSubject names the table in a refusal.
func rebuildSubject(name string) string {
	return fmt.Sprintf("rebuilding table %q", name)
}

// planRebuilds decides which tables the plan rebuilds: every table carrying a
// change that needs one, when the caller asked for rebuilds. It returns the
// rebuilds keyed by table identity, or the refusal for a table that cannot be
// rebuilt.
func (p *Planner) planRebuilds(
	diff *difftypes.SchemaDiff,
	removedTables, addedTables map[string]bool,
	semantics identifierSemantics,
) (map[string]*tableRebuild, error) {
	if !p.rebuild {
		return nil, nil
	}
	rebuilds := make(map[string]*tableRebuild)
	var order []string
	want := func(name string, tableDiff *difftypes.TableDiff) {
		key := semantics.TableIdentityKey(name)
		if removedTables[key] || addedTables[key] {
			return
		}
		existing, ok := rebuilds[key]
		if !ok {
			existing = &tableRebuild{name: name}
			rebuilds[key] = existing
			order = append(order, key)
		}
		if tableDiff != nil {
			existing.tableDiff = tableDiff
		}
	}
	for i := range diff.TablesModified {
		if p.needsRebuild(diff.TablesModified[i]) {
			want(diff.TablesModified[i].TableName, &diff.TablesModified[i])
		}
	}
	if !p.caps.Has(capability.PrimaryKeyAlterable) {
		for _, added := range diff.ConstraintsAdded {
			if isPrimaryKey(added.Type) {
				want(added.TableName, nil)
			}
		}
		for _, removed := range diff.ConstraintsRemoved {
			if isPrimaryKey(removed.Type) {
				want(removed.TableName, nil)
			}
		}
	}
	for _, key := range order {
		rebuild := rebuilds[key]
		if rebuild.tableDiff == nil {
			rebuild.tableDiff = modificationOf(diff, key, semantics)
		}
		if err := p.prepareRebuild(diff, rebuild, semantics); err != nil {
			return nil, err
		}
	}
	return rebuilds, nil
}

// identifierSemantics is the part of identifier.Semantics a rebuild reads.
type identifierSemantics interface {
	TableIdentityKey(name string) string
}

// modificationOf is the modification of the table key names, when the diff
// carries one.
func modificationOf(diff *difftypes.SchemaDiff, key string, semantics identifierSemantics) *difftypes.TableDiff {
	for i := range diff.TablesModified {
		if semantics.TableIdentityKey(diff.TablesModified[i].TableName) == key {
			return &diff.TablesModified[i]
		}
	}
	return nil
}

// needsRebuild reports whether a modification carries a change YDB can make
// only by recreating the table on this target.
func (p *Planner) needsRebuild(tableDiff difftypes.TableDiff) bool {
	if partitioningNeedsRebuild(tableDiff.YDBPartitioningChange) {
		return true
	}
	if !p.caps.Has(capability.PrimaryKeyAlterable) &&
		slices.ContainsFunc(tableDiff.ColumnsAdded, func(column schemamodel.Field) bool { return column.Primary }) {
		return true
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		for key, change := range colDiff.Changes {
			switch {
			case key == "type" && !p.caps.Has(capability.AlterColumnType),
				key == "primary_key" && !p.caps.Has(capability.PrimaryKeyAlterable),
				key == "nullable" && strings.HasSuffix(change, "-> false") && !p.caps.Has(capability.AlterColumnSetNotNull):
				return true
			}
		}
	}
	return false
}

// prepareRebuild finds the declaration a rebuild writes the new table from,
// refuses a table the rebuild would damage, and picks the scratch names.
func (p *Planner) prepareRebuild(diff *difftypes.SchemaDiff, rebuild *tableRebuild, semantics identifierSemantics) error {
	subject := rebuildSubject(rebuild.name)
	if !p.caps.Has(capability.RenameTable) {
		return refuseKey(capability.RenameTable, subject+", which swaps the new table into place by renaming it")
	}
	declaration, ok := rebuildDeclaration(diff, rebuild, semantics)
	if !ok {
		return refuseFact(subject, "the plan carries no declaration of the table to write the new one from")
	}
	rebuild.declaration = declaration
	if !declaresKey(declaration) {
		return refuseKey(capability.PrimaryKeyRequired, fmt.Sprintf("table %q declares no primary key", rebuild.name))
	}
	for _, field := range declaration.Fields {
		mapping, err := ydbtype.Map(field.Type, p.caps)
		if (err == nil && mapping.Serial) || field.AutoInc || field.IdentityGeneration != "" {
			return refuseFact(subject, fmt.Sprintf("column %q takes its values from a sequence. The new table's "+
				"sequence would start at 1 while the copied rows keep theirs, so the next insert would collide "+
				"(`Conflict with existing key`), and YDB's ALTER SEQUENCE ... RESTART WITH takes only a literal, "+
				"so no statement can move it past the copied rows", field.Name))
		}
	}
	if err := p.refuseRebuiltFamilies(declaration, subject); err != nil {
		return err
	}
	if settings := undescribedSettings(diff.CurrentNotDescribed, declaration.Table); len(settings) > 0 {
		return refuseFact(subject, fmt.Sprintf("the table carries %s, which Ptah does not model and so cannot "+
			"write on the new table: recreating it would drop them. Change the table by hand, or remove those "+
			"settings first", strings.Join(settings, ", ")))
	}
	scratch, err := freeTableName(diff, declaration.Table, "__ptah_rebuild_")
	if err != nil {
		return err
	}
	replaced, err := freeTableName(diff, declaration.Table, "__ptah_replaced_")
	if err != nil {
		return err
	}
	rebuild.scratch, rebuild.replaced = scratch, replaced
	rebuild.held, rebuild.heldIndexes = heldSettings(diff, rebuild, semantics)
	return nil
}

// heldSettings finds what the old table holds of its settings, and what each
// index the declaration names holds of its partitioning: under the
// declaration's name, or under the name the plan renames it from, since a
// rebuilt table takes its indexes under their new names.
func heldSettings(
	diff *difftypes.SchemaDiff,
	rebuild *tableRebuild,
	semantics identifierSemantics,
) (*ast.YDBTablePartitioningSpec, map[string]*ast.IndexPartitioningSpec) {
	key := semantics.TableIdentityKey(rebuild.name)
	index := slices.IndexFunc(diff.CurrentYDBSettings, func(held difftypes.YDBHeldSettings) bool {
		return semantics.TableIdentityKey(held.TableName) == key
	})
	if index < 0 {
		return nil, nil
	}
	held := diff.CurrentYDBSettings[index]
	formerName := make(map[string]string)
	for _, rename := range diff.IndexesRenamed {
		if semantics.TableIdentityKey(rename.TableName) == key {
			formerName[rename.To] = rename.From
		}
	}
	indexes := make(map[string]*ast.IndexPartitioningSpec)
	for _, declared := range rebuild.declaration.Indexes {
		name := declared.Name
		if former, renamed := formerName[name]; renamed {
			name = former
		}
		if spec, ok := held.Indexes[name]; ok {
			indexes[declared.Name] = spec
		}
	}
	return held.Partitioning, indexes
}

// rebuiltPartitioning is the settings the new table is created with: each one
// the declaration names, and the held value of every other one, all named, so
// the new table takes none from the cluster's table profile. Measured on
// 25.1.4.7 and 26.2.1.14, a table created under a dynamic configuration of the
// cluster splits by size where it would not otherwise, so a new table left to
// the profile could change a setting nobody declared. The starting layout is
// the declaration's.
func rebuiltPartitioning(subject string, declared, held *ast.YDBTablePartitioningSpec) (*ast.YDBTablePartitioningSpec, error) {
	current, err := ydbpartition.HeldTable(held)
	if err != nil {
		return nil, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	settings, err := ydbpartition.ResolveTable(declared, current)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	spec := settings.Explicit()
	if declared != nil {
		spec.UniformPartitions = declared.UniformPartitions
		spec.PartitionAtKeys = declared.Clone().PartitionAtKeys
	}
	return spec, nil
}

// rebuiltIndexPartitioning is the partitioning an index of the new table is
// given, for the reason [rebuiltPartitioning] gives: each setting the
// declaration names, and the held value of every other one, all named.
func rebuiltIndexPartitioning(subject string, declared, held *ast.IndexPartitioningSpec) (*ast.IndexPartitioningSpec, error) {
	current, err := ydbindex.Held(held)
	if err != nil {
		return nil, refuseFact(subject, "the settings it holds: "+err.Error())
	}
	settings, err := ydbindex.Resolve(declared, current)
	if err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	return ydbindex.Explicit(settings), nil
}

// rebuildDeclaration is the desired table a rebuild writes: the
// modification's own, or, for a table whose key is its only change, the one
// the diff carries for the tables its constraint changes name.
func rebuildDeclaration(
	diff *difftypes.SchemaDiff,
	rebuild *tableRebuild,
	semantics identifierSemantics,
) (difftypes.TableDeclaration, bool) {
	if rebuild.tableDiff != nil && rebuild.tableDiff.Desired.HasTable() {
		return rebuild.tableDiff.Desired, true
	}
	key := semantics.TableIdentityKey(rebuild.name)
	for _, host := range diff.DeclaredConstraintHosts {
		if host.HasTable() && semantics.TableIdentityKey(host.Table.QualifiedName()) == key {
			return host, true
		}
	}
	return difftypes.TableDeclaration{}, false
}

// settingKinds are the table settings the YDB reader records as not described
// and a recreation would drop, with the words a refusal names each with.
var settingKinds = []struct {
	kind  coverage.Kind
	words string
}{
	{coverage.TTL, "a TTL run interval or tiering policy"},
	{coverage.Changefeed, "changefeeds with settings Ptah does not read"},
	{coverage.ColumnFamily, "column families with settings Ptah does not read"},
	{coverage.TableOption, "storage settings (commit log pools, an external pool or external blobs)"},
}

// undescribedSettings names the settings of table that the read of the
// database recorded as not described; see [recordsSetting].
func undescribedSettings(set coverage.Set, table schemamodel.Table) []string {
	var settings []string
	for _, setting := range settingKinds {
		if recordsSetting(set, setting.kind, table) {
			settings = append(settings, setting.words)
		}
	}
	return settings
}

// rebuildNameAttempts bounds the search for a free scratch name.
const rebuildNameAttempts = 100

// freeTableName picks a scratch name for table under prefix that neither the
// declaration holds nor the plan drops, in the table's directory.
func freeTableName(diff *difftypes.SchemaDiff, table schemamodel.Table, prefix string) (string, error) {
	base := prefix + table.Name
	for attempt := range rebuildNameAttempts {
		candidate := base
		if attempt > 0 {
			candidate = base + "_" + strconv.Itoa(attempt)
		}
		qualified := schemamodel.QualifyTableName(table.Schema, candidate)
		declared := slices.ContainsFunc(diff.DeclaredTables, func(other schemamodel.Table) bool {
			return other.Schema == table.Schema && other.Name == candidate
		})
		if !declared && !slices.Contains(diff.TablesRemoved, qualified) {
			return candidate, nil
		}
	}
	return "", refuseFact(rebuildSubject(table.QualifiedName()), fmt.Sprintf(
		"no scratch table name is free: %s and %d numbered variants are all taken", base, rebuildNameAttempts-1))
}

// refuseRebuiltTableChanges refuses what a rebuild cannot carry in a table's
// modification. The new table is written from the declaration, so a column
// added or dropped, a default, an index change, the TTL, the column families
// (prepareRebuild refuses families the new table cannot take) and the table's
// partitioning, read replicas and key bloom filter travel with it; a comment
// and a constraint other than the key do not exist on YDB.
func (p *Planner) refuseRebuiltTableChanges(tableDiff difftypes.TableDiff) error {
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	if err := p.refuseTableSettings(tableDiff, subject); err != nil {
		return err
	}
	if err := p.refuseRebuiltPartitioning(tableDiff); err != nil {
		return err
	}
	for _, column := range tableDiff.ColumnsAdded {
		hasDefault := column.DefaultSet || column.Default != "" || column.DefaultExpr != ""
		if !hasDefault && !column.Nullable {
			return refuseFact(fmt.Sprintf("adding column %q to table %q in a rebuild", column.Name, tableDiff.TableName),
				"the copy has no value for a NOT NULL column without a default: give it a default or make it nullable")
		}
		if column.DefaultExpr != "" && !p.caps.Has(capability.ExpressionDefaults) {
			return refuseKey(capability.ExpressionDefaults,
				fmt.Sprintf("column %q of table %q defaults to the expression %s", column.Name, tableDiff.TableName, column.DefaultExpr))
		}
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		if err := p.refuseRebuiltColumnChange(tableDiff.TableName, colDiff); err != nil {
			return err
		}
	}
	return nil
}

// refuseRebuiltColumnChange refuses a column change a rebuild does not carry.
func (p *Planner) refuseRebuiltColumnChange(table string, colDiff difftypes.ColumnDiff) error {
	subject := fmt.Sprintf("column %q of table %q", colDiff.ColumnName, table)
	if colDiff.CommentChange != nil || colDiff.NotNullConstraintNameChange != nil {
		return p.refuseColumnChange(table, colDiff)
	}
	for _, key := range sortedKeys(colDiff.Changes) {
		switch key {
		case "type", "nullable", "primary_key", "default":
			continue
		case "default_expr":
			if colDiff.Desired.DefaultExpr != "" && !p.caps.Has(capability.ExpressionDefaults) {
				return refuseKey(capability.ExpressionDefaults, subject+" defaults to the expression "+colDiff.Desired.DefaultExpr)
			}
		default:
			if err := p.refuseChangeKey(subject, key, colDiff); err != nil {
				return err
			}
		}
	}
	return nil
}

// rebuildNodes writes one rebuild: the notes above it, then its steps.
func (p *Planner) rebuildNodes(rebuild *tableRebuild) ([]ast.Node, error) {
	table := rebuild.declaration.Table
	oldPath := sqlident.Qualified(platform.YDB, table.Schema, table.Name)
	scratchName := schemamodel.QualifyTableName(table.Schema, rebuild.scratch)
	replacedName := schemamodel.QualifyTableName(table.Schema, rebuild.replaced)

	create := modelast.FromTableWithConstraints(table, rebuild.declaration.Fields, rebuild.declaration.Enums,
		platform.YDB, rebuild.declaration.Constraints)
	create.Name = scratchName
	partitioning, err := rebuiltPartitioning(rebuildSubject(rebuild.name), table.YDBPartitioning, rebuild.held)
	if err != nil {
		return nil, err
	}
	create.YDBPartitioning = partitioning
	// The scratch table takes no changefeed: YDB would refuse to move it into
	// place (`Cannot move table with cdc streams`), so the changefeeds are
	// added once it holds the table's name.
	create.Changefeeds = nil
	current, desired := rebuildChangefeeds(rebuild)
	for _, index := range rebuild.declaration.Indexes {
		index.TableName = scratchName
		subject := fmt.Sprintf("index %q of %s", index.Name, rebuildSubject(rebuild.name))
		if index.Partitioning, err = rebuiltIndexPartitioning(subject, index.Partitioning, rebuild.heldIndexes[index.Name]); err != nil {
			return nil, err
		}
		create.AddIndex(modelast.FromIndex(index))
	}
	copyStatement, err := p.copyStatement(rebuild, oldPath)
	if err != nil {
		return nil, err
	}
	shown := strings.Trim(oldPath, "`")
	nodes := []ast.Node{
		ast.NewComment(fmt.Sprintf("Rebuild of table %s: YDB cannot make this change in place, so the table is "+
			"created anew, the rows are copied, and the two tables are swapped.", shown)),
		ast.NewComment(fmt.Sprintf("The steps are not atomic. Rows written to %s between the copy and the swap are "+
			"lost, and YDB has no lock to stop them: stop writing to the table until the last step has run.", shown)),
		ast.NewComment("The copy is one query. YDB refuses one that carries more than about 48 MiB on 25.1 or " +
			"64 MiB on 26.2; nothing is then copied, and the old table keeps serving."),
	}
	if len(current) > 0 {
		nodes = append(nodes, ast.NewComment(rebuildChangefeedNote(shown, current)))
	}
	nodes = append(nodes, create, ast.NewRawSQL(copyStatement))
	for _, changefeed := range current {
		nodes = append(nodes, &ast.AlterTableNode{Name: rebuild.name,
			Operations: []ast.AlterOperation{&ast.DropChangefeedOperation{Name: changefeed.Name}}})
	}
	nodes = append(nodes,
		ast.NewRawSQL("ALTER TABLE "+oldPath+" RENAME TO "+sqlident.Qualified(platform.YDB, table.Schema, rebuild.replaced)),
		ast.NewRawSQL("ALTER TABLE "+sqlident.Qualified(platform.YDB, table.Schema, rebuild.scratch)+" RENAME TO "+oldPath),
	)
	for _, changefeed := range desired {
		nodes = append(nodes, &ast.AlterTableNode{Name: rebuild.name,
			Operations: []ast.AlterOperation{&ast.AddChangefeedOperation{Changefeed: changefeed.Clone()}}})
	}
	return append(nodes, ast.NewDropTable(replacedName)), nil
}

// copyStatement writes the INSERT that copies the old table's rows into the
// new one: every declared column the old table has, each converted to what
// the declaration says it becomes. A column the modification adds is left to
// its default.
func (p *Planner) copyStatement(rebuild *tableRebuild, oldPath string) (string, error) {
	added := make(map[string]bool)
	changes := make(map[string]map[string]string)
	if rebuild.tableDiff != nil {
		for _, column := range rebuild.tableDiff.ColumnsAdded {
			added[column.Name] = true
		}
		for _, colDiff := range rebuild.tableDiff.ColumnsModified {
			changes[colDiff.ColumnName] = colDiff.Changes
		}
	}
	var names, values []string
	for _, field := range rebuild.declaration.Fields {
		if added[field.Name] {
			continue
		}
		value, err := p.copiedValue(rebuild.name, field, isKeyColumn(rebuild.declaration, field), changes[field.Name])
		if err != nil {
			return "", err
		}
		names = append(names, sqlident.Quote(platform.YDB, field.Name))
		values = append(values, value+" AS "+sqlident.Quote(platform.YDB, field.Name))
	}
	if len(names) == 0 {
		return "", refuseFact(rebuildSubject(rebuild.name),
			"the new table keeps none of the old table's columns, so there is nothing to copy and every row would be lost")
	}
	scratchPath := sqlident.Qualified(platform.YDB, rebuild.declaration.Table.Schema, rebuild.scratch)
	return "INSERT INTO " + scratchPath + " (" + strings.Join(names, ", ") + ") SELECT " +
		strings.Join(values, ", ") + " FROM " + oldPath, nil
}

// copiedValue is the expression that reads one column of the old table as the
// new table declares it.
//
// A changed type is converted with CAST, which yields NULL on a value it
// cannot convert, so the result is unwrapped: a value that does not convert
// fails the copy with a message rather than becoming NULL. A column that
// becomes NOT NULL is unwrapped too, which fails the copy on a NULL row and
// also satisfies YQL, which refuses an optional value for a NOT NULL column
// at compile time whether or not a row holds NULL. Both measured on 26.2.1.14
// and 25.1.4.7. A key column is always unwrapped: the renderer writes every
// key column NOT NULL, a YDB key column may be nullable all the same, and the
// comparison does not report a key column's nullability, so the old one may
// be optional with no change recorded.
func (p *Planner) copiedValue(table string, field schemamodel.Field, isKey bool, changes map[string]string) (string, error) {
	column := sqlident.Quote(platform.YDB, field.Name)
	_, typeChanged := changes["type"]
	becomesNotNull := strings.HasSuffix(changes["nullable"], "-> false")
	value := column
	if typeChanged {
		mapping, err := ydbtype.Map(field.Type, p.caps)
		if err != nil {
			return "", refuseFact(rebuildSubject(table), fmt.Sprintf("column %q: %v", field.Name, err))
		}
		value = "CAST(" + column + " AS " + mapping.Type + ")"
	}
	notNull := isKey || !field.Nullable
	switch {
	case typeChanged && notNull:
		return p.unwrapped(value, fmt.Sprintf("rebuilding table %s: column %s holds NULL or a value that does not "+
			"convert to its new type", table, field.Name))
	case typeChanged:
		unwrapped, err := p.unwrapped(value, fmt.Sprintf("rebuilding table %s: column %s holds a value that does "+
			"not convert to its new type", table, field.Name))
		if err != nil {
			return "", err
		}
		return "IF(" + column + " IS NULL, NULL, " + unwrapped + ")", nil
	case notNull && (becomesNotNull || isKey):
		return p.unwrapped(value, fmt.Sprintf("rebuilding table %s: column %s holds NULL, and the new table "+
			"declares it NOT NULL", table, field.Name))
	default:
		return value, nil
	}
}

// isKeyColumn reports whether field is part of the declaration's key.
func isKeyColumn(declaration difftypes.TableDeclaration, field schemamodel.Field) bool {
	return field.Primary || slices.Contains(declaration.Table.PrimaryKey, field.Name)
}

// unwrapped is `Unwrap(value, message)`.
func (p *Planner) unwrapped(value, message string) (string, error) {
	literal, err := ydbtype.Literal(ydbtype.Utf8, message, p.caps)
	if err != nil {
		return "", err
	}
	return "Unwrap(" + value + ", " + literal + ")", nil
}

// isPrimaryKey reports whether a constraint change is to the primary key.
func isPrimaryKey(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "PRIMARY KEY")
}

// withoutKeysOfRebuiltTables is diff without the key changes of the tables
// the plan rebuilds: the new table carries the new key.
func withoutKeysOfRebuiltTables(
	diff *difftypes.SchemaDiff,
	rebuilds map[string]*tableRebuild,
	semantics identifierSemantics,
) *difftypes.SchemaDiff {
	if len(rebuilds) == 0 {
		return diff
	}
	scoped := *diff
	rebuilt := func(table, constraintType string) bool {
		_, ok := rebuilds[semantics.TableIdentityKey(table)]
		return ok && isPrimaryKey(constraintType)
	}
	scoped.ConstraintsAdded = slices.DeleteFunc(slices.Clone(diff.ConstraintsAdded),
		func(added difftypes.ConstraintAdditionInfo) bool { return rebuilt(added.TableName, added.Type) })
	scoped.ConstraintsRemoved = slices.DeleteFunc(slices.Clone(diff.ConstraintsRemoved),
		func(removed difftypes.ConstraintRemovalInfo) bool { return rebuilt(removed.TableName, removed.Type) })
	return &scoped
}
