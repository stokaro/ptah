// Package ydb plans a YDB migration from a schema difference.
//
// It is a planner of its own rather than another dialect's under a different
// name. YDB changes a table in place far less than the SQL engines do -- no key
// change, no type change, no SET NOT NULL, and on the older lines no default
// change and no column added with a default -- and the statements it does take
// are not atomic together: a query of several DDL statements keeps the ones
// that ran before a failure. So the planner refuses what YDB cannot do before
// it emits anything, decides every refusal with a capability key rather than
// with the dialect's name, and orders what it emits so that no statement needs
// one that has not run yet:
//
//  1. CREATE TABLE for every added table, with the indexes it gains written
//     inside the statement, because YDB has no CREATE INDEX
//     ([capability.CreateIndexStatement]);
//  2. DROP INDEX for every index the plan removes, before any column it names
//     is dropped (measured: `Impossible drop column because table has an index
//     with that column`, and the same for a covered column);
//  3. per table, ADD COLUMN, then the in-place column changes, then DROP
//     COLUMN;
//  4. ADD INDEX for every index added to a table that already exists, one per
//     statement (`Only one index can be added by one operation`), after the
//     columns it names exist;
//  5. DROP TABLE for every removed table, last.
//
// Each node renders as statements of its own, and the executor runs one per
// query: YDB compiles a query against the schema as it stood before the query,
// so a statement that needs another's effect fails when the two share one.
package ydb

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/ydbgap"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/schemadiff/difftypes"
)

// Planner plans YDB migrations for one capability set.
type Planner struct {
	caps capability.Capabilities
}

// New returns a planner for the newest YDB line Ptah measured.
func New() *Planner { return NewWithCapabilities(capability.YDB262()) }

// NewWithCapabilities returns a planner for a concrete server capability set.
// The set is cloned, so a caller mutating it later does not change a plan.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return &Planner{caps: caps.Clone()}
}

// GenerateMigrationAST returns the nodes that take a YDB database from the
// state diff describes to the declared one, in the order the package
// documentation gives. A change YDB cannot make is refused before any node is
// returned, with an error satisfying errors.Is(err, ptaherr.ErrUnsupportedFeature)
// that names the capability key the target lacks.
func (p *Planner) GenerateMigrationAST(diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if diff == nil {
		return nil, fmt.Errorf("%w: schema diff is nil", ptaherr.ErrInvalidSchemaDiff)
	}
	if err := schemaprecondition.RefuseServerSchemas(platform.YDB, diff); err != nil {
		return nil, err
	}
	semantics := diff.EffectiveIdentifierSemantics(platform.YDB)
	if err := indexscope.ValidateDiffWithSemantics(platform.YDB, semantics, diff); err != nil {
		return nil, err
	}
	removedTables := tableSet(diff.TablesRemoved, semantics)
	if err := p.refuseObjects(withoutKeysOfDroppedTables(diff, removedTables, semantics)); err != nil {
		return nil, err
	}
	addedTables := make(map[string]bool, len(diff.TablesAdded))
	for _, creation := range diff.TablesAdded {
		addedTables[semantics.TableIdentityKey(creation.Name)] = true
	}
	for _, tableDiff := range diff.TablesModified {
		if err := p.refuseTableChanges(tableDiff); err != nil {
			return nil, err
		}
	}
	// The tables whose indexes go inside their CREATE TABLE: every added
	// table, on a target without a CREATE INDEX statement. The render path
	// decides the same question through the same predicate, so `schema
	// render` and a plan write a new table's indexes in the same place.
	inlineIndexes := make(map[string]bool)
	if schemaprep.DeclaresIndexesInCreateTable(p.caps) {
		inlineIndexes = addedTables
	}
	if err := p.refuseIndexAdditions(diff, inlineIndexes, semantics); err != nil {
		return nil, err
	}

	var result []ast.Node
	result = append(result, p.createTables(diff, inlineIndexes, semantics)...)
	result = append(result, dropIndexes(diff.IndexRemovals(), removedTables, semantics)...)
	for _, tableDiff := range diff.TablesModified {
		result = append(result, p.changeTable(tableDiff, diff.DeclaredUserTypes.Enums)...)
	}
	result = append(result, addIndexes(diff.IndexesAdded, inlineIndexes, semantics)...)
	for _, name := range diff.TablesRemoved {
		result = append(result, ast.NewDropTable(name))
	}
	return result, nil
}

// createTables writes each added table, with the indexes the plan gives it
// when inlineIndexes names the table.
func (p *Planner) createTables(
	diff *difftypes.SchemaDiff,
	inlineIndexes map[string]bool,
	semantics identifier.Semantics,
) []ast.Node {
	creations := diff.TablesAdded.Qualified(diff.DeclaredUserTypes, platform.YDB).InDependencyOrder()
	nodes := make([]ast.Node, 0, len(creations))
	for _, creation := range creations {
		table := modelast.FromTableWithConstraints(creation.Table, creation.Fields, creation.Enums, platform.YDB, creation.Constraints)
		key := semantics.TableIdentityKey(creation.Name)
		if !inlineIndexes[key] {
			nodes = append(nodes, table)
			continue
		}
		for _, change := range diff.IndexesAdded {
			if semantics.TableIdentityKey(change.TableName) != key {
				continue
			}
			index := change.Index
			index.TableName = change.TableName
			table.AddIndex(modelast.FromIndex(index))
		}
		nodes = append(nodes, table)
	}
	return nodes
}

// dropIndexes drops each removed index through its table. An index of a table
// the plan drops goes with the table.
func dropIndexes(refs []difftypes.IndexRef, removedTables map[string]bool, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, ref := range refs {
		if removedTables[semantics.TableIdentityKey(ref.TableName)] {
			continue
		}
		nodes = append(nodes, ast.NewDropIndex(ref.Name).SetTable(ref.TableName))
	}
	return nodes
}

// addIndexes adds each index the plan gives a table, one per statement, except
// on the tables inlineIndexes names, whose CREATE TABLE carries them.
func addIndexes(changes difftypes.IndexChanges, inlineIndexes map[string]bool, semantics identifier.Semantics) []ast.Node {
	var nodes []ast.Node
	for _, change := range changes {
		if inlineIndexes[semantics.TableIdentityKey(change.TableName)] {
			continue
		}
		index := change.Index
		index.TableName = change.TableName
		node := modelast.FromIndex(index)
		// YDB's ADD INDEX has no IF NOT EXISTS, and the plan adds an index the
		// comparison found missing, so none is asked for.
		node.IfNotExists = false
		nodes = append(nodes, node)
	}
	return nodes
}

// changeTable writes one table's column changes: additions, then in-place
// changes, then drops. The drops come after the index drops the plan emitted
// before it, so an indexed or covered column is free by then.
func (p *Planner) changeTable(tableDiff difftypes.TableDiff, enums []schemamodel.Enum) []ast.Node {
	var nodes []ast.Node
	alter := func(operation ast.AlterOperation) {
		nodes = append(nodes, &ast.AlterTableNode{Name: tableDiff.TableName, Operations: []ast.AlterOperation{operation}})
	}
	for _, column := range tableDiff.ColumnsAdded {
		alter(&ast.AddColumnOperation{Column: modelast.FromField(column, enums, platform.YDB)})
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		changed := columnchange.Properties(colDiff)
		if !changed.Any() {
			continue
		}
		alter(&ast.ModifyColumnOperation{
			Column:     modelast.FromField(colDiff.Desired, enums, platform.YDB),
			Changed:    changed,
			HasChanged: true,
		})
	}
	for _, column := range tableDiff.ColumnsRemoved {
		alter(&ast.DropColumnOperation{ColumnName: column.Name})
	}
	return nodes
}

// tableSet keys table names by the target's identity rule.
// withoutKeysOfDroppedTables is diff without the primary keys of the tables it
// drops. The reader reports a table's key as a constraint, so a dropped table
// reaches the planner with its key's removal too; DROP TABLE removes the key
// with the table, and refusing that removal as a key change would refuse every
// plan that drops a table.
func withoutKeysOfDroppedTables(
	diff *difftypes.SchemaDiff,
	removedTables map[string]bool,
	semantics identifier.Semantics,
) *difftypes.SchemaDiff {
	scoped := *diff
	scoped.ConstraintsRemoved = slices.DeleteFunc(slices.Clone(diff.ConstraintsRemoved),
		func(removal difftypes.ConstraintRemovalInfo) bool {
			return strings.EqualFold(strings.TrimSpace(removal.Type), "PRIMARY KEY") &&
				removedTables[semantics.TableIdentityKey(removal.TableName)]
		})
	return &scoped
}

func tableSet(names []string, semantics identifier.Semantics) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[semantics.TableIdentityKey(name)] = true
	}
	return set
}

// refuseIndexAdditions refuses, before anything is emitted, an index added
// through ALTER TABLE which YDB would refuse when the plan reaches it: an index
// on a table that exists, or on a new table that inlineIndexes does not name.
// A unique one needs [capability.UniqueIndexOnExistingTable]: measured on every
// line, `Adding a unique index to an existing table is disabled`, even on an
// empty table. Where the plan carries the table's declaration, because it
// changes the table too, the index is held to [ydbindex.ShapeRefusal] the way
// a new table's is. An index added to a table the plan does not otherwise
// change comes with no declaration; schema validation renders that table
// whole, its indexes inside it, and refuses the same shapes before a plan is
// made.
func (p *Planner) refuseIndexAdditions(
	diff *difftypes.SchemaDiff,
	inlineIndexes map[string]bool,
	semantics identifier.Semantics,
) error {
	declarations := make(map[string]difftypes.TableDeclaration, len(diff.TablesModified))
	for _, tableDiff := range diff.TablesModified {
		if tableDiff.Desired.HasTable() {
			declarations[semantics.TableIdentityKey(tableDiff.TableName)] = tableDiff.Desired
		}
	}
	for _, change := range diff.IndexesAdded {
		table := semantics.TableIdentityKey(change.TableName)
		if inlineIndexes[table] {
			continue
		}
		if change.Index.Unique && !p.caps.Has(capability.UniqueIndexOnExistingTable) {
			return refuseKey(capability.UniqueIndexOnExistingTable, fmt.Sprintf(
				"adding unique index %q to table %q, which exists already", change.Index.Name, change.TableName))
		}
		declaration, declared := declarations[table]
		if !declared {
			continue
		}
		if reason := p.indexShapeRefusal(change.Index, declaration); reason != "" {
			return refuseFact(fmt.Sprintf("adding index %q to table %q", change.Index.Name, change.TableName), reason)
		}
	}
	return nil
}

// indexShapeRefusal asks [ydbindex.ShapeRefusal] about an index on a declared
// table: its key from the table or from its key fields, and each column's
// type through the map the renderer writes with. A column whose type the map
// refuses is answered as orderable, because that refusal is the column's and
// is reported where the column is written.
func (p *Planner) indexShapeRefusal(index schemamodel.Index, declaration difftypes.TableDeclaration) string {
	types := make(map[string]string, len(declaration.Fields))
	var fieldKey []string
	for _, field := range declaration.Fields {
		mapping, err := ydbtype.Map(field.Type, p.caps)
		if err != nil {
			mapping = ydbtype.Mapping{}
		}
		types[field.Name] = mapping.Type
		if field.Primary {
			fieldKey = append(fieldKey, field.Name)
		}
	}
	key := declaration.Table.PrimaryKey
	if len(key) == 0 {
		key = fieldKey
	}
	columnType := func(column string) (string, bool) {
		ydbType, declared := types[column]
		return ydbType, declared
	}
	return ydbindex.ShapeRefusal(indexKeyColumns(index), index.IncludeColumns, key, columnType)
}

// indexKeyColumns are the columns an index is keyed on, from its structured
// parts where it has them.
func indexKeyColumns(index schemamodel.Index) []string {
	if len(index.Parts) == 0 {
		return index.Fields
	}
	columns := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		columns = append(columns, part.Name)
	}
	return columns
}

// refuseTableChanges refuses the column changes YDB cannot make in place on
// this target.
func (p *Planner) refuseTableChanges(tableDiff difftypes.TableDiff) error {
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	switch {
	case tableDiff.CommentChange != nil:
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
	case tableDiff.RowTTLChange != nil:
		return p.keyed(capability.RowLevelTTL, "row-level TTL", "the row-level TTL of "+subject)
	case tableDiff.RowDeletionPolicyChange != nil:
		return refuseGap(ydbgap.TableSettings, "the row deletion policy of "+subject)
	case len(tableDiff.ConstraintsAdded)+len(tableDiff.ConstraintsRemoved) > 0:
		return refuseFact(subject, "YDB has no constraint but the key, and the key never changes")
	}
	if tableDiff.Desired.HasTable() && !declaresKey(tableDiff.Desired) {
		return refuseKey(capability.PrimaryKeyRequired, fmt.Sprintf("table %q declares no primary key", tableDiff.TableName))
	}
	for _, column := range tableDiff.ColumnsAdded {
		if err := p.refuseColumnAddition(tableDiff.TableName, column); err != nil {
			return err
		}
	}
	for _, colDiff := range tableDiff.ColumnsModified {
		if err := p.refuseColumnChange(tableDiff.TableName, colDiff); err != nil {
			return err
		}
	}
	return nil
}

// declaresKey reports whether a desired table names a key, on itself or on a
// column.
func declaresKey(declaration difftypes.TableDeclaration) bool {
	if len(declaration.Table.PrimaryKey) > 0 {
		return true
	}
	return slices.ContainsFunc(declaration.Fields, func(field schemamodel.Field) bool {
		return field.Primary && field.StructName == declaration.Table.StructName
	})
}

// refuseColumnAddition refuses the ADD COLUMN shapes YDB refuses on an existing
// table; the renderer holds the same rules, and stating them here refuses the
// plan before any of it is emitted.
func (p *Planner) refuseColumnAddition(table string, column schemamodel.Field) error {
	subject := fmt.Sprintf("adding column %q to table %q", column.Name, table)
	mapping, err := ydbtype.Map(column.Type, p.caps)
	hasDefault := column.DefaultSet || column.Default != "" || column.DefaultExpr != ""
	switch {
	case column.Primary:
		return p.keyed(capability.PrimaryKeyAlterable, "key change", subject+" as part of the key")
	case err == nil && mapping.Serial, column.AutoInc, column.IdentityGeneration != "":
		return refuseFact(subject, "YDB adds no Serial column to an existing table (`Column addition with serial data type is unsupported`)")
	case hasDefault && !p.caps.Has(capability.AddColumnWithDefault):
		return refuseKey(capability.AddColumnWithDefault, subject+" with a default")
	case !hasDefault && !column.Nullable:
		return refuseFact(subject, "YDB adds a NOT NULL column only with a default (`Cannot add not null column without default value`)")
	}
	return nil
}

// refuseColumnChange refuses a modification YDB cannot make in place. The
// change keys are the comparator's; a key this planner does not know is
// refused rather than skipped, because skipping it would report the column
// synced while it is not.
func (p *Planner) refuseColumnChange(table string, colDiff difftypes.ColumnDiff) error {
	subject := fmt.Sprintf("column %q of table %q", colDiff.ColumnName, table)
	if colDiff.CommentChange != nil {
		return refuseGap(ydbgap.Comments, "the comment on "+subject)
	}
	if colDiff.NotNullConstraintNameChange != nil {
		return p.keyed(capability.NamedNotNullConstraints, "named NOT NULL constraint", "the NOT NULL name of "+subject)
	}
	for _, key := range slices.Sorted(maps.Keys(colDiff.Changes)) {
		if err := p.refuseChangeKey(subject, key, colDiff); err != nil {
			return err
		}
	}
	return nil
}

// refuseChangeKey refuses one recorded property change of a column.
func (p *Planner) refuseChangeKey(subject, key string, colDiff difftypes.ColumnDiff) error {
	change := colDiff.Changes[key]
	switch key {
	case "primary_key":
		return p.keyed(capability.PrimaryKeyAlterable, "key change", "changing whether "+subject+" is part of the key ("+change+")")
	case "type":
		return p.keyed(capability.AlterColumnType, "column type change", "changing the type of "+subject+" ("+change+")")
	case "unique":
		return p.keyed(capability.UniqueConstraints, "UNIQUE constraint", "changing whether "+subject+" is UNIQUE ("+change+")")
	case "generated":
		return p.keyed(capability.GeneratedColumns, "generated column", "changing the generation of "+subject)
	case "nullable":
		if strings.HasSuffix(change, "-> false") && !p.caps.Has(capability.AlterColumnSetNotNull) {
			return refuseKey(capability.AlterColumnSetNotNull, "making "+subject+" NOT NULL")
		}
		if strings.HasSuffix(change, "-> true") && !p.caps.Has(capability.AlterColumnDropNotNull) {
			return refuseKey(capability.AlterColumnDropNotNull, "making "+subject+" nullable")
		}
		return nil
	case "default", "default_expr":
		if !p.caps.Has(capability.AlterColumnDefault) {
			return refuseKey(capability.AlterColumnDefault, "changing the default of "+subject+" ("+change+")")
		}
		if colDiff.Desired.DefaultExpr != "" && !p.caps.Has(capability.ExpressionDefaults) {
			return refuseKey(capability.ExpressionDefaults, subject+" defaults to the expression "+colDiff.Desired.DefaultExpr)
		}
		return nil
	default:
		return refuseFact(subject, fmt.Sprintf("YDB cannot change %s of a column in place (%s)", key, change))
	}
}

// keyed refuses through refuseKey when the target lacks key and through
// refuseUnplanned when it holds it.
func (p *Planner) keyed(key capability.Capability, feature, subject string) error {
	if !p.caps.Has(key) {
		return refuseKey(key, subject)
	}
	return refuseUnplanned(feature, subject)
}

func refuseKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: platform.YDB,
		Feature: string(key),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
			subject, key, platform.YDB),
	}
}

func refuseUnplanned(feature, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: platform.YDB,
		Feature: feature,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: the %s planner plans no %s", subject, platform.YDB, feature),
	}
}

func refuseGap(layer ydbgap.Layer, subject string) error {
	return &ptaherr.CapabilityError{
		Dialect: platform.YDB,
		Feature: subject,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, layer.Message()),
	}
}

func refuseFact(subject, reason string) error {
	return &ptaherr.CapabilityError{
		Dialect: platform.YDB,
		Feature: subject,
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, reason),
	}
}
