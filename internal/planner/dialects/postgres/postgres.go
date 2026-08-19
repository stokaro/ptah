package postgres

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
	"go.5x5.cz/ptah/internal/planner/tablelookup"
	"go.5x5.cz/ptah/internal/rlsscope"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

const (
	// DialectName is the PostgreSQL dialect identifier
	DialectName = "postgres"
)

// Planner implements PostgreSQL-specific migration planning functionality.
//
// The Planner is responsible for converting schema differences into PostgreSQL-compatible
// AST nodes that can be rendered into executable SQL statements. It handles PostgreSQL-specific
// features like ENUM types, SERIAL columns, and proper dependency ordering.
//
// # Usage Example
//
//	planner := &postgres.Planner{}
//
//	// Schema differences from comparison
//	diff := &differtypes.SchemaDiff{
//		EnumsAdded:  []string{"user_status"},
//		TablesAdded: []string{"users"},
//	}
//
//	// Target schema from Go struct parsing
//	generated := &goschema.Database{
//		Enums: []goschema.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//		},
//	}
//
//	// Generate migration AST nodes
//	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
//	if err != nil {
//		return err
//	}
//
// # Thread Safety
//
// The Planner carries only immutable configuration (a capability set and
// emission policy flags) and is safe for concurrent use across multiple
// goroutines. Each call to GenerateMigrationSQL operates independently
// without shared state.
type Planner struct {
	// caps describes what the concrete target accepts (issue #225/#226); the
	// nil zero value defaults to the current target-dialect preset via the
	// capabilities accessor. Version presets live in the capability package.
	caps capability.Capabilities
	// dialect selects namespace and diff-policy semantics within the shared
	// PostgreSQL-family planner. The zero value defaults to PostgreSQL.
	dialect string
	// concurrentIndexes requests CREATE INDEX CONCURRENTLY for new indexes.
	// It is a POLICY choice (concurrent builds cannot run inside a
	// transaction, so generator callers split such statements into
	// no_transaction migration files), and it only takes effect when the target
	// also has the capability.CreateIndexConcurrently capability — a future
	// postgres-compatible preset without it (CockroachDB, issue #171) keeps
	// plain CREATE INDEX no matter the policy.
	concurrentIndexes bool
	// concurrentIndexRefs requests CREATE INDEX CONCURRENTLY only for the
	// listed newly added indexes. Table-qualified identity lets the generator
	// target one of two same-named indexes in different schemas.
	concurrentIndexRefs map[types.IndexRef]struct{}
	// concurrentIndexDrops requests DROP INDEX CONCURRENTLY for every standalone
	// index removal. Like concurrentIndexes it is a POLICY choice and is gated
	// on capability.DropIndexConcurrently.
	concurrentIndexDrops bool
	// concurrentIndexDropRefs requests DROP INDEX CONCURRENTLY only for the
	// listed removed indexes. It is a SEPARATE set from concurrentIndexRefs on
	// purpose: the two directions are chosen by different callers (a down file
	// reverses a concurrent build; an up file honors an explicit drop policy),
	// and folding them together would make a concurrent build silently rewrite
	// unrelated drops in the same plan.
	concurrentIndexDropRefs map[types.IndexRef]struct{}
	// skip lists destructive change kinds this planner omits from the plan,
	// emitting a clearly-marked comment in their place. See diffpolicy.
	skip diffpolicy.SkipSet
}

// New returns a planner configured with the current PostgreSQL line preset
// (capability.Postgres17: PostgreSQL 17+).
func New() *Planner {
	return NewWithCapabilities(capability.Postgres17())
}

// NewWithCapabilities returns a planner for a specific capability set — e.g.
// capability.Postgres13() for a PostgreSQL 12/13 target, or a set resolved
// from a live server via capability.ForServerVersion. The set is expected to
// be valid (capability.Capabilities.Validate); presets always are. The set is
// cloned, so later mutations by the caller cannot affect the planner. A nil
// set defaults to the capability.Postgres17 preset.
func NewWithCapabilities(caps capability.Capabilities) *Planner {
	return NewForDialect(DialectName, caps)
}

// NewForDialect returns a PostgreSQL-family planner for one concrete dialect.
// The dialect controls object namespace and diff-policy semantics while caps
// control the SQL features available on the selected server version.
func NewForDialect(dialect string, caps capability.Capabilities) *Planner {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == "" {
		normalized = DialectName
	}
	return &Planner{caps: caps.Clone(), dialect: normalized}
}

// capabilities returns the planner's capability set, defaulting the nil zero
// value to the current PostgreSQL line preset so a bare &Planner{} behaves
// exactly like New(). Restriction must be an explicit choice, never a
// zero-value surprise.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps == nil {
		return capability.ForDialect(p.targetDialect())
	}
	return p.caps
}

func (p *Planner) targetDialect() string {
	if p.dialect == "" {
		return DialectName
	}
	return p.dialect
}

// WithConcurrentIndexes returns a copy of the planner that emits
// CREATE [UNIQUE] INDEX CONCURRENTLY for newly added indexes, provided the
// target capability set includes capability.CreateIndexConcurrently. The
// receiver is not modified. Concurrent index builds cannot run inside a
// transaction block; callers must arrange no_transaction execution for such
// statements.
func (p *Planner) WithConcurrentIndexes() *Planner {
	cp := *p
	cp.concurrentIndexes = true
	return &cp
}

// WithConcurrentIndexRefs returns a copy of the planner that emits
// CREATE [UNIQUE] INDEX CONCURRENTLY only for the listed newly added indexes,
// provided the target capability set includes capability.CreateIndexConcurrently.
func (p *Planner) WithConcurrentIndexRefs(indexRefs ...types.IndexRef) *Planner {
	cp := *p
	cp.concurrentIndexRefs = maps.Clone(p.concurrentIndexRefs)
	if cp.concurrentIndexRefs == nil {
		cp.concurrentIndexRefs = make(map[types.IndexRef]struct{}, len(indexRefs))
	}
	for _, ref := range indexRefs {
		if strings.TrimSpace(ref.Name) != "" && strings.TrimSpace(ref.TableName) != "" {
			cp.concurrentIndexRefs[ref] = struct{}{}
		}
	}
	return &cp
}

// WithConcurrentIndexDrops returns a copy of the planner that emits
// DROP INDEX CONCURRENTLY for every standalone index removal, provided the
// target capability set includes capability.DropIndexConcurrently. The receiver
// is not modified. An index that is dropped and recreated under the same
// identity is a redefinition, not a standalone removal, and keeps the blocking
// drop the planner pairs with the rebuild.
func (p *Planner) WithConcurrentIndexDrops() *Planner {
	cp := *p
	cp.concurrentIndexDrops = true
	return &cp
}

// WithConcurrentIndexDropRefs returns a copy of the planner that emits
// DROP INDEX CONCURRENTLY only for the listed removed indexes, provided the
// target capability set includes capability.DropIndexConcurrently. Concurrent
// drops cannot run inside a transaction block; callers must arrange
// no_transaction execution for such statements.
func (p *Planner) WithConcurrentIndexDropRefs(indexRefs ...types.IndexRef) *Planner {
	cp := *p
	cp.concurrentIndexDropRefs = maps.Clone(p.concurrentIndexDropRefs)
	if cp.concurrentIndexDropRefs == nil {
		cp.concurrentIndexDropRefs = make(map[types.IndexRef]struct{}, len(indexRefs))
	}
	for _, ref := range indexRefs {
		if strings.TrimSpace(ref.Name) != "" && strings.TrimSpace(ref.TableName) != "" {
			cp.concurrentIndexDropRefs[ref] = struct{}{}
		}
	}
	return &cp
}

// WithSkipChangeKinds returns a copy of the planner that omits the listed
// destructive change kinds from the plan, emitting a clearly-marked comment in
// their place instead of the DDL. The receiver is not modified. Passing no
// kinds returns the receiver unchanged.
func (p *Planner) WithSkipChangeKinds(kinds ...diffpolicy.ChangeKind) *Planner {
	if len(kinds) == 0 {
		return p
	}
	cp := *p
	cp.skip = make(diffpolicy.SkipSet, len(p.skip)+len(kinds))
	maps.Copy(cp.skip, p.skip)
	for _, kind := range kinds {
		cp.skip[kind] = struct{}{}
	}
	return &cp
}

func (p *Planner) usesConcurrentIndex(ref types.IndexRef) bool {
	if p.concurrentIndexes {
		return true
	}
	_, ok := p.concurrentIndexRefs[ref]
	return ok
}

// usesConcurrentIndexDrop reports whether this index removal should be emitted
// as DROP INDEX CONCURRENTLY. The drop side has its own policy flag and its own
// ref set, so turning on concurrent index BUILDS never rewrites a drop the
// caller did not ask for.
func (p *Planner) usesConcurrentIndexDrop(ref types.IndexRef) bool {
	if p.concurrentIndexDrops {
		return true
	}
	_, ok := p.concurrentIndexDropRefs[ref]
	return ok
}

func (p *Planner) addNewEnums(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, enumName := range diff.EnumsAdded {
		// The diff names enums by qualified name, so the lookup does too --
		// under the target's identifier rules rather than as raw text. Building
		// the node through fromschema.FromEnum keeps the CREATE TYPE identifier
		// and the column type that references it derived from one place
		// (stokaro/ptah#1276).
		if enum := objectlookup.Qualified(generated.Enums, enumName, semantics); enum != nil {
			result = append(result, fromschema.FromEnum(*enum))
		}
	}
	return result
}

type postgresEnumColumnUsage struct {
	Table       string
	Column      string
	Default     string
	DefaultSet  bool
	DefaultExpr string
}

func (p *Planner) modifyExistingEnums(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, enumDiff := range diff.EnumsModified {
		if len(enumDiff.ValuesRemoved) > 0 {
			values, ok := postgresEnumValues(generated, enumDiff.EnumName, semantics)
			if !ok {
				result = append(result, ast.NewComment(fmt.Sprintf(
					"WARNING: Cannot remove enum values %v from %s because the target enum definition was not found",
					enumDiff.ValuesRemoved,
					enumDiff.EnumName,
				)))
				continue
			}
			result = append(result, ast.NewRawSQL(postgresEnumValueRemovalSQL(
				enumDiff.EnumName,
				values,
				postgresEnumColumnUsages(generated, enumDiff.EnumName),
			)))
			continue
		}

		astNode := ast.NewAlterType(enumDiff.EnumName)
		for _, value := range enumDiff.ValuesAdded {
			addEnumAst := ast.NewAddEnumValueOperation(value)
			astNode.AddOperation(addEnumAst)
		}
		result = append(result, astNode)
	}
	return result
}

func postgresEnumValues(
	generated *goschema.Database,
	enumName string,
	semantics identifier.Semantics,
) ([]string, bool) {
	if generated == nil {
		return nil, false
	}
	enum := objectlookup.Qualified(generated.Enums, enumName, semantics)
	if enum == nil {
		return nil, false
	}
	return append([]string(nil), enum.Values...), true
}

func postgresEnumColumnUsages(generated *goschema.Database, enumName string) []postgresEnumColumnUsage {
	if generated == nil {
		return nil
	}
	tablesByStruct := make(map[string]goschema.Table, len(generated.Tables))
	for _, table := range generated.Tables {
		tablesByStruct[table.StructName] = table
	}

	// A field names its declared type by bare name -- that is what
	// fromschema.declaredEnum matches on -- while the diff names the enum by
	// qualified name. Both spellings are accepted so a rebuild of an enum in a
	// non-default schema still finds the columns it has to convert. Where two
	// schemas hold an enum of one name the bare spelling cannot separate them;
	// see the residual note on stokaro/ptah#1276.
	bareName := postgresBaseName(enumName)
	usages := make([]postgresEnumColumnUsage, 0)
	for _, field := range generated.Fields {
		if field.Type != enumName && field.Type != bareName {
			continue
		}
		table, ok := tablesByStruct[field.StructName]
		if !ok {
			continue
		}
		usages = append(usages, postgresEnumColumnUsage{
			Table:       table.QualifiedName(),
			Column:      field.Name,
			Default:     field.Default,
			DefaultSet:  field.DefaultSet,
			DefaultExpr: field.DefaultExpr,
		})
	}
	return usages
}

func postgresEnumValueRemovalSQL(enumName string, values []string, usages []postgresEnumColumnUsage) string {
	oldName := postgresTemporaryEnumName(enumName)
	enumIdent := quotePostgresIdentifierPath(enumName)
	oldIdent := quotePostgresIdentifierPath(oldName)

	var sql strings.Builder
	for _, usage := range usages {
		if usage.DefaultSet || usage.Default != "" || usage.DefaultExpr != "" {
			fmt.Fprintf(&sql, "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;\n",
				quotePostgresIdentifierPath(usage.Table),
				quotePostgresIdentifier(usage.Column),
			)
		}
	}
	fmt.Fprintf(&sql, "ALTER TYPE %s RENAME TO %s;\n", enumIdent, quotePostgresIdentifier(postgresBaseName(oldName)))
	fmt.Fprintf(&sql, "CREATE TYPE %s AS ENUM (%s);\n", enumIdent, postgresEnumValueList(values))
	for _, usage := range usages {
		fmt.Fprintf(&sql, "ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::text::%s;\n",
			quotePostgresIdentifierPath(usage.Table),
			quotePostgresIdentifier(usage.Column),
			enumIdent,
			quotePostgresIdentifier(usage.Column),
			enumIdent,
		)
		if defaultSQL, ok := postgresDefaultSQL(usage); ok {
			fmt.Fprintf(&sql, "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;\n",
				quotePostgresIdentifierPath(usage.Table),
				quotePostgresIdentifier(usage.Column),
				defaultSQL,
			)
		}
	}
	fmt.Fprintf(&sql, "DROP TYPE %s;", oldIdent)
	return sql.String()
}

func postgresTemporaryEnumName(enumName string) string {
	ref, ok := tableref.Parse(enumName)
	if !ok {
		return enumName + "__ptah_old"
	}
	return tableref.Canonical(ref.Schema, ref.Name+"__ptah_old")
}

func postgresBaseName(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	return ref.Name
}

func postgresEnumValueList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quotePostgresLiteral(value))
	}
	return strings.Join(quoted, ", ")
}

func postgresDefaultSQL(usage postgresEnumColumnUsage) (string, bool) {
	if usage.DefaultExpr != "" {
		return usage.DefaultExpr, true
	}
	if usage.DefaultSet || usage.Default != "" {
		return postgresDefaultLiteral(usage.Default), true
	}
	return "", false
}

func postgresDefaultLiteral(value string) string {
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value
	}
	return quotePostgresLiteral(value)
}

func quotePostgresLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quotePostgresIdentifierPath(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return quotePostgresIdentifier(name)
	}
	if !ref.Qualified {
		return quotePostgresIdentifier(ref.Name)
	}
	return quotePostgresIdentifier(ref.Schema) + "." + quotePostgresIdentifier(ref.Name)
}

func quotePostgresIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (p *Planner) addNewTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	orderedTables := deporder.TablesForCreate(generated, diff.TablesAdded)

	// Phase 1: Create tables without foreign key constraints
	result = p.createTablesWithoutForeignKeys(result, generated, orderedTables)

	return result
}

func (p *Planner) addForeignKeyConstraintsForNewTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	return p.addForeignKeyConstraints(result, generated, deporder.TablesForCreate(generated, diff.TablesAdded))
}

// addSchemaPreconditions creates every schema this migration is about to put an
// object into, before it puts anything into any of them.
//
// It reads the diff rather than the desired state, so a run that changes nothing
// emits nothing and an inspect/apply round trip stays the clean no-op it has to
// be. And it reads EVERY added-object list rather than the tables alone, which
// is the defect it replaces: the preconditions used to be derived from
// diff.TablesAdded inside addNewTables, so they were emitted after the types,
// sequences and functions phases and covered none of them. Measured on
// PostgreSQL 17.10, applying a `schema inspect` document of a multi-schema
// database to an empty one:
//
//	CREATE SEQUENCE "s_misc"."m_seq" ...
//	ERROR: schema "s_misc" does not exist (SQLSTATE 3F000)
//
// with `CREATE SCHEMA IF NOT EXISTS s_misc` seventeen statements further down.
// The same shape reaches CREATE DOMAIN, CREATE TYPE and CREATE FUNCTION
// (stokaro/ptah#1276).
//
// The schema is taken from the qualified name each comparison already puts on
// the diff -- tables, enums, domains, composites, ranges, sequences, functions,
// views, materialized views and a trigger's target relation are all named that
// way. Extensions keep installation schema on their desired definitions, so
// their additions contribute that exact identifier directly. Names that carry
// no schema contribute none, which is every name on a single-schema migration.
func (p *Planner) addSchemaPreconditions(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) []ast.Node {
	names := make([]string, 0, len(diff.TablesAdded))
	names = append(names, diff.TablesAdded...)
	names = append(names, diff.EnumsAdded...)
	names = append(names, diff.DomainsAdded...)
	names = append(names, diff.CompositeTypesAdded...)
	names = append(names, diff.RangesAdded...)
	names = append(names, diff.SequencesAdded...)
	names = append(names, diff.FunctionsAdded...)
	names = append(names, diff.ViewsAdded...)
	names = append(names, diff.MaterializedViewsAdded...)
	for _, trigger := range diff.TriggersAdded {
		names = append(names, trigger.TableName)
	}
	seen := make(map[string]struct{}, len(names))
	schemas := make([]string, 0, len(names))
	for _, name := range names {
		ref, ok := tableref.Parse(name)
		if !ok || !ref.Qualified {
			continue
		}
		schema := strings.TrimSpace(ref.Schema)
		if schema == "" {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		schemas = append(schemas, schema)
	}
	for _, name := range diff.ExtensionsAdded {
		for _, extension := range generated.Extensions {
			if extension.Name != name || extension.Schema == "" ||
				schemaselection.IsPostgresSystemSchema(extension.Schema) {
				continue
			}
			if _, ok := seen[extension.Schema]; !ok {
				seen[extension.Schema] = struct{}{}
				schemas = append(schemas, extension.Schema)
			}
			break
		}
	}
	slices.Sort(schemas)
	for _, schema := range schemas {
		result = append(result, &ast.CreateSchemaNode{Name: schema, IfNotExists: true})
	}
	return result
}

// createTablesWithoutForeignKeys creates all tables without foreign key constraints
func (p *Planner) createTablesWithoutForeignKeys(result []ast.Node, generated *goschema.Database, tables []goschema.Table) []ast.Node {
	allFields := generated.Fields

	for _, table := range tables {
		astNode := fromschema.FromTable(table, allFields, generated.Enums, DialectName)
		for _, column := range astNode.Columns {
			column.ForeignKey = nil
		}
		result = append(result, astNode)
	}

	return result
}

// addForeignKeyConstraints adds foreign key constraints via ALTER TABLE statements
func (p *Planner) addForeignKeyConstraints(result []ast.Node, generated *goschema.Database, tables []goschema.Table) []ast.Node {
	for _, table := range tables {
		result = p.addRegularForeignKeys(result, generated, table)
		result = p.addSelfReferencingForeignKeys(result, generated, table)
	}

	return result
}

// addRegularForeignKeys adds regular (non-self-referencing) foreign key constraints
func (p *Planner) addRegularForeignKeys(result []ast.Node, generated *goschema.Database, table goschema.Table) []ast.Node {
	for _, field := range generated.Fields {
		if !isRegularForeignKeyField(field, table) {
			continue
		}

		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef == nil {
			continue
		}
		qualifyForeignKeyRef(generated, table, fkRef)
		if foreignKeyTargetsTable(fkRef, table) {
			continue
		}
		fkRef.OnDelete = field.OnDelete
		fkRef.OnUpdate = field.OnUpdate
		result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), foreignKeyName(table.Name, field), []string{field.Name}, fkRef))
	}
	return result
}

func foreignKeyTargetsTable(fkRef *ast.ForeignKeyRef, table goschema.Table) bool {
	return fkRef.Table == table.QualifiedName()
}

func qualifyForeignKeyRef(generated *goschema.Database, current goschema.Table, fkRef *ast.ForeignKeyRef) {
	fkRef.Table = tablelookup.ResolveReference(generated.Tables, current, fkRef.Table)
}

// addSelfReferencingForeignKeys adds self-referencing foreign key constraints
func (p *Planner) addSelfReferencingForeignKeys(result []ast.Node, generated *goschema.Database, table goschema.Table) []ast.Node {
	selfRefFKs, exists := generated.SelfReferencingForeignKeys[table.QualifiedName()]
	if !exists {
		return result
	}

	for _, selfRefFK := range selfRefFKs {
		fkRef := fromschema.ParseForeignKeyReference(selfRefFK.Foreign)
		if fkRef != nil {
			qualifyForeignKeyRef(generated, table, fkRef)
			fkRef.OnDelete = selfRefFK.OnDelete
			fkRef.OnUpdate = selfRefFK.OnUpdate
			result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), selfReferencingForeignKeyName(table.Name, selfRefFK), []string{selfRefFK.FieldName}, fkRef))
		}
	}

	return result
}

// selfReferencingForeignKeyName returns the constraint name for a
// self-referencing field-level foreign key, deriving the conventional
// fk_<table>_<field> name when foreign_key_name= was omitted (same rule as the
// regular field path in foreignKeyName).
func selfReferencingForeignKeyName(tableName string, fk goschema.SelfReferencingFK) string {
	if fk.ForeignKeyName != "" {
		return fk.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, fk.FieldName)
}

// isRegularForeignKeyField checks if a field is a regular foreign key field for the given table.
//
// A field-level foreign= annotation is a foreign key regardless of whether the
// author supplied an explicit foreign_key_name=. When the name is omitted the
// planner derives the conventional fk_<table>_<column> name (see
// foreignKeyName) so the constraint is actually created in the database with a
// stable, named identity. Without this an anonymous field-level FK on a newly
// created table was silently dropped from the migration, which made the
// schemadiff comparator (which synthesizes the FK under the conventional name)
// re-report it as missing forever (issue #189 round-trip failure).
func isRegularForeignKeyField(field goschema.Field, table goschema.Table) bool {
	return field.StructName == table.StructName && field.Foreign != ""
}

// foreignKeyName returns the constraint name to use for a field-level foreign
// key: the explicit foreign_key_name= when set, otherwise the conventional
// fk_<table>_<column> name shared with the schemadiff comparator and the down
// path via fromschema.GenerateForeignKeyName.
func foreignKeyName(tableName string, field goschema.Field) string {
	if field.ForeignKeyName != "" {
		return field.ForeignKeyName
	}
	return fromschema.GenerateForeignKeyName(tableName, field.Name)
}

// createForeignKeyAlterStatement creates an ALTER TABLE statement for adding a foreign key constraint
func (p *Planner) createForeignKeyAlterStatement(tableName, constraintName string, columns []string, fkRef *ast.ForeignKeyRef) *ast.AlterTableNode {
	fkRef.Name = constraintName
	fkConstraint := ast.NewForeignKeyConstraint(constraintName, columns, fkRef)

	return &ast.AlterTableNode{
		Name:       tableName,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: fkConstraint}},
	}
}

func (p *Planner) addNewTableColumns(
	result []ast.Node,
	tableDiff types.TableDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	for _, colName := range tableDiff.ColumnsAdded {
		// Find the field definition for this column
		// We need to find the struct name that corresponds to this table name
		var targetField *goschema.Field
		var targetStructName string

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName, semantics); table != nil {
			targetStructName = table.StructName
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		if targetField != nil {
			columnNode := fromschema.FromFieldWithoutForeignKeys(*targetField, generated.Enums, "postgres")

			// Only add the column - foreign key constraints will be added separately
			// to ensure proper dependency ordering (columns must exist before FK constraints)
			operations := []ast.AlterOperation{&ast.AddColumnOperation{Column: columnNode}}

			// Generate ALTER TABLE statement with only the ADD COLUMN operation
			alterNode := &ast.AlterTableNode{
				Name:       tableDiff.TableName,
				Operations: operations,
			}
			result = append(result, alterNode)
		}
	}
	return result
}

// addForeignKeyConstraintsForNewColumns adds foreign key constraints for newly added columns.
// This method is called after all columns have been added to ensure that referenced columns exist.
func (p *Planner) addForeignKeyConstraintsForNewColumns(
	result []ast.Node,
	tableDiff types.TableDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	for _, colName := range tableDiff.ColumnsAdded {
		// Find the field definition for this column
		var targetField *goschema.Field
		var targetStructName string
		var targetTableName string
		var targetTable *goschema.Table

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName, semantics); table != nil {
			targetTable = table
			targetStructName = table.StructName
			targetTableName = table.Name
		}

		// Now find the field using the correct struct name
		for _, field := range generated.Fields {
			if field.StructName == targetStructName && field.Name == colName {
				targetField = &field
				break
			}
		}

		// Only process fields that have foreign key constraints
		if targetField != nil && targetField.Foreign != "" {
			// Parse the foreign key reference
			fkRef := fromschema.ParseForeignKeyReference(targetField.Foreign)
			if fkRef != nil {
				if targetTable != nil {
					qualifyForeignKeyRef(generated, *targetTable, fkRef)
				}
				fkName := foreignKeyName(targetTableName, *targetField)
				fkRef.Name = fkName
				fkRef.OnDelete = targetField.OnDelete
				fkRef.OnUpdate = targetField.OnUpdate

				// Create foreign key constraint
				fkConstraint := ast.NewForeignKeyConstraint(
					fkName,
					[]string{targetField.Name},
					fkRef,
				)

				// Create ALTER TABLE statement with only the ADD CONSTRAINT operation
				alterNode := &ast.AlterTableNode{
					Name:       tableDiff.TableName,
					Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: fkConstraint}},
				}
				result = append(result, alterNode)
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingTableColumns(
	result []ast.Node,
	tableDiff types.TableDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	allFields := fromschema.ProcessEmbeddedFields(generated.EmbeddedFields, generated.Fields)
	for _, colDiff := range tableDiff.ColumnsModified {
		// Find the target field definition for this column
		// We need to find the struct name that corresponds to this table name
		var targetField *goschema.Field
		var targetStructName string

		// First, find the struct name for this table
		if table := findGeneratedTableByDiffName(generated, tableDiff.TableName, semantics); table != nil {
			targetStructName = table.StructName
		}

		// Now find the field using the correct struct name
		for _, field := range allFields {
			if field.StructName == targetStructName && field.Name == colDiff.ColumnName {
				targetField = &field
				break
			}
		}

		if targetField == nil {
			astCommentNode := ast.NewComment(fmt.Sprintf("ERROR: Could not find field definition for %s.%s (struct: %s)", tableDiff.TableName, colDiff.ColumnName, targetStructName))
			result = append(result, astCommentNode)
			continue
		}

		// Create a column definition with the target field properties
		columnNode := fromschema.FromField(*targetField, generated.Enums, "postgres")
		if isGeneratedColumnChange(colDiff) {
			result = p.modifyGeneratedColumnExpression(result, tableDiff.TableName, colDiff, columnNode)
			continue
		}

		// Generate ALTER COLUMN statements using AST
		alterNode := &ast.AlterTableNode{
			Name: tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
				Column:              columnNode,
				PreviousType:        previousColumnType(colDiff.Changes["type"]),
				PreviousNullable:    previousColumnNullable(colDiff.Changes["nullable"]),
				HasPreviousNullable: colDiff.Changes["nullable"] != "",
			}},
		}
		result = append(result, alterNode)

		// Add a comment showing what changes are being made. Iterate the
		// changes in sorted key order so migration output is deterministic
		// (issue #59).
		changesList := make([]string, 0, len(colDiff.Changes))
		for _, changeType := range slices.Sorted(maps.Keys(colDiff.Changes)) {
			changesList = append(changesList, fmt.Sprintf("%s: %s", changeType, colDiff.Changes[changeType]))
		}
		astCommentNode := ast.NewComment(fmt.Sprintf("Modify column %s.%s: %s", tableDiff.TableName, colDiff.ColumnName, strings.Join(changesList, ", ")))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) modifyGeneratedColumnExpression(
	result []ast.Node,
	tableName string,
	colDiff types.ColumnDiff,
	columnNode *ast.ColumnNode,
) []ast.Node {
	if columnNode == nil || strings.TrimSpace(columnNode.GeneratedExpression) == "" {
		return append(result, ast.NewComment(fmt.Sprintf(
			"WARNING: Generated column %s.%s changed, but the target schema is not a generated column; manual migration required.",
			tableName,
			colDiff.ColumnName,
		)))
	}
	if !p.capabilities().Has(capability.AlterGeneratedColumnExpression) {
		// The refusal is a capability verdict, so it reads as one. A plan for
		// a PostgreSQL-compatible engine, a managed provider that withholds
		// the statement, or a preset composed with .With(..., false) lands
		// here with a version number that explains nothing; the release that
		// added the statement follows the key as its reason.
		return append(result, ast.NewComment(fmt.Sprintf(
			"WARNING: Generated column %s.%s changed, but ALTER COLUMN SET EXPRESSION requires target capability %s, unavailable on this target (PostgreSQL added it in 17); manual migration required.",
			tableName,
			colDiff.ColumnName,
			capability.AlterGeneratedColumnExpression,
		)))
	}

	alterNode := &ast.AlterTableNode{
		Name: tableName,
		Operations: []ast.AlterOperation{
			&ast.AlterGeneratedColumnExpressionOperation{
				ColumnName: colDiff.ColumnName,
				Expression: columnNode.GeneratedExpression,
			},
		},
	}
	result = append(result, alterNode)
	result = append(result, ast.NewComment(fmt.Sprintf(
		"Modify generated column %s.%s: %s",
		tableName,
		colDiff.ColumnName,
		colDiff.Changes["generated"],
	)))
	return result
}

func isGeneratedColumnChange(colDiff types.ColumnDiff) bool {
	_, ok := colDiff.Changes["generated"]
	return ok
}

// findGeneratedTableByDiffName resolves the declared table a TableDiff names.
//
// The two do not always spell the schema the same way. A TableDiff carries the
// name the DESIRED schema spells, while the schema this resolves against is the
// pre-change database converted back to a goschema in the down direction --
// where every name is spelled the way the catalog reported it. Table comparison
// keys through identifier semantics and therefore reports `users` and
// `public.users` as ONE modified table, so an `==` here split what the
// comparator had already joined: the lookup answered nil, no field was found,
// and addNewTableColumns emitted NOTHING. A rollback of a DROP COLUMN then
// omitted the ALTER TABLE ... ADD COLUMN that restores it, exited 0 and reported
// success.
func findGeneratedTableByDiffName(
	generated *goschema.Database,
	tableName string,
	semantics identifier.Semantics,
) *goschema.Table {
	return objectlookup.Qualified(generated.Tables, tableName, semantics)
}

// findGeneratedTableByStructName resolves a declared table from the Go struct it
// was declared on. A struct name is not a database identifier, so it carries no
// schema and no folding rule: it is matched verbatim, and identifier semantics
// have nothing to say about it.
func findGeneratedTableByStructName(generated *goschema.Database, structName string) *goschema.Table {
	for i := range generated.Tables {
		if generated.Tables[i].StructName == structName {
			return &generated.Tables[i]
		}
	}
	return nil
}

func previousColumnType(change string) string {
	before, _, ok := strings.Cut(change, " -> ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(before)
}

func previousColumnNullable(change string) bool {
	before, _, ok := strings.Cut(change, " -> ")
	return ok && strings.TrimSpace(before) == "true"
}

func (p *Planner) removeTableColumnsFromDiff(result []ast.Node, tableDiff types.TableDiff) []ast.Node {
	for _, colName := range tableDiff.ColumnsRemoved {
		// Generate DROP COLUMN statement using AST with CASCADE to handle dependencies
		dropOp := &ast.DropColumnOperation{
			ColumnName: colName,
			Cascade:    true, // Use CASCADE to automatically drop dependent RLS policies
		}
		alterNode := &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{dropOp},
		}
		result = append(result, alterNode)
		astCommentNode := ast.NewComment(fmt.Sprintf("WARNING: Dropping column %s.%s with CASCADE - This will delete data and dependent objects!", tableDiff.TableName, colName))
		result = append(result, astCommentNode)
	}
	return result
}

func (p *Planner) addAndModifyTableColumns(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsModified) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add new columns
			result = p.addNewTableColumns(result, tableDiff, generated, semantics)

			// Modify existing columns
			result = p.modifyExistingTableColumns(result, tableDiff, generated, semantics)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewComment(fmt.Sprintf("Add/modify columns for table: %s", tableDiff.TableName))
				// Insert the comment before the operations we just added
				newResult := make([]ast.Node, 0, len(result)+1)
				newResult = append(newResult, result[:initialLength]...)
				newResult = append(newResult, astCommentNode)
				newResult = append(newResult, result[initialLength:]...)
				result = newResult
			}
		}
	}
	return result
}

// addForeignKeyConstraintsForModifiedTables adds foreign key constraints for all newly added columns
// across all modified tables. This ensures that all columns exist before any foreign key constraints
// are created, preventing dependency ordering issues.
func (p *Planner) addForeignKeyConstraintsForModifiedTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add foreign key constraints for new columns
			result = p.addForeignKeyConstraintsForNewColumns(result, tableDiff, generated, semantics)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewComment(fmt.Sprintf("Add foreign key constraints for table: %s", tableDiff.TableName))
				// Insert the comment before the operations we just added
				newResult := make([]ast.Node, 0, len(result)+1)
				newResult = append(newResult, result[:initialLength]...)
				newResult = append(newResult, astCommentNode)
				newResult = append(newResult, result[initialLength:]...)
				result = newResult
			}
		}
	}
	return result
}

func (p *Planner) removeTableColumns(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsRemoved) > 0 {
			astCommentNode := ast.NewComment(fmt.Sprintf("Remove columns from table: %s", tableDiff.TableName))
			result = append(result, astCommentNode)

			// Remove columns (dangerous!)
			result = p.removeTableColumnsFromDiff(result, tableDiff)
		}
	}
	return result
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *types.SchemaDiff,
	indexes *indexscope.Resolver,
) ([]ast.Node, error) {
	indexRemovals := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff.IndexRemovals(),
	)
	guardedDrops := p.capabilities().Has(capability.DropIndexIfExists)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()

	for _, ref := range diff.IndexAdditions() {
		index, err := indexes.Resolve(ref)
		if err != nil {
			return nil, err
		}
		for removal := range indexRemovals.Matches(ref) {
			if _, ownedByConstraint := constraintBacked[removal]; ownedByConstraint {
				result = append(result, p.constraintBackedIndexDropNode(removal))
				continue
			}
			dropIndexNode := ast.NewDropIndex(removal.Name).
				SetTable(removal.TableName)
			if guardedDrops {
				dropIndexNode.SetIfExists()
			}
			result = append(result, dropIndexNode)
		}
		// IndexRef is the validated owner identity. Apply it to the local copy
		// before conversion so source metadata cannot reintroduce an
		// unqualified or ambiguous table association.
		index.TableName = ref.TableName
		indexNode := fromschema.FromIndex(index)
		// CONCURRENTLY is opt-in policy AND capability-gated: the
		// planner never emits it for a target that rejects it
		// (issue #226; CockroachDB-style presets keep plain
		// CREATE INDEX even when the policy is on).
		if p.usesConcurrentIndex(ref) && p.capabilities().Has(capability.CreateIndexConcurrently) {
			indexNode.Concurrently = true
		}
		result = append(result, indexNode)
	}
	return result, nil
}

func (p *Planner) removeIndexes(
	result []ast.Node,
	diff *types.SchemaDiff,
) []ast.Node {
	// IF EXISTS on DROP INDEX is capability-gated intent, mirroring the MySQL
	// planner (issue #226). Every supported PostgreSQL line has the guard, so
	// the default preset keeps today's output; a preset without it (or a
	// composed set) actually changes the plan.
	guarded := p.capabilities().Has(capability.DropIndexIfExists)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()
	rebuiltAsConstraint := diff.IndexRemovalsRebuiltAsUniqueConstraints()
	indexAdditions := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff.IndexAdditions(),
	)
	for _, ref := range diff.IndexRemovals() {
		if indexAdditions.Contains(ref) {
			continue
		}
		// A removal a UNIQUE constraint addition rebuilds was already emitted
		// ahead of that addition, which is the only order the server accepts;
		// dropping it again here would land after the add and delete the index
		// the constraint now needs.
		if _, rebuilt := rebuiltAsConstraint[ref]; rebuilt {
			continue
		}
		if _, ownedByConstraint := constraintBacked[ref]; ownedByConstraint {
			result = append(result, p.constraintBackedIndexDropNode(ref))
			continue
		}
		dropIndexNode := ast.NewDropIndex(ref.Name).
			SetTable(ref.TableName)
		if guarded {
			dropIndexNode.SetIfExists()
		}
		// CONCURRENTLY on a drop is opt-in policy AND capability-gated, exactly
		// like the build side. A redefinition never reaches here (it is skipped
		// above as an addition conflict), and a constraint's backing index left
		// through the branch above, so a concurrent drop is always a standalone
		// index removal — never the drop half of a rebuild and never an index
		// backing a constraint, both of which PostgreSQL refuses to drop
		// concurrently.
		if p.usesConcurrentIndexDrop(ref) && p.capabilities().Has(capability.DropIndexConcurrently) {
			dropIndexNode.SetConcurrently()
		}
		result = append(result, dropIndexNode)
	}
	return result
}

// constraintBackedIndexDropNode removes an index a UNIQUE constraint enforces,
// through the constraint.
//
// PostgreSQL does not accept the index spelling for one: `DROP INDEX
// "uq_users_email"` comes back as `cannot drop index uq_users_email because
// constraint uq_users_email on table users requires it (SQLSTATE 2BP01)`,
// measured on 17.10. Dropping the constraint takes its index with it, which is
// what the pinned community binary v1.3.0 plans for the same change:
// `ALTER TABLE "users" DROP CONSTRAINT "uq_users_email"`, followed by the
// CREATE INDEX that replaces it when there is one. The comparator marks these
// removals (ConstraintBackedIndexRemovals) because it is the side that read the
// constraint catalog.
//
// IF EXISTS is unconditional, matching removeConstraints: every supported
// PostgreSQL line accepts it on DROP CONSTRAINT, and the DropIndexIfExists
// capability speaks for the DROP INDEX spelling only.
func (p *Planner) constraintBackedIndexDropNode(ref types.IndexRef) ast.Node {
	return &ast.AlterTableNode{
		Name: ref.TableName,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: ref.Name,
			IfExists:       true,
		}},
	}
}

// appendSkipComments emits one clearly-marked comment per change omitted by the
// diff policy, so the omission is visible in the generated migration rather than
// silent.
func appendSkipComments(result []ast.Node, skipped []diffpolicy.SkippedChange) []ast.Node {
	for _, change := range skipped {
		result = append(result, ast.NewComment(change.Comment()))
	}
	return result
}

func (p *Planner) removeTables(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, tableName := range deporder.TableDropOrder(diff.TablesRemoved, generated) {
		dropTableNode := ast.NewDropTable(tableName).
			SetIfExists().
			SetCascade().
			SetComment("WARNING: This will delete all data!")

		result = append(result, dropTableNode)
	}
	return result
}

func (p *Planner) removeEnums(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, enumName := range diff.EnumsRemoved {
		dropTypeNode := ast.NewDropType(enumName).
			SetIfExists().
			SetCascade().
			SetComment("WARNING: Make sure no tables use this enum!")

		result = append(result, dropTypeNode)
	}
	return result
}

// plannedUserType pairs a user-defined type's dependency identity with the node
// that creates it, so one ordering covers domains, ranges and composites
// together.
type plannedUserType struct {
	dep  deporder.UserType
	node ast.Node
}

// addNewUserTypes emits CREATE DOMAIN / CREATE TYPE for newly added domains,
// ranges, and composite types. It runs before tables so columns can reference
// them, and orders the three kinds against each other so a type is created
// before whatever names it.
//
// Emitting kind by kind is not enough, because the three kinds name each other
// in both directions. `CREATE DOMAIN d_comp AS addr` needs the composite `addr`
// first and `CREATE TYPE addr AS (f d_int)` needs the domain `d_int` first, so
// no fixed order of kinds can serve both. Emitting domains first sent
// `CREATE DOMAIN "d_comp" AS addr;` out ahead of `CREATE TYPE "addr" AS (...)`
// and the script stopped at `ERROR: type "addr" does not exist` -- measured on
// PostgreSQL 17.10 with psql -v ON_ERROR_STOP=1, exit 3.
//
// Enums are not in the set: they carry no reference to another user-defined
// type and are already emitted before this runs.
func (p *Planner) addNewUserTypes(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	planned := p.plannedUserTypes(diff, generated, diff.EffectiveIdentifierSemantics(p.targetDialect()))
	byName := make(map[string]ast.Node, len(planned))
	deps := make([]deporder.UserType, 0, len(planned))
	for _, userType := range planned {
		byName[userType.dep.Name] = userType.node
		deps = append(deps, userType.dep)
	}

	for _, name := range deporder.UserTypesForCreate(deps) {
		if node, ok := byName[name]; ok {
			result = append(result, node)
		}
	}
	return result
}

// plannedUserTypes collects every domain, range and composite this plan
// creates, newly added ones first and then the ones recreated in place of a
// modification, each with the type spellings its definition names.
func (p *Planner) plannedUserTypes(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []plannedUserType {
	var planned []plannedUserType
	for _, name := range diff.DomainsAdded {
		if domain := findDomain(generated.Domains, name, semantics); domain != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: name, References: []string{domain.BaseType}},
				node: fromschema.FromDomain(*domain),
			})
		}
	}
	for _, name := range diff.RangesAdded {
		if rangeType := findRange(generated.Ranges, name, semantics); rangeType != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: name, References: []string{rangeType.Subtype}},
				node: fromschema.FromRange(*rangeType),
			})
		}
	}
	for _, name := range diff.CompositeTypesAdded {
		if composite := findCompositeType(generated.CompositeTypes, name, semantics); composite != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: name, References: compositeFieldTypes(*composite)},
				node: fromschema.FromCompositeType(*composite),
			})
		}
	}
	// A modification with no in-place ALTER is handled as drop + recreate. The
	// recreations join the same ordering: a new domain over a recreated
	// composite has to wait for the recreation, which dropModifiedUserTypes has
	// already removed by this point.
	for _, domainDiff := range diff.DomainsModified {
		if domainIsAlterableInPlace(domainDiff) {
			// Paired with the same guard in dropModifiedUserTypes: no drop was
			// emitted, so there is nothing to put back.
			continue
		}
		if domain := findDomain(generated.Domains, domainDiff.DomainName, semantics); domain != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: domainDiff.DomainName, References: []string{domain.BaseType}},
				node: fromschema.FromDomain(*domain).SetComment(fmt.Sprintf("Recreate domain %s", domainDiff.DomainName)),
			})
		}
	}
	for _, compositeDiff := range diff.CompositeTypesModified {
		if composite := findCompositeType(generated.CompositeTypes, compositeDiff.TypeName, semantics); composite != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: compositeDiff.TypeName, References: compositeFieldTypes(*composite)},
				node: fromschema.FromCompositeType(*composite).SetComment(fmt.Sprintf("Recreate composite type %s", compositeDiff.TypeName)),
			})
		}
	}
	// PostgreSQL has no ALTER TYPE ... AS RANGE, so a changed range type takes
	// the same drop-and-recreate path domains and composites already use
	// (stokaro/ptah#931 item 2).
	for _, rangeDiff := range diff.RangesModified {
		if rangeType := findRange(generated.Ranges, rangeDiff.RangeName, semantics); rangeType != nil {
			planned = append(planned, plannedUserType{
				dep:  deporder.UserType{Name: rangeDiff.RangeName, References: []string{rangeType.Subtype}},
				node: fromschema.FromRange(*rangeType).SetComment(fmt.Sprintf("Recreate range type %s", rangeDiff.RangeName)),
			})
		}
	}
	return planned
}

func compositeFieldTypes(composite goschema.CompositeType) []string {
	references := make([]string, 0, len(composite.Fields))
	for _, field := range composite.Fields {
		references = append(references, field.Type)
	}
	return references
}

// removeUserTypes emits DROP DOMAIN / DROP TYPE for removed and modified
// domains, ranges, and composite types. Modified objects are dropped here and
// recreated by addNewUserTypes (there is no in-place ALTER for these forms).
func (p *Planner) removeUserTypes(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.DomainsRemoved {
		result = append(result, ast.NewDropType(name).SetDomain().SetIfExists().SetCascade().
			SetComment("WARNING: Make sure no columns use this domain!"))
	}
	for _, name := range diff.CompositeTypesRemoved {
		result = append(result, ast.NewDropType(name).SetIfExists().SetCascade().
			SetComment("WARNING: Make sure no columns use this composite type!"))
	}
	for _, name := range diff.RangesRemoved {
		result = append(result, ast.NewDropType(name).SetIfExists().SetCascade().
			SetComment("WARNING: Make sure no columns use this range type!"))
	}
	return result
}

// dropModifiedUserTypes drops domains/composites that changed, so
// addNewUserTypes can recreate them in their new shape.
//
// The drop is deliberately NOT CASCADE: a domain/composite that is still in use
// by a column cannot be dropped, so the migration fails loudly rather than
// silently dropping dependent columns. Reconciling a modification while the type
// is in use requires a manual migration (PostgreSQL offers ALTER DOMAIN /
// ALTER TYPE for the in-place cases).
//
// The order comes from the CURRENT definitions the diff carries, not from the
// desired ones addNewUserTypes creates in. A DROP executes against the database
// as it stands, so the only references that can block it are the ones that
// database holds now: `DROP TYPE cc` fails while `CREATE DOMAIN dd AS cc` is
// still on disk, whatever the desired schema says either type should become.
//
// The two graphs disagree exactly when the modification is what moved the
// reference, and no single order serves both. That is not a conflict to
// resolve, because they answer different questions: the create graph is the
// shape being built, the drop graph is the shape being taken apart.
//
// # The drop is emitted only where the recreate is
//
// This step and addNewUserTypes are two halves of one statement pair, and they
// used to disagree about what they cover: the drop was emitted for every
// modified entry unconditionally, while the recreate was emitted only where the
// target definition could be resolved out of the schema. A modification whose
// definition did not resolve therefore produced a DROP with nothing to put the
// type back, which is not a failed migration -- it is a successful one that
// leaves the database short of a type.
//
// Resolving both halves through the same lookup is what removes the disagreement
// in the cases it can. Where the definition still does not resolve, the pair is
// withheld and a comment says so: a type Ptah cannot rebuild is a type Ptah must
// not drop.
func (p *Planner) dropModifiedUserTypes(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	byName := make(map[string]ast.Node, len(diff.DomainsModified)+len(diff.CompositeTypesModified))
	deps := make([]deporder.UserType, 0, len(diff.DomainsModified)+len(diff.CompositeTypesModified))
	var unresolved []ast.Node
	for _, domainDiff := range diff.DomainsModified {
		if domainIsAlterableInPlace(domainDiff) {
			// alterModifiedDomains reconciles this one with ALTER DOMAIN.
			// Dropping it here as well would take the domain apart to apply a
			// change that needed no such thing -- and would fail outright on
			// any domain a column uses.
			continue
		}
		if findDomain(generated.Domains, domainDiff.DomainName, semantics) == nil {
			unresolved = append(unresolved, unrecreatableUserTypeComment("domain", domainDiff.DomainName))
			continue
		}
		byName[domainDiff.DomainName] = ast.NewDropType(domainDiff.DomainName).SetDomain().SetIfExists().
			SetComment("Recreate modified domain; drop is non-CASCADE and fails if the domain is in use")
		deps = append(deps, deporder.UserType{Name: domainDiff.DomainName, References: currentDomainReferences(domainDiff)})
	}
	for _, compositeDiff := range diff.CompositeTypesModified {
		if findCompositeType(generated.CompositeTypes, compositeDiff.TypeName, semantics) == nil {
			unresolved = append(unresolved, unrecreatableUserTypeComment("composite type", compositeDiff.TypeName))
			continue
		}
		byName[compositeDiff.TypeName] = ast.NewDropType(compositeDiff.TypeName).SetIfExists().
			SetComment("Recreate modified composite type; drop is non-CASCADE and fails if the type is in use")
		deps = append(deps, deporder.UserType{Name: compositeDiff.TypeName, References: compositeDiff.CurrentFieldTypes})
	}
	for _, rangeDiff := range diff.RangesModified {
		if findRange(generated.Ranges, rangeDiff.RangeName, semantics) == nil {
			unresolved = append(unresolved, unrecreatableUserTypeComment("range type", rangeDiff.RangeName))
			continue
		}
		byName[rangeDiff.RangeName] = ast.NewDropType(rangeDiff.RangeName).SetIfExists().
			SetComment("Recreate modified range type; drop is non-CASCADE and fails if the type is in use")
		deps = append(deps, deporder.UserType{Name: rangeDiff.RangeName, References: currentRangeReferences(rangeDiff)})
	}

	result = append(result, unresolved...)
	for _, name := range deporder.UserTypesForDrop(deps) {
		if node, ok := byName[name]; ok {
			result = append(result, node)
		}
	}
	return result
}

// unrecreatableUserTypeComment reports a modification whose target definition the
// schema does not hold, in place of the DROP that would have had no recreate.
func unrecreatableUserTypeComment(kind, name string) ast.Node {
	return ast.NewComment(fmt.Sprintf(
		"WARNING: %s %s changed, but the target definition was not found in the schema; "+
			"neither DROP nor CREATE emitted, manual migration required",
		kind,
		name,
	))
}

// currentDomainReferences reports the from-side base type of a modified domain,
// or nothing when the diff does not carry one. It never reads Changes: that map
// is a human-readable "old -> new" rendering, and recovering a type name by
// splitting prose is not a basis for statement ordering.
func currentDomainReferences(domainDiff types.DomainDiff) []string {
	if domainDiff.CurrentBaseType == "" {
		return nil
	}
	return []string{domainDiff.CurrentBaseType}
}

// currentRangeReferences reports the from-side subtype of a modified range, for
// the same reason and with the same Changes-is-prose caveat as
// currentDomainReferences.
func currentRangeReferences(rangeDiff types.RangeDiff) []string {
	if rangeDiff.CurrentSubtype == "" {
		return nil
	}
	return []string{rangeDiff.CurrentSubtype}
}

func findDomain(domains []goschema.Domain, name string, semantics identifier.Semantics) *goschema.Domain {
	return objectlookup.Qualified(domains, name, semantics)
}

func findCompositeType(
	composites []goschema.CompositeType,
	name string,
	semantics identifier.Semantics,
) *goschema.CompositeType {
	return objectlookup.Qualified(composites, name, semantics)
}

func findRange(ranges []goschema.Range, name string, semantics identifier.Semantics) *goschema.Range {
	return objectlookup.Qualified(ranges, name, semantics)
}

// GenerateMigrationASTChecked generates PostgreSQL-specific migration AST statements from schema differences.
//
// This method transforms the schema differences captured in the SchemaDiff into executable
// PostgreSQL AST statements that can be applied to bring the database schema in line with the target
// schema. The generated AST follows PostgreSQL-specific syntax and best practices.
//
// # Migration Order
//
// The SQL statements are generated in a specific order to avoid dependency conflicts:
//  1. Create new enum types (required before tables that use them)
//  2. Modify existing enum types (add new values)
//  3. Create new tables
//  4. Modify existing tables (add/modify/remove columns)
//  5. Add new indexes
//  6. Remove indexes (safe operations)
//  7. Remove tables (dangerous - commented out by default)
//  8. Remove enum types (dangerous - commented out by default)
//
// # PostgreSQL-Specific Features
//
//   - Native ENUM types with CREATE TYPE and ALTER TYPE statements
//   - SERIAL columns for auto-increment functionality
//   - Proper handling of enum value limitations (cannot remove values easily)
//   - PostgreSQL-specific syntax for ALTER statements
//
// # Parameters
//
//   - diff: The schema differences to be applied
//   - generated: The target schema parsed from Go struct annotations
//
// # Examples
//
// Basic enum and table creation:
//
//	diff := &differtypes.SchemaDiff{
//		EnumsAdded:  []string{"user_status"},
//		TablesAdded: []string{"users"},
//	}
//
//	generated := &goschema.Database{
//		Enums: []goschema.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []goschema.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []goschema.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//			{Name: "status", Type: "user_status", StructName: "User"},
//		},
//	}
//
//	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
//	if err != nil {
//		return err
//	}
//	// Results in:
//	// 1. CREATE TYPE user_status AS ENUM ('active', 'inactive');
//	// 2. CREATE TABLE users (id SERIAL PRIMARY KEY, status user_status);
//
// Table modification with column changes:
//
//	diff := &differtypes.SchemaDiff{
//		TablesModified: []differtypes.TableDiff{
//			{
//				TableName:    "users",
//				ColumnsAdded: []string{"email"},
//				ColumnsModified: []differtypes.ColumnDiff{
//					{ColumnName: "name", Changes: map[string]string{"type": "VARCHAR(255)"}},
//				},
//			},
//		},
//	}
//	// Results in ALTER TABLE statements for adding and modifying columns
//
// # Return Value
//
// Returns a slice of AST nodes representing SQL statements or an error when
// the diff cannot be planned safely. Each node can be rendered to SQL using a
// PostgreSQL-specific visitor.
//
// # Object kinds the target cannot host
//
// No phase below asks whether the target hosts the object kind it emits. The
// planner emits the node and the renderer answers, because the renderer is the
// one component both this path and the offline `schema render` converter pass
// through. Keeping that answer in one place prevents the plan path from
// silently dropping an unsupported object while `schema render` reports it
// differently (stokaro/ptah#929).
func (p *Planner) GenerateMigrationASTChecked(diff *types.SchemaDiff, generated *goschema.Database) ([]ast.Node, error) {
	var result []ast.Node
	if generated == nil {
		generated = &goschema.Database{}
	}
	if err := p.validateExtensionInstallationSchemas(diff, generated); err != nil {
		return nil, err
	}
	result, err := p.planExtensionChanges(result, diff)
	if err != nil {
		return nil, err
	}
	// One set of identifier rules for the whole plan. Every question of the form
	// "do these two spellings name the same object" is answered with it, so the
	// resolvers below and the statements that accompany what they resolve cannot
	// disagree about which table a reference belongs to.
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	indexes, err := indexscope.NewResolverWithSemantics(
		p.targetDialect(),
		semantics,
		diff,
		generated,
	)
	if err != nil {
		return nil, err
	}
	// Row-level security references are validated with the indexes, before any
	// node is emitted, for the reason the index resolver is: a reference the
	// target schema cannot resolve used to be skipped, so the plan came back
	// successful with an access-control operation missing from it
	// (stokaro/ptah#1311). Validating the diff as it arrived, ahead of the skip
	// policy below, means a malformed reference is refused even when the policy
	// would have removed it -- the diff is either coherent or it is not.
	policies, err := rlsscope.NewResolverWithSemantics(
		p.targetDialect(),
		semantics,
		diff,
		generated,
	)
	if err != nil {
		return nil, err
	}

	// Apply the diff policy first so skipped destructive changes never reach the
	// per-object emission below (and so a skipped DROP never trips the coarse
	// destructive gate downstream). The omissions are surfaced as comments.
	if !p.skip.Empty() {
		var skipped []diffpolicy.SkippedChange
		diff, skipped = diffpolicy.ApplyForDialect(diff, p.skip, p.targetDialect())
		result = appendSkipComments(result, skipped)
	}

	// 0. Create the schemas this migration adds objects to, before anything is
	// created in them. Every phase below can name a schema, so this cannot sit
	// inside one of them.
	result = p.addSchemaPreconditions(result, diff, generated)

	// 0b. Add new extensions (PostgreSQL extensions should be created before other objects)
	result = p.addNewExtensions(result, diff, generated)

	// 1. Add new roles (roles may be referenced by RLS policies and functions)
	result = p.addNewRoles(result, diff, generated)

	// 2. Add new functions (functions may be used by RLS policies)
	result = p.addNewFunctions(result, diff, generated)

	// 2b. Modify existing function definitions (body, volatility, security, language).
	// PostgreSQL CREATE OR REPLACE FUNCTION updates the live definition in place
	// without affecting policies or triggers that reference the function.
	result = p.modifyExistingFunctions(result, diff, generated)

	// 2c. Add new sequences before tables, since a table column may draw its
	// DEFAULT from a sequence. OWNED BY is applied later, after tables exist.
	result = p.addNewSequences(result, diff, generated)

	// 3. Add new enums (PostgreSQL requires enum types to exist before tables use them)
	result = p.addNewEnums(result, diff, generated)

	// 3c. Recreate changed user-defined types (drop then create), then create
	// new domains/ranges/composites before tables can reference them.
	result = p.dropModifiedUserTypes(result, diff, generated)
	result = p.addNewUserTypes(result, diff, generated)

	// 3d. Change in place the domains that need no rebuild. It follows the
	// creations so that a domain added in this same plan is not also altered,
	// and it stays ahead of the tables, whose columns take their default and
	// their constraint from the domain as it is when the column is created.
	result = p.alterModifiedDomains(result, diff, generated)

	// 4. Modify existing enums
	result = p.modifyExistingEnums(result, diff, generated)

	// 5. Add new tables
	result = p.addNewTables(result, diff, generated)

	// 6. Add and modify table columns (must be done before creating RLS policies that depend on columns)
	result = p.addAndModifyTableColumns(result, diff, generated)

	// 6.5. Add foreign key constraints for newly added columns (must be done after all columns exist)
	result = p.addForeignKeyConstraintsForModifiedTables(result, diff, generated)

	// 6.7. Associate added sequences with their owning table.column and apply
	// option changes to existing sequences, now that tables exist.
	result = p.addSequenceOwnership(result, diff, generated)
	result = p.modifyExistingSequences(result, diff, generated)

	// 6.6. Add and modify views, materialized views, and triggers after their tables/functions exist.
	result = p.addNewViewLikeObjects(result, diff, generated)
	result = p.modifyExistingViews(result, diff, generated)
	result = p.retargetSynonyms(result, diff, generated)
	result = p.addNewSynonyms(result, diff, generated)
	result = p.modifyExistingMaterializedViews(result, diff, generated)
	result = p.addNewTriggers(result, diff, generated)
	result = p.modifyExistingTriggers(result, diff, generated)

	// 7. Modify existing roles (must be done before RLS policies that reference them)
	result = p.modifyExistingRoles(result, diff, generated)

	// 7.5. Revoke removed grants before adding replacement grants.
	result = p.removeGrants(result, diff)
	result = p.revokeGrantOptions(result, diff)

	// 8. Enable RLS on tables (must be done after table creation and modification)
	result = p.enableRLSOnTables(result, diff, generated, semantics)

	// 9. Add RLS policies (must be done after RLS is enabled and columns exist)
	result, err = p.addNewRLSPolicies(result, diff, policies)
	if err != nil {
		return nil, err
	}
	result, err = p.modifyExistingRLSPolicies(result, diff, policies)
	if err != nil {
		return nil, err
	}

	// 9.5. Add role privilege grants after roles and target objects exist.
	result = p.addNewGrants(result, diff)

	// 10. Add new indexes
	result, err = p.addNewIndexes(result, diff, indexes)
	if err != nil {
		return nil, err
	}

	// 10.5. Add new constraints (must be done after tables and columns exist)
	result = p.addNewConstraints(result, diff, generated)

	// 10.6. Add field-level foreign keys for new tables after referenced
	// unique indexes and constraints have been created.
	result = p.addForeignKeyConstraintsForNewTables(result, diff, generated)

	// 11. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff)

	// 12. Remove RLS policies (must be done before disabling RLS and before dropping columns)
	result = p.removeRLSPolicies(result, diff)

	// 11. Disable RLS on tables (must be done after removing policies)
	result = p.disableRLSOnTables(result, diff)

	// 12. Remove table columns (must be done after removing RLS policies that depend on columns)
	result = p.removeTableColumns(result, diff)

	// 12.4. Row-level TTL, after the columns a TTL expression may refer to
	// exist and before anything is dropped. A policy whose expression names a
	// column added in the same plan cannot be set before that column is there
	// (stokaro/ptah#1027).
	if p.planningRowTTL() {
		result = p.applyRowTTLChanges(result, diff)
	}

	// 12.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff)

	// 12.6. Remove triggers and view-like objects before dropping tables/functions they depend on.
	result = p.removeTriggers(result, diff)
	result = p.removeMaterializedViews(result, diff)
	result = p.removeViews(result, diff)
	result = p.removeSynonyms(result, diff)

	// 13. Remove tables (dangerous!)
	result = p.removeTables(result, diff, generated)

	// 13.5. Remove standalone sequences after tables that may default from them.
	result = p.removeSequences(result, diff)

	// 13. Remove functions (must be done after removing policies that might use them)
	result = p.removeFunctions(result, diff)

	// 14. Remove roles (must be done after removing functions and policies that depend on them)
	result = p.removeRoles(result, diff)

	// 15. Remove enums (dangerous!)
	result = p.removeUserTypes(result, diff)
	result = p.removeEnums(result, diff)

	// 16. Remove extensions (dangerous!)
	result = p.removeExtensions(result, diff)

	return result, nil
}

func (p *Planner) validateExtensionInstallationSchemas(diff *types.SchemaDiff, generated *goschema.Database) error {
	if p.targetDialect() == platform.Postgres || p.targetDialect() == platform.YugabyteDB || diff == nil {
		return nil
	}
	for _, name := range diff.ExtensionsAdded {
		for _, extension := range generated.Extensions {
			if extension.Name == name && extension.Schema != "" {
				return fmt.Errorf(
					"%w: %s does not support PostgreSQL extension installation schema %q for extension %q",
					ptaherr.ErrUnsupportedFeature,
					p.targetDialect(),
					extension.Schema,
					extension.Name,
				)
			}
		}
	}
	if len(diff.ExtensionsModified) > 0 {
		change := diff.ExtensionsModified[0]
		return fmt.Errorf(
			"%w: %s does not support PostgreSQL extension installation schema placement for extension %q",
			ptaherr.ErrUnsupportedFeature,
			p.targetDialect(),
			change.Name,
		)
	}
	return nil
}

func (p *Planner) addNewRoles(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, roleName := range diff.RolesAdded {
		// Find the role definition
		for _, role := range generated.Roles {
			if role.Name == roleName {
				roleNode := fromschema.FromRole(role)
				result = append(result, roleNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) modifyExistingRoles(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, roleDiff := range diff.RolesModified {
		targetRole := p.findTargetRole(roleDiff.RoleName, generated)
		if targetRole == nil {
			continue // Skip if role not found in target schema
		}

		alterRoleNode := p.buildAlterRoleNode(roleDiff, targetRole)
		if len(alterRoleNode.Operations) > 0 {
			alterRoleNode.SetComment(fmt.Sprintf("Modify role %s attributes", roleDiff.RoleName))
			result = append(result, alterRoleNode)
		}
	}
	return result
}

// findTargetRole finds a role by name in the generated database schema
func (p *Planner) findTargetRole(roleName string, generated *goschema.Database) *goschema.Role {
	for _, role := range generated.Roles {
		if role.Name == roleName {
			return &role
		}
	}
	return nil
}

// buildAlterRoleNode creates an ALTER ROLE node with operations based on role changes
func (p *Planner) buildAlterRoleNode(roleDiff types.RoleDiff, targetRole *goschema.Role) *ast.AlterRoleNode {
	alterRoleNode := ast.NewAlterRole(roleDiff.RoleName)

	// Sorted key order keeps the ALTER ROLE operation order deterministic
	// across runs (issue #59).
	for _, changeType := range slices.Sorted(maps.Keys(roleDiff.Changes)) {
		p.addRoleOperation(alterRoleNode, changeType, roleDiff.Changes[changeType], targetRole)
	}

	return alterRoleNode
}

// addRoleOperation adds the appropriate operation to the ALTER ROLE node based on change type and value
func (p *Planner) addRoleOperation(alterRoleNode *ast.AlterRoleNode, changeType, changeValue string, targetRole *goschema.Role) {
	switch changeType {
	case "login":
		p.addLoginOperation(alterRoleNode, changeValue)
	case "password":
		p.addPasswordOperation(alterRoleNode, changeValue, targetRole)
	case "superuser":
		p.addSuperuserOperation(alterRoleNode, changeValue)
	case "createdb", "create_db":
		p.addCreateDBOperation(alterRoleNode, changeValue)
	case "createrole", "create_role":
		p.addCreateRoleOperation(alterRoleNode, changeValue)
	case "inherit":
		p.addInheritOperation(alterRoleNode, changeValue)
	case "replication":
		p.addReplicationOperation(alterRoleNode, changeValue)
	}
}

// addLoginOperation adds a login operation to the ALTER ROLE node
func (p *Planner) addLoginOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetLoginOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetLoginOperation(false))
	}
}

// addSuperuserOperation adds a superuser operation to the ALTER ROLE node
func (p *Planner) addSuperuserOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetSuperuserOperation(false))
	}
}

// addCreateDBOperation adds a createdb operation to the ALTER ROLE node
func (p *Planner) addCreateDBOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetCreateDBOperation(false))
	}
}

// addCreateRoleOperation adds a createrole operation to the ALTER ROLE node
func (p *Planner) addCreateRoleOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetCreateRoleOperation(false))
	}
}

// addInheritOperation adds an inherit operation to the ALTER ROLE node
func (p *Planner) addInheritOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetInheritOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetInheritOperation(false))
	}
}

// addReplicationOperation adds a replication operation to the ALTER ROLE node
func (p *Planner) addReplicationOperation(alterRoleNode *ast.AlterRoleNode, changeValue string) {
	if strings.Contains(changeValue, "-> true") {
		alterRoleNode.AddOperation(ast.NewSetReplicationOperation(true))
	} else if strings.Contains(changeValue, "-> false") {
		alterRoleNode.AddOperation(ast.NewSetReplicationOperation(false))
	}
}

// addPasswordOperation adds a password operation to the ALTER ROLE node
func (p *Planner) addPasswordOperation(alterRoleNode *ast.AlterRoleNode, changeValue string, targetRole *goschema.Role) {
	if changeValue == "password_update_required" {
		// Use the target role to get the new password
		if targetRole != nil && targetRole.Password != "" {
			alterRoleNode.AddOperation(ast.NewSetPasswordOperation(targetRole.Password))
		}
	}
}

func (p *Planner) removeRoles(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, roleName := range diff.RolesRemoved {
		dropRoleNode := ast.NewDropRole(roleName).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this role")
		result = append(result, dropRoleNode)
	}
	return result
}

func (p *Planner) addNewGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsAdded {
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(grant.WithOption)
		result = append(result, node)
	}
	for _, grant := range diff.GrantOptionsAdded {
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetWithOption(true)
		result = append(result, node)
	}
	return result
}

func (p *Planner) removeGrants(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantsRemoved {
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege})
		result = append(result, node)
	}
	return result
}

func (p *Planner) revokeGrantOptions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, grant := range diff.GrantOptionsRevoked {
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetGrantOptionFor(true)
		result = append(result, node)
	}
	return result
}

func (p *Planner) addNewExtensions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, extensionName := range diff.ExtensionsAdded {
		// Find the extension definition
		for _, ext := range generated.Extensions {
			if ext.Name == extensionName {
				extensionNode := fromschema.FromExtension(ext)
				result = append(result, extensionNode)
				break
			}
		}
	}
	return result
}

func (p *Planner) removeExtensions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// Generate DROP EXTENSION statements with comprehensive safety warnings
	// Extension removal is potentially dangerous and requires careful consideration
	for i, extensionName := range diff.ExtensionsRemoved {
		// Add comprehensive warning comments before each DROP EXTENSION statement
		warningComment1 := ast.NewComment(fmt.Sprintf("WARNING: Removing extension '%s' may break existing functionality that depends on it", extensionName))
		warningComment2 := ast.NewComment("Consider reviewing all database objects that use this extension before proceeding")
		warningComment3 := ast.NewComment("Extension removal may cascade to dependent objects - review carefully")

		result = append(result, warningComment1)
		result = append(result, warningComment2)
		result = append(result, warningComment3)

		// Create DROP EXTENSION statement with IF EXISTS for safety
		dropExtension := ast.NewDropExtension(extensionName).
			SetIfExists().
			SetComment(fmt.Sprintf("Remove extension '%s' as it's no longer required by the schema", extensionName))

		result = append(result, dropExtension)

		// Add blank line for readability between extension removals (not after the last one)
		if i < len(diff.ExtensionsRemoved)-1 {
			blankLine := ast.NewComment("")
			result = append(result, blankLine)
		}
	}
	return result
}

func (p *Planner) addNewFunctions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, fn := range deporder.FunctionsForCreate(generated, diff.FunctionsAdded) {
		result = append(result, fromschema.FromFunction(fn))
	}
	return result
}

func (p *Planner) modifyExistingFunctions(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, fnDiff := range diff.FunctionsModified {
		// Find the target function definition. Without it we can't emit a
		// faithful CREATE OR REPLACE, so skip silently (the diff alone would
		// not tell us the new body/attributes).
		var target *goschema.Function
		for i := range generated.Functions {
			if generated.Functions[i].Name == fnDiff.FunctionName {
				target = &generated.Functions[i]
				break
			}
		}
		if target == nil {
			continue
		}

		functionNode := fromschema.FromFunction(*target)
		functionNode.SetComment(fmt.Sprintf("Modify function %s: %s", target.Name, summarizeFunctionChanges(fnDiff)))
		result = append(result, functionNode)
	}
	return result
}

// summarizeFunctionChanges produces a deterministic one-line summary of the
// changed attributes for use as a SQL comment.
func summarizeFunctionChanges(fnDiff types.FunctionDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(fnDiff.Changes)), ", ")
}

func (p *Planner) removeFunctions(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, functionName := range diff.FunctionsRemoved {
		dropFunctionNode := ast.NewDropFunction(functionName).
			SetIfExists().
			SetComment("WARNING: Ensure no other objects depend on this function")
		result = append(result, dropFunctionNode)
	}
	return result
}

// findSequence returns the generated sequence the diff entry names, under the
// identifier rules of the target dialect.
func findSequence(
	sequences []goschema.Sequence,
	name string,
	semantics identifier.Semantics,
) *goschema.Sequence {
	return objectlookup.Qualified(sequences, name, semantics)
}

// addNewSequences emits CREATE SEQUENCE for newly added sequences. The OWNED BY
// association is deliberately omitted here and emitted later by
// addSequenceOwnership, because a sequence referenced by a column DEFAULT must
// be created before its table while OWNED BY requires the table to already
// exist.
func (p *Planner) addNewSequences(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, name := range diff.SequencesAdded {
		sequence := findSequence(generated.Sequences, name, semantics)
		if sequence == nil {
			continue
		}
		sequenceNode := fromschema.FromSequence(*sequence)
		sequenceNode.OwnedBy = ""
		result = append(result, sequenceNode)
	}
	return result
}

// addSequenceOwnership emits ALTER SEQUENCE ... OWNED BY for newly added
// sequences that declare an owner, after their owning tables exist.
func (p *Planner) addSequenceOwnership(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, name := range diff.SequencesAdded {
		sequence := findSequence(generated.Sequences, name, semantics)
		if sequence == nil || sequence.OwnedBy == "" {
			continue
		}
		node := ast.NewAlterSequence(sequence.Name).SetOwnedBy(sequence.OwnedBy)
		if sequence.Schema != "" {
			node.SetSchema(sequence.Schema)
		}
		result = append(result, node)
	}
	return result
}

// modifyExistingSequences emits ALTER SEQUENCE for sequences whose options
// changed. Only the changed options (per the diff) are emitted.
func (p *Planner) modifyExistingSequences(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, sequenceDiff := range diff.SequencesModified {
		sequence := findSequence(generated.Sequences, sequenceDiff.SequenceName, semantics)
		if sequence == nil {
			continue
		}
		node := alterSequenceFromDiff(*sequence, sequenceDiff.Changes)
		node.SetComment(fmt.Sprintf("Modify sequence %s: %s", sequenceDiff.SequenceName, summarizeSequenceChanges(sequenceDiff)))
		result = append(result, node)
	}
	return result
}

// alterSequenceFromDiff builds an ALTER SEQUENCE node carrying only the options
// that the diff reports as changed, sourced from the target definition.
func alterSequenceFromDiff(target goschema.Sequence, changes map[string]string) *ast.AlterSequenceNode {
	node := ast.NewAlterSequence(target.Name)
	if target.Schema != "" {
		node.SetSchema(target.Schema)
	}
	if _, ok := changes["as"]; ok && target.AsType != "" {
		node.SetAs(target.AsType)
	}
	if _, ok := changes["start"]; ok && target.Start != nil {
		node.SetStart(*target.Start)
	}
	if _, ok := changes["increment"]; ok && target.Increment != nil {
		node.SetIncrement(*target.Increment)
	}
	if _, ok := changes["minvalue"]; ok && target.MinValue != nil {
		node.SetMinValue(*target.MinValue)
	}
	if _, ok := changes["maxvalue"]; ok && target.MaxValue != nil {
		node.SetMaxValue(*target.MaxValue)
	}
	if _, ok := changes["cache"]; ok && target.Cache != nil {
		node.SetCache(*target.Cache)
	}
	if _, ok := changes["cycle"]; ok {
		node.SetCycle(target.Cycle)
	}
	if _, ok := changes["owned_by"]; ok && target.OwnedBy != "" {
		node.SetOwnedBy(target.OwnedBy)
	}
	return node
}

// summarizeSequenceChanges produces a deterministic one-line summary of the
// changed options for use as a SQL comment.
func summarizeSequenceChanges(sequenceDiff types.SequenceDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(sequenceDiff.Changes)), ", ")
}

// removeSequences emits DROP SEQUENCE for sequences no longer present in the
// target schema. It runs after table removal so a table that draws a column
// default from the sequence is gone first.
func (p *Planner) removeSequences(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.SequencesRemoved {
		schemaName, sequenceName := splitQualifiedSequenceName(name)
		dropSequence := ast.NewDropSequence(sequenceName).
			SetIfExists().
			SetComment("WARNING: Ensure no column default still draws from this sequence")
		if schemaName != "" {
			dropSequence.SetSchema(schemaName)
		}
		result = append(result, dropSequence)
	}
	return result
}

// splitQualifiedSequenceName splits a "schema.name" qualified sequence name into
// its parts. An unqualified name yields an empty schema.
func splitQualifiedSequenceName(name string) (schema, sequence string) {
	ref, ok := tableref.Parse(name)
	if !ok {
		return "", name
	}
	return ref.Schema, ref.Name
}

func (p *Planner) addNewViewLikeObjects(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	objects := make([]deporder.ViewLike, 0, len(diff.ViewsAdded)+len(diff.MaterializedViewsAdded))
	for _, viewName := range diff.ViewsAdded {
		if view := findView(generated.Views, viewName, semantics); view != nil {
			objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body})
		}
	}
	for _, viewName := range diff.MaterializedViewsAdded {
		if view := findMaterializedView(generated.MaterializedViews, viewName, semantics); view != nil {
			objects = append(objects, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
		}
	}

	// The dialect is what lets a body reference resolve through PostgreSQL's
	// quoting rules. Without it a view referenced by its quoted qualified
	// spelling -- `"analytics"."base"` -- matches no declaration, gains no
	// dependency edge, and is created AFTER the view that reads it, so the plan
	// renders cleanly and fails when it runs.
	for _, object := range deporder.ViewLikesForCreateForDialect(objects, p.targetDialect()) {
		if object.Materialized {
			if view := findMaterializedView(generated.MaterializedViews, object.Name, semantics); view != nil {
				result = append(result, fromschema.FromMaterializedView(*view))
			}
			continue
		}
		if view := findView(generated.Views, object.Name, semantics); view != nil {
			result = append(result, fromschema.FromView(*view))
		}
	}
	return result
}

// modifyExistingViews re-renders each modified view, choosing between
// CREATE OR REPLACE VIEW and DROP + CREATE per view.
//
// Both statements cost something, and the two costs land in different places.
// PostgreSQL accepts CREATE OR REPLACE VIEW only for appending trailing columns;
// dropping, renaming or retyping a projected column is refused at execution
// time, so the replace can render perfectly and fail when it runs. The drop
// always applies, but it carries CASCADE: it takes dependent views and
// materialized views with it, along with the privileges granted on the view
// itself, none of which the replace disturbs.
//
// So the choice is made per view from what viewReplaceLegality can prove, and
// the undecidable case is resolved by the direction being planned:
//
//	appends columns    replace, both directions -- PostgreSQL accepts it
//	moves columns      drop and recreate, both directions -- the replace would
//	                   be refused, and rendering a statement we know the engine
//	                   rejects helps nobody
//	undecidable        replace going forward, drop on a rollback. Forward, a
//	                   body this parser cannot read is usually a predicate-only
//	                   edit to a WITH / star / set-operation view, where the
//	                   column list never moves and the replace is accepted; if
//	                   it is not, PostgreSQL says so and the migration stops
//	                   with nothing destroyed. A rollback cannot afford that
//	                   answer -- it runs while an operator is undoing a
//	                   migration, and a rollback that fails is discovered during
//	                   the incident it was meant to end -- so it takes the
//	                   statement that always applies.
//
// Whatever the drop path takes with it, this step puts back: every declared view
// and materialized view that reads a dropped view, transitively, is recreated
// after it. Without that the plan silently left the database short of the schema
// it was generated from, and re-planning did not converge (issue #1287). What
// CASCADE removes and this cannot rebuild is anything Ptah does not declare -- a
// hand-made view, a rule, privileges on the view -- which is why the drop is
// taken only where it buys something.
//
// The drop and the create are emitted from inside the modify step, not by
// pushing the view into ViewsRemoved and ViewsAdded. The plan runs additions
// before removals, so a modification expressed from outside comes out
// create-then-drop and ends with no view at all.
func (p *Planner) modifyExistingViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	var dropped []string
	replaced := make([]deporder.ViewLike, 0, len(diff.ViewsModified))
	for _, viewDiff := range diff.ViewsModified {
		view := objectlookup.View(generated.Views, viewDiff.ViewName, semantics)
		if view == nil {
			continue
		}
		if viewReplaceKeepsDependents(viewDiff, view.Body) {
			replaced = append(replaced, deporder.ViewLike{Name: view.Name, Body: view.Body})
			continue
		}
		dropped = append(dropped, view.Name)
	}

	for _, name := range dropped {
		result = append(result, ast.NewDropView(name).SetIfExists().SetCascade())
	}

	// A view on the replace path can also be a dependent of one on the drop
	// path, in which case it is on both lists and must still be rendered once.
	recreate := viewLikesLostToCascade(generated, dropped, semantics)
	named := make(map[string]bool, len(recreate))
	for _, object := range recreate {
		named[object.Name] = true
	}
	for _, object := range replaced {
		if !named[object.Name] {
			recreate = append(recreate, object)
		}
	}

	for _, object := range deporder.ViewLikesForCreateForDialect(recreate, p.targetDialect()) {
		result = p.appendViewLikeRecreate(result, generated, object, dropped, semantics)
	}
	return result
}

// appendViewLikeRecreate renders the CREATE that puts one view-like object back.
//
// A view this step dropped itself is created outright. Everything else is
// re-asserted rather than created blind: a dependent is on the list because its
// body names a dropped view in code, which is a syntactic test rather than a
// resolved one, and a CREATE OR REPLACE costs nothing if the object survived
// after all while a bare CREATE would fail on "already exists". A materialized
// view has no in-place replace, so it is dropped first for the same reason.
func (p *Planner) appendViewLikeRecreate(
	result []ast.Node,
	generated *goschema.Database,
	object deporder.ViewLike,
	dropped []string,
	semantics identifier.Semantics,
) []ast.Node {
	if object.Materialized {
		view := objectlookup.MaterializedView(generated.MaterializedViews, object.Name, semantics)
		if view == nil {
			return result
		}
		result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists().SetCascade())
		return append(result, fromschema.FromMaterializedView(*view))
	}

	view := objectlookup.View(generated.Views, object.Name, semantics)
	if view == nil {
		return result
	}
	if slices.Contains(dropped, object.Name) {
		return append(result, fromschema.FromView(*view))
	}
	return append(result, fromschema.FromView(*view).SetReplace())
}

// viewReplaceKeepsDependents decides whether one modified view keeps the
// in-place replace, which is the statement that leaves dependents and grants
// alone. The table in modifyExistingViews explains the three answers.
func viewReplaceKeepsDependents(viewDiff types.ViewDiff, targetBody string) bool {
	switch viewReplaceLegality(viewDiff.PreviousBody, targetBody) {
	case viewReplaceAppendsColumns:
		return true
	case viewReplaceMovesColumns:
		return false
	default:
		return !viewDiff.Rollback
	}
}

// viewLikesLostToCascade returns the declared views and materialized views that
// DROP VIEW ... CASCADE removes when it drops the named views, the dropped views
// themselves included, so the caller can put every one of them back.
//
// Dependency is read off the declared bodies, the same test that orders view
// creation, and it is applied transitively: dropping a view takes the view that
// reads it, and the view that reads that one.
//
// The test reads the code of those bodies and not their text. A name inside a
// string literal or a comment reads nothing, and putting it on this list is not
// a harmless over-approximation: everything on the list is answered with a
// statement, and for a materialized view that statement is
// DROP MATERIALIZED VIEW ... CASCADE. On PostgreSQL 17.10 that took a hand-made
// dependent view, a unique index and a GRANT off an object no part of the
// migration touched -- none of them declared, so nothing put them back.
func viewLikesLostToCascade(
	generated *goschema.Database,
	dropped []string,
	semantics identifier.Semantics,
) []deporder.ViewLike {
	if len(dropped) == 0 {
		return nil
	}

	candidates := make([]deporder.ViewLike, 0, len(generated.Views)+len(generated.MaterializedViews))
	for _, view := range generated.Views {
		candidates = append(candidates, deporder.ViewLike{Name: view.Name, Body: view.Body})
	}
	for _, view := range generated.MaterializedViews {
		candidates = append(candidates, deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true})
	}

	lost := make([]deporder.ViewLike, 0, len(dropped))
	taken := make(map[string]bool, len(dropped))
	frontier := make([]string, 0, len(dropped))
	for _, name := range dropped {
		if taken[name] {
			continue
		}
		taken[name] = true
		frontier = append(frontier, name)
		if view := objectlookup.View(generated.Views, name, semantics); view != nil {
			lost = append(lost, deporder.ViewLike{Name: view.Name, Body: view.Body})
		}
	}

	for len(frontier) > 0 {
		gone := frontier[0]
		frontier = frontier[1:]
		for _, candidate := range candidates {
			if taken[candidate.Name] || !deporder.ReferencesIdentifier(candidate.Body, gone) {
				continue
			}
			taken[candidate.Name] = true
			frontier = append(frontier, candidate.Name)
			lost = append(lost, candidate)
		}
	}
	return lost
}

func (p *Planner) removeViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, viewName := range diff.ViewsRemoved {
		result = append(result, ast.NewDropView(viewName).SetIfExists().SetCascade())
	}
	return result
}

func (p *Planner) modifyExistingMaterializedViews(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, viewDiff := range diff.MaterializedViewsModified {
		if view := findMaterializedView(generated.MaterializedViews, viewDiff.ViewName, semantics); view != nil {
			result = append(result, ast.NewDropMaterializedView(view.Name).SetIfExists().SetCascade())
			result = append(result, fromschema.FromMaterializedView(*view))
		}
	}
	return result
}

func (p *Planner) removeMaterializedViews(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, viewName := range diff.MaterializedViewsRemoved {
		result = append(result, ast.NewDropMaterializedView(viewName).SetIfExists().SetCascade())
	}
	return result
}

// addNewSynonyms emits the declared synonyms a diff adds.
//
// This planner backs PostgreSQL, CockroachDB, YugabyteDB and Spanner, none of
// which has a synonym object. The node is emitted anyway and the renderer names
// it as skipped, which is the same contract every other kind this family lacks
// follows: the plan and the render have to agree about which objects exist, and
// a planner that dropped the node instead would make a declared object vanish
// from the plan while the render still reported it.
func (p *Planner) addNewSynonyms(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, name := range diff.SynonymsAdded {
		if synonym := findSynonym(generated.Synonyms, name); synonym != nil {
			result = append(result, fromschema.FromSynonym(*synonym))
		}
	}
	return result
}

// retargetSynonyms drops and recreates a synonym whose target changed, in that
// order, because no dialect has an ALTER SYNONYM to do it in one statement.
func (p *Planner) retargetSynonyms(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	for _, synonymDiff := range diff.SynonymsModified {
		synonym := findSynonym(generated.Synonyms, synonymDiff.SynonymName)
		if synonym == nil {
			continue
		}
		result = append(result, ast.NewDropSynonym(synonymDiff.SynonymName).SetIfExists())
		result = append(result, fromschema.FromSynonym(*synonym))
	}
	return result
}

func (p *Planner) removeSynonyms(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, name := range diff.SynonymsRemoved {
		result = append(result, ast.NewDropSynonym(name).SetIfExists())
	}
	return result
}

// findSynonym returns the declared synonym with the given qualified name.
func findSynonym(synonyms []goschema.Synonym, name string) *goschema.Synonym {
	for i := range synonyms {
		if synonyms[i].QualifiedName() == name {
			return &synonyms[i]
		}
	}
	return nil
}

func (p *Planner) addNewTriggers(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, triggerRef := range diff.TriggersAdded {
		if trigger := findTrigger(generated.Triggers, triggerRef.TableName, triggerRef.TriggerName, semantics); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger))
		}
	}
	return result
}

func (p *Planner) modifyExistingTriggers(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, triggerDiff := range diff.TriggersModified {
		if trigger := findTrigger(generated.Triggers, triggerDiff.TableName, triggerDiff.TriggerName, semantics); trigger != nil {
			result = append(result, fromschema.FromTrigger(*trigger).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeTriggers(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, triggerRef := range diff.TriggersRemoved {
		functionName := goschema.Trigger{Name: triggerRef.TriggerName, Table: triggerRef.TableName}.FunctionName()
		result = append(result, ast.NewDropTrigger(triggerRef.TriggerName, triggerRef.TableName).
			SetIfExists().
			SetCascade().
			SetFunctionName(functionName))
	}
	return result
}

func findView(views []goschema.View, name string, semantics identifier.Semantics) *goschema.View {
	return objectlookup.View(views, name, semantics)
}

func findMaterializedView(
	views []goschema.MaterializedView,
	name string,
	semantics identifier.Semantics,
) *goschema.MaterializedView {
	return objectlookup.MaterializedView(views, name, semantics)
}

func findTrigger(
	triggers []goschema.Trigger,
	tableName, triggerName string,
	semantics identifier.Semantics,
) *goschema.Trigger {
	return objectlookup.Trigger(triggers, tableName, triggerName, semantics)
}

// enableRLSOnTables emits ALTER TABLE ... ENABLE ROW LEVEL SECURITY.
//
// Two sources feed it, and both are needed.
//
// RLSEnabledTablesAdded is the comparator's verdict on the desired schema's
// enablement declarations against pg_class.relrowsecurity. It is the only
// source that covers an existing table whose row-level security was turned off
// in the database, and a table that declares enablement without declaring a
// policy. Until stokaro/ptah#1284 nothing read it, so a database with RLS off
// and a schema demanding it on produced no statement at all.
//
// New tables carrying a policy are the second source. A desired schema may
// declare a policy without a separate enablement annotation, and CREATE POLICY
// on a table whose row-level security is off protects nothing, so the
// enablement is emitted with the table rather than left to the operator.
//
// Which table a policy belongs to is decided under the target's identifier
// semantics, the same rules [addNewRLSPolicies] resolves the policy itself
// with. `orders` and `public.orders` are one relation and two strings, so
// asking `slices.Contains(diff.TablesAdded, policy.Table)` answered no whenever
// the desired table and the policy's declaration were spelled differently: the
// plan emitted CREATE POLICY and no ENABLE ROW LEVEL SECURITY, applied cleanly,
// and left a pg_policy row on a relation whose pg_class.relrowsecurity was
// still false. The policy was inert and the plan reported success. Measured on
// PostgreSQL 17.10 -- the fourth appearance of one mistake, after
// stokaro/ptah#1276, stokaro/ptah#1311 and stokaro/ptah#1347.
//
// The map is keyed by identity and carries the spelling to render, so a table
// named by both sources is enabled once. The rendered spelling comes from the
// diff -- the comparator's own verdict, or the name the plan creates the table
// under -- rather than from the declaration, so the statement always names an
// object this plan is known to have produced.
func (p *Planner) enableRLSOnTables(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	semantics identifier.Semantics,
) []ast.Node {
	tablesNeedingRLS := make(map[string]string)
	rememberTable := func(tableName string) {
		key := semantics.QualifiedTableIdentityKey(tableName)
		if _, seen := tablesNeedingRLS[key]; !seen {
			tablesNeedingRLS[key] = tableName
		}
	}
	for _, tableName := range diff.RLSEnabledTablesAdded {
		rememberTable(tableName)
	}

	addedTables := make(map[string]string, len(diff.TablesAdded))
	for _, tableName := range diff.TablesAdded {
		key := semantics.QualifiedTableIdentityKey(tableName)
		if _, seen := addedTables[key]; !seen {
			addedTables[key] = tableName
		}
	}
	for _, policy := range generated.RLSPolicies {
		addedTable, isNew := addedTables[semantics.QualifiedTableIdentityKey(policy.Table)]
		if !isNew {
			continue
		}
		rememberTable(addedTable)
	}

	// Iterate in sorted order so migration output is deterministic (issue #59).
	for _, tableName := range slices.Sorted(maps.Values(tablesNeedingRLS)) {
		enableRLSNode := ast.NewAlterTableEnableRLS(tableName).
			SetComment(fmt.Sprintf("Enable RLS for %s table", tableName))
		result = append(result, enableRLSNode)
	}
	return result
}

// disableRLSOnTables emits ALTER TABLE ... DISABLE ROW LEVEL SECURITY for the
// tables the comparator recorded in RLSEnabledTablesRemoved, and keeps the
// advisory comment for a table that merely lost policies.
//
// A table that is being dropped is left out: DROP TABLE removes its row-level
// security with it, and disabling first would emit a statement whose only
// effect is on an object that no longer exists two statements later.
//
// Losing every policy is not the same as losing enablement. The desired schema
// may keep row-level security on to deny by default, which is what a table with
// enablement and no policy means, so a table with removed policies that the
// comparator did not list for disablement keeps its enablement and gets the
// comment.
func (p *Planner) disableRLSOnTables(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	droppedTables := make(map[string]bool, len(diff.TablesRemoved))
	for _, tableName := range diff.TablesRemoved {
		droppedTables[tableName] = true
	}

	tablesToDisable := make(map[string]bool)
	for _, tableName := range diff.RLSEnabledTablesRemoved {
		if droppedTables[tableName] {
			continue
		}
		tablesToDisable[tableName] = true
	}

	// Iterate in sorted order so migration output is deterministic (issue #59).
	for _, tableName := range slices.Sorted(maps.Keys(tablesToDisable)) {
		disableRLSNode := ast.NewAlterTableDisableRLS(tableName).
			SetComment(fmt.Sprintf("Disable RLS for %s table", tableName))
		result = append(result, disableRLSNode)
	}

	tablesWithRemovedPolicies := make(map[string]bool)
	for _, policyRef := range diff.RLSPoliciesRemoved {
		if tablesToDisable[policyRef.TableName] || droppedTables[policyRef.TableName] {
			continue
		}
		tablesWithRemovedPolicies[policyRef.TableName] = true
	}

	for _, tableName := range slices.Sorted(maps.Keys(tablesWithRemovedPolicies)) {
		warningComment := ast.NewComment(fmt.Sprintf("NOTE: RLS policies were removed from table %s - verify if RLS should be disabled", tableName))
		result = append(result, warningComment)
	}
	return result
}

// addNewRLSPolicies emits one CREATE POLICY per added reference.
//
// The reference is resolved through [rlsscope.Resolver], which keys by the
// owning table under the target's identifier semantics and by the policy's own
// name. Two things follow, and both are the point of the resolver existing.
//
// The name alone does not identify a policy: two tables may each carry a
// policy called "tenant_isolation", and matching on the name picked whichever
// was declared first. And the table alone is not a string comparison either:
// `orders` and `public.orders` are one table, which is exactly the pair the
// comparator normalizes and a raw-string lookup missed.
//
// An unresolved reference is an error rather than a skip. A plan that omits a
// CREATE POLICY and still reports success leaves the database without the
// protection the migration was generated to add (stokaro/ptah#1311).
func (p *Planner) addNewRLSPolicies(
	result []ast.Node,
	diff *types.SchemaDiff,
	policies *rlsscope.Resolver,
) ([]ast.Node, error) {
	for _, policyRef := range diff.RLSPoliciesAdded {
		policy, err := policies.Resolve(policyRef)
		if err != nil {
			return nil, err
		}
		policyNode := fromschema.FromRLSPolicy(policy)
		// Set Replace flag to handle conflicts gracefully during migrations
		policyNode.Replace = true
		result = append(result, policyNode)
	}
	return result, nil
}

// modifyExistingRLSPolicies re-renders each modified policy from the schema the
// plan is targeting, through the same resolver [addNewRLSPolicies] uses.
//
// The two sides of a modification do not spell the owning table the same way.
// The comparator normalizes `orders` and `public.orders` to one table and then
// reports the DESIRED side's spelling, while a rollback plans against the
// INTROSPECTED schema, whose policy carries the database's spelling. A
// raw-string lookup therefore found nothing on the down direction and the
// generated rollback was `-- No rollback operations needed`: a policy body
// changed on the way up and nothing put it back (stokaro/ptah#1311).
func (p *Planner) modifyExistingRLSPolicies(
	result []ast.Node,
	diff *types.SchemaDiff,
	policies *rlsscope.Resolver,
) ([]ast.Node, error) {
	for _, policyDiff := range diff.RLSPoliciesModified {
		policy, err := policies.Resolve(types.RLSPolicyRef{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
		})
		if err != nil {
			return nil, err
		}
		policyNode := fromschema.FromRLSPolicy(policy).SetReplace()
		policyNode.SetComment(fmt.Sprintf("Modify RLS policy %s on table %s: %s",
			policyDiff.PolicyName,
			policyDiff.TableName,
			summarizeRLSChanges(policyDiff),
		))
		result = append(result, policyNode)
	}
	return result, nil
}

func summarizeRLSChanges(policyDiff types.RLSPolicyDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(policyDiff.Changes)), ", ")
}

func (p *Planner) removeRLSPolicies(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	for _, policyRef := range diff.RLSPoliciesRemoved {
		// Now we have both policy name and table name, so we can generate proper DROP POLICY statements
		dropPolicyNode := ast.NewDropPolicy(policyRef.PolicyName, policyRef.TableName).
			SetIfExists().
			SetComment(fmt.Sprintf("Drop RLS policy %s from table %s", policyRef.PolicyName, policyRef.TableName))
		result = append(result, dropPolicyNode)
	}
	return result
}

// addNewConstraints adds new table-level constraints via ALTER TABLE statements.
//
// This method processes constraints defined through Go struct annotations and creates
// appropriate ALTER TABLE ADD CONSTRAINT statements. It handles different constraint
// types including EXCLUDE, CHECK, UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints.
//
// # Constraint Processing Order
//
// Constraints are processed in the order they appear in the generated schema.
// This method assumes that all referenced tables and columns already exist.
//
// # Supported Constraint Types
//
//   - EXCLUDE: PostgreSQL EXCLUDE constraints for preventing conflicts
//   - CHECK: Table-level CHECK constraints for data validation
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
//
// # Example Generated SQL
//
//	ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings
//	  EXCLUDE USING gist (room_id WITH =, during WITH &&);
//
//	ALTER TABLE products ADD CONSTRAINT positive_price
//	  CHECK (price > 0);
func (p *Planner) addNewConstraints(result []ast.Node, diff *types.SchemaDiff, generated *goschema.Database) []ast.Node {
	// Resolve struct → table name once for the field-level synthesis fallbacks.
	structToTable := make(map[string]string, len(generated.Tables))
	for _, t := range generated.Tables {
		structToTable[t.StructName] = t.QualifiedName()
	}

	state := newConstraintPlanState(diff)

	result = p.addPrimaryKeyConstraintsWithTables(result, diff.ConstraintsAddedWithTables, state.removalByTableName, state.handled, state.droppedForModify)
	result = p.addCheckAndUniqueConstraintsWithTables(
		result,
		diff.ConstraintsAddedWithTables,
		state.removalByTableName,
		state.handled,
		state.droppedForModify,
		diff.IndexRemovalsRebuiltAsUniqueConstraints(),
	)
	result = p.addNamedConstraintsByKind(result, diff, generated, structToTable, state, nonForeignKeyConstraints)
	result = p.addForeignKeyConstraintsWithTables(result, diff.ConstraintsAddedWithTables, state)
	result = p.addNamedConstraintsByKind(result, diff, generated, structToTable, state, foreignKeyConstraints)
	return result
}

type constraintKindFilter int

const (
	nonForeignKeyConstraints constraintKindFilter = iota
	foreignKeyConstraints
)

type constraintPlanState struct {
	removedNames       map[string]struct{}
	removalByTableName map[constraintHostKey]types.ConstraintRemovalInfo
	removalsByName     map[string][]types.ConstraintRemovalInfo
	addedHostsByName   map[string]map[string]struct{}
	handled            map[string]struct{}
	droppedForModify   map[constraintHostKey]struct{}
}

// constraintHostKey is the shared identity model's comparison value for a
// constraint, under this file's own name.
//
// It was a private struct here and a byte-identical one in the MySQL
// planner. Two copies of "which constraint is this" is how one planner comes to
// pair a drop with a different constraint than the other does
// (stokaro/ptah#1345).
type constraintHostKey = objectidentity.Key

// constraintIdentities folds nothing, which is what the planner requires.
//
// Both spellings a host key is built from arrive from the same [types.SchemaDiff],
// already normalized by the comparator that produced it. Folding again here
// would apply the rule twice on one side of the pipeline and once on the other,
// and a drop would then pair with a constraint the comparator never removed.
// The tightening criterion is the diff carrying its identities rather than its
// spellings; until then this is the semantics that keeps the two ends agreeing.
var constraintIdentities = objectidentity.NewBuilder(identifier.Semantics{})

// constraintHost builds a host key from an owning table spelling and a
// constraint name.
func constraintHost(table, name string) constraintHostKey {
	return constraintIdentities.ConstraintPartsVerbatim(table, name).Key()
}

func newConstraintPlanState(diff *types.SchemaDiff) constraintPlanState {
	// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved
	// is a modification (the comparator expresses a changed constraint as
	// remove + add of the same name — e.g. an on_delete change on a field-level
	// FK, issue #189). For those we must DROP the old definition before adding
	// the new one, otherwise the ADD CONSTRAINT collides with the still-present
	// constraint of the same name. removeConstraints runs later in the pipeline
	// and deliberately skips these names, so the drop+add is owned here and
	// ordered correctly.
	removedNames := make(map[string]struct{}, len(diff.ConstraintsRemoved))
	for _, name := range diff.ConstraintsRemoved {
		removedNames[name] = struct{}{}
	}

	// Prefer the table-qualified additions when the comparator supplied them.
	// A field-level FK contributed by an embedded inline-relation mixin shares
	// one constraint name across every host table, so the bare ConstraintsAdded
	// name list (and a field scan keyed on the Go struct name) cannot target the
	// right table — it would emit ALTER TABLE <MixinStruct> once per host
	// (issue #197). ConstraintsAddedWithTables carries the concrete table and
	// the full FK definition, so each host gets its own correct ALTER. Names
	// handled here are recorded so the legacy name loop below skips them.
	//
	// A modified FK (dropped + re-added) must be dropped before its re-add. The
	// authoritative "is this host a modification" signal is whether the exact
	// (table, name) pair appears in the removal set — NOT whether the name alone
	// was removed somewhere. In the MIXED case a shared FK name can be a modify
	// on host A and a pure ADD on host B (B has no removal entry); keying the
	// modify decision on the name alone would emit a phantom
	// `ALTER TABLE B DROP CONSTRAINT IF EXISTS <name>` for the pure-add host.
	// Keying the drop decision on (table, name) — mirroring MySQL's
	// removalByTableName — gives the pure-add host no drop.
	removalByTableName := make(map[constraintHostKey]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalByTableName[constraintHost(info.TableName, info.Name)] = info
	}

	// Index removals by bare name as well, so the legacy ConstraintsAdded loop
	// below can scope a modified non-FK constraint's DROP to its concrete host
	// table(s). The comparator records every removal in
	// ConstraintsRemovedWithTables in lockstep with the bare ConstraintsRemoved
	// list, so a modified constraint's host is normally known here even though
	// the bare loop iterates names alone.
	removalsByName := make(map[string][]types.ConstraintRemovalInfo, len(diff.ConstraintsRemovedWithTables))
	for _, info := range diff.ConstraintsRemovedWithTables {
		removalsByName[info.Name] = append(removalsByName[info.Name], info)
	}

	// Index the hosts that are actually being re-ADDED under each name. A modified
	// constraint's pre-drop must hit only the hosts whose constraint is being
	// re-added — NOT every host that merely has a removal entry for the name. In
	// the MIXED case (issue #206) a shared name is a modify on host A (re-added)
	// and a PURE removal on host B (not re-added): B's drop is owned by
	// removeConstraints, so the add-side modify-drop must leave B alone or it
	// would be dropped twice.
	addedHostsByName := make(map[string]map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// An addition entry with no recorded host is hostless: a "" host
			// would match no removal entry, so keeping it here would make
			// emitModifyDropForName filter out every REAL removal host and
			// skip a required pre-drop — the re-ADD then collides with the
			// still-present constraint (42710; IF EXISTS on the drop cannot
			// help because the drop was never emitted). Treat the name as if
			// it had no recorded addition hosts at all (issue #229, mirroring
			// the MySQL planner).
			continue
		}
		hosts := addedHostsByName[add.Name]
		if hosts == nil {
			hosts = make(map[string]struct{})
			addedHostsByName[add.Name] = hosts
		}
		hosts[add.TableName] = struct{}{}
	}

	return constraintPlanState{
		removedNames:       removedNames,
		removalByTableName: removalByTableName,
		removalsByName:     removalsByName,
		addedHostsByName:   addedHostsByName,
		handled:            make(map[string]struct{}),
		droppedForModify:   make(map[constraintHostKey]struct{}),
	}
}

func (p *Planner) addNamedConstraintsByKind(
	result []ast.Node,
	diff *types.SchemaDiff,
	generated *goschema.Database,
	structToTable map[string]string,
	state constraintPlanState,
	kind constraintKindFilter,
) []ast.Node {
	wantForeignKey := kind == foreignKeyConstraints
	for _, constraintName := range diff.ConstraintsAdded {
		// Already emitted via the table-qualified FK path above.
		if _, done := state.handled[constraintName]; done {
			continue
		}
		if p.constraintNameIsForeignKey(constraintName, generated, structToTable) != wantForeignKey {
			continue
		}

		// For a modification, emit the DROP first so it precedes the re-add,
		// scoped to the constraint's concrete host table when the comparator
		// recorded it (issue #199) — never a name-only resolution that could drop
		// a same-named constraint on the wrong table.
		if _, modified := state.removedNames[constraintName]; modified {
			result = p.emitModifyDropForName(
				result,
				constraintName,
				state.removalsByName,
				state.addedHostsByName[constraintName],
				state.droppedForModify,
			)
		}

		// Resolve the ADD CONSTRAINT node, in precedence order:
		//  1. explicit table-level //ptah:schema:constraint
		//  2. synthesized field-level check= (issue #112 / PR #123)
		//  3. synthesized field-level foreign= action drift (issue #189)
		// The two field-level fallbacks exist because the comparator
		// synthesizes those constraints into diff.ConstraintsAdded by name only
		// — they never reach generated.Constraints, so without the fallbacks an
		// ADD CONSTRAINT for an existing column would be silently dropped.
		if node, ok := p.addConstraintNodeFor(constraintName, generated, structToTable); ok {
			if node != nil {
				result = append(result, node)
			}
			continue
		}
	}
	return result
}

func (p *Planner) addForeignKeyConstraintsWithTables(
	result []ast.Node,
	additions []types.ConstraintAdditionInfo,
	state constraintPlanState,
) []ast.Node {
	for _, add := range additions {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// Only emit the DROP-before-ADD when this exact host's FK is being
		// modified (its (table, name) is in the removal set). A pure-add host
		// gets no phantom drop.
		key := constraintHost(add.TableName, add.Name)
		if _, modified := state.removalByTableName[key]; modified {
			result = p.emitModifyDrop(result, add, state.droppedForModify)
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		state.handled[add.Name] = struct{}{}
	}
	return result
}

func (p *Planner) constraintNameIsForeignKey(constraintName string, generated *goschema.Database, structToTable map[string]string) bool {
	for _, constraint := range generated.Constraints {
		if constraint.Name == constraintName {
			return strings.EqualFold(constraint.Type, "FOREIGN KEY")
		}
	}
	for _, field := range generated.Fields {
		if field.Foreign == "" {
			continue
		}
		tableName := structToTable[field.StructName]
		if tableName == "" {
			tableName = field.StructName
		}
		if foreignKeyName(unqualifiedTableName(tableName), field) == constraintName {
			return true
		}
	}
	return false
}

func (p *Planner) addCheckAndUniqueConstraintsWithTables(
	result []ast.Node,
	additions []types.ConstraintAdditionInfo,
	removalByTableName map[constraintHostKey]types.ConstraintRemovalInfo,
	handled map[string]struct{},
	droppedForModify map[constraintHostKey]struct{},
	rebuiltIndexes map[types.IndexRef]struct{},
) []ast.Node {
	for _, add := range additions {
		constraint := constraintAdditionNode(add)
		if constraint == nil {
			continue
		}
		key := constraintHost(add.TableName, add.Name)
		if _, modified := removalByTableName[key]; modified {
			result = p.emitModifyDrop(result, add, droppedForModify)
		}
		result = p.dropIndexRebuiltAsConstraint(result, add, rebuiltIndexes)
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}},
		})
		handled[add.Name] = struct{}{}
	}
	return result
}

// dropIndexRebuiltAsConstraint drops the index this UNIQUE constraint addition
// is about to rebuild, before the addition rather than after it.
//
// ADD CONSTRAINT ... UNIQUE builds an index named after the constraint, so an
// index of that name on that table has to be gone first: PostgreSQL 17.10
// answers `relation "uq_users_email" already exists (SQLSTATE 42P07)`
// otherwise. The pipeline emits constraint additions before index removals, so
// the drop is emitted here and [Planner.removeIndexes] leaves it alone; see
// [types.SchemaDiff.IndexRemovalsRebuiltAsUniqueConstraints] for the shape that
// produces the collision.
func (p *Planner) dropIndexRebuiltAsConstraint(
	result []ast.Node,
	add types.ConstraintAdditionInfo,
	rebuiltIndexes map[types.IndexRef]struct{},
) []ast.Node {
	ref := types.IndexRef{Name: add.Name, TableName: add.TableName}
	if _, rebuilt := rebuiltIndexes[ref]; !rebuilt {
		return result
	}
	dropIndexNode := ast.NewDropIndex(ref.Name).SetTable(ref.TableName)
	if p.capabilities().Has(capability.DropIndexIfExists) {
		dropIndexNode.SetIfExists()
	}
	return append(result, dropIndexNode)
}

func constraintAdditionNode(add types.ConstraintAdditionInfo) *ast.ConstraintNode {
	if add.TableName == "" {
		return nil
	}
	switch add.Type {
	case "CHECK":
		if add.CheckExpression == "" {
			return nil
		}
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       add.Name,
			Expression: add.CheckExpression,
		}
	case "UNIQUE":
		if len(add.Columns) == 0 {
			return nil
		}
		constraint := ast.NewUniqueConstraint(add.Name, add.Columns...)
		constraint.IncludeColumns = append([]string(nil), add.IncludeColumns...)
		constraint.NullsDistinct = cloneBoolPtr(add.NullsDistinct)
		return constraint
	default:
		return nil
	}
}

func (p *Planner) addPrimaryKeyConstraintsWithTables(
	result []ast.Node,
	additions []types.ConstraintAdditionInfo,
	removalByTableName map[constraintHostKey]types.ConstraintRemovalInfo,
	handled map[string]struct{},
	droppedForModify map[constraintHostKey]struct{},
) []ast.Node {
	for _, add := range additions {
		if add.Type != "PRIMARY KEY" || add.TableName == "" || len(add.Columns) == 0 {
			continue
		}
		key := constraintHost(add.TableName, add.Name)
		if _, modified := removalByTableName[key]; modified {
			result = p.emitModifyDrop(result, add, droppedForModify)
		}
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: ast.NewPrimaryKeyConstraint(add.Columns...)}},
		})
		handled[add.Name] = struct{}{}
	}
	return result
}

// emitModifyDrop appends the DROP that must precede the re-ADD of a modified
// field-level FK (a constraint whose (table, name) is in both the additions and
// the removals). It always emits a table-qualified
// ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>, deduped per (host, name),
// because the concrete host is carried on ConstraintAdditionInfo.TableName and
// is therefore always known at emit time.
//
// This is unconditional regardless of how many hosts share the FK name:
//   - When the name lands on >=2 host tables (an inline-relation mixin embedded
//     into many tables, issue #197), each modify host's old constraint must be
//     dropped before its own ADD; a name-only resolution would only reach one.
//   - When the name lands on a single host (the #189 on_delete/on_update action
//     drift case), the table is equally known, so scoping the drop directly is
//     both simpler and safe. The earlier single-host branch fell back to the
//     name-only information_schema DO block (p.dropConstraintNode), which resolves
//     the owning table with LIMIT 1 and no table_name filter. PostgreSQL
//     constraint names are unique per table, not per schema, so that lookup could
//     drop a same-named constraint on the WRONG table (issue #199). Emitting the
//     direct table-qualified drop eliminates the ambiguity.
//
// The name-only DO block (dropConstraintNode) is no longer used for a modify
// whose host the comparator recorded: the legacy ConstraintsAdded modify path
// scopes its DROP via emitModifyDropForName too, and removeConstraints scopes
// pure removals table-qualified as well. It remains in use only as a defensive
// fallback for a synthetic diff that carries no ConstraintsRemovedWithTables
// entry.
func (p *Planner) emitModifyDrop(
	result []ast.Node,
	add types.ConstraintAdditionInfo,
	droppedForModify map[constraintHostKey]struct{},
) []ast.Node {
	return p.appendScopedDrop(result, add.TableName, add.Name, droppedForModify)
}

// emitModifyDropForName appends the DROP(s) that must precede the re-ADD of a
// modified constraint reached via the bare ConstraintsAdded name list (the
// non-FK and field-level synthesis paths; FK modifies are handled per-host in
// the ConstraintsAddedWithTables loop). The comparator records every removal in
// ConstraintsRemovedWithTables in lockstep with the bare list, so the owning
// table is normally known: the modified host gets a direct, table-qualified
// ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>, deduped per (host, name).
// This scopes the drop to the exact host instead of the name-only
// information_schema LIMIT 1 lookup, which — because constraint names are unique
// per table, not per schema — could drop a same-named constraint on the wrong
// table (issue #199).
//
// The drop is restricted to addedHosts: the hosts whose constraint is actually
// being re-added under this name (ConstraintsAddedWithTables). In the MIXED case
// (issue #206) a shared name is a modify on host A (re-added) and a PURE removal
// on host B (not re-added); B's drop is owned by removeConstraints, so dropping
// it here too would emit the drop twice. Restricting to addedHosts leaves B to
// removeConstraints. A removal host absent from addedHosts is therefore skipped.
//
// When addedHosts is empty the re-added hosts are unknown — e.g. a down/reverse
// diff fills ConstraintsRemovedWithTables but not ConstraintsAddedWithTables
// because the prior definition could not be reconstructed from schema context.
// In that case the drop is still scoped to every recorded removal host (the
// pre-#206 behavior), NOT the name-only DO block — otherwise the reverse
// direction would regress a known-host drop back to the information_schema LIMIT
// 1 lookup. Only a name with no recorded removal host at all falls back to the
// DO block.
func (p *Planner) emitModifyDropForName(
	result []ast.Node,
	name string,
	removalsByName map[string][]types.ConstraintRemovalInfo,
	addedHosts map[string]struct{},
	droppedForModify map[constraintHostKey]struct{},
) []ast.Node {
	if len(addedHosts) > 0 {
		// Re-added hosts are known: drop ONLY those. A removal host that is not
		// being re-added is a pure removal owned by removeConstraints, so dropping
		// it here too would emit the drop twice (issue #206).
		for _, info := range removalsByName[name] {
			if info.TableName == "" {
				continue
			}
			if _, reAdded := addedHosts[info.TableName]; !reAdded {
				continue
			}
			result = p.appendScopedDrop(result, info.TableName, info.Name, droppedForModify)
		}
		return result
	}
	// addedHosts unknown: scope by every recorded removal host before resorting to
	// the name-only DO block, so the reverse/down direction keeps the table-scoped
	// drop it had before issue #206.
	scoped := false
	for _, info := range removalsByName[name] {
		if info.TableName == "" {
			continue
		}
		result = p.appendScopedDrop(result, info.TableName, info.Name, droppedForModify)
		scoped = true
	}
	if scoped {
		return result
	}
	// No host recorded for this name — fall back to the runtime DO block.
	fallbackKey := constraintHost("", name)
	if _, done := droppedForModify[fallbackKey]; done {
		return result
	}
	droppedForModify[fallbackKey] = struct{}{}
	return append(result, p.dropConstraintNode(name))
}

// appendScopedDrop appends a single direct, table-qualified
// ALTER TABLE <table> DROP CONSTRAINT IF EXISTS <name>, deduped per (table, name)
// via droppedForModify so a constraint name shared across host tables is dropped
// once per host and never twice for the same host.
func (p *Planner) appendScopedDrop(
	result []ast.Node,
	table, name string,
	droppedForModify map[constraintHostKey]struct{},
) []ast.Node {
	dedupKey := constraintHost(table, name)
	if _, done := droppedForModify[dedupKey]; done {
		return result
	}
	droppedForModify[dedupKey] = struct{}{}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: name,
			IfExists:       true,
		}},
	})
}

// foreignKeyAdditionNode builds the ALTER TABLE ADD CONSTRAINT node for a
// table-qualified field-level FK addition (ConstraintsAddedWithTables). The
// table comes straight from the comparator's synthesized constraint, so this
// path is correct for FK names that repeat across the many tables embedding an
// inline-relation mixin (issue #197), unlike the legacy field-scan fallback
// that re-derived the table from a Go struct name.
func (p *Planner) foreignKeyAdditionNode(add types.ConstraintAdditionInfo) *ast.AlterTableNode {
	fkRef := &ast.ForeignKeyRef{
		Table:    add.ForeignTable,
		Column:   add.ForeignColumn,
		Columns:  add.ForeignColumns,
		OnDelete: add.OnDelete,
		OnUpdate: add.OnUpdate,
	}
	return p.createForeignKeyAlterStatement(add.TableName, add.Name, add.Columns, fkRef)
}

// addConstraintNodeFor resolves the ADD CONSTRAINT node for a constraint known
// only by name, trying the explicit table-level constraints first and then the
// synthesized field-level check= / foreign= fallbacks (see addNewConstraints).
// The returned bool reports whether a matching definition was found; the node
// may still be nil when a match exists but produces no valid AST (e.g. an
// EXCLUDE constraint, which convertConstraintToAST cannot represent).
func (p *Planner) addConstraintNodeFor(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, constraint := range generated.Constraints {
		if constraint.Name != constraintName {
			continue
		}
		if astConstraint := p.convertConstraintToAST(constraint); astConstraint != nil {
			return &ast.AlterTableNode{
				Name:       constraint.Table,
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: astConstraint}},
			}, true
		}
		return nil, true
	}

	if node, ok := p.fieldLevelCheckConstraintNode(constraintName, generated, structToTable); ok {
		return node, true
	}

	return p.fieldLevelForeignKeyConstraintNode(constraintName, generated, structToTable)
}

// fieldLevelCheckConstraintNode builds the ADD CONSTRAINT node for a synthesized
// field-level check= constraint (issue #112 / PR #123). New columns are handled
// by the inline CHECK in ALTER TABLE ADD COLUMN, and the comparator deliberately
// skips synthesizing those, so only existing-column field-level CHECKs reach
// here.
func (p *Planner) fieldLevelCheckConstraintNode(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range generated.Fields {
		if f.Check == "" {
			continue
		}
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}
		name := f.CheckName
		if name == "" {
			name = unqualifiedTableName(tableName) + "_" + f.Name + "_check"
		}
		if name != constraintName {
			continue
		}
		return &ast.AlterTableNode{
			Name: tableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type:       ast.CheckConstraint,
				Name:       name,
				Expression: f.Check,
			}}},
		}, true
	}
	return nil, false
}

// fieldLevelForeignKeyConstraintNode builds the ADD CONSTRAINT node for a
// synthesized field-level foreign= constraint whose on_delete / on_update action
// changed (issue #189). Without this the FK would be dropped (via
// removeConstraints) but never re-added with the new action — a destructive,
// silently-broken migration. New columns/tables are handled by the inline FK in
// CREATE TABLE / ALTER TABLE ADD COLUMN and the comparator deliberately skips
// synthesizing those, so only existing-column FK action changes reach here.
func (p *Planner) fieldLevelForeignKeyConstraintNode(constraintName string, generated *goschema.Database, structToTable map[string]string) (ast.Node, bool) {
	for _, f := range generated.Fields {
		if f.Foreign == "" {
			continue
		}
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}
		name := foreignKeyName(unqualifiedTableName(tableName), f)
		if name != constraintName {
			continue
		}
		fkRef := fromschema.ParseForeignKeyReference(f.Foreign)
		if fkRef == nil {
			continue
		}
		if table := findGeneratedTableByStructName(generated, f.StructName); table != nil {
			qualifyForeignKeyRef(generated, *table, fkRef)
		}
		fkRef.OnDelete = f.OnDelete
		fkRef.OnUpdate = f.OnUpdate
		return p.createForeignKeyAlterStatement(tableName, name, []string{f.Name}, fkRef), true
	}
	return nil, false
}

func unqualifiedTableName(tableName string) string {
	ref, ok := tableref.Parse(tableName)
	if !ok {
		return tableName
	}
	return ref.Name
}

// removeConstraints removes table-level constraints via ALTER TABLE statements.
//
// This method generates ALTER TABLE DROP CONSTRAINT statements for constraints
// that exist in the database but not in the generated schema.
//
// # Safety Considerations
//
// Dropping constraints can affect data integrity and application behavior:
//   - Removing CHECK constraints may allow invalid data
//   - Removing UNIQUE constraints may allow duplicate data
//   - Removing FOREIGN KEY constraints may allow orphaned records
//   - Removing EXCLUDE constraints may allow conflicting data
//
// # Example Generated SQL
//
//	ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlapping_bookings;
//	ALTER TABLE products DROP CONSTRAINT IF EXISTS positive_price;
func (p *Planner) removeConstraints(result []ast.Node, diff *types.SchemaDiff) []ast.Node {
	// A removed constraint is dropped from its exact owning table with a direct,
	// table-qualified ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>. The
	// comparator records that host in ConstraintsRemovedWithTables in lockstep
	// with the bare ConstraintsRemoved name list, so real diff output always
	// carries it.
	//
	// The name-only information_schema DO block (dropConstraintNode) is used ONLY
	// as a defensive fallback for a synthetic, hand-built diff that lists a
	// removed constraint by name with no ConstraintsRemovedWithTables host — it
	// resolves the owning table at execution time via information_schema LIMIT 1.
	// That LIMIT 1 lookup is unsafe for real removals because PostgreSQL
	// constraint names are unique per table, not per schema, so a same-named
	// constraint could be dropped from the WRONG table (issue #199), and a name
	// that lands on multiple host tables would be dropped from only one of them
	// (issue #197). The table-qualified drop avoids both.
	//
	// A constraint that appears in BOTH the additions and the removals for the
	// SAME (table, name) is a modification (the comparator expresses a changed
	// constraint as remove + add of the same name). Those are emitted as
	// DROP-then-ADD by addNewConstraints, which runs earlier in the pipeline so
	// the drop precedes the re-add; dropping them again here would remove the
	// freshly added constraint, so they are skipped.
	//
	// The skip MUST be keyed on (table, name), not the bare name. A shared
	// constraint name can be a modify on host A (its name lands in
	// ConstraintsAdded) and a PURE removal on host B (B has no addition). Keying
	// the skip on the name alone would treat B's removal as a modify owned by
	// addNewConstraints and skip it, leaving the stale constraint on B forever
	// (issue #206). The comparator records every addition's host in
	// ConstraintsAddedWithTables in lockstep with the bare list, so the modify
	// owner is always known per host.
	modifySet := make(map[constraintHostKey]struct{}, len(diff.ConstraintsAddedWithTables))
	addedHostCounts := make(map[string]int, len(diff.ConstraintsAddedWithTables))
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			// Hostless addition entries do not count as recorded hosts —
			// mirroring addedHostsByName in addNewConstraints — so the
			// hostless-re-add rule below still engages (issue #229).
			continue
		}
		modifySet[constraintHost(add.TableName, add.Name)] = struct{}{}
		addedHostCounts[add.Name]++
	}

	// Bare added names, for re-adds whose hosts were NOT recorded
	// (ConstraintsAdded carries the name but ConstraintsAddedWithTables has no
	// entry for it — reverse/down diffs of non-FK modifies, legacy callers
	// without an introspected schema, and hand-built diffs). For those,
	// emitModifyDropForName cannot restrict its pre-drop to the re-added
	// hosts, so it drops EVERY recorded removal host BEFORE the re-add;
	// dropping any of them again here would land AFTER the re-add and delete
	// the freshly restored constraint — IF EXISTS is no protection against
	// dropping a constraint that now exists again. This silently destroyed
	// the constraint on every non-FK down migration (issue #229).
	addedBareNamesHosted := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedBareNamesHosted[name] = struct{}{}
	}

	// When the comparator supplied the owning table (ConstraintsRemovedWithTables),
	// drop the constraint from that exact table with a direct ALTER TABLE … DROP
	// CONSTRAINT IF EXISTS, deduped per (table, name) via appendScopedDrop. This
	// is required for a field-level FK whose name repeats across the many tables
	// embedding an inline-relation mixin (issue #197): the name-only DO block
	// below resolves a single table via information_schema LIMIT 1, so it would
	// drop the constraint from only one of the host tables and silently leave the
	// rest. Names that carried at least one host are recorded so the bare
	// fallback below — which exists only for synthetic diffs — does not re-emit
	// the name-only DO block for them.
	dropped := make(map[constraintHostKey]struct{})
	namesWithHost := make(map[string]struct{})
	for _, info := range diff.ConstraintsRemovedWithTables {
		if info.TableName == "" {
			// No host recorded for this entry; defer to the bare fallback.
			continue
		}
		namesWithHost[info.Name] = struct{}{}
		key := constraintHost(info.TableName, info.Name)
		if _, modified := modifySet[key]; modified {
			// addNewConstraints owns this host's DROP-then-ADD; do not re-drop.
			continue
		}
		if _, added := addedBareNamesHosted[info.Name]; added && addedHostCounts[info.Name] == 0 {
			// Hostless re-add: addNewConstraints already dropped every
			// recorded removal host for this name before the re-add
			// (emitModifyDropForName with unknown addedHosts). A second drop
			// here would follow the re-add and delete the fresh constraint
			// (issue #229).
			continue
		}
		result = p.appendScopedDrop(result, info.TableName, info.Name, dropped)
	}

	// Bare fallback for synthetic diffs only: a hand-built diff may list a
	// removed constraint by name with no ConstraintsRemovedWithTables host. Such
	// names have genuinely no table to scope by, so the runtime information_schema
	// DO block (dropConstraintNode) remains the only option. Real comparator
	// output always carries the host, so it is fully handled above and skipped
	// here. A bare modify (name in ConstraintsAdded with no recorded host) is
	// owned by addNewConstraints and skipped.
	addedBareNames := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range diff.ConstraintsAdded {
		addedBareNames[name] = struct{}{}
	}
	for _, constraintName := range diff.ConstraintsRemoved {
		if _, hadHost := namesWithHost[constraintName]; hadHost {
			continue
		}
		if _, modified := addedBareNames[constraintName]; modified {
			continue
		}
		result = append(result, p.dropConstraintNode(constraintName))
	}
	return result
}

// dropConstraintNode builds a self-contained DROP CONSTRAINT statement for a
// constraint known only by name. The diff layer discards the table name for
// removed constraints (field-level CHECK / FK constraints are synthesized and
// presented by name alone), so the table is resolved at execution time from
// information_schema via a DO block.
//
// Constraint-name safety. Postgres constraint names should be plain ASCII
// alnum + underscore; we reject only the chars that would actually break our
// specific DO-block template:
//   - `$` would collide with the `$ptah$` dollar-quote tag and terminate the
//     body early.
//   - newline / carriage return would terminate the leading `--` comment line
//     and dump whatever follows as bare SQL.
//
// Anything else (apostrophe) is handled by SQL-literal escaping. Unsafe names
// produce a DO block whose only action is RAISE EXCEPTION, so the migration
// fails loudly rather than silently looping forever on subsequent runs.
func (p *Planner) dropConstraintNode(constraintName string) ast.Node {
	escaped := strings.ReplaceAll(constraintName, "'", "''")
	if strings.ContainsAny(constraintName, "$\n\r") {
		// Build a printable, single-quoted SQL string literal of the
		// rejected name so the operator's error output shows what was
		// rejected. `$` is rendered as `\$` so the surrounding `$ptah$`
		// dollar quoting can't be prematurely terminated; `\n` / `\r` /
		// `\t` are rendered as their printable escapes; apostrophes are
		// SQL-escaped via `''`. The result is plain ASCII inside `'…'`.
		visible := strings.NewReplacer(
			"\n", `\n`,
			"\r", `\r`,
			"\t", `\t`,
			"$", `\$`,
		).Replace(constraintName)
		visible = strings.ReplaceAll(visible, "'", "''")

		failBlock := fmt.Sprintf(`-- Unsafe constraint name rejected by the migration generator; the
-- following DO block raises an exception so the migration fails loudly.
DO $ptah$
BEGIN
    RAISE EXCEPTION 'refusing to drop constraint with unsafe name ''%s''; rename the constraint and regenerate the migration';
END
$ptah$`, visible)
		return ast.NewRawSQL(failBlock)
	}
	doBlock := fmt.Sprintf(`-- Drop constraint %s (table resolved at runtime from information_schema)
DO $ptah$
DECLARE
    target_table TEXT;
BEGIN
    SELECT table_name INTO target_table
    FROM information_schema.table_constraints
    WHERE constraint_name = '%s'
      AND table_schema = current_schema()
    LIMIT 1;

    IF target_table IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %%I DROP CONSTRAINT IF EXISTS %%I', target_table, '%s');
        RAISE NOTICE 'Dropped constraint %s from table %%', target_table;
    ELSE
        RAISE NOTICE 'Constraint %s not found in current schema';
    END IF;
END
$ptah$`, constraintName, escaped, escaped, escaped, escaped)

	return ast.NewRawSQL(doBlock)
}

// convertConstraintToAST converts a goschema.Constraint to an ast.ConstraintNode.
//
// This helper method handles the conversion between the schema annotation representation
// and the AST representation used for SQL generation.
func (p *Planner) convertConstraintToAST(constraint goschema.Constraint) *ast.ConstraintNode {
	switch constraint.Type {
	case "EXCLUDE":
		if constraint.UsingMethod == "" || constraint.ExcludeElements == "" {
			return nil // Invalid EXCLUDE constraint
		}
		astConstraint := ast.NewExcludeConstraint(constraint.Name, constraint.UsingMethod, constraint.ExcludeElements)
		if constraint.WhereCondition != "" {
			astConstraint.SetWhereCondition(constraint.WhereCondition)
		}
		return astConstraint

	case "CHECK":
		if constraint.CheckExpression == "" {
			return nil // Invalid CHECK constraint
		}
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       constraint.Name,
			Expression: constraint.CheckExpression,
		}

	case "UNIQUE":
		if len(constraint.Columns) == 0 {
			return nil // Invalid UNIQUE constraint
		}
		astConstraint := ast.NewUniqueConstraint(constraint.Name, constraint.Columns...)
		astConstraint.IncludeColumns = append([]string(nil), constraint.IncludeColumns...)
		astConstraint.NullsDistinct = cloneBoolPtr(constraint.NullsDistinct)
		return astConstraint

	case "PRIMARY KEY":
		if len(constraint.Columns) == 0 {
			return nil // Invalid PRIMARY KEY constraint
		}
		return ast.NewPrimaryKeyConstraint(constraint.Columns...)

	case "FOREIGN KEY":
		if len(constraint.Columns) == 0 || constraint.ForeignTable == "" || len(constraint.ForeignColumnsOrDefault()) == 0 {
			return nil // Invalid FOREIGN KEY constraint
		}
		ref := &ast.ForeignKeyRef{
			Table:    constraint.ForeignTable,
			Column:   constraint.ForeignColumn,
			Columns:  constraint.ForeignColumns,
			OnDelete: constraint.OnDelete,
			OnUpdate: constraint.OnUpdate,
			Name:     constraint.Name,
		}
		return ast.NewForeignKeyConstraint(constraint.Name, constraint.Columns, ref)

	default:
		return nil // Unsupported constraint type
	}
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}
