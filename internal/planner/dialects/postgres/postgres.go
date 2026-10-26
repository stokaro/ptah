package postgres

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/deporder"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/modelast"
	"ptah.run/internal/objectlookup"
	"ptah.run/internal/pgname"
	"ptah.run/internal/pgsystemschema"
	"ptah.run/internal/planner/columnchange"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/planner/keyrelease"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tablelookup"
	"ptah.run/internal/tableref"
	"ptah.run/migration/diffpolicy"
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
//	generated := &schemamodel.Database{
//		Enums: []schemamodel.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []schemamodel.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []schemamodel.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//		},
//	}
//
//	// Generate migration AST nodes
//	nodes, err := planner.GenerateMigrationAST(ctx, runtime, diff)
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
	concurrentIndexRefs map[difftypes.IndexRef]struct{}
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
	concurrentIndexDropRefs map[difftypes.IndexRef]struct{}
	// skip lists destructive change kinds this planner omits from the plan,
	// emitting a clearly-marked comment in their place. See diffpolicy.
	skip diffpolicy.SkipSet
	// statements is the statement style the plan is spelled in. The zero
	// value is Ptah's own; see statementstyle.go for what the exact style
	// leaves out and why.
	statements planext.StatementStyle
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
	return &Planner{caps: caps, dialect: normalized}
}

// capabilities returns the planner's capability set, defaulting the nil zero
// value to the current PostgreSQL line preset so a bare &Planner{} behaves
// exactly like New(). Restriction must be an explicit choice, never a
// zero-value surprise.
func (p *Planner) capabilities() capability.Capabilities {
	if p.caps.Len() == 0 {
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
func (p *Planner) WithConcurrentIndexRefs(indexRefs ...difftypes.IndexRef) *Planner {
	cp := *p
	cp.concurrentIndexRefs = maps.Clone(p.concurrentIndexRefs)
	if cp.concurrentIndexRefs == nil {
		cp.concurrentIndexRefs = make(map[difftypes.IndexRef]struct{}, len(indexRefs))
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
func (p *Planner) WithConcurrentIndexDropRefs(indexRefs ...difftypes.IndexRef) *Planner {
	cp := *p
	cp.concurrentIndexDropRefs = maps.Clone(p.concurrentIndexDropRefs)
	if cp.concurrentIndexDropRefs == nil {
		cp.concurrentIndexDropRefs = make(map[difftypes.IndexRef]struct{}, len(indexRefs))
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
func (p *Planner) WithSkipChangeKinds(kinds ...planext.ChangeKind) *Planner {
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

// WithStatementStyle returns a copy of the planner that spells its statements
// in style. The receiver is not modified.
func (p *Planner) WithStatementStyle(style planext.StatementStyle) *Planner {
	cp := *p
	cp.statements = style
	return &cp
}

func (p *Planner) usesConcurrentIndex(ref difftypes.IndexRef) bool {
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
func (p *Planner) usesConcurrentIndexDrop(ref difftypes.IndexRef) bool {
	if p.concurrentIndexDrops {
		return true
	}
	_, ok := p.concurrentIndexDropRefs[ref]
	return ok
}

func (p *Planner) addNewEnums(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// No lookup: the change carries the enum (stokaro/ptah#2315). Building the
	// node through modelast.FromEnum still keeps the CREATE TYPE identifier
	// and the column type that references it derived from one place
	// (stokaro/ptah#1276).
	for _, enum := range diff.EnumsAdded {
		result = append(result, modelast.FromEnum(enum))
	}
	return result
}

func (p *Planner) modifyExistingEnums(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, enumDiff := range diff.EnumsModified {
		if len(enumDiff.ValuesRemoved) > 0 {
			declared, rebuilt := enumRebuild(diff.DeclaredUserTypes, enumDiff, semantics)
			if !rebuilt {
				result = append(result, ast.NewComment(fmt.Sprintf(
					"WARNING: Cannot remove enum values %v from %s because the target enum definition was not found",
					enumDiff.ValuesRemoved,
					enumDiff.EnumName,
				)))
				continue
			}
			result = append(result, ast.NewRawSQL(postgresEnumValueRemovalSQL(
				enumDiff.EnumName,
				declared.Values,
				enumDiff.Usages,
			)))
			// The enum the rebuild creates is a new type, and the comment
			// stayed on the old one it dropped.
			if declared.Comment != "" {
				result = append(result, ast.NewObjectComment(ast.CommentedType, enumDiff.EnumName, declared.Comment))
			}
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

// enumRebuild answers with the declaration an enum is rebuilt from. PostgreSQL
// has no statement that removes a value, so an enum that loses one is renamed,
// created again from the declaration, and the old type dropped.
//
// It is the one answer to whether an enum is rebuilt. modifyExistingEnums
// writes the rebuild from it, and changeObjectComments reads it to know that
// the enum is a new type and that the rebuild wrote its comment.
//
// The declaration comes from the vocabulary the diff carries rather than from
// the document: it is the same four lists a created column is typed against,
// and the comparison fills it on every run (stokaro/ptah#2315).
func enumRebuild(
	vocabulary difftypes.UserTypeVocabulary,
	enumDiff difftypes.EnumDiff,
	semantics identifier.Semantics,
) (*schemamodel.Enum, bool) {
	if len(enumDiff.ValuesRemoved) == 0 {
		return nil, false
	}
	enum := objectlookup.Qualified(vocabulary.Enums, enumDiff.EnumName, semantics)
	return enum, enum != nil
}

func postgresEnumValueRemovalSQL(enumName string, values []string, usages []difftypes.EnumColumnUsage) string {
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

func postgresDefaultSQL(usage difftypes.EnumColumnUsage) (string, bool) {
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

func (p *Planner) addNewTables(result []ast.Node, diff *difftypes.SchemaDiff, routines relationRoutines) []ast.Node {
	// Phase 1: create the tables without their foreign keys, in an order where
	// a table comes after everything it references.
	//
	// Both halves read the diff and nothing else. A creation carries the
	// declaration, this table's columns and the enums they name, and the
	// ordering rules read the same three plus the document's dependency map,
	// which travels too (stokaro/ptah#2315).
	//
	// A routine a column default or a CHECK calls, placed with the relations
	// because it reads one, is created here between the tables; see
	// routines.tableSteps.
	creations := diff.TablesAdded.Qualified(diff.DeclaredUserTypes, DialectName).InDependencyOrder()
	byName := make(map[string]difftypes.TableCreation, len(creations))
	for _, creation := range creations {
		byName[creation.Name] = creation
	}
	for _, step := range routines.tableSteps {
		if step.Routine {
			result = append(result, routines.nodes[step.Name]...)
			continue
		}
		creation := byName[step.Name]
		astNode := modelast.FromTableWithConstraints(creation.Table, creation.Fields, creation.Enums, DialectName, creation.Constraints)
		for _, column := range astNode.Columns {
			column.ForeignKey = nil
		}
		// A column's own UNIQUE over the primary key's column is not built
		// when the CREATE TABLE writes both, so it is added after the table.
		keys := modelast.ColumnKeysAfterTable(astNode, creation.Table, creation.Fields, creation.Constraints, DialectName)
		result = append(result, astNode)
		for _, key := range keys {
			result = append(result, &ast.AlterTableNode{
				Name:       astNode.Name,
				Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: modelast.FromConstraint(key)}},
			})
		}
	}

	return result
}

func (p *Planner) addForeignKeyConstraintsForNewTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	return p.addForeignKeyConstraints(
		result,
		diff.TablesAdded.Qualified(diff.DeclaredUserTypes, DialectName).InDependencyOrder(),
		diff.DeclaredTables,
	)
}

// addSchemaPreconditions creates every schema this migration is about to put an
// object into, before it puts anything into any of them.
//
// It reads the diff rather than the desired state, so a run that changes nothing
// emits nothing and an inspect/apply round trip stays the clean no-op it has to
// be. And it reads EVERY added-object list rather than the tables alone.
// Deriving the preconditions from diff.TablesAdded inside addNewTables emits
// them after the types, sequences and functions phases and covers none of
// those. Measured on PostgreSQL 17.10, applying a `schema inspect` document of
// a multi-schema database to an empty one:
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
// way. Extensions keep installation schema on their desired definitions, an
// extension that moves names the schema it moves to, and a default privilege
// keeps the schema its IN SCHEMA clause names, so all three contribute that
// exact identifier directly. Names that carry no schema
// contribute none, which is every name on a single-schema migration.
func (p *Planner) addSchemaPreconditions(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
) []ast.Node {
	names := make([]string, 0, len(diff.TablesAdded))
	names = append(names, diff.TablesAdded.Names()...)
	names = append(names, diff.EnumsAdded.Names()...)
	names = append(names, diff.SequencesAdded.Names()...)
	names = append(names, diff.FunctionsAdded.Names()...)
	names = append(names, diff.ViewsAdded.Names()...)
	names = append(names, diff.MaterializedViewsAdded.Names()...)
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
		// A schema the target owns is reached without a precondition. On
		// PostgreSQL that spares a statement nobody needs; on Spanner it is
		// what makes the migration run at all, because `public` there is
		// implicit and CREATE SCHEMA for it is refused by name.
		if pgsystemschema.IsUncreatableSchema(p.dialect, schema) {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		schemas = append(schemas, schema)
	}
	for _, schema := range p.placedSchemas(diff) {
		if schema == "" || pgsystemschema.IsUncreatableSchema(p.dialect, schema) {
			continue
		}
		if _, ok := seen[schema]; !ok {
			seen[schema] = struct{}{}
			schemas = append(schemas, schema)
		}
	}
	slices.Sort(schemas)
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, schema := range schemas {
		result = append(result, schemaprecondition.Node(schema, diff.DeclaredSchemas, semantics))
	}
	return result
}

// placedSchemas are the schemas the plan puts owners' objects into, such as
// an extension the owner installs or moves; see [schemaext.SchemaPlacement].
// ALTER EXTENSION ... SET SCHEMA into a schema nothing created answers
// `schema "ext" does not exist`, measured on PostgreSQL 18. A change carries
// its schema resolved under the connection's default, and one that names the
// default, like one that names no schema, needs no precondition.
func (p *Planner) placedSchemas(diff *difftypes.SchemaDiff) []string {
	var schemas []string
	defaultSchema := diff.EffectiveIdentifierSemantics(p.targetDialect()).DefaultSchema
	for _, change := range diff.FeatureChanges {
		if placed, ok := change.Value.(schemaext.SchemaPlacement); ok {
			if schema := placed.PlacementSchema(change.Subject); schema != defaultSchema {
				schemas = append(schemas, schema)
			}
		}
	}
	return schemas
}

// addForeignKeyConstraints adds foreign key constraints via ALTER TABLE
// statements, for the tables this plan creates.
//
// Each creation carries the columns whose references become constraints and the
// self-references the declaration recorded for it. What it cannot carry is the
// table a reference POINTS AT -- usually one this diff does not touch -- so the
// declared table list travels on the diff instead (stokaro/ptah#2315).
func (p *Planner) addForeignKeyConstraints(
	result []ast.Node,
	creations difftypes.TableChanges,
	declaredTables []schemamodel.Table,
) []ast.Node {
	for _, creation := range creations {
		result = p.addRegularForeignKeys(result, creation, declaredTables)
		result = p.addSelfReferencingForeignKeys(result, creation, declaredTables)
	}

	return result
}

// addRegularForeignKeys adds regular (non-self-referencing) foreign key constraints
func (p *Planner) addRegularForeignKeys(
	result []ast.Node,
	creation difftypes.TableCreation,
	declaredTables []schemamodel.Table,
) []ast.Node {
	table := creation.Table
	for _, field := range creation.Fields {
		if !isRegularForeignKeyField(field, table) {
			continue
		}

		parsed := schemaprep.ParseForeignKeyReference(field.Foreign)
		if parsed == nil {
			continue
		}
		fkRef := astForeignKeyReference(parsed)
		qualifyForeignKeyRef(declaredTables, table, fkRef)
		if foreignKeyTargetsTable(fkRef, table) {
			continue
		}
		withFieldForeignKeyClauses(fkRef, field)
		result = append(result, p.createForeignKeyAlterStatement(table.QualifiedName(), foreignKeyName(table.Name, field), []string{field.Name}, fkRef))
	}
	return result
}

func foreignKeyTargetsTable(fkRef *ast.ForeignKeyRef, table schemamodel.Table) bool {
	return fkRef.Table == table.QualifiedName()
}

func qualifyForeignKeyRef(
	declaredTables []schemamodel.Table,
	current schemamodel.Table,
	fkRef *ast.ForeignKeyRef,
) {
	fkRef.Table = tablelookup.ResolveReference(declaredTables, current, fkRef.Table)
}

// withFieldForeignKeyClauses gives fkRef what the field declares about its
// foreign key beyond the reference: the actions, the deferral, the MATCH type
// and enforcement. A reference built with fewer adds a key that checks what
// the declaration said it does not (stokaro/ptah#3853).
func withFieldForeignKeyClauses(fkRef *ast.ForeignKeyRef, field schemamodel.Field) {
	fkRef.OnDelete = field.OnDelete
	fkRef.OnUpdate = field.OnUpdate
	fkRef.Deferrable = field.Deferrable
	fkRef.Initially = field.Initially
	fkRef.Match = field.ForeignKeyMatch
	fkRef.NotEnforced = field.ForeignKeyNotEnforced
}

func astForeignKeyReference(reference *schemaprep.ForeignKeyReference) *ast.ForeignKeyRef {
	return &ast.ForeignKeyRef{
		Table:   reference.Table,
		Column:  reference.Column,
		Columns: slices.Clone(reference.Columns),
	}
}

// addSelfReferencingForeignKeys adds self-referencing foreign key constraints
func (p *Planner) addSelfReferencingForeignKeys(
	result []ast.Node,
	creation difftypes.TableCreation,
	declaredTables []schemamodel.Table,
) []ast.Node {
	table := creation.Table
	for _, selfRefFK := range creation.SelfReferencingForeignKeys {
		parsed := schemaprep.ParseForeignKeyReference(selfRefFK.Foreign)
		if parsed != nil {
			fkRef := astForeignKeyReference(parsed)
			qualifyForeignKeyRef(declaredTables, table, fkRef)
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
func selfReferencingForeignKeyName(tableName string, fk schemamodel.SelfReferencingFK) string {
	if fk.ForeignKeyName != "" {
		return fk.ForeignKeyName
	}
	return schemaprep.GenerateForeignKeyName(tableName, fk.FieldName)
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
func isRegularForeignKeyField(field schemamodel.Field, table schemamodel.Table) bool {
	return field.StructName == table.StructName && field.Foreign != ""
}

// foreignKeyName returns the constraint name to use for a field-level foreign
// key: the explicit foreign_key_name= when set, otherwise the conventional
// fk_<table>_<column> name shared with the schemadiff comparator and the down
// path via schemaprep.GenerateForeignKeyName.
func foreignKeyName(tableName string, field schemamodel.Field) string {
	if field.ForeignKeyName != "" {
		return field.ForeignKeyName
	}
	return schemaprep.GenerateForeignKeyName(tableName, field.Name)
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
	tableDiff difftypes.TableDiff,
	declared []schemamodel.Table,
	vocabulary difftypes.UserTypeVocabulary,
	semantics identifier.Semantics,
) []ast.Node {
	// The TABLE still has to be declared. The column travels with the change,
	// so a field lookup here cannot fail -- but the guard is load-bearing on
	// its own: a diff naming `app.users` against a schema that declares
	// `reporting.users` must write no DDL, because the statement would apply
	// cleanly to a relation nobody declared. Measured on
	// PostgreSQL 17.10; see TestPlannerWritesNoDDLForARelationTheSchemaDoesNotDeclare.
	if findGeneratedTableByDiffName(declared, tableDiff.TableName, semantics) == nil {
		return result
	}

	// The column itself is not looked up. Finding it by the table's Go STRUCT
	// name and a scan of every field in the schema is a parser artifact
	// reaching into the planner (stokaro/ptah#2315).
	for _, column := range tableDiff.ColumnsAdded {
		columnNode := modelast.FromFieldWithoutForeignKeys(column, vocabulary.Enums, "postgres")

		// Only add the column - foreign key constraints will be added separately
		// to ensure proper dependency ordering (columns must exist before FK constraints)
		result = append(result, &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: columnNode}},
		})
	}
	return result
}

// addForeignKeyConstraintsForNewColumns adds foreign key constraints for newly added columns.
// This method is called after all columns have been added to ensure that referenced columns exist.
func (p *Planner) addForeignKeyConstraintsForNewColumns(
	result []ast.Node,
	tableDiff difftypes.TableDiff,
	declared []schemamodel.Table,
	semantics identifier.Semantics,
) []ast.Node {
	for _, column := range tableDiff.ColumnsAdded {
		var targetTableName string
		var targetTable *schemamodel.Table

		// The TABLE is still looked up, because qualifying the reference needs
		// the declaration's own schema. The COLUMN is not: it travels with the
		// change (stokaro/ptah#2315).
		if table := findGeneratedTableByDiffName(declared, tableDiff.TableName, semantics); table != nil {
			targetTable = table
			targetTableName = table.Name
		}
		targetField := &column

		// Only process fields that have foreign key constraints
		if targetField.Foreign != "" {
			// Parse the foreign key reference
			parsed := schemaprep.ParseForeignKeyReference(targetField.Foreign)
			if parsed != nil {
				fkRef := astForeignKeyReference(parsed)
				if targetTable != nil {
					qualifyForeignKeyRef(declared, *targetTable, fkRef)
				}
				fkName := foreignKeyName(targetTableName, *targetField)
				fkRef.Name = fkName
				withFieldForeignKeyClauses(fkRef, *targetField)

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

// modifyExistingTableColumns renders each modified column from the operand the
// comparison carried for it.
//
// Resolving the column instead -- struct name for the table, an embedded-field
// fold over the whole declaration, then a linear scan -- fails by emitting
// `ERROR: Could not find field definition ...` as a COMMENT and moving on: a
// column left as it was, in a migration that applies cleanly
// (stokaro/ptah#2315).
//
// The vocabulary is the diff's own, for the same reason a created column's is:
// a column may name an enum this diff does not change, so the type list cannot
// come from the change itself.
func (p *Planner) modifyExistingTableColumns(
	result []ast.Node,
	tableDiff difftypes.TableDiff,
	vocabulary difftypes.UserTypeVocabulary,
) []ast.Node {
	for _, colDiff := range tableDiff.ColumnsModified {
		if isGeneratedColumnChange(colDiff) {
			node, ok := columnNodeFor(colDiff, vocabulary)
			if !ok {
				result = append(result, missingColumnDefinition(tableDiff.TableName, colDiff))
				continue
			}
			result = p.modifyGeneratedColumnExpression(result, tableDiff.TableName, colDiff, node)
			continue
		}

		// A comment is not part of ALTER TABLE on PostgreSQL, so it is emitted
		// beside the column's other changes rather than inside them. A column
		// whose ONLY difference is its comment gets no ALTER at all: the
		// statement would be `ALTER COLUMN ... SET NOT NULL` for a column that
		// is already NOT NULL, which is noise the operator has to read past
		// and a write to a table nothing needed to write to (stokaro/ptah#2168).
		result = appendColumnComment(result, tableDiff.TableName, colDiff)
		result = p.appendNotNullConstraintRename(result, tableDiff.TableName, colDiff)
		if len(colDiff.Changes) == 0 {
			continue
		}

		// Only the statements below render the COLUMN. A comment transition and
		// a NOT NULL constraint rename are written from the diff alone, so they
		// are emitted above this and reach a column the diff carries no
		// definition for.
		changed := columnchange.Properties(colDiff)
		changed.Default = changed.Default || restatesDefault(colDiff)
		_, uniqueChanged := colDiff.Changes["unique"]
		if (changed.Any() || uniqueChanged) && colDiff.Desired.Name == "" {
			result = append(result, missingColumnDefinition(tableDiff.TableName, colDiff))
			continue
		}

		// The comment goes before the statements it describes. Written after
		// them, it would be the last fragment of a plan that ends on a column
		// change, describing nothing below it.
		result = append(result, modifyColumnComment(tableDiff.TableName, colDiff))

		if changed.Any() {
			columnNode, _ := columnNodeFor(colDiff, vocabulary)
			columnNode.EnumType = columnNode.EnumType || domainOverEnum(colDiff.Desired.Type, vocabulary)
			result = append(result, &ast.AlterTableNode{
				Name: tableDiff.TableName,
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
					Column:              columnNode,
					PreviousType:        previousColumnType(colDiff.Changes["type"]),
					PreviousNullable:    previousColumnNullable(colDiff.Changes["nullable"]),
					HasPreviousNullable: colDiff.Changes["nullable"] != "",
					Changed:             changed,
					HasChanged:          true,
				}},
			})
		}
		if node := addedColumnUnique(tableDiff, colDiff); node != nil {
			result = append(result, node)
		}
	}
	return result
}

// restatesDefault reports whether a column whose type changes away from a
// type of the schema's own sets its declared default again after the TYPE
// clause.
//
// PostgreSQL converts the stored default to the new type and keeps the old
// one inside it: `'a'::status` becomes `('a'::status)::text`. The comparison
// reads that back as the declared `'a'`, so it reports no change, but the
// default still depends on the old type, and `DROP TYPE status` is refused
// while it does (`default value for column s of table t depends on type
// status`); with CASCADE the server drops the default instead. Setting the
// declared default again leaves the column with no trace of the old type.
//
// The old type counts unless it is one of PostgreSQL's own, which nothing
// drops: an enum, an array of one, a domain over one, a composite or a range
// is restated alike, whether the schema still declares it or removes it here.
func restatesDefault(colDiff difftypes.ColumnDiff) bool {
	previous := previousColumnType(colDiff.Changes["type"])
	if previous == "" || builtinColumnType(previous) {
		return false
	}
	return colDiff.Desired.DefaultSet || colDiff.Desired.Default != "" || colDiff.Desired.DefaultExpr != ""
}

// domainOverEnum reports whether spelled names a domain the declaration holds
// whose base type is a declared enum, or an array of either. A column changing
// type into one takes the cast and the default handling an enum target does:
// text casts to the domain through its enum, and nothing else reaches it.
func domainOverEnum(spelled string, vocabulary difftypes.UserTypeVocabulary) bool {
	enums := make(map[string]bool, len(vocabulary.Enums))
	for _, enum := range vocabulary.Enums {
		enums[bareName(enum.Name)] = true
	}
	name := bareName(strings.TrimSuffix(strings.TrimSpace(spelled), "[]"))
	// A domain may be over another domain; each step names a declared type,
	// so the walk ends within as many steps as there are domains.
	for range len(vocabulary.Defined) + 1 {
		base, ok := domainBase(name, vocabulary.Defined)
		if !ok {
			return false
		}
		if enums[base] {
			return true
		}
		name = base
	}
	return false
}

// domainBase is the bare name of the base type of the declared domain name,
// without an array suffix.
func domainBase(name string, defined []schemaext.DefinedType) (string, bool) {
	for _, definition := range defined {
		if definition.ID.Kind != objectidentity.KindDomain || len(definition.References) == 0 ||
			bareName(definition.ID.Name.Source) != name {
			continue
		}
		return bareName(strings.TrimSuffix(strings.TrimSpace(definition.References[0]), "[]")), true
	}
	return "", false
}

// builtinTypeNames are PostgreSQL's own type names as a comparison spells a
// column's type, with no length, precision or array suffix.
var builtinTypeNames = map[string]bool{
	"smallint": true, "int2": true, "integer": true, "int": true, "int4": true, "bigint": true, "int8": true,
	"smallserial": true, "serial2": true, "serial": true, "serial4": true, "bigserial": true, "serial8": true,
	"decimal": true, "numeric": true, "real": true, "float4": true, "double precision": true, "float8": true, "float": true,
	"money": true, "text": true, "varchar": true, "character varying": true, "char": true, "character": true,
	"bpchar": true, "name": true, "bytea": true, "boolean": true, "bool": true, "date": true, "time": true,
	"timetz": true, "time without time zone": true, "time with time zone": true, "timestamp": true,
	"timestamptz": true, "timestamp without time zone": true, "timestamp with time zone": true,
	"interval": true, "uuid": true, "json": true, "jsonb": true, "xml": true, "inet": true, "cidr": true,
	"macaddr": true, "macaddr8": true, "bit": true, "varbit": true, "bit varying": true, "tsvector": true,
	"tsquery": true, "point": true, "line": true, "lseg": true, "box": true, "path": true, "polygon": true,
	"circle": true, "oid": true, "regclass": true,
}

// builtinColumnType reports whether spelled names one of PostgreSQL's own
// types, or an array of one.
func builtinColumnType(spelled string) bool {
	name := strings.ToLower(strings.TrimSpace(spelled))
	name = strings.TrimSpace(strings.TrimSuffix(name, "[]"))
	if open := strings.IndexByte(name, '('); open >= 0 {
		name = strings.TrimSpace(name[:open])
	}
	name = strings.TrimPrefix(name, "pg_catalog.")
	return builtinTypeNames[name]
}

// addedColumnUnique is the ADD CONSTRAINT for a column that gains a
// column-level UNIQUE, or nil when it gains none.
//
// No ALTER COLUMN clause carries uniqueness, so the column's own statements
// leave it out, and without this the plan says `unique: false -> true` and
// adds nothing (stokaro/ptah#3649). The constraint takes the name PostgreSQL
// gives the same UNIQUE inside CREATE TABLE, which is also what Atlas CE
// v1.3.0 writes: `ALTER TABLE t ADD CONSTRAINT t_c_key UNIQUE (c)`, and
// `t_c_key1` where another relation or constraint of the schema holds t_c_key.
// The comparison derives the name and the diff carries it; see
// [difftypes.TableDiff.ColumnKeyNames]. Written without the number beside an
// object the plan keeps, the statement is refused (stokaro/ptah#3859). A diff
// built by hand carries no name, and the name the server tries first stands in.
//
// The other direction needs nothing here. The live UNIQUE is a constraint the
// desired schema does not have, so the plan drops it by its name as a removed
// constraint. A column declaring unique_expr gets no constraint: uniqueness
// over the raw column is not what it asked for, and no target renders the
// expression.
func addedColumnUnique(tableDiff difftypes.TableDiff, colDiff difftypes.ColumnDiff) ast.Node {
	if _, changed := colDiff.Changes["unique"]; !changed || !colDiff.Desired.Unique ||
		strings.TrimSpace(colDiff.Desired.UniqueExpr) != "" {
		return nil
	}
	name := tableDiff.ColumnKeyNames[colDiff.ColumnName]
	if name == "" {
		table := tableDiff.TableName
		if ref, ok := tableref.Parse(tableDiff.TableName); ok {
			table = ref.Name
		}
		name = pgname.ColumnKey(table, colDiff.ColumnName)
	}
	return &ast.AlterTableNode{
		Name: tableDiff.TableName,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
			Constraint: ast.NewUniqueConstraint(name, colDiff.ColumnName),
		}},
	}
}

// modifyColumnComment describes a column's changes. The changes are listed in
// sorted key order so migration output is deterministic (issue #59).
func modifyColumnComment(tableName string, colDiff difftypes.ColumnDiff) ast.Node {
	changesList := make([]string, 0, len(colDiff.Changes))
	for _, changeType := range slices.Sorted(maps.Keys(colDiff.Changes)) {
		changesList = append(changesList, fmt.Sprintf("%s: %s", changeType, colDiff.Changes[changeType]))
	}
	return ast.NewDescriptiveComment(fmt.Sprintf("Modify column %s.%s: %s", tableName, colDiff.ColumnName, strings.Join(changesList, ", ")))
}

// columnNodeFor renders the column a modification carries, and reports whether
// it carried one.
//
// The vocabulary is the diff's rather than the change's: a column may name an
// enum this diff does not touch, so the type list cannot come from the change.
func columnNodeFor(
	colDiff difftypes.ColumnDiff,
	vocabulary difftypes.UserTypeVocabulary,
) (*ast.ColumnNode, bool) {
	if colDiff.Desired.Name == "" {
		return nil, false
	}
	return modelast.FromField(colDiff.Desired, vocabulary.Enums, "postgres"), true
}

// missingColumnDefinition reports a modification whose operand never arrived.
//
// Reported rather than skipped: a column the plan was asked to change and did
// not is a fact the operator needs, and rendering it anyway spells the column as
// the empty string -- `ALTER COLUMN "" TYPE` against a real table.
func missingColumnDefinition(tableName string, colDiff difftypes.ColumnDiff) ast.Node {
	return ast.NewComment(fmt.Sprintf(
		"ERROR: the diff carries no column definition for %s.%s", tableName, colDiff.ColumnName))
}

// appendNotNullConstraintRename emits the column's NOT NULL constraint name
// transition, if it has one and the target can keep a name at all.
//
// A rename rather than a drop and an add, because for a NOT NULL those are not
// equivalent: PostgreSQL generates a name for an unnamed one, so re-adding
// through `ALTER COLUMN ... SET NOT NULL` lands on the generated name rather
// than the declared one, and the column is momentarily nullable in between.
//
// Like a comment, it sits beside the column's other changes rather than inside
// them, so a column whose only difference is the constraint's name gets no
// ALTER COLUMN at all.
//
// The gate is the capability rather than the dialect name: the comparator can
// only produce this change from a description that named the constraint, and a
// target that does not persist the name has no constraint to rename
// (stokaro/ptah#2161).
func (p *Planner) appendNotNullConstraintRename(
	result []ast.Node,
	table string,
	colDiff difftypes.ColumnDiff,
) []ast.Node {
	if colDiff.NotNullConstraintNameChange == nil {
		return result
	}
	// A target that does not persist the name says so, rather than dropping the
	// declaration on the floor. The renderer refuses this at CREATE TABLE; a
	// column that already exists never reaches that path, so without this the
	// declared name simply vanished from the plan with no diagnostic -- the
	// silent drop this whole path exists to prevent.
	//
	// A comment rather than an error, following the precedent one function
	// down: a capability-gated change the planner cannot make is stated in the
	// plan, naming the key and the release that added it.
	if !p.caps.Has(capability.NamedNotNullConstraints) {
		return append(result, ast.NewComment(fmt.Sprintf(
			"WARNING: Column %s.%s declares NOT NULL constraint name %q, but naming a NOT NULL requires target capability %s, unavailable on this target (PostgreSQL added it in 18); the constraint is applied without the name.",
			table,
			colDiff.ColumnName,
			colDiff.NotNullConstraintNameChange.Desired,
			capability.NamedNotNullConstraints,
		)))
	}
	// Nothing to rename FROM. The catalog reported no name, so the constraint
	// has to be established under the declared one instead, which is the
	// column's own NOT NULL change rather than a rename.
	if colDiff.NotNullConstraintNameChange.Current == "" {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.RenameConstraintOperation{
			From: colDiff.NotNullConstraintNameChange.Current,
			To:   colDiff.NotNullConstraintNameChange.Desired,
		}},
	})
}

// appendColumnComment emits the column's comment transition, if it has one.
func appendColumnComment(result []ast.Node, table string, colDiff difftypes.ColumnDiff) []ast.Node {
	if colDiff.CommentChange == nil {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.SetCommentOperation{
			Column:     colDiff.ColumnName,
			Comment:    colDiff.CommentChange.Desired,
			HasCurrent: colDiff.CommentChange.Current != "",
		}},
	})
}

func (p *Planner) modifyGeneratedColumnExpression(
	result []ast.Node,
	tableName string,
	colDiff difftypes.ColumnDiff,
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
	result = append(result, ast.NewDescriptiveComment(fmt.Sprintf(
		"Modify generated column %s.%s: %s",
		tableName,
		colDiff.ColumnName,
		colDiff.Changes["generated"],
	)))
	return result
}

func isGeneratedColumnChange(colDiff difftypes.ColumnDiff) bool {
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
	declared []schemamodel.Table,
	tableName string,
	semantics identifier.Semantics,
) *schemamodel.Table {
	return objectlookup.Qualified(declared, tableName, semantics)
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

// dropsWithCascade reports whether a planned removal may carry CASCADE, which
// on PostgreSQL takes the objects that depend on the removed one with it, such
// as the RLS policies that name a column.
//
// Spanner has one drop mode. Its PostgreSQL interface refuses the clause on
// ALTER TABLE ... DROP COLUMN ("Only <RESTRICT> drop mode is supported in
// <ALTER> statement operations") and on DROP TABLE and DROP VIEW ("Only
// <RESTRICT> behavior is supported by <DROP> statement"), so every planned
// removal carrying it failed there (stokaro/ptah#4280, stokaro/ptah#4362). A
// plan on Spanner removes what depends on an object before the object: the
// indexes of a removed table are dropped ahead of it, and the views ahead of
// the tables they read.
func (p *Planner) dropsWithCascade() bool {
	return p.targetDialect() != platform.Spanner
}

// removeTableColumnsFromDiff drops the columns of tableDiff the declaration no
// longer holds. A generated column goes first: it may be computed from another
// removed column, which a drop could otherwise take only with CASCADE.
func (p *Planner) removeTableColumnsFromDiff(result []ast.Node, diff *difftypes.SchemaDiff, tableDiff difftypes.TableDiff, earlyColumns map[string]bool, ahead routinesAhead) []ast.Node {
	columns := make([]schemamodel.Field, 0, len(tableDiff.ColumnsRemoved))
	for _, generated := range []bool{true, false} {
		for _, column := range tableDiff.ColumnsRemoved {
			if isGenerated(column) == generated {
				columns = append(columns, column)
			}
		}
	}
	for _, column := range columns {
		if earlyColumns[tableDiff.TableName+"\x00"+column.Name] {
			continue
		}
		result = p.appendRoutinesAheadColumn(result, diff, tableDiff.TableName, column.Name, ahead)
		cascade := p.cascades(columnDropRelies(diff, tableDiff, column.Name, ahead))
		dropOp := &ast.DropColumnOperation{
			ColumnName: column.Name,
			Cascade:    cascade,
		}
		alterNode := &ast.AlterTableNode{
			Name:       tableDiff.TableName,
			Operations: []ast.AlterOperation{dropOp},
		}
		result = append(result, alterNode)
		warning := fmt.Sprintf("WARNING: Dropping column %s.%s - This will delete data!", tableDiff.TableName, column.Name)
		if cascade {
			warning = fmt.Sprintf("WARNING: Dropping column %s.%s with CASCADE - This will delete data and dependent objects!", tableDiff.TableName, column.Name)
		}
		result = append(result, ast.NewDescriptiveComment(warning))
	}
	return result
}

// appendTableComment emits the table's comment transition, if it has one.
func appendTableComment(result []ast.Node, tableDiff difftypes.TableDiff) []ast.Node {
	if tableDiff.CommentChange == nil {
		return result
	}
	return append(result, &ast.AlterTableNode{
		Name: tableDiff.TableName,
		Operations: []ast.AlterOperation{&ast.SetCommentOperation{
			Comment:    tableDiff.CommentChange.Desired,
			HasCurrent: tableDiff.CommentChange.Current != "",
		}},
	})
}

func (p *Planner) addAndModifyTableColumns(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, tableDiff := range diff.TablesModified {
		// The table's own comment
		// are emitted before its columns' changes, so a table whose only
		// difference is its comment still produces a statement: the condition
		// below asks about columns, and the comment belongs to the table
		// (stokaro/ptah#2168).
		result = appendTableComment(result, tableDiff)
		if len(tableDiff.ColumnsAdded) > 0 || len(tableDiff.ColumnsModified) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add new columns
			result = p.addNewTableColumns(result, tableDiff, diff.DeclaredTables, diff.DeclaredUserTypes, semantics)

			// Modify existing columns
			result = p.modifyExistingTableColumns(result, tableDiff, diff.DeclaredUserTypes)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewDescriptiveComment(fmt.Sprintf("Add/modify columns for table: %s", tableDiff.TableName))
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
func (p *Planner) addForeignKeyConstraintsForModifiedTables(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsAdded) > 0 {
			// Track the initial length to see if any actual operations were added
			initialLength := len(result)

			// Add foreign key constraints for new columns
			result = p.addForeignKeyConstraintsForNewColumns(result, tableDiff, diff.DeclaredTables, semantics)

			// Only add the comment if actual operations were performed
			if len(result) > initialLength {
				// Insert the comment at the beginning of the operations for this table
				astCommentNode := ast.NewDescriptiveComment(fmt.Sprintf("Add foreign key constraints for table: %s", tableDiff.TableName))
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

func (p *Planner) removeTableColumns(result []ast.Node, diff *difftypes.SchemaDiff, earlyColumns map[string]bool, ahead routinesAhead) []ast.Node {
	for _, tableDiff := range diff.TablesModified {
		if len(tableDiff.ColumnsRemoved) > 0 {
			astCommentNode := ast.NewDescriptiveComment(fmt.Sprintf("Remove columns from table: %s", tableDiff.TableName))
			result = append(result, astCommentNode)

			// Remove columns (dangerous!)
			result = p.removeTableColumnsFromDiff(result, diff, tableDiff, earlyColumns, ahead)
		}
	}
	return result
}

func (p *Planner) removeEarlyGeneratedColumns(result []ast.Node, diff *difftypes.SchemaDiff, earlyColumns map[string]bool) []ast.Node {
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsRemoved {
			if !earlyColumns[table.TableName+"\x00"+column.Name] {
				continue
			}
			result = append(result, &ast.AlterTableNode{Name: table.TableName, Operations: []ast.AlterOperation{
				&ast.DropColumnOperation{ColumnName: column.Name},
			}})
		}
	}
	return result
}

func (p *Planner) addNewIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
) ([]ast.Node, error) {
	indexRemovals := indexscope.NewConflictSetWithSemantics(
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff.IndexRemovals(),
	)
	guardedDrops := p.capabilities().Has(capability.DropIndexIfExists)
	constraintBacked := diff.ConstraintBackedIndexRemovalSet()

	// The declaration travels WITH the addition (stokaro/ptah#2315).
	for _, change := range diff.IndexesAdded {
		ref := difftypes.IndexRef{Name: change.Index.Name, TableName: change.TableName}
		index := change.Index
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
		indexNode := modelast.FromIndex(index)
		// CONCURRENTLY is opt-in policy AND capability-gated: the
		// planner never emits it for a target that rejects it
		// (issue #226; CockroachDB-style presets keep plain
		// CREATE INDEX even when the policy is on).
		//
		// Assigned rather than switched on, because the converter carries the
		// declaration too (stokaro/ptah#3042) and this is the path that owns
		// the answer. A conditional that could only turn it ON would let a
		// declaration past both gates here: the capability check above, and
		// the partitioned-table exclusion concurrentindex.DeclaredRefs makes,
		// where PostgreSQL refuses a concurrent build outright.
		indexNode.Concurrently = p.usesConcurrentIndex(ref) &&
			p.capabilities().Has(capability.CreateIndexConcurrently)
		result = append(result, indexNode)
	}
	return result, nil
}

func (p *Planner) removeIndexes(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	released map[difftypes.IndexRef]struct{},
) []ast.Node {
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
		// the constraint now needs. A released one went ahead of the column
		// changes, for the same reason.
		_, rebuilt := rebuiltAsConstraint[ref]
		_, dropped := released[ref]
		if rebuilt || dropped {
			continue
		}
		result = append(result, p.dropIndexNode(diff, ref))
	}
	return result
}

// dropIndexNode is the statement that removes the index ref.
func (p *Planner) dropIndexNode(diff *difftypes.SchemaDiff, ref difftypes.IndexRef) ast.Node {
	if _, ownedByConstraint := diff.ConstraintBackedIndexRemovalSet()[ref]; ownedByConstraint {
		return p.constraintBackedIndexDropNode(ref)
	}
	dropIndexNode := ast.NewDropIndex(ref.Name).
		SetTable(ref.TableName)
	// IF EXISTS on DROP INDEX is capability-gated intent, mirroring the MySQL
	// planner (issue #226). Every supported PostgreSQL line has the guard, so
	// the default preset keeps today's output; a preset without it (or a
	// composed set) actually changes the plan.
	if p.capabilities().Has(capability.DropIndexIfExists) {
		dropIndexNode.SetIfExists()
	}
	// CONCURRENTLY on a drop is opt-in policy AND capability-gated, exactly
	// like the build side. A redefinition never reaches here (removeIndexes
	// skips it as an addition conflict), and a constraint's backing index left
	// through the branch above, so a concurrent drop is always a standalone
	// index removal — never the drop half of a rebuild and never an index
	// backing a constraint, both of which PostgreSQL refuses to drop
	// concurrently.
	if p.usesConcurrentIndexDrop(ref) && p.capabilities().Has(capability.DropIndexConcurrently) {
		dropIndexNode.SetConcurrently()
	}
	return dropIndexNode
}

// releaseKeyNames drops the removed keys released holds, each as its removal
// step would drop it; see [keyrelease.Find].
func (p *Planner) releaseKeyNames(result []ast.Node, diff *difftypes.SchemaDiff, released keyrelease.Releases) []ast.Node {
	state := constraintPlanState{droppedForModify: make(map[constraintHostKey]struct{})}
	for _, info := range released.Constraints {
		result = p.appendScopedDrop(result, info.TableName, info.Name, info.Identity, state)
	}
	for _, ref := range released.Indexes {
		result = append(result, p.dropIndexNode(diff, ref))
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
func (p *Planner) constraintBackedIndexDropNode(ref difftypes.IndexRef) ast.Node {
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

// removeTables drops the tables the declaration no longer holds. A table whose
// row-level security policy reads another removed table goes before that
// table, since the policy goes with its own table; see [tableDropRelies].
func (p *Planner) removeTables(result []ast.Node, diff *difftypes.SchemaDiff, ahead routinesAhead) []ast.Node {
	dropped := make(map[string]bool, len(diff.TablesRemoved))
	for _, group := range p.removedTableGroups(diff, ahead) {
		for _, tableName := range group {
			result = p.appendRoutinesAhead(result, diff, tableName, ahead)
		}
		dropTableNode := ast.NewDropTable(group[0]).SetNames(group).
			SetIfExists()
		if p.describes() {
			dropTableNode.SetComment("WARNING: This will delete all data!")
		}
		for _, tableName := range group {
			dropped[tableName] = true
		}
		relied := false
		for _, tableName := range group {
			relied = relied || tableDropRelies(diff, tableName, dropped, ahead)
		}
		if p.cascades(relied) {
			dropTableNode.SetCascade()
		}

		result = append(result, dropTableNode)
	}
	return result
}

// removedTableGroups is the removed tables in the order they are dropped, each
// group one statement. A table is a group of its own, except in the exact
// style, where tables whose captured objects read each other in a cycle, such
// as two policies that each read the other table, go in one DROP TABLE.
// PostgreSQL drops the tables one statement names together, so their
// dependencies on each other need no CASCADE; one drop at a time, the first
// would rely on it.
func (p *Planner) removedTableGroups(diff *difftypes.SchemaDiff, ahead routinesAhead) [][]string {
	order := removedTableOrder(diff)
	groups := make([][]string, 0, len(order))
	if !p.exact() {
		for _, name := range order {
			groups = append(groups, []string{name})
		}
		return groups
	}
	readers := make(map[string]schemaext.Objects, len(diff.TablesRemoved))
	for _, removal := range diff.TablesRemoved {
		readers[removal.Name] = removal.Current.OwnedObjects
	}
	grouped := make(map[string]bool, len(order))
	for i, name := range order {
		if grouped[name] {
			continue
		}
		group := []string{name}
		grouped[name] = true
		// A table a later table's object reads joins that table, and so
		// does every table the group's members are read by in turn.
		for changed := true; changed; {
			changed = false
			for _, later := range order[i+1:] {
				if grouped[later] {
					continue
				}
				if slices.ContainsFunc(group, func(member string) bool { return capturedReads(readers[later], member, relationSemantics(diff)) }) {
					group = append(group, later)
					grouped[later] = true
					changed = true
				}
			}
		}
		groups = append(groups, group)
	}
	return groups
}

// removedTableOrder is the order the removed tables are dropped in: children
// before the tables their foreign keys reference, and a table whose captured
// object reads another removed table before that table.
func removedTableOrder(diff *difftypes.SchemaDiff) []string {
	names := deporder.TableDropOrderWithDependencies(diff.TablesRemoved.Names(), diff.DeclaredTables, diff.DeclaredTableDependencies)
	readers := make(map[string][]string)
	for _, removal := range diff.TablesRemoved {
		for _, name := range names {
			if name != removal.Name && capturedReads(removal.Current.OwnedObjects, name, relationSemantics(diff)) {
				readers[removal.Name] = append(readers[removal.Name], name)
			}
		}
	}
	return deporder.StableReverseDependencySort(names, readers)
}

func (p *Planner) removeEnums(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, enum := range diff.EnumsRemoved {
		// Every column, routine and type that can use the enum is changed or
		// removed by an earlier step, so the drop never relies on CASCADE.
		dropTypeNode := ast.NewDropType(enum.QualifiedName()).
			SetIfExists()
		if p.cascades(false) {
			dropTypeNode.SetCascade()
		}
		if p.describes() {
			dropTypeNode.SetComment("WARNING: Make sure no tables use this enum!")
		}

		result = append(result, dropTypeNode)
	}
	return result
}

// refuseYDBChanges refuses the changes only the YDB planner plans: a table's
// column families and partitioning settings, and role memberships. A diff
// reaches this planner with one only when it was built by hand or for YDB, and
// planning nothing would report the change applied. Feature changes go to
// their owners through the runtime instead, which refuses a YDB-owned change
// on this target because no owner plans it here.
func (p *Planner) refuseYDBChanges(diff *difftypes.SchemaDiff) error {
	if err := schemaprecondition.RefuseRoleMemberships(p.targetDialect(), diff); err != nil {
		return err
	}
	return nil
}

// GenerateMigrationAST generates PostgreSQL-specific migration AST statements from schema differences.
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
//	generated := &schemamodel.Database{
//		Enums: []schemamodel.Enum{
//			{Name: "user_status", Values: []string{"active", "inactive"}},
//		},
//		Tables: []schemamodel.Table{
//			{Name: "users", StructName: "User"},
//		},
//		Fields: []schemamodel.Field{
//			{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
//			{Name: "status", Type: "user_status", StructName: "User"},
//		},
//	}
//
//	nodes, err := planner.GenerateMigrationAST(ctx, runtime, diff)
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
func (p *Planner) GenerateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff) (plannedNodes []ast.Node, planErr error) {
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	defer func() {
		if err := ctx.Err(); err != nil {
			plannedNodes, planErr = nil, err
		}
	}()

	var windows featurehost.Windows
	nodes, err := p.generateMigrationAST(ctx, runtime, diff, &windows)
	if err != nil {
		return nil, err
	}
	nodes, err = p.scheduleFeatures(ctx, runtime, diff, nodes, windows)
	if err != nil {
		return nil, err
	}
	if p.exact() {
		// Over the scheduled plan, owner operations included, because what a
		// guard is for depends on every statement before it.
		settleGuards(nodes)
	}
	return nodes, nil
}

func (p *Planner) generateMigrationAST(ctx context.Context, runtime featureplan.Runtime, diff *difftypes.SchemaDiff, windows *featurehost.Windows) ([]ast.Node, error) {
	if err := schemaprecondition.RefuseServerSchemas(DialectName, diff); err != nil {
		return nil, err
	}
	// No owner on this family plans a setting attached to a materialized view,
	// and scheduleFeatures dispatches only standalone and table changes.
	if err := schemaprecondition.RefuseMaterializedViewFeatureChanges(p.targetDialect(), diff); err != nil {
		return nil, err
	}
	if err := p.refuseIndexChangesInPlace(diff); err != nil {
		return nil, err
	}
	if err := schemaprecondition.RefuseSerialSequenceChanges(p.targetDialect(), diff); err != nil {
		return nil, err
	}
	if err := p.refuseYDBChanges(diff); err != nil {
		return nil, err
	}
	var result []ast.Node
	var err error
	// One set of identifier rules for the whole plan. Every question of the form
	// "do these two spellings name the same object" is answered with it, so the
	// resolvers below and the statements that accompany what they resolve cannot
	// disagree about which table a reference belongs to.
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	// The identity check alone. Nothing is resolved: an addition carries its
	// own declaration (stokaro/ptah#2315).
	if err := indexscope.ValidateDiffWithSemantics(
		p.targetDialect(),
		diff.EffectiveIdentifierSemantics(p.targetDialect()),
		diff,
	); err != nil {
		return nil, err
	}
	// Row-level security belongs to its feature owner on this family, which
	// plans it through the runtime. A shared row-security entry reaches this
	// planner only in a diff built by hand, and planning nothing for it
	// would report an access-control change applied.
	if err := schemaprecondition.RefuseSharedRowSecurity(p.targetDialect(), diff); err != nil {
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
	// The views, materialized views and routines a column type change, a
	// materialized view change or a view change the server cannot make in
	// place drops and creates again. Every step below that would plan one of
	// them leaves it to the rebuild. It is planned after the skip policy,
	// which may leave a change out: only the drops the plan makes can take a
	// policy with them.
	rebuild := planViewRebuild(diff)
	if err := p.refusePoliciesLostToCascade(diff, rebuild); err != nil {
		return nil, err
	}
	if err := refuseUnrebuildable(diff, rebuild, semantics); err != nil {
		return nil, err
	}
	// Hand-built diffs may lack identities. Resolve them before either the
	// early dependency releases or the later replacement pairing uses them.
	constraintscope.Normalize(diff, semantics)

	// 0. Create the schemas this migration adds objects to, before anything is
	// created in them. Every phase below can name a schema, so this cannot sit
	// inside one of them.
	result = p.addSchemaPreconditions(result, diff)

	// Feature operations of the prerequisite phase that create or change an
	// object join the plan here: after the schemas they read and before every
	// object that may use what they supply. Their drops join it at the end.
	prerequisites := &featurehost.PrerequisiteWindows{Creation: len(result)}

	// 1. Add new roles (roles may be referenced by RLS policies and functions)
	result = p.addNewRoles(result, diff)

	// 2. Add the routines whose definitions name nothing this plan creates.
	// They go first because a domain CHECK, a column DEFAULT, a policy or a
	// trigger may call them. A routine that names a type or a relation this plan
	// creates waits for it, at step 4b or among the view-likes at step 6.6; see
	// deporder.PlaceRoutines.
	placements, err := p.routinePlacements(diff, rebuild)
	if err != nil {
		return nil, err
	}
	relationPlaced := p.relationRoutines(diff, placements)
	result = p.addNewFunctions(result, diff, placements, deporder.RoutineBeforeTypes)

	// 2b. Modify existing function definitions (body, volatility, security, language).
	// PostgreSQL CREATE OR REPLACE FUNCTION updates the live definition in place
	// without affecting policies or triggers that reference the function. A
	// change the server refuses to replace is a drop and a create instead; see
	// replacementIsRefused.
	result = p.modifyExistingFunctions(result, diff, placements, deporder.RoutineBeforeTypes)

	// 2c. Add new sequences before tables, since a table column may draw its
	// DEFAULT from a sequence. OWNED BY is applied later, after tables exist.
	result = p.addNewSequences(result, diff)

	// 3. Add new enums (PostgreSQL requires enum types to exist before tables use them)
	result = p.addNewEnums(result, diff)

	// 3c. Feature operations of the type phase that create a type, change one
	// in place or rebuild one join the plan here: after the enums and the
	// routines that name no type the plan creates, which a type may name, and
	// before the routines whose signatures name a type and the tables whose
	// columns may. Their pure drops join it at step 15.
	types := &featurehost.TypeWindows{Creation: featurehost.Span{Start: len(result), End: len(result)}}

	// 4. Modify existing enums
	result = p.modifyExistingEnums(result, diff)

	// 4b. The routines whose signatures name a type created above, before the
	// tables whose defaults may call them.
	result = p.addNewFunctions(result, diff, placements, deporder.RoutineAfterTypes)
	result = p.modifyExistingFunctions(result, diff, placements, deporder.RoutineAfterTypes)

	// 5. Add new tables
	result = p.addNewTables(result, diff, relationPlaced)

	// 5b. Drop a removed key that holds the name a column's own UNIQUE takes,
	// before the column changes below add that key. ADD CONSTRAINT c_x_key
	// otherwise answers that the name exists, and ADD COLUMN x int UNIQUE names
	// the key c_x_key1. Steps 11 and 12.5 skip what this drops.
	released := keyrelease.Find(diff, p.targetDialect())
	// Removed foreign keys must release referenced keys before any column or
	// constraint replacement. The same identity set suppresses later re-drops,
	// including the drop half of a foreign-key replacement.
	result, foreignKeysReleased := p.releaseRemovedForeignKeys(result, diff)
	result = p.releaseKeyNames(result, diff, released)

	// 5c. Views that read a column whose type changes leave first: PostgreSQL
	// refuses ALTER COLUMN TYPE under a view. A materialized view that changes,
	// and a view the server cannot replace in place, leaves with everything
	// that depends on it. Step 6.6 puts them back.
	var earlyColumns map[string]bool
	if p.exact() {
		earlyColumns = earlyGeneratedColumns(diff)
	}
	ahead := p.routinesAheadOf(diff, rebuild, earlyColumns)
	result = p.dropRebuiltViewLikes(result, diff, rebuild)

	// 6. Add and modify table columns (must be done before creating RLS policies that depend on columns)
	result = p.addAndModifyTableColumns(result, diff)

	// 6.5. Add foreign key constraints for newly added columns (must be done after all columns exist)
	result = p.addForeignKeyConstraintsForModifiedTables(result, diff)

	// 6.7. Associate added sequences with their owning table.column and apply
	// option changes to existing sequences, now that tables exist.
	result = p.addSequenceOwnership(result, diff)
	result = p.modifyExistingSequences(result, diff)

	// Feature operations that create or change objects join the plan here:
	// after the tables and columns they may read exist, and before the views
	// and materialized views that may read them. A TimescaleDB continuous
	// aggregate reads a hypertable or another aggregate, and a view can read
	// an aggregate.
	windows.Creation = len(result)

	// 6.6. Add and modify views, materialized views, and triggers after their
	// tables/functions exist. The routines that name a relation this plan
	// creates are ordered with the views, since either may read the other.
	result = p.addNewViewLikeObjects(result, diff, relationPlaced, rebuild)
	result = p.modifyExistingViews(result, diff, rebuild)
	result = p.createRebuiltViewLikes(result, diff, rebuild, relationPlaced)
	result = p.addNewTriggers(result, diff)
	result = p.modifyExistingTriggers(result, diff)

	// 7. Modify existing roles (must be done before RLS policies that reference them)
	result = p.modifyExistingRoles(result, diff)

	// 7.5. Revoke removed grants before adding replacement grants.
	result = p.removeGrants(result, diff, rebuild)
	result = p.revokeGrantOptions(result, diff, rebuild)

	// Feature operations of the dependent phase join the plan here: after the
	// views, role changes, row-security switches and policies they may name,
	// and before every removal step. Their owner orders its creations and
	// drops among themselves.
	windows.Dependent = len(result)

	// 9.1. Comments on objects that already existed, after every step that
	// creates, replaces or recreates one of them. The policies are the last
	// of those.
	result = p.changeObjectComments(result, diff)

	// 9.5. Add role privilege grants after roles and target objects exist.
	result = p.addNewGrants(result, diff, rebuild)

	// 10. Add new indexes
	result, err = p.addNewIndexes(result, diff)
	if err != nil {
		return nil, err
	}

	// 10.1. Comments on the indexes the plan keeps. An index the plan
	// creates writes its own comment above, and one it drops takes its
	// comment with it.
	result = changeIndexComments(result, diff)

	// 10.5. Add new constraints (must be done after tables and columns exist)
	result, err = p.addNewConstraints(result, diff, foreignKeysReleased)
	if err != nil {
		return nil, err
	}

	// 10.6. Add field-level foreign keys for new tables after referenced
	// unique indexes and constraints have been created.
	result = p.addForeignKeyConstraintsForNewTables(result, diff)

	// 10.7. Comments on the constraints both sides already hold. A constraint
	// added above takes its comment from the statement that adds it.
	result = changeConstraintComments(result, diff)

	// 10.8. Validate the constraints the database holds NOT VALID and the
	// declaration holds validated.
	result = validateConstraints(result, diff)

	// 10.9. An index the declaration shows or hides from the optimizer.
	// CockroachDB takes the change in place; on the other engines of this
	// family the renderer refuses it by the capability rather than leave the
	// change unplanned.
	result = changeIndexVisibility(result, diff)

	// 11. Remove indexes (safe operations)
	result = p.removeIndexes(result, diff, released.IndexSet())

	// 11.8. Owned table settings, CockroachDB row-level TTL and the Spanner
	// row deletion policy among them, after the columns a policy may refer to
	// exist and before anything is dropped -- INCLUDING a column. Both halves
	// are load-bearing: a policy retargeted to a column the same plan adds
	// cannot run before that column is there, and the column it currently
	// names cannot be dropped while the policy still points at it. A plan that
	// moved an expression off a column and dropped it once emitted `DROP
	// COLUMN … CASCADE` first (stokaro/ptah#1027, stokaro/ptah#2236).
	result, err = p.planTableFeatures(ctx, runtime, result, diff)
	if err != nil {
		return nil, err
	}
	// A removed CHECK on a table we keep may call a routine this plan removes.
	// Release it before moving that routine ahead of a removed relation. The
	// constraint's body is absent from the diff, so waiting until step 12.5
	// would make the relation drop rely on CASCADE for an unknown reader.
	releasedConstraints := released.ConstraintSet()
	maps.Copy(releasedConstraints, foreignKeysReleased)
	if p.exact() {
		result = p.removeConstraints(result, diff, releasedConstraints, constraintsBeforeRelations)
	}

	// 11.9. Remove triggers and view-like objects before the columns, the
	// constraints and the tables they depend on. A trigger may fire on UPDATE
	// OF a column, and a view reads columns and, through GROUP BY, a primary
	// key; dropped first, neither leaves a column drop relying on CASCADE.
	result = p.removeTriggers(result, diff)
	result = p.removeEarlyGeneratedColumns(result, diff, earlyColumns)
	result = p.removeViewLikes(result, diff, rebuild, ahead)

	// 12. Remove table columns (must be done after removing RLS policies that depend on columns)
	result = p.removeTableColumns(result, diff, earlyColumns, ahead)

	// 12.5. Remove constraints (must be done before removing tables)
	result = p.removeConstraints(result, diff, releasedConstraints, constraintsAfterColumns)

	// Feature operations that drop objects join the plan here: after the views
	// and materialized views that may read them are dropped, and before the
	// tables they may read.
	windows.Removal = len(result)

	// 13. Remove tables (dangerous!)
	result = p.removeTables(result, diff, ahead)

	// 13.5. Remove standalone sequences after tables that may default from them.
	result = p.removeSequences(result, diff)

	// 13. Remove functions (must be done after removing policies that might use them)
	result = p.removeFunctions(result, diff, rebuild, ahead, routinesBeforeTypes)

	// 14. Ordinarily roles leave after routines and before types. If a removed
	// domain still holds a routine, keep roles until after that routine leaves.
	postTypeRoutines := slices.ContainsFunc(diff.FunctionsRemoved, func(routine difftypes.RoutineChange) bool {
		return removedDomainCallsRoutine(diff, routine)
	}) || slices.ContainsFunc(diff.ProceduresRemoved, func(routine difftypes.RoutineChange) bool {
		return removedDomainCallsRoutine(diff, routine)
	})
	if !postTypeRoutines {
		result = p.removeRoles(result, diff)
	}

	// 15. The type phase's drops run after tables and routines that may use a
	// type. A removed domain whose CHECK or DEFAULT calls a removed routine
	// leaves here, before that routine: PostgreSQL records the dependency.
	types.Removal = featurehost.Span{Start: len(result), End: len(result)}
	result = p.removeFunctions(result, diff, rebuild, ahead, routinesAfterTypes)
	if postTypeRoutines {
		result = p.removeRoles(result, diff)
	}

	// 16. Remove enums after domains that may use them.
	result = p.removeEnums(result, diff)

	prerequisites.Removal = len(result)
	windows.Prerequisites = prerequisites
	windows.Types = types
	return result, nil
}

func (p *Planner) addNewRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The attributes travel WITH the change, so this renders what it was
	// handed rather than scanning the desired schema for a role of that name
	// (stokaro/ptah#2315).
	for _, role := range diff.RolesAdded {
		result = append(result, modelast.FromRole(role))
	}
	return result
}

func (p *Planner) modifyExistingRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The role travels WITH the change (stokaro/ptah#2315). It supplies the one
	// value the change map cannot: the password a `password_update_required`
	// entry asks to be set.
	for _, roleDiff := range diff.RolesModified {
		if roleDiff.Desired.Name == "" {
			continue // The pre-change database held no role of this name.
		}
		targetRole := &roleDiff.Desired

		alterRoleNode := p.buildAlterRoleNode(roleDiff, targetRole)
		if len(alterRoleNode.Operations) > 0 {
			if p.describes() {
				alterRoleNode.SetComment(fmt.Sprintf("Modify role %s attributes", roleDiff.RoleName))
			}
			result = append(result, alterRoleNode)
		}
	}
	return result
}

// buildAlterRoleNode creates an ALTER ROLE node with operations based on role changes
func (p *Planner) buildAlterRoleNode(roleDiff difftypes.RoleDiff, targetRole *schemamodel.Role) *ast.AlterRoleNode {
	alterRoleNode := ast.NewAlterRole(roleDiff.RoleName)

	// Sorted key order keeps the ALTER ROLE operation order deterministic
	// across runs (issue #59).
	for _, changeType := range slices.Sorted(maps.Keys(roleDiff.Changes)) {
		p.addRoleOperation(alterRoleNode, changeType, roleDiff.Changes[changeType], targetRole)
	}

	return alterRoleNode
}

// addRoleOperation adds the appropriate operation to the ALTER ROLE node based on change type and value
func (p *Planner) addRoleOperation(alterRoleNode *ast.AlterRoleNode, changeType, changeValue string, targetRole *schemamodel.Role) {
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
func (p *Planner) addPasswordOperation(alterRoleNode *ast.AlterRoleNode, changeValue string, targetRole *schemamodel.Role) {
	if changeValue == "password_update_required" {
		// Use the target role to get the new password
		if targetRole != nil && targetRole.Password != "" {
			alterRoleNode.AddOperation(ast.NewSetPasswordOperation(targetRole.Password))
		}
	}
}

func (p *Planner) removeRoles(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, role := range diff.RolesRemoved {
		dropRoleNode := ast.NewDropRole(role.Name).
			SetIfExists()
		if p.describes() {
			dropRoleNode.SetComment("WARNING: Ensure no other objects depend on this role")
		}
		result = append(result, dropRoleNode)
	}
	return result
}

// grantColumns is the column list of the statement for grant: its column, or
// none for a privilege on the whole object.
func grantColumns(grant difftypes.GrantRef) []string {
	if grant.Column == "" {
		return nil
	}
	return []string{grant.Column}
}

// addNewGrants grants the privileges the plan adds. A privilege on an object
// the view rebuild drops for good is left out, as it is by the revoking steps:
// the object is gone by then.
func (p *Planner) addNewGrants(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, grant := range diff.GrantsAdded {
		if rebuild.dropsForGood(grant, semantics) {
			continue
		}
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetArguments(grant.Arguments).
			SetColumns(grantColumns(grant)).
			SetWithOption(grant.WithOption)
		result = append(result, node)
	}
	for _, grant := range diff.GrantOptionsAdded {
		if rebuild.dropsForGood(grant, semantics) {
			continue
		}
		node := ast.NewGrantPrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetArguments(grant.Arguments).
			SetColumns(grantColumns(grant)).
			SetWithOption(true)
		result = append(result, node)
	}
	return result
}

// removeGrants revokes the privileges the plan removes, other than the ones on
// an object the view rebuild drops for good, which took its privileges with
// it before this step.
func (p *Planner) removeGrants(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, grant := range diff.GrantsRemoved {
		if rebuild.dropsForGood(grant, semantics) {
			continue
		}
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetArguments(grant.Arguments).
			SetColumns(grantColumns(grant))
		result = append(result, node)
	}
	return result
}

func (p *Planner) revokeGrantOptions(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, grant := range diff.GrantOptionsRevoked {
		if rebuild.dropsForGood(grant, semantics) {
			continue
		}
		node := ast.NewRevokePrivilege(grant.Role, grant.ObjectType, grant.ObjectName, []string{grant.Privilege}).
			SetArguments(grant.Arguments).
			SetColumns(grantColumns(grant)).
			SetGrantOptionFor(true)
		result = append(result, node)
	}
	return result
}

// modifiedFunctionNodes is the statements that replace one routine: CREATE OR
// REPLACE, or a drop and a create where the server refuses the replacement.
func (p *Planner) modifiedFunctionNodes(fnDiff difftypes.FunctionDiff) []ast.Node {
	target := fnDiff.Desired
	var result []ast.Node
	if replacementIsRefused(fnDiff) {
		// Not IF EXISTS: the routine was just read from the database, so a
		// drop that matches nothing means the signature is wrong, and
		// saying so here is clearer than the 42P13 the create would answer.
		// Not CASCADE either: a view, policy or trigger that uses the
		// routine makes the server refuse the drop, which stops the plan
		// rather than removing an object nobody asked to remove.
		drop := ast.NewDropFunction(target.Name).
			SetKind(target.Kind)
		if p.describes() {
			drop.SetComment(fmt.Sprintf("Drop function %s to recreate it: its %s changed",
				target.Name, refusedReplacementChanges(fnDiff)))
		}
		// An empty list is still a list: the routine that takes no
		// arguments is dropped as `f()`, and only a change that recorded
		// no identity at all leaves the statement naming the routine.
		if fnDiff.CurrentSignature != nil {
			drop.SetParameters(*fnDiff.CurrentSignature)
		}
		result = append(result, drop)
	}

	// The note is a statement of its own: the create node's comment is the
	// routine's, which the renderer writes with COMMENT ON.
	result = append(result, ast.NewDescriptiveComment(fmt.Sprintf("Modify function %s: %s",
		target.Name, summarizeFunctionChanges(fnDiff))))
	// A replacement even after the drop above: the plan still rewrites a
	// routine that exists, and the drop took its grants with it. Only an added
	// routine is a plain CREATE.
	return append(result, modelast.FromFunctionFor(target, p.targetDialect()).SetReplace())
}

// replacementIsRefused reports whether PostgreSQL refuses this modification
// through CREATE OR REPLACE, so the routine has to be dropped before the new
// definition is created.
//
// Two changes are refused, and the second is refused silently, which is worse.
//
//   - A return type. The server answers `cannot change return type of existing
//     function` (SQLSTATE 42P13), so a plan made of the replacement alone could
//     be applied in neither direction (stokaro/ptah#3288).
//   - The parameter list. Renaming one is refused outright with
//     `cannot change name of input parameter "n"` (42P13). Changing one's type
//     is ACCEPTED and creates a second overload, so the database ends with both
//     `scalar(integer)` and `scalar(bigint)` where the author declared one
//     routine, and the next comparison finds a routine nobody wrote
//     (stokaro/ptah#3327). Both were measured on PostgreSQL 18.
//
// Dropping by the signature the database holds now is what makes the second
// case right: the drop names the old parameter list and the create names the
// new one, so exactly one routine survives.
//
// The rule is keyed on the change rather than applied to every modification,
// because a drop is not free: it fails on any routine a view, policy or trigger
// uses, where a replacement keeps those objects in place.
func replacementIsRefused(fnDiff difftypes.FunctionDiff) bool {
	return refusedReplacementChanges(fnDiff) != ""
}

// refusedReplacementChanges names the changes [replacementIsRefused] drops the
// routine for, as the drop's comment says them, or answers empty for none. A
// fixed reason would name a return type for a routine whose parameters
// changed, and send a reader looking for a change nobody made
// (stokaro/ptah#3673).
func refusedReplacementChanges(fnDiff difftypes.FunctionDiff) string {
	var changed []string
	for _, change := range []struct{ key, words string }{
		{key: "returns", words: "return type"},
		{key: "parameters", words: "parameters"},
	} {
		if _, found := fnDiff.Changes[change.key]; found {
			changed = append(changed, change.words)
		}
	}
	return strings.Join(changed, " and ")
}

// summarizeFunctionChanges produces a deterministic one-line summary of the
// changed attributes for use as a SQL comment.
func summarizeFunctionChanges(fnDiff difftypes.FunctionDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(fnDiff.Changes)), ", ")
}

// routineRemovals prefers the list that carries signatures, falling back to the
// bare names for a diff built by a producer that fills only those.
//
// The fallback is not a formality: a SchemaDiff can be decoded from JSON
// written before the richer list existed, and a bare name still drops a routine
// correctly on every schema that does not overload it.

// removeFunctions drops the routines the declaration no longer holds, other
// than the ones dropped already: ahead of the relation they name, or by a view
// rebuild.
type routineRemovalPhase uint8

const (
	routinesBeforeTypes routineRemovalPhase = iota
	routinesAfterTypes
)

func (p *Planner) removeFunctions(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild, ahead routinesAhead, phase routineRemovalPhase) []ast.Node {
	for _, routine := range diff.FunctionsRemoved {
		if !ahead.moves(routine) && !rebuild.ownsRoutine(routine.Name) && removedDomainCallsRoutine(diff, routine) == (phase == routinesAfterTypes) {
			result = append(result, p.dropRoutineNode(routine, schemamodel.FunctionKindFunction))
		}
	}
	for _, routine := range diff.ProceduresRemoved {
		if !ahead.moves(routine) && !rebuild.ownsRoutine(routine.Name) && removedDomainCallsRoutine(diff, routine) == (phase == routinesAfterTypes) {
			result = append(result, p.dropRoutineNode(routine, schemamodel.FunctionKindProcedure))
		}
	}
	return result
}

// dropRoutineNode is the statement that drops a removed routine of kind, a
// function or a procedure; the list the removal came from says which.
func (p *Planner) dropRoutineNode(routine difftypes.RoutineChange, kind string) ast.Node {
	// The signature is what makes the statement addressable. A name alone is
	// refused with `function name "f" is not unique` on any schema that
	// overloads, and IF EXISTS does not help because the refusal is about
	// ambiguity rather than existence (stokaro/ptah#2296).
	node := ast.NewDropFunction(routine.Name).SetIfExists()
	if kind == schemamodel.FunctionKindProcedure {
		// A procedure is dropped with its own verb. PostgreSQL answers
		// `DROP FUNCTION` aimed at one with `could not find a function named
		// ...`, so the kind the comparator kept is what makes the statement
		// run (stokaro/ptah#1722).
		node = node.SetKind(schemamodel.FunctionKindProcedure)
		if p.describes() {
			node.SetComment("WARNING: Ensure no other objects depend on this procedure")
		}
	} else if p.describes() {
		node.SetComment("WARNING: Ensure no other objects depend on this function")
	}
	// An empty list addresses the overload that takes no arguments, so it is
	// carried like any other; only a removal that recorded no identity leaves
	// the statement naming the routine.
	if routine.Signature != nil {
		node = node.SetParameters(*routine.Signature)
	}
	return node
}

// appendRoutinesAhead appends the drops of the routines the plan drops just
// ahead of relation; see [routinesAhead].
func (p *Planner) appendRoutinesAhead(result []ast.Node, diff *difftypes.SchemaDiff, relation string, ahead routinesAhead) []ast.Node {
	for _, routine := range ahead.of(relation) {
		kind := schemamodel.FunctionKindFunction
		if slices.ContainsFunc(diff.ProceduresRemoved, func(candidate difftypes.RoutineChange) bool {
			return routineKey(candidate) == routineKey(routine)
		}) {
			kind = schemamodel.FunctionKindProcedure
		}
		result = append(result, p.dropRoutineNode(routine, kind))
	}
	return result
}

func (p *Planner) appendRoutinesAheadColumn(result []ast.Node, diff *difftypes.SchemaDiff, table, column string, ahead routinesAhead) []ast.Node {
	for _, routine := range ahead.ofColumn(table, column) {
		kind := schemamodel.FunctionKindFunction
		if slices.ContainsFunc(diff.ProceduresRemoved, func(candidate difftypes.RoutineChange) bool {
			return routineKey(candidate) == routineKey(routine)
		}) {
			kind = schemamodel.FunctionKindProcedure
		}
		result = append(result, p.dropRoutineNode(routine, kind))
	}
	return result
}

// addNewSequences emits CREATE SEQUENCE for newly added sequences. The OWNED BY
// association is deliberately omitted here and emitted later by
// addSequenceOwnership, because a sequence referenced by a column DEFAULT must
// be created before its table while OWNED BY requires the table to already
// exist.
func (p *Planner) addNewSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// No lookup: the change carries the sequence (stokaro/ptah#2315).
	for _, sequence := range diff.SequencesAdded {
		sequenceNode := modelast.FromSequence(sequence)
		sequenceNode.OwnedBy = ""
		result = append(result, sequenceNode)
	}
	return result
}

// addSequenceOwnership emits ALTER SEQUENCE ... OWNED BY for newly added
// sequences that declare an owner, after their owning tables exist.
func (p *Planner) addSequenceOwnership(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, sequence := range diff.SequencesAdded {
		if sequence.OwnedBy == "" {
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
func (p *Planner) modifyExistingSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The sequence travels WITH the change; the change map names the options
	// that moved and this carries their values (stokaro/ptah#2315).
	for _, sequenceDiff := range diff.SequencesModified {
		sequence := sequenceDiff.Desired
		if sequence.Name == "" {
			continue
		}
		node := alterSequenceFromDiff(sequence, sequenceDiff.Changes)
		if p.describes() {
			node.SetComment(fmt.Sprintf("Modify sequence %s: %s", sequenceDiff.SequenceName, summarizeSequenceChanges(sequenceDiff)))
		}
		result = append(result, node)
	}
	return result
}

// alterSequenceFromDiff builds an ALTER SEQUENCE node carrying only the options
// that the diff reports as changed, sourced from the target definition.
func alterSequenceFromDiff(target schemamodel.Sequence, changes map[string]string) *ast.AlterSequenceNode {
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
func summarizeSequenceChanges(sequenceDiff difftypes.SequenceDiff) string {
	return strings.Join(slices.Sorted(maps.Keys(sequenceDiff.Changes)), ", ")
}

// removeSequences emits DROP SEQUENCE for sequences no longer present in the
// target schema. It runs after table removal so a table that draws a column
// default from the sequence is gone first.
func (p *Planner) removeSequences(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, sequence := range diff.SequencesRemoved {
		schemaName, sequenceName := splitQualifiedSequenceName(sequence.QualifiedName())
		dropSequence := ast.NewDropSequence(sequenceName).
			SetIfExists()
		if p.describes() {
			dropSequence.SetComment("WARNING: Ensure no column default still draws from this sequence")
		}
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

// addNewViewLikeObjects creates the views and materialized views the plan
// adds, with the routines placed among them. An added object that reads one a
// view rebuild takes out, and a routine that names one, waits for the rebuild
// to put it back; see [Planner.createRebuiltViewLikes].
func (p *Planner) addNewViewLikeObjects(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	routines relationRoutines,
	rebuild viewRebuild,
) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	deferred := make(viewLikeSet)
	for _, object := range append(slices.Clone(rebuild.deferred), deferredRoutines(rebuild, routines)...) {
		deferred[viewLikeKey(object)] = true
	}
	var objects []deporder.ViewLike
	for _, routine := range routines.viewLikes() {
		if !deferred.has(routine) {
			objects = append(objects, routine)
		}
	}
	routineNodes := routines.nodes
	for _, view := range diff.ViewsAdded {
		// The body travels WITH the change, so the dependency order this
		// computes does not depend on finding the view again.
		if object := (deporder.ViewLike{Name: view.Name, Body: view.Body}); !deferred.has(object) {
			objects = append(objects, object)
		}
	}
	for _, view := range diff.MaterializedViewsAdded {
		// The body travels WITH the change, so the dependency order this
		// computes does not depend on finding the view again.
		if object := (deporder.ViewLike{Name: view.Name, Body: view.Body, Materialized: true}); !deferred.has(object) {
			objects = append(objects, object)
		}
	}

	// The dialect is what lets a body reference resolve through PostgreSQL's
	// quoting rules. Without it a view referenced by its quoted qualified
	// spelling -- `"analytics"."base"` -- matches no declaration, gains no
	// dependency edge, and is created AFTER the view that reads it, so the plan
	// renders cleanly and fails when it runs.
	for _, object := range deporder.ViewLikesForCreateIn(objects, p.targetDialect(), diff.EffectiveIdentifierSemantics(p.targetDialect())) {
		if object.Routine {
			// Every overload of the name and its replacement statements, once.
			result = append(result, routineNodes[object.Name]...)
			delete(routineNodes, object.Name)
			continue
		}
		if object.Materialized {
			if view := findMaterializedView(diff.DeclaredViewLikes.MaterializedViews, object.Name, semantics); view != nil {
				result = append(result, modelast.FromMaterializedView(*view))
			}
			continue
		}
		if view := findView(diff.DeclaredViewLikes.Views, object.Name, semantics); view != nil {
			result = append(result, modelast.FromView(*view))
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
// applies once everything that depends on the view is dropped before it, and
// each of those has to be created again, with its privileges; the replace
// disturbs none of them.
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
// Whatever the drop path takes with it comes back: every declared view and
// materialized view that reads a dropped view, transitively, the routines that
// use its row type, and the indexes, triggers and privileges on them. Without
// that the plan silently left the database short of the schema it was
// generated from, and re-planning did not converge (issue #1287). What CASCADE
// removes in Ptah's own style and nothing can rebuild is anything Ptah does not
// declare -- a hand-made view, a rule -- which is why the drop is taken only
// where it buys something.
//
// A view on the drop path is rebuilt, with everything that depends on it; see
// [viewRebuild]. This step replaces the views on the replace path that the
// rebuild neither owns nor replaced before its drops, in creation order.
func (p *Planner) modifyExistingViews(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild) []ast.Node {
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	owned, replaced := rebuild.owns(), rebuild.replaces()
	for _, object := range deporder.ViewLikesForCreateIn(modifiedViewPaths(diff).replaced, p.targetDialect(), semantics) {
		if !owned.has(object) && !replaced.has(object) {
			result = appendNode(result, p.viewReplace(diff, object, semantics))
		}
	}
	return result
}

// viewPaths is the split [Planner.modifyExistingViews] makes of the modified
// views: the ones the rebuild drops and creates again, and the ones this step
// replaces in place.
type viewPaths struct {
	dropped  []deporder.ViewLike
	replaced []deporder.ViewLike
}

func modifiedViewPaths(diff *difftypes.SchemaDiff) viewPaths {
	var paths viewPaths
	for _, viewDiff := range diff.ViewsModified {
		// The view this change leaves behind travels WITH it, in the direction
		// being planned (stokaro/ptah#2315).
		view := &viewDiff.Desired
		if view.Name == "" {
			continue
		}
		object := deporder.ViewLike{Name: view.Name, Body: view.Body}
		if viewReplaceKeepsDependents(viewDiff, view.Body) {
			paths.replaced = append(paths.replaced, object)
			continue
		}
		paths.dropped = append(paths.dropped, object)
	}
	return paths
}

// viewReplaceKeepsDependents decides whether one modified view keeps the
// in-place replace, which is the statement that leaves dependents and grants
// alone. The table in modifyExistingViews explains the three answers.
func viewReplaceKeepsDependents(viewDiff difftypes.ViewDiff, targetBody string) bool {
	switch viewReplaceLegality(viewDiff.PreviousBody, targetBody) {
	case viewReplaceAppendsColumns:
		return true
	case viewReplaceMovesColumns:
		return false
	default:
		return !viewDiff.Rollback
	}
}

// removeViewLikes drops the views and materialized views the declaration no
// longer holds, each after every removed one that reads it, so no drop relies
// on CASCADE to take another removed object. One the view rebuild dropped
// already is left out. A drop relies on CASCADE when a removed routine uses
// the object's row type or a policy of a removed table reads it, since both
// are dropped later; see [removalOutlivesDrop].
func (p *Planner) removeViewLikes(result []ast.Node, diff *difftypes.SchemaDiff, rebuild viewRebuild, ahead routinesAhead) []ast.Node {
	owned := rebuild.owns()
	removed := make([]deporder.ViewLike, 0, len(diff.ViewsRemoved)+len(diff.MaterializedViewsRemoved))
	for _, object := range removedViewLikes(diff) {
		if !owned.has(object) {
			removed = append(removed, object)
		}
	}
	for _, object := range deporder.ViewLikesForDropIn(removed, p.targetDialect(), diff.EffectiveIdentifierSemantics(p.targetDialect())) {
		result = p.appendRoutinesAhead(result, diff, object.Name, ahead)
		result = append(result, p.dropViewLikeNode(object, p.cascades(removalOutlivesDrop(diff, object.Name, ahead))))
	}
	return result
}

// dropViewNode is the statement that drops the view name, with CASCADE where
// cascade asks for it and [Planner.dropsWithCascade] allows it.
func (p *Planner) dropViewNode(name string, cascade bool) *ast.DropViewNode {
	drop := ast.NewDropView(name).SetIfExists()
	drop.Cascade = cascade && p.dropsWithCascade()
	return drop
}

// dropMaterializedViewNode is the statement that drops the materialized view
// name, with CASCADE where cascade asks for it. No target without CASCADE has
// materialized views.
func dropMaterializedViewNode(name string, cascade bool) *ast.DropMaterializedViewNode {
	drop := ast.NewDropMaterializedView(name).SetIfExists()
	drop.Cascade = cascade
	return drop
}

func (p *Planner) addNewTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	// The definition travels WITH the entry (stokaro/ptah#2315). An entry
	// carrying none names a trigger nothing can render, so it plans nothing.
	for _, triggerRef := range diff.TriggersAdded {
		if triggerRef.Desired.Name != "" {
			result = append(result, modelast.FromTrigger(triggerRef.Desired))
		}
	}
	return result
}

func (p *Planner) modifyExistingTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, triggerDiff := range diff.TriggersModified {
		if triggerDiff.Desired.Name != "" {
			result = append(result, modelast.FromTrigger(triggerDiff.Desired).SetReplace())
		}
	}
	return result
}

func (p *Planner) removeTriggers(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	for _, triggerRef := range diff.TriggersRemoved {
		// Nothing depends on a trigger, so the drop never relies on CASCADE.
		drop := ast.NewDropTrigger(triggerRef.TriggerName, triggerRef.TableName).SetIfExists()
		if p.cascades(false) {
			drop.SetCascade()
		}
		if triggerRef.ExecuteFunction != "" {
			// The function is declared on its own; dropping it here would take
			// it from every other trigger that runs it (stokaro/ptah#3722).
			result = append(result, drop.SetExternalFunction())
			continue
		}
		functionName := schemamodel.Trigger{Name: triggerRef.TriggerName, Table: triggerRef.TableName}.FunctionName()
		result = append(result, drop.SetFunctionName(functionName))
	}
	return result
}

func findView(views []schemamodel.View, name string, semantics identifier.Semantics) *schemamodel.View {
	return objectlookup.View(views, name, semantics)
}

func findMaterializedView(
	views []schemamodel.MaterializedView,
	name string,
	semantics identifier.Semantics,
) *schemamodel.MaterializedView {
	return objectlookup.MaterializedView(views, name, semantics)
}

// addNewConstraints adds new table-level constraints via ALTER TABLE statements.
//
// This method processes constraints defined through Go struct annotations and creates
// appropriate ALTER TABLE ADD CONSTRAINT statements. It handles different constraint
// types including EXCLUDE, CHECK, UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints.
//
// # Constraint Processing Order
//
// Referenced keys are added before foreign keys. Removed foreign keys have
// already been released before any key or column replacement. This method
// assumes that all referenced tables and columns already exist.
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
func (p *Planner) addNewConstraints(result []ast.Node, diff *difftypes.SchemaDiff, released map[constraintHostKey]struct{}) ([]ast.Node, error) {
	state := newConstraintPlanState(diff, diff.EffectiveIdentifierSemantics(p.targetDialect()))
	maps.Copy(state.droppedForModify, released)

	result = p.addPrimaryKeyConstraintsWithTables(result, diff.ConstraintsAdded, state)
	result = p.addCheckAndUniqueConstraintsWithTables(
		result,
		diff.ConstraintsAdded,
		state,
		diff.IndexRemovalsRebuiltAsUniqueConstraints(),
	)
	result, err := p.addNamedConstraintsByKind(result, diff, state, nonForeignKeyConstraints)
	if err != nil {
		return nil, err
	}
	result = p.addForeignKeyConstraintsWithTables(result, diff.ConstraintsAdded, state)
	result, err = p.addNamedConstraintsByKind(result, diff, state, foreignKeyConstraints)
	if err != nil {
		return nil, err
	}
	return result, nil
}

type constraintKindFilter int

const (
	nonForeignKeyConstraints constraintKindFilter = iota
	foreignKeyConstraints
)

type constraintPlanState struct {
	// semantics is the rule the COMPARATOR resolved this diff's names under,
	// carried so every key built here answers the same question it did.
	semantics          identifier.Semantics
	removedNames       map[string]struct{}
	removalByTableName map[constraintHostKey]difftypes.ConstraintRemovalInfo
	removalsByName     map[string][]difftypes.ConstraintRemovalInfo
	addedHostsByName   map[string]map[constraintHostKey]struct{}
	handled            map[string]struct{}
	droppedForModify   map[constraintHostKey]struct{}
	// replacedPrimaryKeys are the primary keys the diff removes from a table
	// it adds a primary key to; see [replacedPrimaryKeys].
	replacedPrimaryKeys map[string]difftypes.ConstraintRemovalInfo
}

// constraintHostKey is the shared identity model's comparison value for a
// constraint, under this file's own name.
//
// It was a private struct here and a byte-identical one in the MySQL
// planner. Two copies of "which constraint is this" is how one planner comes to
// pair a drop with a different constraint than the other does
// (stokaro/ptah#1345).
type constraintHostKey = difftypes.ConstraintIdentity

func newConstraintPlanState(diff *difftypes.SchemaDiff, semantics identifier.Semantics) constraintPlanState {
	// A constraint name present in BOTH ConstraintsAdded and ConstraintsRemoved
	// is a modification (the comparator expresses a changed constraint as
	// remove + add of the same name — e.g. an on_delete change on a field-level
	// FK, issue #189). For those we must DROP the old definition before adding
	// the new one, otherwise the ADD CONSTRAINT collides with the still-present
	// constraint of the same name. removeConstraints runs later in the pipeline
	// and deliberately skips these names, so the drop+add is owned here and
	// ordered correctly.
	removedNames := make(map[string]struct{}, len(diff.ConstraintsRemoved))
	for _, name := range constraintscope.RemovalNames(diff) {
		removedNames[name] = struct{}{}
	}

	// Prefer the table-qualified additions when the comparator supplied them.
	// A field-level FK contributed by an embedded inline-relation mixin shares
	// one constraint name across every host table, so the bare ConstraintsAdded
	// name list (and a field scan keyed on the Go struct name) cannot target the
	// right table — it would emit ALTER TABLE <MixinStruct> once per host
	// (issue #197). ConstraintsAdded carries the concrete table and
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
	removalByTableName := make(map[constraintHostKey]difftypes.ConstraintRemovalInfo, len(diff.ConstraintsRemoved))
	for _, info := range diff.ConstraintsRemoved {
		removalByTableName[info.Identity] = info
	}

	// Index removals by bare name as well, so the legacy ConstraintsAdded loop
	// below can scope a modified non-FK constraint's DROP to its concrete host
	// table(s). The comparator records every removal in
	// ConstraintsRemoved in lockstep with the bare ConstraintsRemoved
	// list, so a modified constraint's host is normally known here even though
	// the bare loop iterates names alone.
	removalsByName := make(map[string][]difftypes.ConstraintRemovalInfo, len(diff.ConstraintsRemoved))
	for _, info := range diff.ConstraintsRemoved {
		removalsByName[info.Name] = append(removalsByName[info.Name], info)
	}

	// Index the hosts that are actually being re-ADDED under each name. A modified
	// constraint's pre-drop must hit only the hosts whose constraint is being
	// re-added — NOT every host that merely has a removal entry for the name. In
	// the MIXED case (issue #206) a shared name is a modify on host A (re-added)
	// and a PURE removal on host B (not re-added): B's drop is owned by
	// removeConstraints, so the add-side modify-drop must leave B alone or it
	// would be dropped twice.
	// Keyed by the WHOLE identity, not by its table half: the outer map already
	// scopes by the constraint's name, so inside one bucket the name is a
	// constant and carrying it costs nothing.
	addedHostsByName := make(map[string]map[constraintHostKey]struct{}, len(diff.ConstraintsAdded))
	for _, add := range diff.ConstraintsAdded {
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
			hosts = make(map[constraintHostKey]struct{})
			addedHostsByName[add.Name] = hosts
		}
		hosts[add.Identity] = struct{}{}
	}

	return constraintPlanState{
		semantics:           semantics,
		removedNames:        removedNames,
		removalByTableName:  removalByTableName,
		removalsByName:      removalsByName,
		addedHostsByName:    addedHostsByName,
		handled:             make(map[string]struct{}),
		droppedForModify:    make(map[constraintHostKey]struct{}),
		replacedPrimaryKeys: replacedPrimaryKeys(diff, semantics),
	}
}

// replacedPrimaryKeys answers the primary keys diff removes from a table it
// adds a primary key to, keyed by table. A table holds one primary key, so the
// removed key is dropped before the added one whatever either is called. Under
// two names the pair is no modification, and the ADD ran ahead of the DROP
// the removals get, which the server refuses (stokaro/ptah#3959).
//
// The planner reads it twice: [Planner.addPrimaryKeyConstraintsWithTables]
// drops each key ahead of its replacement, and [Planner.removeConstraints]
// leaves it out of the removals, where a second drop would follow the ADD.
func replacedPrimaryKeys(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[string]difftypes.ConstraintRemovalInfo {
	adding := make(map[string]struct{})
	for _, add := range diff.ConstraintsAdded {
		if add.TableName != "" && strings.EqualFold(add.Type, "PRIMARY KEY") {
			adding[semantics.QualifiedTableIdentityKey(add.TableName)] = struct{}{}
		}
	}
	replaced := make(map[string]difftypes.ConstraintRemovalInfo)
	for _, info := range diff.ConstraintsRemoved {
		if info.TableName == "" || !strings.EqualFold(info.Type, "PRIMARY KEY") {
			continue
		}
		table := semantics.QualifiedTableIdentityKey(info.TableName)
		if _, found := adding[table]; found {
			replaced[table] = info
		}
	}
	return replaced
}

// addNamedConstraintsByKind refuses an addition the record-driven passes could
// not render.
//
// A comparison describes every constraint it adds, the synthesized ones
// included: `ConstraintsWithSemantics` folds a field's `check=` and `foreign=`
// into the same map before it compares, so they reach the diff as records like
// any other. A name arriving here therefore has no body, and a diff that names a
// constraint it does not describe is a caller error.
//
// Resolving such a name against a declaration handed to the planner, in three
// precedence steps, emits nothing when all three miss. That is a second input
// shape -- a diff of names, planned against a schema that describes them -- and
// withdrawing it is what lets a planner take no declaration at all
// (stokaro/ptah#2315).
func (p *Planner) addNamedConstraintsByKind(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	state constraintPlanState,
	kind constraintKindFilter,
) ([]ast.Node, error) {
	wantForeignKey := kind == foreignKeyConstraints
	for _, constraintName := range constraintscope.AdditionNames(diff) {
		// Already emitted via one of the record-driven passes.
		if _, done := state.handled[constraintName]; done {
			continue
		}
		if constraintRecordIsForeignKey(diff.ConstraintsAdded, constraintName) != wantForeignKey {
			continue
		}
		message := fmt.Sprintf(
			"%s: constraint %q is added without a definition the planner can render; "+
				"a diff has to describe the constraints it names",
			ptaherr.ErrInvalidSchemaDiff, constraintName,
		)
		return nil, (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.InvalidSchema, Kind: "constraint", Object: constraintName, Message: message,
		}}}).Err(p.targetDialect())
	}
	return result, nil
}

// constraintRecordIsForeignKey answers which of the two passes a named addition
// belongs to, from the record the comparison carries for it.
//
// A record carrying no kind answers false, so such a name is refused once, by
// the non-foreign-key pass, rather than by both.
func constraintRecordIsForeignKey(additions difftypes.ConstraintAdditions, name string) bool {
	for _, add := range additions {
		if add.Name == name {
			return strings.EqualFold(add.Type, "FOREIGN KEY")
		}
	}
	return false
}

func (p *Planner) addForeignKeyConstraintsWithTables(
	result []ast.Node,
	additions difftypes.ConstraintAdditions,
	state constraintPlanState,
) []ast.Node {
	for _, add := range additions {
		if add.Type != "FOREIGN KEY" || add.TableName == "" {
			continue
		}
		// Only emit the DROP-before-ADD when this exact host's FK is being
		// modified (its (table, name) is in the removal set). A pure-add host
		// gets no phantom drop.
		key := add.Identity
		if _, modified := state.removalByTableName[key]; modified {
			result = p.emitModifyDrop(result, add, state)
		}
		result = append(result, p.foreignKeyAdditionNode(add))
		state.handled[add.Name] = struct{}{}
	}
	return result
}

func (p *Planner) addCheckAndUniqueConstraintsWithTables(
	result []ast.Node,
	additions difftypes.ConstraintAdditions,
	state constraintPlanState,
	rebuiltIndexes map[difftypes.IndexRef]struct{},
) []ast.Node {
	for _, add := range additions {
		constraint := constraintAdditionNode(add)
		if constraint == nil {
			continue
		}
		key := add.Identity
		if _, modified := state.removalByTableName[key]; modified {
			result = p.emitModifyDrop(result, add, state)
		}
		result = p.dropIndexRebuiltAsConstraint(result, add, rebuiltIndexes)
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}},
		})
		state.handled[add.Name] = struct{}{}
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
// [difftypes.SchemaDiff.IndexRemovalsRebuiltAsUniqueConstraints] for the shape that
// produces the collision.
func (p *Planner) dropIndexRebuiltAsConstraint(
	result []ast.Node,
	add difftypes.ConstraintAdditionInfo,
	rebuiltIndexes map[difftypes.IndexRef]struct{},
) []ast.Node {
	ref := difftypes.IndexRef{Name: add.Name, TableName: add.TableName}
	if _, rebuilt := rebuiltIndexes[ref]; !rebuilt {
		return result
	}
	dropIndexNode := ast.NewDropIndex(ref.Name).SetTable(ref.TableName)
	if p.capabilities().Has(capability.DropIndexIfExists) {
		dropIndexNode.SetIfExists()
	}
	return append(result, dropIndexNode)
}

func constraintAdditionNode(add difftypes.ConstraintAdditionInfo) *ast.ConstraintNode {
	node := constraintAdditionNodeByType(add)
	if node == nil {
		return nil
	}
	node.Comment = add.Comment
	node.Facets = add.Facets
	return node
}

// constraintAdditionNodeByType builds the node one constraint type needs.
//
// The comment is attached by the caller rather than here, so a constraint type
// added to this switch cannot lose it by omission.
func constraintAdditionNodeByType(add difftypes.ConstraintAdditionInfo) *ast.ConstraintNode {
	if add.TableName == "" {
		return nil
	}
	switch add.Type {
	case "CHECK":
		if add.CheckExpression == "" {
			return nil
		}
		return &ast.ConstraintNode{
			Type:        ast.CheckConstraint,
			Name:        add.Name,
			Expression:  add.CheckExpression,
			NotEnforced: add.NotEnforced,
			NotValid:    add.NotValid,
		}
	case "UNIQUE":
		if len(add.Columns) == 0 {
			return nil
		}
		constraint := ast.NewUniqueConstraint(add.Name, add.Columns...)
		constraint.IncludeColumns = append([]string(nil), add.IncludeColumns...)
		constraint.NullsDistinct = cloneBoolPtr(add.NullsDistinct)
		constraint.Deferrable, constraint.Initially = add.Deferrable, add.Initially
		return constraint
	default:
		return nil
	}
}

func (p *Planner) addPrimaryKeyConstraintsWithTables(
	result []ast.Node,
	additions difftypes.ConstraintAdditions,
	state constraintPlanState,
) []ast.Node {
	for _, add := range additions {
		if add.Type != "PRIMARY KEY" || add.TableName == "" || len(add.Columns) == 0 {
			continue
		}
		if replaced, found := state.replacedPrimaryKeys[state.semantics.QualifiedTableIdentityKey(add.TableName)]; found {
			result = p.appendScopedDrop(result, replaced.TableName, replaced.Name, replaced.Identity, state)
		}
		// The payload rides with the key, exactly as it does for a covering
		// UNIQUE. Without it the re-ADD builds a plain key and the INCLUDE the
		// DROP just removed never comes back (stokaro/ptah#2199).
		primaryKey := ast.NewPrimaryKeyConstraint(add.Columns...)
		primaryKey.Name = writtenPrimaryKeyName(add)
		primaryKey.IncludeColumns = append([]string(nil), add.IncludeColumns...)
		// So does the deferral (stokaro/ptah#3824).
		primaryKey.Deferrable, primaryKey.Initially = add.Deferrable, add.Initially
		result = append(result, &ast.AlterTableNode{
			Name:       add.TableName,
			Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: primaryKey}},
		})
		state.handled[add.Name] = struct{}{}
	}
	return result
}

// writtenPrimaryKeyName is the name an added primary key is written with: the
// name it asks for, or none where that is the name PostgreSQL gives an unnamed
// key. Without the name, a declared key is built as `<table>_pkey`, and the
// next comparison looks for it under the name it was declared with
// (stokaro/ptah#3959).
func writtenPrimaryKeyName(add difftypes.ConstraintAdditionInfo) string {
	table := add.TableName
	if ref, ok := tableref.Parse(add.TableName); ok {
		table = ref.Name
	}
	if add.Name == pgname.Object(table, "", "pkey") {
		return ""
	}
	return add.Name
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
// The name-only DO block (dropConstraintNode) is not used for a modify
// whose host the comparator recorded: the legacy ConstraintsAdded modify path
// scopes its DROP via emitModifyDropForName too, and removeConstraints scopes
// pure removals table-qualified as well. It remains in use only as a defensive
// fallback for a synthetic diff that carries no ConstraintsRemoved
// entry.
func (p *Planner) emitModifyDrop(
	result []ast.Node,
	add difftypes.ConstraintAdditionInfo,
	state constraintPlanState,
) []ast.Node {
	return p.appendScopedDrop(result, add.TableName, add.Name, add.Identity, state)
}

// appendScopedDrop appends a single direct, table-qualified
// ALTER TABLE <table> DROP CONSTRAINT IF EXISTS <name>, deduped per (table, name)
// via droppedForModify so a constraint name shared across host tables is dropped
// once per host and never twice for the same host.
func (p *Planner) appendScopedDrop(
	result []ast.Node,
	table, name string,
	dedupKey constraintHostKey,
	state constraintPlanState,
) []ast.Node {
	if _, done := state.droppedForModify[dedupKey]; done {
		return result
	}
	state.droppedForModify[dedupKey] = struct{}{}
	return append(result, &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: name,
			IfExists:       true,
		}},
	})
}

// foreignKeyAdditionNode builds the ALTER TABLE ADD CONSTRAINT node for a
// table-qualified field-level FK addition (ConstraintsAdded). The
// table comes straight from the comparator's synthesized constraint, so this
// path is correct for FK names that repeat across the many tables embedding an
// inline-relation mixin (issue #197), unlike the legacy field-scan fallback
// that re-derived the table from a Go struct name.
func (p *Planner) foreignKeyAdditionNode(add difftypes.ConstraintAdditionInfo) *ast.AlterTableNode {
	fkRef := &ast.ForeignKeyRef{
		Table:       add.ForeignTable,
		Column:      add.ForeignColumn,
		Columns:     add.ForeignColumns,
		OnDelete:    add.OnDelete,
		OnUpdate:    add.OnUpdate,
		Deferrable:  add.Deferrable,
		Initially:   add.Initially,
		Match:       add.Match,
		NotEnforced: add.NotEnforced,
	}
	fkRef.Name = add.Name
	constraint := ast.NewForeignKeyConstraint(add.Name, add.Columns, fkRef)
	// The renderer writes the comment as COMMENT ON CONSTRAINT after the ADD.
	// Without it a commented foreign key is added bare, and the next
	// comparison plans its comment on its own.
	constraint.Comment = add.Comment
	constraint.NotValid = add.NotValid
	constraint.Facets = add.Facets
	return &ast.AlterTableNode{
		Name:       add.TableName,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: constraint}},
	}
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
type constraintRemovalPhase uint8

const (
	constraintsBeforeRelations constraintRemovalPhase = iota
	constraintsAfterColumns
)

func (p *Planner) removeConstraints(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	released map[constraintHostKey]struct{},
	phase constraintRemovalPhase,
) []ast.Node {
	earlyChecks := phase == constraintsBeforeRelations
	// The pure-removal path keys its hosts by the same rule addNewConstraints
	// does, for the same reason: the two sides of a modify spell one table two
	// ways, and a verbatim key answers that they are two (stokaro/ptah#1987).
	// Its dedup set is its own -- these drops are not the modify pre-drops.
	state := constraintPlanState{
		semantics:        diff.EffectiveIdentifierSemantics(p.targetDialect()),
		droppedForModify: make(map[constraintHostKey]struct{}),
	}
	replacedKeys := replacedPrimaryKeys(diff, state.semantics)
	droppedTables := make(map[string]struct{}, len(diff.TablesRemoved))
	for _, name := range diff.TablesRemoved.Names() {
		droppedTables[state.semantics.QualifiedTableIdentityKey(name)] = struct{}{}
	}

	// A removed constraint is dropped from its exact owning table with a direct,
	// table-qualified ALTER TABLE <host> DROP CONSTRAINT IF EXISTS <name>. The
	// comparator records that host in ConstraintsRemoved in lockstep
	// with the bare ConstraintsRemoved name list, so real diff output always
	// carries it.
	//
	// The name-only information_schema DO block (dropConstraintNode) is used ONLY
	// as a defensive fallback for a synthetic, hand-built diff that lists a
	// removed constraint by name with no ConstraintsRemoved host — it
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
	// ConstraintsAdded in lockstep with the bare list, so the modify
	// owner is always known per host.
	modifySet := make(map[constraintHostKey]struct{}, len(diff.ConstraintsAdded))
	addedHostCounts := make(map[string]int, len(diff.ConstraintsAdded))
	for _, add := range diff.ConstraintsAdded {
		if add.TableName == "" {
			// Hostless addition entries do not count as recorded hosts —
			// mirroring addedHostsByName in addNewConstraints — so the
			// hostless-re-add rule below still engages (issue #229).
			continue
		}
		modifySet[add.Identity] = struct{}{}
		addedHostCounts[add.Name]++
	}

	// Bare added names, for re-adds whose hosts were NOT recorded
	// (ConstraintsAdded carries the name but ConstraintsAdded has no
	// entry for it — reverse/down diffs of non-FK modifies, legacy callers
	// without an introspected schema, and hand-built diffs). For those,
	// emitModifyDropForName cannot restrict its pre-drop to the re-added
	// hosts, so it drops EVERY recorded removal host BEFORE the re-add;
	// dropping any of them again here would land AFTER the re-add and delete
	// the freshly restored constraint — IF EXISTS is no protection against
	// dropping a constraint that now exists again. This silently destroyed
	// the constraint on every non-FK down migration (issue #229).
	addedBareNamesHosted := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range constraintscope.AdditionNames(diff) {
		addedBareNamesHosted[name] = struct{}{}
	}

	// When the comparator supplied the owning table (ConstraintsRemoved),
	// drop the constraint from that exact table with a direct ALTER TABLE … DROP
	// CONSTRAINT IF EXISTS, deduped per (table, name) via appendScopedDrop. This
	// is required for a field-level FK whose name repeats across the many tables
	// embedding an inline-relation mixin (issue #197): the name-only DO block
	// below resolves a single table via information_schema LIMIT 1, so it would
	// drop the constraint from only one of the host tables and silently leave the
	// rest. Names that carried at least one host are recorded so the bare
	// fallback below — which exists only for synthetic diffs — does not re-emit
	// the name-only DO block for them.
	namesWithHost := make(map[string]struct{})
	for _, info := range diff.ConstraintsRemoved {
		if info.TableName == "" {
			if earlyChecks {
				continue
			}
			// No host recorded for this entry; defer to the bare fallback.
			continue
		}
		namesWithHost[info.Name] = struct{}{}
		_, tableRemoved := droppedTables[state.semantics.QualifiedTableIdentityKey(info.TableName)]
		if (p.exact() && strings.EqualFold(strings.TrimSpace(info.Type), "CHECK") && !tableRemoved) != earlyChecks {
			continue
		}
		if skipConstraintRemoval(info, state.semantics, replacedKeys, droppedTables,
			modifySet, released, addedBareNamesHosted, addedHostCounts) {
			continue
		}
		result = p.appendScopedDrop(result, info.TableName, info.Name, info.Identity, state)
	}

	// Bare fallback for synthetic diffs only: a hand-built diff may list a
	// removed constraint by name with no ConstraintsRemoved host. Such
	// names have genuinely no table to scope by, so the runtime information_schema
	// DO block (dropConstraintNode) remains the only option. Real comparator
	// output always carries the host, so it is fully handled above and skipped
	// here. A bare modify (name in ConstraintsAdded with no recorded host) is
	// owned by addNewConstraints and skipped.
	addedBareNames := make(map[string]struct{}, len(diff.ConstraintsAdded))
	for _, name := range constraintscope.AdditionNames(diff) {
		addedBareNames[name] = struct{}{}
	}
	if earlyChecks {
		return result
	}
	for _, constraintName := range constraintscope.RemovalNames(diff) {
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

// skipConstraintRemoval leaves drops owned by another planning phase to that
// phase. A second drop can remove a freshly restored constraint.
func skipConstraintRemoval(
	info difftypes.ConstraintRemovalInfo,
	semantics identifier.Semantics,
	replacedKeys map[string]difftypes.ConstraintRemovalInfo,
	droppedTables map[string]struct{},
	modifySet, released map[constraintHostKey]struct{},
	addedBareNamesHosted map[string]struct{},
	addedHostCounts map[string]int,
) bool {
	key := info.Identity
	if _, modified := modifySet[key]; modified {
		return true
	}
	if replaced, found := replacedKeys[semantics.QualifiedTableIdentityKey(info.TableName)]; found && replaced.Identity == key {
		return true
	}
	if _, dropped := droppedTables[semantics.QualifiedTableIdentityKey(info.TableName)]; dropped && strings.EqualFold(info.Type, "PRIMARY KEY") {
		// PostgreSQL removes the key with its table. CockroachDB refuses an
		// isolated primary key drop (stokaro/ptah#4277).
		return true
	}
	if _, dropped := released[key]; dropped {
		return true
	}
	if _, added := addedBareNamesHosted[info.Name]; added && addedHostCounts[info.Name] == 0 {
		// addNewConstraints already dropped every recorded host before the
		// hostless re-add; repeating it would delete the new constraint.
		return true
	}
	return false
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

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}
