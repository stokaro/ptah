// Package postgres implements the PostgreSQL SQL renderer, turning Ptah AST
// nodes into PostgreSQL DDL including enums, sequences, roles, row-level
// security policies, and check constraints.
package postgres

import (
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/defaultlit"
)

// Renderer provides PostgreSQL-specific SQL rendering
type Renderer struct {
	// currentEnums stores enum names available in the current rendering context
	currentEnums []string
	dialect      string
	dialectUpper string
	caps         capability.Capabilities
	w            bufwriter.Writer
}

func (r *Renderer) VisitUpsert(_ *ast.UpsertNode) error {
	return unsupportedFeaturef("upsert rendering is not implemented for %s", r.dialect)
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	// Build DROP INDEX statement for PostgreSQL
	var parts []string
	parts = append(parts, "DROP INDEX")

	// CONCURRENTLY precedes IF EXISTS in PostgreSQL's grammar:
	// DROP INDEX CONCURRENTLY [ IF EXISTS ] name.
	if node.Concurrently && r.capabilities().Has(capability.DropIndexConcurrently) {
		parts = append(parts, "CONCURRENTLY")
	}

	if node.IfExists && r.capabilities().Has(capability.DropIndexIfExists) {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, r.qualifiedIndexTarget(node.Table, node.Name))

	if node.Cascade {
		parts = append(parts, "CASCADE")
	}

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

// qualifiedIndexTarget spells an index the way a statement that names the index
// itself -- DROP INDEX, COMMENT ON INDEX -- has to spell it.
//
// An index lives in its table's namespace, so the qualifier is borrowed from the
// table rather than taken from the index name. CockroachDB does not address an
// index by a schema-qualified name at all; it addresses it as table@index.
func (r *Renderer) qualifiedIndexTarget(table, name string) string {
	if r.dialect == platform.CockroachDB && table != "" {
		return r.escapeQualifiedIdentifier(table) + "@" + r.escapeIdentifier(name)
	}
	tableParts := splitQualifiedIdentifier(table)
	if len(tableParts) < 2 {
		// No table namespace to borrow, so the index name is the only place a
		// qualifier can be, and `DROP INDEX app.idx` puts one there. Escaping
		// it whole would emit "app.idx" as a single identifier and name an
		// index nobody created. See [ast.DropIndexNode.Name].
		return r.escapeQualifiedIdentifier(name)
	}
	schemaParts := tableParts[:len(tableParts)-1]
	return r.escapeQualifiedIdentifier(strings.Join(schemaParts, ".")) + "." + r.escapeIdentifier(name)
}

// VisitCreateSchema renders a CREATE SCHEMA statement.
func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}
	schemaName := r.escapeIdentifier(node.Name)
	r.w.WriteLinef("CREATE SCHEMA%s %s;", guard, schemaName)
	if node.Comment != "" {
		r.w.WriteLinef("COMMENT ON SCHEMA %s IS %s;", schemaName, r.escapeValue(node.Comment))
	}
	return nil
}

// VisitCreateDatabase renders a CREATE DATABASE statement.
func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	if node.IfNotExists {
		return fmt.Errorf("create database if not exists is not supported in PostgreSQL")
	}
	r.w.WriteLinef("CREATE DATABASE %s;", r.escapeIdentifier(node.Name))
	return nil
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Handle different type definitions
	switch typeDef := node.TypeDef.(type) {
	case *ast.EnumTypeDef:
		// CREATE TYPE name AS ENUM (value1, value2, ...)
		values := make([]string, len(typeDef.Values))
		for i, value := range typeDef.Values {
			values[i] = r.escapeValue(value)
		}
		r.w.WriteLinef("CREATE TYPE %s AS ENUM (%s);", r.escapeQualifiedIdentifier(node.Name), strings.Join(values, ", "))

	case *ast.CompositeTypeDef:
		// CREATE TYPE name AS (field1 type1, field2 type2, ...)
		fields := make([]string, len(typeDef.Fields))
		for i, field := range typeDef.Fields {
			fields[i] = fmt.Sprintf("%s %s", r.escapeIdentifier(field.Name), field.Type)
		}
		r.w.WriteLinef("CREATE TYPE %s AS (%s);", r.escapeQualifiedIdentifier(node.Name), strings.Join(fields, ", "))

	case *ast.DomainTypeDef:
		// CREATE DOMAIN name AS base_type [NOT NULL] [DEFAULT value] [CHECK (constraint)]
		sql := fmt.Sprintf("CREATE DOMAIN %s AS %s", r.escapeQualifiedIdentifier(node.Name), typeDef.BaseType)

		// Add NOT NULL if specified
		if !typeDef.Nullable {
			sql += " NOT NULL"
		}

		// Add DEFAULT if specified
		if typeDef.Default != nil {
			if typeDef.Default.HasLiteral() {
				sql += fmt.Sprintf(" DEFAULT %s", r.renderDefaultLiteral(typeDef.Default.Value))
			} else if typeDef.Default.Expression != "" {
				sql += fmt.Sprintf(" DEFAULT %s", typeDef.Default.Expression)
			}
		}

		// Add CHECK constraint if specified
		if typeDef.Check != "" {
			sql += fmt.Sprintf(" CHECK (%s)", typeDef.Check)
		}

		r.w.WriteLinef("%s;", sql)

	case *ast.RangeTypeDef:
		// CREATE TYPE name AS RANGE (SUBTYPE = ..., [SUBTYPE_OPCLASS = ...], ...)
		options := []string{fmt.Sprintf("SUBTYPE = %s", typeDef.Subtype)}
		if typeDef.SubtypeOpClass != "" {
			options = append(options, fmt.Sprintf("SUBTYPE_OPCLASS = %s", typeDef.SubtypeOpClass))
		}
		if typeDef.Collation != "" {
			options = append(options, fmt.Sprintf("COLLATION = %s", typeDef.Collation))
		}
		if typeDef.Canonical != "" {
			options = append(options, fmt.Sprintf("CANONICAL = %s", typeDef.Canonical))
		}
		if typeDef.SubtypeDiff != "" {
			options = append(options, fmt.Sprintf("SUBTYPE_DIFF = %s", typeDef.SubtypeDiff))
		}
		r.w.WriteLinef("CREATE TYPE %s AS RANGE (%s);", r.escapeQualifiedIdentifier(node.Name), strings.Join(options, ", "))

	default:
		return fmt.Errorf("unsupported type definition: %T", typeDef)
	}

	return nil
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	// Process each operation
	for _, operation := range node.Operations {
		switch op := operation.(type) {
		case *ast.AddEnumValueOperation:
			// ALTER TYPE name ADD VALUE 'new_value' [BEFORE 'existing_value' | AFTER 'existing_value']
			sql := fmt.Sprintf("ALTER TYPE %s ADD VALUE %s", r.escapeQualifiedIdentifier(node.Name), r.escapeValue(op.Value))

			if op.Before != "" {
				sql += fmt.Sprintf(" BEFORE %s", r.escapeValue(op.Before))
			} else if op.After != "" {
				sql += fmt.Sprintf(" AFTER %s", r.escapeValue(op.After))
			}

			r.w.WriteLinef("%s;", sql)

		case *ast.RenameEnumValueOperation:
			// ALTER TYPE name RENAME VALUE 'old_value' TO 'new_value'
			r.w.WriteLinef("ALTER TYPE %s RENAME VALUE %s TO %s;",
				r.escapeQualifiedIdentifier(node.Name), r.escapeValue(op.OldValue), r.escapeValue(op.NewValue))

		case *ast.RenameTypeOperation:
			// ALTER TYPE name RENAME TO new_name
			r.w.WriteLinef("ALTER TYPE %s RENAME TO %s;", r.escapeQualifiedIdentifier(node.Name), r.escapeIdentifier(op.NewName))

		default:
			return fmt.Errorf("unsupported alter type operation: %T", operation)
		}
	}

	return nil
}

// New creates a new PostgreSQL renderer
func New() *Renderer {
	return NewWithCapabilities(capability.Postgres17(), platform.Postgres)
}

// NewWithCapabilities creates a PostgreSQL-family renderer configured for a
// concrete target capability set. The dialect label controls diagnostics and
// GetDialect output; SQL emission is controlled by caps.
func NewWithCapabilities(caps capability.Capabilities, dialect string) *Renderer {
	normalized := platform.NormalizeDialect(dialect)
	if normalized == "" {
		normalized = platform.Postgres
	}
	return &Renderer{
		currentEnums: nil,
		dialect:      normalized,
		dialectUpper: strings.ToUpper(normalized),
		caps:         caps.Clone(),
	}
}

func (r *Renderer) Dialect() string {
	return r.dialect
}

func (r *Renderer) capabilities() capability.Capabilities {
	return r.caps
}

func (r *Renderer) Reset() {
	r.w.Reset()
}

func (r *Renderer) Output() string {
	return r.w.Output()
}

// Render renders an AST node to SQL and returns the result
func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		return "", err
	}
	return r.Output(), nil
}

// escapeValue properly escapes a string value for use in SQL
func (r *Renderer) escapeValue(value string) string {
	// Escape single quotes by doubling them (PostgreSQL standard)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

// renderDefaultLiteral quotes value only when it is not already written as a
// literal. A default reaches the renderer either bare, the way a struct tag
// supplies it, or already quoted, the way the SQL parser read it; escaping the
// second form a second time changes the value it stands for. See the defaultlit
// package.
func (r *Renderer) renderDefaultLiteral(value string) string {
	return defaultlit.Render(value, r.escapeValue)
}

// escapeIdentifier safely escapes SQL identifiers (table/column names) for PostgreSQL
func (r *Renderer) escapeIdentifier(identifier string) string {
	// Escape double quotes by doubling them and wrap in double quotes
	unquoted := unquoteIdentifier(identifier)
	escaped := strings.ReplaceAll(unquoted, `"`, `""`)
	return `"` + escaped + `"`
}

func (r *Renderer) escapeQualifiedIdentifier(identifier string) string {
	parts := splitQualifiedIdentifier(identifier)
	for i, part := range parts {
		parts[i] = r.escapeIdentifier(part)
	}
	return strings.Join(parts, ".")
}

func (r *Renderer) escapeIdentifierList(identifiers []string) []string {
	escaped := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		escaped[i] = r.escapeIdentifier(identifier)
	}
	return escaped
}

func (r *Renderer) escapeQualifiedIdentifierList(identifiers []string) []string {
	escaped := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		escaped[i] = r.escapeQualifiedIdentifier(identifier)
	}
	return escaped
}

func (r *Renderer) escapeFunctionSignature(name, parameters string) string {
	signature := r.escapeQualifiedIdentifier(name)
	if parameters == "" {
		return signature + "()"
	}
	return signature + "(" + parameters + ")"
}

func (r *Renderer) escapeRoleTarget(role string) string {
	role = strings.TrimSpace(role)
	if isPostgreSQLRoleKeyword(role) {
		return strings.ToUpper(role)
	}
	return r.escapeIdentifier(role)
}

func (r *Renderer) escapeRoleTargetList(roles string) string {
	parts := strings.Split(roles, ",")
	for i, role := range parts {
		parts[i] = r.escapeRoleTarget(role)
	}
	return strings.Join(parts, ", ")
}

func isPostgreSQLRoleKeyword(role string) bool {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "PUBLIC", "CURRENT_ROLE", "CURRENT_USER", "SESSION_USER":
		return true
	default:
		return false
	}
}

func unquoteIdentifier(identifier string) string {
	if len(identifier) >= 2 && identifier[0] == '"' && identifier[len(identifier)-1] == '"' {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], `""`, `"`)
	}
	return identifier
}

// splitQualifiedIdentifier splits on the dots that separate name parts while
// leaving dots inside a double-quoted part alone. A doubled quote is SQL's
// escape for a literal quote and does not end the quoted part.
//
// Each part is a SLICE of the input, never a character-by-character copy. The
// two delimiters this scan recognizes are ASCII, and UTF-8 is self
// synchronizing -- no byte of a multi-byte sequence is ever below 0x80 -- so a
// byte scan can find them without decoding, and slicing hands every other byte
// back exactly as it arrived. The previous form accumulated `string(character)`
// from a byte, which re-encodes each byte as its own code point: `Ä` (C3 84)
// came back out as `Ã` plus U+0084, renaming every non-ASCII object. See
// stokaro/ptah#1352.
//
// Decoding to runes would fix that case and introduce another: text that is not
// valid UTF-8 -- a Latin-1 schema file, say -- decodes to U+FFFD per bad byte
// and would be rewritten just as silently. A splitter owes its caller the bytes
// it was given.
func splitQualifiedIdentifier(identifier string) []string {
	var parts []string
	start := 0
	inQuotes := false
	for i := 0; i < len(identifier); i++ {
		switch {
		case identifier[i] == '"' && inQuotes && i+1 < len(identifier) && identifier[i+1] == '"':
			i++
		case identifier[i] == '"':
			inQuotes = !inQuotes
		case identifier[i] == '.' && !inQuotes:
			parts = append(parts, identifier[start:i])
			start = i + 1
		}
	}
	return append(parts, identifier[start:])
}

// GetDialect returns the database dialect (alias for Dialect for compatibility)
func (r *Renderer) GetDialect() string {
	return r.Dialect()
}

// GetOutput returns the current generated SQL output (alias for Output for compatibility)
func (r *Renderer) GetOutput() string {
	return r.Output()
}

// VisitCreateTable renders CREATE TABLE with PostgreSQL-specific handling
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	if node.Comment != "" {
		r.w.WriteLinef("-- %s TABLE: %s (%s) --", r.dialectUpper, node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- %s TABLE: %s --", r.dialectUpper, node.Name)
	}

	guard := ""
	if node.IfNotExists {
		guard = " IF NOT EXISTS"
	}

	if node.SelectBody != "" {
		return r.visitCreateTableAsSelect(node, guard)
	}

	// The lines are rendered before the CREATE TABLE header so that a foreign
	// key this target cannot host is named on its own line rather than left as
	// a hole in the column list.
	lines, refused, err := r.renderCreateTableLines(node)
	if err != nil {
		return err
	}
	for _, name := range refused {
		r.writeObjectSkipped(foreignKeyConstraintKind, name)
	}

	r.w.WriteLinef("CREATE TABLE%s %s (", guard, r.escapeQualifiedIdentifier(node.Name))
	for i, line := range lines {
		if i == len(lines)-1 {
			r.w.WriteLine(line) // Last line without comma
		} else {
			r.w.WriteLinef("%s,", line)
		}
	}

	r.w.Write(")")

	if node.Partition != nil {
		partition, err := r.renderPartition(node.Partition)
		if err != nil {
			return err
		}
		r.w.Write(" ")
		r.w.Write(partition)
	}

	// Table options (PostgreSQL-specific filtering applied)
	if len(node.Options) > 0 {
		options := r.renderTableOptions(node.Options)
		if options != "" {
			r.w.Write(" ")
			r.w.Write(options)
		}
	}

	// Row-level TTL is a storage parameter, so it shares the WITH position with
	// the options above; the two are rendered separately because a TTL is
	// refused on a target that lacks the capability rather than filtered out of
	// a map (stokaro/ptah#1027).
	rowTTL, err := r.renderRowTTL(node)
	if err != nil {
		return err
	}
	r.w.Write(rowTTL)

	r.w.WriteLine(";")
	r.w.WriteLine("")

	return nil
}

func (r *Renderer) visitCreateTableAsSelect(node *ast.CreateTableNode, guard string) error {
	if len(node.Columns) > 0 || len(node.Constraints) > 0 {
		return fmt.Errorf("postgres: create table as select with explicit column definitions is not supported")
	}

	r.w.WriteLinef("CREATE TABLE%s %s AS", guard, r.escapeQualifiedIdentifier(node.Name))
	r.w.WriteLine(strings.TrimSpace(node.SelectBody))
	r.w.WriteLine(";")
	r.w.WriteLine("")
	return nil
}

// renderCreateTableLines renders the body of a CREATE TABLE and, separately,
// the identities of the foreign keys this target cannot host. The caller names
// those on their own lines; returning them rather than writing them keeps the
// skip comments out of the parenthesized column list.
func (r *Renderer) renderCreateTableLines(node *ast.CreateTableNode) (lines, refused []string, err error) {
	lines = make([]string, 0, len(node.Columns)+len(node.Constraints))
	for _, column := range node.Columns {
		line, err := r.renderColumn(column)
		if err != nil {
			return nil, nil, fmt.Errorf("error rendering column %s: %w", column.Name, err)
		}
		lines = append(lines, line)
	}

	for _, constraint := range node.Constraints {
		line, err := r.renderConstraint(constraint)
		if err != nil {
			return nil, nil, fmt.Errorf("error rendering constraint: %w", err)
		}
		if line == "" {
			refused = append(refused, foreignKeyIdentity(constraint))
			continue
		}
		lines = append(lines, line)
	}

	return r.appendColumnForeignKeyLines(lines, refused, node.Columns)
}

func (r *Renderer) renderPartition(partition *ast.PartitionSpec) (string, error) {
	partitionType := strings.ToUpper(strings.TrimSpace(partition.Type))
	if partitionType == "" {
		return "", fmt.Errorf("postgres partition requires type")
	}
	if len(partition.Parts) == 0 {
		return "", fmt.Errorf("postgres partition requires at least one key")
	}
	parts := make([]string, 0, len(partition.Parts))
	for _, part := range partition.Parts {
		switch {
		case part.Name != "" && part.Expr != "":
			return "", fmt.Errorf("postgres partition key cannot set both column and expression")
		case part.Name != "":
			parts = append(parts, r.escapeIdentifier(part.Name))
		case part.Expr != "":
			parts = append(parts, renderPartitionExpression(part.Expr))
		default:
			return "", fmt.Errorf("postgres partition key cannot be empty")
		}
	}
	return fmt.Sprintf("PARTITION BY %s (%s)", partitionType, strings.Join(parts, ", ")), nil
}

func renderPartitionExpression(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		return expr
	}
	return "(" + expr + ")"
}

func (r *Renderer) appendColumnForeignKeyLines(
	lines, refused []string,
	columns []*ast.ColumnNode,
) (outLines, outRefused []string, err error) {
	for _, column := range columns {
		if column.ForeignKey == nil {
			continue
		}
		constraint := columnForeignKeyConstraint(column)
		line, err := r.renderConstraint(constraint)
		if err != nil {
			return nil, nil, fmt.Errorf("error rendering foreign key constraint: %w", err)
		}
		// An empty line here used to be appended anyway, which put a blank
		// entry between two commas inside the column list of a target that
		// cannot host foreign keys.
		if line == "" {
			refused = append(refused, foreignKeyIdentity(constraint))
			continue
		}
		lines = append(lines, line)
	}
	return lines, refused, nil
}

func columnForeignKeyConstraint(column *ast.ColumnNode) *ast.ConstraintNode {
	fk := column.ForeignKey
	return &ast.ConstraintNode{
		Type:    ast.ForeignKeyConstraint,
		Name:    fk.Name,
		Columns: []string{column.Name},
		Reference: &ast.ForeignKeyRef{
			Table:    fk.Table,
			Column:   fk.Column,
			OnDelete: fk.OnDelete,
			OnUpdate: fk.OnUpdate,
			Name:     fk.Name,
		},
	}
}

// VisitAlterTable renders PostgreSQL-specific ALTER TABLE statements
func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	r.w.WriteLine("-- ALTER statements: --")

	for _, operation := range node.Operations {
		switch op := operation.(type) {
		case *ast.AddColumnOperation:
			line, err := r.renderColumn(op.Column)
			if err != nil {
				return fmt.Errorf("error rendering add column: %w", err)
			}
			// Remove the leading spaces from column rendering for ALTER
			line = strings.TrimPrefix(line, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD COLUMN %s;", r.escapeQualifiedIdentifier(node.Name), line)
		case *ast.AddConstraintOperation:
			constraintLine, err := r.renderConstraint(op.Constraint)
			if err != nil {
				return fmt.Errorf("error rendering add constraint: %w", err)
			}
			if constraintLine == "" {
				r.writeObjectSkipped(foreignKeyConstraintKind, foreignKeyIdentity(op.Constraint))
				continue
			}
			// Remove the leading spaces from constraint rendering for ALTER
			constraintLine = strings.TrimPrefix(constraintLine, "  ")
			r.w.WriteLinef("ALTER TABLE %s ADD %s;", r.escapeQualifiedIdentifier(node.Name), constraintLine)
		case *ast.DropConstraintOperation:
			dropSQL := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT", r.escapeQualifiedIdentifier(node.Name))
			if op.IfExists {
				dropSQL += " IF EXISTS"
			}
			dropSQL += fmt.Sprintf(" %s", r.escapeIdentifier(op.ConstraintName))
			r.w.WriteLinef("%s;", dropSQL)
		case *ast.DropColumnOperation:
			dropSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", r.escapeQualifiedIdentifier(node.Name), r.escapeIdentifier(op.ColumnName))
			if op.Cascade {
				dropSQL += " CASCADE"
			}
			r.w.WriteLinef("%s;", dropSQL)
		case *ast.ModifyColumnOperation:
			// PostgreSQL uses different syntax for modifying columns
			r.renderPostgreSQLModifyColumn(node.Name, op.Column)
		case *ast.AlterGeneratedColumnExpressionOperation:
			if !r.capabilities().Has(capability.AlterGeneratedColumnExpression) {
				// Name the capability, not the version. The gate is the
				// capability set, and the set says no on targets whose version
				// number says nothing about it: a PostgreSQL-compatible engine,
				// a managed provider that withholds the statement, a preset
				// composed with .With(..., false). The release that added it
				// stays in the sentence as the reason, not as the verdict.
				r.w.WriteLinef(
					"-- %s: ALTER COLUMN SET EXPRESSION requires target capability %s, unavailable on this target (PostgreSQL added it in 17); generated column %q was not changed.",
					r.dialectUpper,
					capability.AlterGeneratedColumnExpression,
					op.ColumnName,
				)
				continue
			}
			expression := strings.TrimSpace(op.Expression)
			if expression == "" {
				return fmt.Errorf("postgres: generated column %q has empty expression", op.ColumnName)
			}
			r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s SET EXPRESSION AS (%s);",
				r.escapeQualifiedIdentifier(node.Name),
				r.escapeIdentifier(op.ColumnName),
				expression,
			)
		case *ast.RenameColumnOperation:
			// PostgreSQL has supported `ALTER TABLE x RENAME COLUMN old TO new`
			// for a long time; emit it unconditionally.
			r.w.WriteLinef("ALTER TABLE %s RENAME COLUMN %s TO %s;",
				r.escapeQualifiedIdentifier(node.Name), r.escapeIdentifier(op.OldName), r.escapeIdentifier(op.NewName))
		case *ast.RenameTableOperation:
			r.w.WriteLinef("ALTER TABLE %s RENAME TO %s;", r.escapeQualifiedIdentifier(node.Name), r.escapeIdentifier(op.NewName))
		case *ast.AddSkippingIndexOperation, *ast.ModifyTTLOperation:
			// Two ClickHouse-specific constructs with no PostgreSQL equivalent,
			// sharing one arm so this switch keeps its complexity budget. The
			// ClickHouse table TTL is a different feature from the CockroachDB
			// row-level TTL the next arm carries: that one is a column
			// expression on a MergeTree table, this one a set of storage
			// parameters.
			r.writeClickHouseOnlyOperation(operation)
		case *ast.SetRowTTLOperation, *ast.ResetRowTTLOperation:
			// Both row-level TTL operations share one branch so this switch
			// keeps its complexity budget; writeRowTTLOperation re-selects
			// between them, which is a two-case type switch of its own rather
			// than another arm here (stokaro/ptah#1027).
			if err := r.writeRowTTLOperation(node, operation); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown alter operation type: %T", operation)
		}
	}

	r.w.WriteLine("")

	return nil
}

func (r *Renderer) VisitColumn(node *ast.ColumnNode) error {
	// This is typically called from within other visitors
	// The actual rendering is done by RenderColumn
	return nil
}

func (r *Renderer) VisitConstraint(node *ast.ConstraintNode) error {
	// This is typically called from within other visitors
	// The actual rendering is done by RenderConstraint
	return nil
}

func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	var parts []string

	parts = append(parts, "CREATE")

	if node.Unique {
		parts = append(parts, "UNIQUE")
	}
	if node.NullsDistinct != nil && !node.Unique {
		return fmt.Errorf("postgresql NULLS DISTINCT is only valid for unique indexes")
	}

	parts = append(parts, "INDEX")

	// CONCURRENTLY precedes IF NOT EXISTS in the PostgreSQL grammar:
	// CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ...
	if node.Concurrently && r.capabilities().Has(capability.CreateIndexConcurrently) {
		parts = append(parts, "CONCURRENTLY")
	}

	if node.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	parts = append(parts, r.escapeIdentifier(node.Name))
	parts = append(parts, "ON")
	parts = append(parts, r.escapeQualifiedIdentifier(node.Table))

	// Add index type (USING clause) for PostgreSQL.
	//
	// The btree comparison is case-insensitive because the access method now
	// has two sources with different conventions: an annotation or HCL source
	// spells it BTREE/GIN, while the live-database reader reports pg_am.amname
	// verbatim, which PostgreSQL spells btree/gin. Both mean the default
	// method, and emitting "USING btree" for every introspected index would be
	// a gratuitous divergence from the pinned binary's output.
	if node.Type != "" && !strings.EqualFold(node.Type, "BTREE") {
		parts = append(parts, "USING")
		parts = append(parts, node.Type)
	}

	// Add columns with optional operator class
	var columnSpecs []string
	for _, part := range node.EffectiveParts() {
		columnSpec := r.renderIndexPart(part)
		if part.Operator != "" {
			columnSpec = fmt.Sprintf("%s %s", columnSpec, part.Operator)
		} else if node.Operator != "" {
			columnSpec = fmt.Sprintf("%s %s", columnSpec, node.Operator)
		}
		if part.Desc {
			columnSpec += " DESC"
		}
		columnSpec += renderIndexPartNullsOrder(part)
		columnSpecs = append(columnSpecs, columnSpec)
	}
	parts = append(parts, fmt.Sprintf("(%s)", strings.Join(columnSpecs, ", ")))

	if len(node.IncludeColumns) > 0 {
		parts = append(parts, fmt.Sprintf("INCLUDE (%s)", strings.Join(r.escapeIdentifierList(node.IncludeColumns), ", ")))
	}

	if node.NullsDistinct != nil {
		parts = append(parts, renderNullsDistinctClause(node.NullsDistinct))
	}

	if len(node.StorageParams) > 0 {
		storageParams, err := r.renderIndexStorageParams(node.StorageParams)
		if err != nil {
			return err
		}
		parts = append(parts, storageParams)
	}

	// Add WHERE condition for partial indexes
	if node.Condition != "" {
		parts = append(parts, "WHERE")
		parts = append(parts, node.Condition)
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	// An index comment is a separate statement: CREATE INDEX has no COMMENT
	// clause in PostgreSQL's grammar, unlike MySQL's index definition. Emitting
	// it here is what keeps the comment attached to the index the statement
	// above just created -- before #1242 the value was carried all the way from
	// the annotation or the HCL document to this node and then dropped, and the
	// index arrived with no comment at exit 0.
	if node.Comment != "" {
		r.w.WriteLinef(
			"COMMENT ON INDEX %s IS %s;",
			r.qualifiedIndexTarget(node.Table, node.Name),
			r.escapeValue(node.Comment),
		)
	}

	return nil
}

// renderIndexPartNullsOrder renders the NULLS clause for one index part.
//
// It renders whatever the part carries rather than second-guessing it. Deciding
// that a clause is redundant belongs to whoever produced the part: PostgreSQL's
// defaults are NULLS LAST for ASC and NULLS FIRST for DESC, and the live-
// database reader already declines to record an ordering that matches its
// direction's default, so nothing introspected reaches here with a redundant
// value. A source that spelled one out explicitly gets it back.
func renderIndexPartNullsOrder(part ast.IndexPart) string {
	switch strings.ToUpper(part.NullsOrder) {
	case ast.NullsOrderFirst:
		return " NULLS FIRST"
	case ast.NullsOrderLast:
		return " NULLS LAST"
	default:
		return ""
	}
}

func (r *Renderer) renderIndexStorageParams(params map[string]string) (string, error) {
	keys := make([]string, 0, len(params))
	for key := range params {
		if !validIndexStorageParamName(key) {
			return "", fmt.Errorf("invalid PostgreSQL index storage parameter %q", key)
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)

	rendered := make([]string, 0, len(keys))
	for _, key := range keys {
		rendered = append(rendered, fmt.Sprintf("%s=%s", key, r.escapeValue(params[key])))
	}
	return fmt.Sprintf("WITH (%s)", strings.Join(rendered, ", ")), nil
}

func validIndexStorageParamName(name string) bool {
	if name == "" {
		return false
	}
	for i, ch := range name {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch == '_':
		case i > 0 && ch >= '0' && ch <= '9':
		default:
			return false
		}
	}
	return true
}

func (r *Renderer) renderIndexPart(part ast.IndexPart) string {
	if part.Expr != "" {
		return fmt.Sprintf("(%s)", part.Expr)
	}
	return r.escapeIdentifier(part.Name)
}

func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	var parts []string

	parts = append(parts, "CREATE EXTENSION")

	if node.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	// Extension names are database-wide single identifiers. Treating a dot as
	// a schema separator would render an extension named `audit.tools` as the
	// invalid two-part identity `"audit"."tools"`, even though extension
	// placement belongs exclusively to the separate SCHEMA option.
	parts = append(parts, r.escapeIdentifier(node.Name))
	if node.Schema != "" {
		parts = append(parts, "WITH", "SCHEMA", r.escapeIdentifier(node.Schema))
	}

	// Add version specification if provided
	if node.Version != "" {
		parts = append(parts, fmt.Sprintf("VERSION %s", r.escapeValue(node.Version)))
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	return nil
}

// VisitEnum renders CREATE TYPE ... AS ENUM for PostgreSQL
func (r *Renderer) VisitEnum(node *ast.EnumNode) error {
	if r.refuses(capability.EnumCustomType, "enum type", node.Name) {
		return nil
	}

	values := make([]string, len(node.Values))
	for i, value := range node.Values {
		values[i] = r.escapeValue(value)
	}

	r.w.WriteLinef("CREATE TYPE %s AS ENUM (%s);", r.escapeQualifiedIdentifier(node.Name), strings.Join(values, ", "))
	return nil
}

// VisitComment renders a comment
func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	r.w.WriteLinef("-- %s --", node.Text)
	return nil
}

func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	// Build DROP TABLE statement with PostgreSQL-specific features
	var parts []string
	parts = append(parts, "DROP TABLE")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, strings.Join(r.escapeQualifiedIdentifierList(node.TableNames()), ", "))

	if node.Cascade {
		parts = append(parts, "CASCADE")
	}

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

// VisitDropType renders PostgreSQL-specific DROP TYPE statements
func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	// Build DROP TYPE / DROP DOMAIN statement (PostgreSQL-specific)
	var parts []string
	if node.Domain {
		parts = append(parts, "DROP DOMAIN")
	} else {
		parts = append(parts, "DROP TYPE")
	}

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, r.escapeQualifiedIdentifier(node.Name))

	if node.Cascade {
		parts = append(parts, "CASCADE")
	}

	sql := strings.Join(parts, " ") + ";"

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLine(sql)
	return nil
}

// renderColumn overrides base column rendering with PostgreSQL-specific handling
func (r *Renderer) renderColumn(column *ast.ColumnNode) (string, error) {
	var parts []string

	// Handle PostgreSQL-specific type conversions using current enum context
	columnType, err := r.processFieldType(column.Type, r.currentEnums)
	if err != nil {
		return "", err
	}

	// Column name and type
	parts = append(parts, fmt.Sprintf("  %s %s", r.escapeIdentifier(column.Name), columnType))

	// Column constraints - PostgreSQL order: PRIMARY KEY, then NOT NULL, then UNIQUE
	if column.Primary {
		parts = append(parts, "PRIMARY KEY")
		// Primary keys are always NOT NULL in PostgreSQL, show it explicitly for schema comparison
		parts = append(parts, "NOT NULL")
	} else {
		if column.Unique {
			parts = append(parts, "UNIQUE")
		}
		if !column.Nullable {
			parts = append(parts, "NOT NULL")
		}
	}

	if column.IdentityGeneration != "" {
		if column.GeneratedExpression != "" {
			return "", fmt.Errorf("postgres column %s cannot be both identity and generated", column.Name)
		}
		identity, err := r.renderIdentity(column)
		if err != nil {
			return "", err
		}
		parts = append(parts, identity)
	}

	// Default value
	switch {
	case column.Default == nil:
		// No default value
	case column.Default.HasLiteral():
		parts = append(parts, fmt.Sprintf("DEFAULT %s", r.renderDefaultLiteral(column.Default.Value)))
	case column.Default.Expression != "":
		parts = append(parts, fmt.Sprintf("DEFAULT %s", column.Default.Expression))
	}
	if column.GeneratedExpression != "" {
		if column.GeneratedKind != "" && !strings.EqualFold(column.GeneratedKind, "STORED") {
			return "", fmt.Errorf("postgres does not support %s generated columns", column.GeneratedKind)
		}
		// This renderer serves YugabyteDB too, whose 2024 LTS line is still
		// PostgreSQL 11 and answers `syntax error at or near "("`. Dropping the
		// clause silently would turn a generated column into an ordinary one,
		// which is a different table, so the refusal is the answer
		// (stokaro/ptah#916).
		if !r.capabilities().Has(capability.GeneratedColumns) {
			return "", unsupportedFeaturef(
				"%s does not support generated columns; column %q declares GENERATED ALWAYS AS",
				r.dialect, column.Name)
		}
		parts = append(parts, fmt.Sprintf("GENERATED ALWAYS AS (%s) STORED", column.GeneratedExpression))
	}

	// Check constraint. Emit `CONSTRAINT <name> CHECK (...)` only when an
	// explicit name was provided via `check_name=` on the field annotation —
	// for the default (unnamed) form, PostgreSQL auto-generates the canonical
	// "<table>_<column>_check" name, which is exactly what the drift detector
	// expects, so we don't burn a constraint name on every column needlessly.
	if column.Check != "" {
		if column.CheckName != "" {
			parts = append(parts, fmt.Sprintf("CONSTRAINT %s CHECK (%s)", r.escapeIdentifier(column.CheckName), column.Check))
		} else {
			parts = append(parts, fmt.Sprintf("CHECK (%s)", column.Check))
		}
	}

	return strings.Join(parts, " "), nil
}

func (r *Renderer) renderIdentity(column *ast.ColumnNode) (string, error) {
	generation, err := renderIdentityGeneration(column.IdentityGeneration)
	if err != nil {
		return "", err
	}
	if column.IdentityOptions != "" {
		return fmt.Sprintf("GENERATED %s AS IDENTITY (%s)", generation, column.IdentityOptions), nil
	}
	options := make([]string, 0, 2)
	if column.IdentityStart != "" {
		options = append(options, fmt.Sprintf("START WITH %s", column.IdentityStart))
	}
	if column.IdentityIncrement != "" {
		options = append(options, fmt.Sprintf("INCREMENT BY %s", column.IdentityIncrement))
	}
	if len(options) == 0 {
		return fmt.Sprintf("GENERATED %s AS IDENTITY", generation), nil
	}
	return fmt.Sprintf("GENERATED %s AS IDENTITY (%s)", generation, strings.Join(options, " ")), nil
}

func renderIdentityGeneration(generation string) (string, error) {
	switch strings.ToUpper(strings.ReplaceAll(generation, " ", "_")) {
	case "ALWAYS":
		return "ALWAYS", nil
	case "BY_DEFAULT":
		return "BY DEFAULT", nil
	default:
		return "", fmt.Errorf("postgres does not support %s identity generation", generation)
	}
}

// processFieldType processes field type for PostgreSQL, handling enums appropriately
func (r *Renderer) processFieldType(fieldType string, enums []string) (string, error) {
	// For PostgreSQL, enum types are used directly (they're defined separately)
	// Check if this type is an enum using the helper method
	if r.isEnumType(fieldType, enums) {
		return fieldType, nil // Use enum type directly
	}

	if strings.EqualFold(fieldType, "XML") && !r.capabilities().Has(capability.XMLType) {
		return "", unsupportedFeaturef("%s does not support XML columns; use a platform-specific type override", r.dialect)
	}
	if sequenceBackedType(fieldType) && !r.capabilities().Has(capability.Sequences) {
		return "", unsupportedFeaturef("%s does not support sequence-backed type %s; use a platform-specific type override", r.dialect, fieldType)
	}

	// Handle other PostgreSQL-specific type mappings if needed
	switch fieldType {
	case "AUTO_INCREMENT":
		return "SERIAL", nil
	case "BIGINT AUTO_INCREMENT":
		return "BIGSERIAL", nil
	default:
		return fieldType, nil
	}
}

func sequenceBackedType(fieldType string) bool {
	switch strings.ToUpper(strings.TrimSpace(fieldType)) {
	case "SMALLSERIAL", "SERIAL", "BIGSERIAL", "AUTO_INCREMENT", "BIGINT AUTO_INCREMENT":
		return true
	default:
		return strings.Contains(strings.ToUpper(fieldType), "AUTO_INCREMENT")
	}
}

// Helper method to check if a type is an enum
func (r *Renderer) isEnumType(fieldType string, enums []string) bool {
	return slices.Contains(enums, fieldType)
}

// RenderConstraint renders a table-level constraint
func (r *Renderer) renderConstraint(constraint *ast.ConstraintNode) (string, error) {
	switch constraint.Type {
	case ast.PrimaryKeyConstraint:
		line := "  "
		if constraint.Name != "" {
			line += fmt.Sprintf("CONSTRAINT %s ", r.escapeIdentifier(constraint.Name))
		}
		line += fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(r.escapeIdentifierList(constraint.Columns), ", "))
		if len(constraint.IncludeColumns) > 0 {
			line += fmt.Sprintf(" INCLUDE (%s)", strings.Join(r.escapeIdentifierList(constraint.IncludeColumns), ", "))
		}
		return line, nil
	case ast.UniqueConstraint:
		clause := "UNIQUE"
		if constraint.NullsDistinct != nil {
			clause += " " + renderNullsDistinctClause(constraint.NullsDistinct)
		}
		columns := fmt.Sprintf("(%s)", strings.Join(r.escapeIdentifierList(constraint.Columns), ", "))
		if len(constraint.IncludeColumns) > 0 {
			columns += fmt.Sprintf(" INCLUDE (%s)", strings.Join(r.escapeIdentifierList(constraint.IncludeColumns), ", "))
		}
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s %s %s", r.escapeIdentifier(constraint.Name), clause, columns), nil
		}
		return fmt.Sprintf("  %s %s", clause, columns), nil
	case ast.ForeignKeyConstraint:
		// The empty string is this function's ONLY "the target cannot host
		// it" answer, and a foreign key is the only constraint kind that can
		// produce it: every other branch either renders text or returns an
		// error. Callers turn it into one named skip comment via
		// foreignKeyIdentity, so all three routes to a refused key -- table
		// constraint, column reference, ALTER TABLE ADD -- say the same
		// sentence (stokaro/ptah#929).
		if !r.capabilities().Has(capability.ForeignKeys) {
			return "", nil
		}
		return r.renderForeignKeyConstraint(constraint)
	case ast.CheckConstraint:
		if constraint.Name != "" {
			return fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", r.escapeIdentifier(constraint.Name), constraint.Expression), nil
		}
		return fmt.Sprintf("  CHECK (%s)", constraint.Expression), nil
	case ast.ExcludeConstraint:
		return r.renderExcludeConstraint(constraint)
	default:
		return "", fmt.Errorf("unknown constraint type: %v", constraint.Type)
	}
}

// foreignKeyConstraintKind is the object kind a refused foreign key is named
// with. It is a table constraint rather than a schema object, so it says
// "foreign key constraint" and not "foreign key".
const foreignKeyConstraintKind = "foreign key constraint"

// foreignKeyIdentity names a foreign key in a skip comment.
//
// A schema author need not name a foreign key, and `constraint ""` would tell
// a reader nothing about what was dropped, so an unnamed key falls back to the
// local columns and the referenced table -- the pair that identifies it.
func foreignKeyIdentity(constraint *ast.ConstraintNode) string {
	if constraint.Name != "" {
		return constraint.Name
	}
	table := ""
	if constraint.Reference != nil {
		table = constraint.Reference.Table
	}
	return fmt.Sprintf("on (%s) references %s", strings.Join(constraint.Columns, ", "), table)
}

func renderNullsDistinctClause(nullsDistinct *bool) string {
	if nullsDistinct != nil && *nullsDistinct {
		return "NULLS DISTINCT"
	}
	return "NULLS NOT DISTINCT"
}

// renderForeignKeyConstraint renders a foreign key constraint
func (r *Renderer) renderForeignKeyConstraint(constraint *ast.ConstraintNode) (string, error) {
	if constraint.Reference == nil {
		return "", fmt.Errorf("foreign key constraint missing reference")
	}

	ref := constraint.Reference
	var result string
	foreignKey := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
		strings.Join(r.escapeIdentifierList(constraint.Columns), ", "),
		r.escapeQualifiedIdentifier(ref.Table),
		strings.Join(r.escapeIdentifierList(ref.ReferencedColumns()), ", "))
	if constraint.Name != "" {
		result = fmt.Sprintf("  CONSTRAINT %s %s", r.escapeIdentifier(constraint.Name), foreignKey)
	} else {
		result = "  " + foreignKey
	}

	if ref.OnDelete != "" {
		result += fmt.Sprintf(" ON DELETE %s", ref.OnDelete)
	}

	if ref.OnUpdate != "" {
		result += fmt.Sprintf(" ON UPDATE %s", ref.OnUpdate)
	}

	return result, nil
}

// renderExcludeConstraint renders an EXCLUDE constraint
func (r *Renderer) renderExcludeConstraint(constraint *ast.ConstraintNode) (string, error) {
	if constraint.UsingMethod == "" || constraint.ExcludeElements == "" {
		return "", fmt.Errorf("exclude constraint missing using method or elements")
	}

	// Build the constraint string
	var result string
	if constraint.Name != "" {
		result = fmt.Sprintf("  CONSTRAINT %s EXCLUDE USING %s (%s)", r.escapeIdentifier(constraint.Name), constraint.UsingMethod, constraint.ExcludeElements)
	} else {
		result = fmt.Sprintf("  EXCLUDE USING %s (%s)", constraint.UsingMethod, constraint.ExcludeElements)
	}

	// Add optional WHERE clause
	if constraint.WhereCondition != "" {
		result += fmt.Sprintf(" WHERE (%s)", constraint.WhereCondition)
	}

	return result, nil
}

// renderTableOptions renders PostgreSQL table options (PostgreSQL doesn't support ENGINE)
func (r *Renderer) renderTableOptions(options map[string]string) string {
	// PostgreSQL doesn't support table options like MySQL's ENGINE
	// We could support other PostgreSQL-specific options here if needed
	var parts []string

	for key, value := range options {
		// Skip MySQL-specific options
		if key == "ENGINE" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}

	return strings.Join(parts, " ")
}

// renderPostgreSQLModifyColumn renders PostgreSQL-specific column modifications
func (r *Renderer) renderPostgreSQLModifyColumn(tableName string, column *ast.ColumnNode) {
	// PostgreSQL requires separate ALTER statements for different column properties

	// Process the column type with enum support
	columnType, err := r.processFieldType(column.Type, r.currentEnums)
	if err != nil {
		r.w.WriteLinef("-- %s: %s", r.dialectUpper, err.Error())
		return
	}

	// Change the column type. An enum target needs an explicit USING cast:
	// PostgreSQL has no assignment cast from varchar (or anything else) to an
	// enum, so a bare `ALTER COLUMN ... TYPE <enum>` aborts the migration with
	// `column "s" cannot be cast automatically to type ...` (SQLSTATE 42804).
	//
	// Whether the target is an enum comes from the schema declaration carried on
	// the node, not from the type name: this used to test
	// strings.HasPrefix(type, "enum_"), so an enum named "status_kind" got no
	// cast and its migration died at execution while an otherwise identical one
	// named "enum_status" applied cleanly (stokaro/ptah#931 item 1).
	targetType := column.Type
	if columnType != column.Type {
		// Type was transformed (e.g., enum handling), use the processed type
		targetType = columnType
	}
	if column.EnumType {
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s;",
			r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name), targetType, r.escapeIdentifier(column.Name), targetType)
	} else {
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
			r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name), targetType)
	}

	// Change nullability.
	//
	// A primary key column is NOT NULL on every engine this renderer serves --
	// PostgreSQL, CockroachDB, YugabyteDB and Spanner -- and PostgreSQL refuses
	// to take that away: `ALTER TABLE "users" ALTER COLUMN "id" DROP NOT NULL`
	// on a key column fails with `column "id" is in a primary key`
	// (SQLSTATE 42P16), so emitting it makes the whole plan unappliable rather
	// than merely verbose. ast.ColumnNode.Nullable no longer carries the rule
	// for the AST, because SQLite does not have it (stokaro/ptah#1235), so the
	// dialects that do have it apply it where the dialect is known. The
	// CREATE TABLE path above writes PRIMARY KEY and NOT NULL together for the
	// same reason.
	if column.Nullable && !column.Primary {
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name))
	} else {
		r.updateNullValuesBeforeNotNull(tableName, column)
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name))
	}

	// Change default value
	switch {
	case column.Default == nil:
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name))
	case column.Default.HasLiteral():
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name), r.renderDefaultLiteral(column.Default.Value))
	case column.Default.Expression != "":
		r.w.WriteLinef("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", r.escapeQualifiedIdentifier(tableName), r.escapeIdentifier(column.Name), column.Default.Expression)
	}
}

// getDefaultValueForType returns a sensible default value for a column type when setting NOT NULL
func (r *Renderer) getDefaultValueForType(columnType string) string {
	switch {
	case strings.Contains(strings.ToLower(columnType), "timestamp"):
		return "CURRENT_TIMESTAMP"
	case strings.Contains(strings.ToLower(columnType), "date"):
		return "CURRENT_DATE"
	case strings.Contains(strings.ToLower(columnType), "time"):
		return "CURRENT_TIME"
	case strings.Contains(strings.ToLower(columnType), "text") || strings.Contains(strings.ToLower(columnType), "varchar"):
		return "''"
	case strings.Contains(strings.ToLower(columnType), "int") || strings.Contains(strings.ToLower(columnType), "serial"):
		return "0"
	case strings.Contains(strings.ToLower(columnType), "decimal") || strings.Contains(strings.ToLower(columnType), "numeric"):
		return "0.0"
	case strings.Contains(strings.ToLower(columnType), "bool"):
		return "false"
	default:
		return "" // No default available, let the constraint fail if there are NULLs
	}
}

// updateNullValuesBeforeNotNull updates existing NULL values before setting NOT NULL constraint
// This prevents "column contains null values" errors during migrations
func (r *Renderer) updateNullValuesBeforeNotNull(tableName string, column *ast.ColumnNode) {
	// First check if there are any NULL values to avoid unnecessary UPDATE operations
	r.w.WriteLinef("DO $$")
	r.w.WriteLinef("BEGIN")
	tableIdentifier := r.escapeQualifiedIdentifier(tableName)
	columnIdentifier := r.escapeIdentifier(column.Name)
	r.w.WriteLinef("    IF EXISTS (SELECT 1 FROM %s WHERE %s IS NULL LIMIT 1) THEN", tableIdentifier, columnIdentifier)

	if column.Default != nil {
		if column.Default.Expression != "" {
			r.w.WriteLinef("        UPDATE %s SET %s = %s WHERE %s IS NULL;", tableIdentifier, columnIdentifier, column.Default.Expression, columnIdentifier)
		} else if column.Default.HasLiteral() {
			r.w.WriteLinef("        UPDATE %s SET %s = %s WHERE %s IS NULL;", tableIdentifier, columnIdentifier, r.renderDefaultLiteral(column.Default.Value), columnIdentifier)
		}
	} else {
		// If no default is specified, use a sensible default based on column type
		defaultValue := r.getDefaultValueForType(column.Type)
		if defaultValue != "" {
			r.w.WriteLinef("        UPDATE %s SET %s = %s WHERE %s IS NULL;", tableIdentifier, columnIdentifier, defaultValue, columnIdentifier)
		}
	}

	r.w.WriteLinef("    END IF;")
	r.w.WriteLinef("END")
	r.w.WriteLinef("$$;")
}

func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	var parts []string

	parts = append(parts, "DROP EXTENSION")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, r.escapeIdentifier(node.Name))

	if node.Cascade {
		parts = append(parts, "CASCADE")
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	return nil
}

// VisitCreateFunction renders a CREATE FUNCTION statement for PostgreSQL
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	if r.refuses(capability.Functions, "function", node.Name) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build CREATE OR REPLACE FUNCTION statement
	var parts []string
	parts = append(parts, "CREATE OR REPLACE FUNCTION")

	// Function parameters are raw SQL fragments; only the function identifier
	// is quoted here.
	parts = append(parts, r.escapeFunctionSignature(node.Name, node.Parameters))

	// Return type
	if node.Returns != "" {
		parts = append(parts, "RETURNS", node.Returns)
	}

	// Language specification and other attributes
	var attributes []string
	if node.Language != "" {
		attributes = append(attributes, fmt.Sprintf("LANGUAGE %s", node.Language))
	}

	// Security attribute
	if node.Security != "" {
		attributes = append(attributes, fmt.Sprintf("SECURITY %s", node.Security))
	}

	// Volatility attribute
	if node.Volatility != "" {
		attributes = append(attributes, node.Volatility)
	}

	if node.BodyKind == ast.FunctionBodyReturn || node.BodyKind == ast.FunctionBodyAtomic {
		parts = append(parts, attributes...)
		switch node.BodyKind {
		case ast.FunctionBodyReturn:
			r.w.WriteLinef("%s RETURN %s;", strings.Join(parts, " "), node.Body)
		case ast.FunctionBodyAtomic:
			r.w.WriteLinef("%s %s;", strings.Join(parts, " "), node.Body)
		}
		return nil
	}

	// Function body with dollar quoting
	r.w.WriteLinef("%s AS $$", strings.Join(parts, " "))
	r.w.WriteLinef("%s", node.Body)

	// Close the function with attributes
	if len(attributes) > 0 {
		r.w.WriteLinef("$$")
		r.w.WriteLinef("%s;", strings.Join(attributes, " "))
		return nil
	}
	r.w.WriteLinef("$$;")

	return nil
}

// VisitCreatePolicy renders a CREATE POLICY statement for PostgreSQL RLS
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	if r.refuses(capability.RowLevelSecurity, "policy", policyIdentity(node.Name, node.Table)) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// If Replace is true, drop the policy first to avoid conflicts
	if node.Replace {
		r.w.WriteLinef("DROP POLICY IF EXISTS %s ON %s;", r.escapeIdentifier(node.Name), r.escapeQualifiedIdentifier(node.Table))
	}

	// Build CREATE POLICY statement
	var parts []string
	parts = append(parts, "CREATE POLICY", r.escapeIdentifier(node.Name), "ON", r.escapeQualifiedIdentifier(node.Table))

	// FOR clause
	if node.PolicyFor != "" {
		parts = append(parts, "FOR", node.PolicyFor)
	}

	// TO clause
	if node.ToRoles != "" {
		parts = append(parts, "TO", r.escapeRoleTargetList(node.ToRoles))
	}

	r.w.WriteLinef("%s", strings.Join(parts, " "))

	// USING clause
	if node.UsingExpression != "" {
		r.w.WriteLinef("    USING (%s)", node.UsingExpression)
	}

	// WITH CHECK clause
	if node.WithCheckExpression != "" {
		r.w.WriteLinef("    WITH CHECK (%s)", node.WithCheckExpression)
	}

	r.w.WriteLinef(";")

	return nil
}

// VisitAlterTableEnableRLS renders an ALTER TABLE ENABLE ROW LEVEL SECURITY statement
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	if r.refuses(capability.RowLevelSecurity, "row-level security", "on "+node.Table) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build ALTER TABLE ENABLE ROW LEVEL SECURITY statement
	r.w.WriteLinef("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", r.escapeQualifiedIdentifier(node.Table))

	return nil
}

// VisitDropFunction renders a DROP FUNCTION statement
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	if r.refuses(capability.Functions, "function", node.Name) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build DROP FUNCTION statement
	var parts []string
	parts = append(parts, "DROP FUNCTION")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	// Function parameters are raw SQL fragments; only the function identifier
	// is quoted here.
	parts = append(parts, r.escapeFunctionSignature(node.Name, node.Parameters))

	if node.Cascade {
		parts = append(parts, "CASCADE")
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	return nil
}

// sequenceIdentifier returns the escaped, optionally schema-qualified sequence
// identifier for name and schema.
func (r *Renderer) sequenceIdentifier(name, schema string) string {
	return r.escapeQualifiedIdentifier(goschema.QualifyTableName(schema, name))
}

// sequenceOwnedByClause renders the OWNED BY target: either NONE or a
// schema-qualified table.column reference.
func (r *Renderer) sequenceOwnedByClause(ownedBy string) string {
	if strings.EqualFold(strings.TrimSpace(ownedBy), "NONE") {
		return "NONE"
	}
	return r.escapeQualifiedIdentifier(ownedBy)
}

// sequenceOptions renders the shared CREATE/ALTER SEQUENCE option clauses in a
// stable order. Only set options are emitted.
func sequenceOptions(asType string, start, increment, minValue, maxValue, cache *int64, cycle *bool) []string {
	var parts []string
	if asType != "" {
		parts = append(parts, "AS "+asType)
	}
	if increment != nil {
		parts = append(parts, fmt.Sprintf("INCREMENT BY %d", *increment))
	}
	if minValue != nil {
		parts = append(parts, fmt.Sprintf("MINVALUE %d", *minValue))
	}
	if maxValue != nil {
		parts = append(parts, fmt.Sprintf("MAXVALUE %d", *maxValue))
	}
	if start != nil {
		parts = append(parts, fmt.Sprintf("START WITH %d", *start))
	}
	if cache != nil {
		parts = append(parts, fmt.Sprintf("CACHE %d", *cache))
	}
	if cycle != nil {
		if *cycle {
			parts = append(parts, "CYCLE")
		} else {
			parts = append(parts, "NO CYCLE")
		}
	}
	return parts
}

// writeObjectSkipped records that a declared object is not emitted for this
// target, naming both the object kind and the object.
//
// Both the offline renderer and the migration planner pass through this
// renderer, so the skip diagnostic is the shared answer shape for a target that
// cannot host an object kind (stokaro/ptah#929).
func (r *Renderer) writeObjectSkipped(kind, name string) {
	r.w.WriteLinef("-- %s: %s %s is not supported by this target; skipped.",
		r.dialectUpper, commentFragment(kind), commentFragment(name))
}

// commentFragment keeps diagnostic-only text inside one SQL line comment.
func commentFragment(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

// refuses reports whether this target cannot host the named object, and writes
// a skip comment when it cannot. Capability-gated visitors use this helper so
// `schema render` and `schema apply --dry-run` cannot disagree by returning an
// error on one path and silently dropping the same object on the other.
func (r *Renderer) refuses(key capability.Capability, kind, name string) bool {
	if r.capabilities().Has(key) {
		return false
	}
	r.writeObjectSkipped(kind, name)
	return true
}

// VisitCreateSequence renders a CREATE SEQUENCE statement for PostgreSQL.
func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"CREATE SEQUENCE"}
	if node.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}
	parts = append(parts, r.sequenceIdentifier(node.Name, node.Schema))

	var cycle *bool
	if node.Cycle {
		cycle = &node.Cycle
	}
	parts = append(parts, sequenceOptions(node.AsType, node.Start, node.Increment, node.MinValue, node.MaxValue, node.Cache, cycle)...)

	if node.OwnedBy != "" {
		parts = append(parts, "OWNED BY "+r.sequenceOwnedByClause(node.OwnedBy))
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitAlterSequence renders an ALTER SEQUENCE statement for PostgreSQL. Only
// the set options are emitted; a node with no set options renders nothing.
func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}

	options := sequenceOptions(node.AsType, node.Start, node.Increment, node.MinValue, node.MaxValue, node.Cache, node.Cycle)
	if node.OwnedBy != "" {
		options = append(options, "OWNED BY "+r.sequenceOwnedByClause(node.OwnedBy))
	}
	if len(options) == 0 {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}
	r.w.WriteLinef("ALTER SEQUENCE %s %s;", r.sequenceIdentifier(node.Name, node.Schema), strings.Join(options, " "))
	return nil
}

// VisitDropSequence renders a DROP SEQUENCE statement for PostgreSQL.
func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	if r.refuses(capability.Sequences, "sequence", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"DROP SEQUENCE"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, r.sequenceIdentifier(node.Name, node.Schema))
	if node.Cascade {
		parts = append(parts, "CASCADE")
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateView renders a CREATE VIEW statement.
func (r *Renderer) VisitCreateView(node *ast.CreateViewNode) error {
	if r.refuses(capability.Views, "view", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	create := "CREATE VIEW"
	if node.Replace {
		create = "CREATE OR REPLACE VIEW"
	}
	r.w.WriteLinef("%s %s AS", create, r.escapeQualifiedIdentifier(node.Name))
	r.w.WriteLine(strings.TrimSpace(node.Body))
	if node.WithCheck {
		r.w.WriteLine("WITH CHECK OPTION")
	}
	r.w.WriteLine(";")
	return nil
}

// VisitDropView renders a DROP VIEW statement.
func (r *Renderer) VisitDropView(node *ast.DropViewNode) error {
	if r.refuses(capability.Views, "view", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"DROP VIEW"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, r.escapeQualifiedIdentifier(node.Name))
	if node.Cascade {
		parts = append(parts, "CASCADE")
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateMaterializedView renders a CREATE MATERIALIZED VIEW statement.
func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	if r.refuses(capability.MaterializedViews, "materialized view", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	r.w.WriteLinef("CREATE MATERIALIZED VIEW %s AS", r.escapeQualifiedIdentifier(node.Name))
	r.w.WriteLine(strings.TrimSpace(node.Body))
	r.w.WriteLine(";")
	return nil
}

// VisitDropMaterializedView renders a DROP MATERIALIZED VIEW statement.
func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	if r.refuses(capability.MaterializedViews, "materialized view", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"DROP MATERIALIZED VIEW"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, r.escapeQualifiedIdentifier(node.Name))
	if node.Cascade {
		parts = append(parts, "CASCADE")
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitRefreshMaterializedView renders a REFRESH MATERIALIZED VIEW statement.
func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	if r.refuses(capability.MaterializedViews, "materialized view", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"REFRESH MATERIALIZED VIEW"}
	if node.Concurrently {
		parts = append(parts, "CONCURRENTLY")
	}
	parts = append(parts, r.escapeQualifiedIdentifier(node.Name))
	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// VisitCreateTrigger renders PostgreSQL trigger creation plus its linked
// trigger function.
func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	// The linked trigger function below is part of the trigger rather than a
	// function the schema declared, so one key answers for the pair and one
	// comment names the object the author actually wrote.
	if r.refuses(capability.Triggers, "trigger", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	functionName := node.FunctionName
	if functionName == "" {
		functionName = postgresTriggerFunctionName(node.Table, node.Name)
	}

	// An external function is referenced, never defined: emitting a body for it
	// would overwrite whatever it already contains.
	if !node.ExternalFunction {
		r.w.WriteLinef("CREATE OR REPLACE FUNCTION %s()", r.escapeQualifiedIdentifier(functionName))
		r.w.WriteLine("RETURNS trigger AS $$")
		r.w.WriteLine(renderPostgreSQLTriggerFunctionBody(node.Body))
		r.w.WriteLine("$$ LANGUAGE plpgsql;")
	}

	if node.Replace && !r.capabilities().Has(capability.CreateOrReplaceTrigger) {
		r.w.WriteLinef("DROP TRIGGER IF EXISTS %s ON %s;", r.escapeIdentifier(node.Name), r.escapeQualifiedIdentifier(node.Table))
	}

	create := "CREATE TRIGGER"
	if node.Replace && r.capabilities().Has(capability.CreateOrReplaceTrigger) {
		create = "CREATE OR REPLACE TRIGGER"
	}

	forEach := node.ForEach
	if forEach == "" {
		forEach = "ROW"
	}
	r.w.WriteLinef("%s %s %s %s ON %s FOR EACH %s EXECUTE FUNCTION %s();",
		create,
		r.escapeIdentifier(node.Name),
		node.Timing,
		node.Event,
		r.escapeQualifiedIdentifier(node.Table),
		forEach,
		r.escapeQualifiedIdentifier(functionName))
	return nil
}

func renderPostgreSQLTriggerFunctionBody(body string) string {
	body = strings.TrimSpace(body)
	upperBody := strings.ToUpper(body)
	if strings.HasPrefix(upperBody, "BEGIN") {
		return body
	}
	return "BEGIN\n" + body + "\nEND;"
}

// VisitDropTrigger renders a DROP TRIGGER statement and drops the linked Ptah
// trigger function when its deterministic name is known.
func (r *Renderer) VisitDropTrigger(node *ast.DropTriggerNode) error {
	if r.refuses(capability.Triggers, "trigger", node.Name) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	parts := []string{"DROP TRIGGER"}
	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}
	parts = append(parts, r.escapeIdentifier(node.Name), "ON", r.escapeQualifiedIdentifier(node.Table))
	if node.Cascade {
		parts = append(parts, "CASCADE")
	}
	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	functionName := node.FunctionName
	if functionName == "" {
		functionName = postgresTriggerFunctionName(node.Table, node.Name)
	}
	r.w.WriteLinef("DROP FUNCTION IF EXISTS %s();", r.escapeQualifiedIdentifier(functionName))
	return nil
}

func postgresTriggerFunctionName(tableName, triggerName string) string {
	name := "ptah_trigger_" + sanitizeTriggerFunctionPart(tableName) + "_" + sanitizeTriggerFunctionPart(triggerName)
	if len(name) <= maxPostgreSQLIdentifierLength {
		return name
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(tableName))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(triggerName))
	suffix := fmt.Sprintf("_%08x", hash.Sum32())
	return name[:maxPostgreSQLIdentifierLength-len(suffix)] + suffix
}

const maxPostgreSQLIdentifierLength = 63

func sanitizeTriggerFunctionPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	builder.Grow(len(value))
	lastUnderscore := false
	for i := range len(value) {
		character := value[i]
		if isIdentifierPart(character) {
			builder.WriteByte(character)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}

	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "object"
	}
	if result[0] >= '0' && result[0] <= '9' {
		return "_" + result
	}
	return result
}

func isIdentifierPart(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= '0' && character <= '9' ||
		character == '_'
}

// VisitDropPolicy renders a DROP POLICY statement
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	if r.refuses(capability.RowLevelSecurity, "policy", policyIdentity(node.Name, node.Table)) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build DROP POLICY statement
	var parts []string
	parts = append(parts, "DROP POLICY")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, r.escapeIdentifier(node.Name), "ON", r.escapeQualifiedIdentifier(node.Table))

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	return nil
}

// VisitAlterTableDisableRLS renders an ALTER TABLE DISABLE ROW LEVEL SECURITY statement
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	if r.refuses(capability.RowLevelSecurity, "row-level security", "on "+node.Table) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build ALTER TABLE DISABLE ROW LEVEL SECURITY statement
	r.w.WriteLinef("ALTER TABLE %s DISABLE ROW LEVEL SECURITY;", r.escapeQualifiedIdentifier(node.Table))

	return nil
}

// VisitCreateRole renders a CREATE ROLE statement for PostgreSQL
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	if r.refuses(capability.RoleManagement, "role", node.Name) {
		return nil
	}

	// Build CREATE ROLE statement
	var parts []string
	parts = append(parts, "CREATE ROLE", r.escapeIdentifier(node.Name))

	// Add role attributes
	var attributes []string

	if node.Login {
		attributes = append(attributes, "LOGIN")
	} else {
		attributes = append(attributes, "NOLOGIN")
	}

	if node.Password != "" {
		// Validate password appears to be encrypted
		if !looksEncrypted(node.Password) {
			// Add a comment warning about potential plaintext password
			r.w.WriteLinef("-- WARNING: Password may not be encrypted - ensure passwords are properly hashed")
		}
		attributes = append(attributes, fmt.Sprintf("PASSWORD %s", r.escapeValue(node.Password)))
	}

	if node.Superuser {
		attributes = append(attributes, "SUPERUSER")
	} else {
		attributes = append(attributes, "NOSUPERUSER")
	}

	if node.CreateDB {
		attributes = append(attributes, "CREATEDB")
	} else {
		attributes = append(attributes, "NOCREATEDB")
	}

	if node.CreateRole {
		attributes = append(attributes, "CREATEROLE")
	} else {
		attributes = append(attributes, "NOCREATEROLE")
	}

	if node.Inherit {
		attributes = append(attributes, "INHERIT")
	} else {
		attributes = append(attributes, "NOINHERIT")
	}

	if node.Replication {
		attributes = append(attributes, "REPLICATION")
	} else {
		attributes = append(attributes, "NOREPLICATION")
	}

	// Combine role name and attributes
	if len(attributes) > 0 {
		parts = append(parts, "WITH", strings.Join(attributes, " "))
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	if node.Comment != "" {
		r.w.WriteLinef(
			"COMMENT ON ROLE %s IS %s;",
			r.escapeIdentifier(node.Name),
			r.escapeValue(node.Comment),
		)
	}

	return nil
}

// VisitDropRole renders a DROP ROLE statement for PostgreSQL
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	if r.refuses(capability.RoleManagement, "role", node.Name) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Build DROP ROLE statement
	var parts []string
	parts = append(parts, "DROP ROLE")

	if node.IfExists {
		parts = append(parts, "IF EXISTS")
	}

	parts = append(parts, r.escapeIdentifier(node.Name))

	r.w.WriteLinef("%s;", strings.Join(parts, " "))

	return nil
}

// VisitGrantPrivilege renders a GRANT statement for PostgreSQL.
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	privileges := strings.Join(node.Privileges, ", ")
	if privileges == "" {
		return fmt.Errorf("GRANT requires at least one privilege")
	}
	if node.Role == "" {
		return fmt.Errorf("GRANT requires a role")
	}
	if node.ObjectType == "" || node.ObjectName == "" {
		return fmt.Errorf("GRANT requires an object type and object name")
	}
	if r.refuses(capability.RoleManagement, "grant", grantIdentity("on", node.ObjectName, "to", node.Role)) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	grantOption := ""
	if node.WithOption {
		grantOption = " WITH GRANT OPTION"
	}
	r.w.WriteLinef("GRANT %s ON %s %s TO %s%s;",
		privileges, node.ObjectType, r.escapeQualifiedIdentifier(node.ObjectName), r.escapeRoleTarget(node.Role), grantOption)
	return nil
}

// VisitRevokePrivilege renders a REVOKE statement for PostgreSQL.
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	privileges := strings.Join(node.Privileges, ", ")
	if privileges == "" {
		return fmt.Errorf("REVOKE requires at least one privilege")
	}
	if node.Role == "" {
		return fmt.Errorf("REVOKE requires a role")
	}
	if node.ObjectType == "" || node.ObjectName == "" {
		return fmt.Errorf("REVOKE requires an object type and object name")
	}
	if r.refuses(capability.RoleManagement, "revoke", grantIdentity("on", node.ObjectName, "from", node.Role)) {
		return nil
	}

	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	prefix := "REVOKE"
	if node.GrantOptionFor {
		prefix = "REVOKE GRANT OPTION FOR"
	}
	r.w.WriteLinef("%s %s ON %s %s FROM %s;",
		prefix, privileges, node.ObjectType, r.escapeQualifiedIdentifier(node.ObjectName), r.escapeRoleTarget(node.Role))
	return nil
}

// VisitRawSQL renders a literal SQL fragment verbatim and appends a trailing
// semicolon if the fragment doesn't already end with one. The caller owns
// correctness of the embedded SQL. The trailing `;` is essential — downstream
// SplitSQLStatements (used by the migrator to apply each statement separately
// for MySQL compatibility) tokenizes on semicolons, and a dollar-quoted body
// that ends with `$tag$\n` would otherwise merge with the following statement
// into one chunk that Postgres rejects with `syntax error at or near "DO"`.
func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	sql := node.SQL
	if !strings.HasSuffix(strings.TrimRight(sql, "\r\n\t "), ";") {
		sql += ";"
	}
	r.w.WriteLine(sql)
	return nil
}

// VisitAlterRole renders an ALTER ROLE statement for PostgreSQL
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	if r.refuses(capability.RoleManagement, "role", node.Name) {
		return nil
	}

	// Add comment if provided
	if node.Comment != "" {
		r.w.WriteLinef("-- %s", node.Comment)
	}

	// Process each operation
	for _, operation := range node.Operations {
		if err := r.renderRoleOperation(node.Name, operation); err != nil {
			return err
		}
	}

	return nil
}

// policyIdentity and grantIdentity name a row-level-security policy and a
// privilege grant in a skip comment.
//
// Neither object is identified by a bare name. A policy called
// "tenant_isolation" can exist on several tables at once, and a grant has no
// name at all -- it is the pair (object, role). Both spellings are built from
// only the fields BOTH paths carry identically: the offline converter emits one
// grant node per declared grant with every privilege on it, while the planner
// emits one per (grant, privilege) pair, so naming the privileges here would
// make the two paths describe the same refused grant differently.
func policyIdentity(name, table string) string {
	return name + " on " + table
}

func grantIdentity(objectPreposition, objectName, rolePreposition, role string) string {
	return objectPreposition + " " + objectName + " " + rolePreposition + " " + role
}

// renderRoleOperation renders a single role operation as an ALTER ROLE statement
func (r *Renderer) renderRoleOperation(roleName string, operation ast.RoleOperation) error {
	var parts []string
	parts = append(parts, "ALTER ROLE", r.escapeIdentifier(roleName))

	switch op := operation.(type) {
	case *ast.SetPasswordOperation:
		// Validate password appears to be encrypted
		if !looksEncrypted(op.Password) {
			// Add a comment warning about potential plaintext password
			r.w.WriteLinef("-- WARNING: Password may not be encrypted - ensure passwords are properly hashed")
		}
		parts = append(parts, fmt.Sprintf("PASSWORD %s", r.escapeValue(op.Password)))

	case *ast.SetLoginOperation:
		if op.Login {
			parts = append(parts, "LOGIN")
		} else {
			parts = append(parts, "NOLOGIN")
		}

	case *ast.SetSuperuserOperation:
		if op.Superuser {
			parts = append(parts, "SUPERUSER")
		} else {
			parts = append(parts, "NOSUPERUSER")
		}

	case *ast.SetCreateDBOperation:
		if op.CreateDB {
			parts = append(parts, "CREATEDB")
		} else {
			parts = append(parts, "NOCREATEDB")
		}

	case *ast.SetCreateRoleOperation:
		if op.CreateRole {
			parts = append(parts, "CREATEROLE")
		} else {
			parts = append(parts, "NOCREATEROLE")
		}

	case *ast.SetInheritOperation:
		if op.Inherit {
			parts = append(parts, "INHERIT")
		} else {
			parts = append(parts, "NOINHERIT")
		}

	case *ast.SetReplicationOperation:
		if op.Replication {
			parts = append(parts, "REPLICATION")
		} else {
			parts = append(parts, "NOREPLICATION")
		}

	default:
		return fmt.Errorf("unsupported alter role operation: %T", operation)
	}

	r.w.WriteLinef("%s;", strings.Join(parts, " "))
	return nil
}

// looksEncrypted checks if a password appears to be encrypted/hashed
// This is a heuristic check to help detect potential plaintext passwords
func looksEncrypted(password string) bool {
	// Empty passwords are considered "encrypted" (no warning needed)
	if password == "" {
		return true
	}

	// Check for common PostgreSQL password hash prefixes
	if strings.HasPrefix(password, "md5") ||
		strings.HasPrefix(password, "SCRAM-SHA-256$") ||
		strings.HasPrefix(password, "$2a$") || // bcrypt
		strings.HasPrefix(password, "$2b$") || // bcrypt
		strings.HasPrefix(password, "$2y$") || // bcrypt
		strings.HasPrefix(password, "$5$") || // SHA-256
		strings.HasPrefix(password, "$6$") { // SHA-512
		return true
	}

	// Check if it looks like a hash (long, contains mix of chars/numbers)
	if len(password) >= 32 {
		hasLower := strings.ContainsAny(password, "abcdefghijklmnopqrstuvwxyz")
		hasUpper := strings.ContainsAny(password, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		hasDigit := strings.ContainsAny(password, "0123456789")
		hasSpecial := strings.ContainsAny(password, "!@#$%^&*()_+-=[]{}|;:,.<>?/")

		// If it has a good mix of character types and is long, likely encrypted
		charTypeCount := 0
		if hasLower {
			charTypeCount++
		}
		if hasUpper {
			charTypeCount++
		}
		if hasDigit {
			charTypeCount++
		}
		if hasSpecial {
			charTypeCount++
		}

		return charTypeCount >= 3
	}

	// If none of the above, likely plaintext
	return false
}

func unsupportedFeaturef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ptaherr.ErrUnsupportedFeature, fmt.Sprintf(format, args...))
}

// writeClickHouseOnlyOperation names a ClickHouse construct this dialect has no
// form for, as a comment rather than as a dropped operation: an operator
// reading the plan has to be able to see that something was declined.
func (r *Renderer) writeClickHouseOnlyOperation(operation ast.AlterOperation) {
	switch operation.(type) {
	case *ast.AddSkippingIndexOperation:
		r.w.WriteLinef("-- %s: data-skipping indexes are ClickHouse-specific; ignored.", r.dialectUpper)
	case *ast.ModifyTTLOperation:
		r.w.WriteLinef("-- %s: table TTL is ClickHouse-specific; ignored.", r.dialectUpper)
	}
}
