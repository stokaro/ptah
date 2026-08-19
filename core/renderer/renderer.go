// Package renderer provides dialect-aware SQL rendering capabilities for the Ptah migration system.
//
// This package serves as the main entry point for converting AST nodes to SQL statements
// across different database dialects. It implements a factory pattern to create appropriate
// dialect renderers and provides a unified interface for SQL generation.
//
// The package supports multiple database platforms including PostgreSQL, MySQL,
// MariaDB, and ClickHouse. Unsupported dialects are reported as errors instead
// of falling back to a generic renderer. Each dialect renderer implements the
// ast.Visitor interface to ensure consistent behavior across different database
// systems.
//
// Example usage:
//
//	renderer, err := renderer.NewRenderer("postgresql")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	sql, err := renderer.Render(astNode)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Println(sql)
//
// The renderer automatically handles dialect-specific SQL generation, including:
//   - Data type mappings
//   - Constraint syntax differences
//   - Enum handling (PostgreSQL vs MySQL inline enums)
//   - Index creation syntax
//   - Table options and engine specifications
package renderer

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/clickhouse"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mariadb"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysql"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysqllike"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/sqlite"
	"go.5x5.cz/ptah/internal/clickhouserbac"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/crdbttl"
	"go.5x5.cz/ptah/internal/matviewrefresh"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/planner/tablelookup"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/internal/schemaselection"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/internal/usertypescope"
)

// RenderVisitor defines the interface for rendering AST nodes to SQL statements.
//
// This interface extends ast.Visitor with methods for managing renderer state
// and retrieving the generated SQL output.
type RenderVisitor interface {
	ast.Visitor

	// Dialect returns the database dialect this renderer targets.
	Dialect() string

	// Reset clears the internal output buffer.
	Reset()

	// Output returns the current generated SQL output.
	Output() string

	// Render renders an AST node to SQL and returns the result.
	Render(node ast.Node) (string, error)

	// GetDialect returns the database dialect.
	GetDialect() string

	// GetOutput returns the current generated SQL output.
	GetOutput() string
}

// SupportedDialects returns a list of all supported database dialects.
func SupportedDialects() []string {
	return []string{"postgresql", "postgres", "mysql", "mariadb", "clickhouse", "sqlite", "sqlite3", "sqlserver", "mssql", "cockroachdb", "yugabytedb", "spanner"}
}

// NewRenderer creates a new renderer for the specified database dialect.
//
// The dialect parameter should be one of the supported dialects returned by
// SupportedDialects(). The function performs case-insensitive matching and
// handles common dialect aliases (e.g., "postgres" for "postgresql").
//
// Returns an error if the dialect is not supported.
func NewRenderer(dialect string) (RenderVisitor, error) {
	return NewRendererWithCapabilities(dialect, capability.ForDialect(dialect))
}

// NewRendererWithCapabilities creates a renderer for a concrete server
// capability set. Use this on live database paths where capabilities were
// resolved from DBInfo.Version; NewRenderer remains the offline default.
func NewRendererWithCapabilities(dialect string, caps capability.Capabilities) (RenderVisitor, error) {
	if err := caps.Validate(); err != nil {
		return nil, &ptaherr.CapabilityError{
			Dialect: dialect,
			Feature: "capability set",
			Err:     err,
			Message: fmt.Sprintf("invalid capabilities for %s: %s", platform.NormalizeDialect(dialect), err),
		}
	}
	normalizedDialect := platform.NormalizeDialect(dialect)
	var raw RenderVisitor

	switch normalizedDialect {
	case platform.Postgres:
		raw = postgres.NewWithCapabilities(caps, normalizedDialect)
	case platform.MySQL:
		raw = mysql.NewWithCapabilities(caps)
	case platform.MariaDB:
		raw = mariadb.NewWithCapabilities(caps)
	case platform.ClickHouse:
		raw = clickhouse.NewWithCapabilities(caps)
	case platform.SQLite:
		raw = sqlite.NewWithCapabilities(caps)
	case platform.SQLServer:
		raw = mssql.NewWithCapabilities(caps)
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		raw = postgres.NewWithCapabilities(caps, normalizedDialect)
	default:
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("unsupported database dialect: %s", dialect),
		}
	}
	return &validatingRenderer{
		RenderVisitor: raw,
		dialect:       dialect,
		capabilities:  caps.Clone(),
	}, nil
}

type validatingRenderer struct {
	RenderVisitor
	dialect      string
	capabilities capability.Capabilities
}

func (r *validatingRenderer) Render(node ast.Node) (string, error) {
	r.Reset()
	prepared, err := prepareASTNodeForRendering(r.dialect, r.capabilities, node)
	if err != nil {
		return "", err
	}
	output, err := r.RenderVisitor.Render(prepared)
	if err != nil {
		r.Reset()
		return "", err
	}
	return output, nil
}

func (r *validatingRenderer) VisitStatementList(list *ast.StatementList) error {
	prepared, err := prepareStatementListNode(r.dialect, r.capabilities, list)
	if err != nil {
		r.Reset()
		return err
	}
	for _, statement := range prepared.Statements {
		if err := statement.Accept(r.RenderVisitor); err != nil {
			r.Reset()
			return err
		}
	}
	return nil
}

func (r *validatingRenderer) VisitCreateTable(node *ast.CreateTableNode) error {
	prepared, err := prepareCreateTableNode(r.dialect, r.capabilities, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitCreateTable(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitAlterTable(node *ast.AlterTableNode) error {
	prepared, err := prepareAlterTableNode(r.dialect, r.capabilities, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitAlterTable(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitColumn(node *ast.ColumnNode) error {
	prepared, err := prepareColumnNode(r.dialect, r.capabilities, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitColumn(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitConstraint(node *ast.ConstraintNode) error {
	prepared, err := prepareConstraintNode(r.dialect, r.capabilities, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitConstraint(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitIndex(node *ast.IndexNode) error {
	prepared, err := prepareIndexNode(r.dialect, r.capabilities, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitIndex(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitExtension(node *ast.ExtensionNode) error {
	prepared, err := prepareExtensionNode(r.dialect, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitExtension(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

func (r *validatingRenderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	prepared, err := prepareCreateMaterializedViewNode(r.dialect, node)
	if err != nil {
		r.Reset()
		return err
	}
	if err := r.RenderVisitor.VisitCreateMaterializedView(prepared); err != nil {
		r.Reset()
		return err
	}
	return nil
}

// RenderSQL is a convenience function that creates a renderer and renders an AST node in one call.
//
// This function is useful for one-off SQL generation where you don't need to reuse the renderer.
// For multiple operations, it's more efficient to create a renderer once and reuse it.
func RenderSQL(dialect string, nodes ...ast.Node) (string, error) {
	return RenderSQLWithCapabilities(dialect, capability.ForDialect(dialect), nodes...)
}

// RenderSQLWithCapabilities renders SQL for a concrete server capability set.
func RenderSQLWithCapabilities(dialect string, caps capability.Capabilities, nodes ...ast.Node) (string, error) {
	r, err := NewRendererWithCapabilities(dialect, caps)
	if err != nil {
		return "", err
	}
	return VisitorRenderSQL(r, nodes...)
}

func VisitorRenderSQL(r RenderVisitor, nodes ...ast.Node) (string, error) {
	r.Reset()
	if validating, ok := r.(*validatingRenderer); ok {
		prepared, err := prepareASTNodesForRendering(
			validating.dialect,
			validating.capabilities,
			nodes,
		)
		if err != nil {
			return "", err
		}
		nodes = prepared
	}
	for _, node := range nodes {
		if err := node.Accept(r); err != nil {
			r.Reset()
			if _, ok := errors.AsType[*ptaherr.RenderError](err); ok {
				return "", err
			}
			return "", &ptaherr.RenderError{
				Dialect: r.GetDialect(),
				Node:    node,
				Err:     err,
				Message: err.Error(),
			}
		}
	}
	return r.Output(), nil
}

func prepareASTNodesForRendering(
	dialect string,
	caps capability.Capabilities,
	nodes []ast.Node,
) ([]ast.Node, error) {
	prepared := make([]ast.Node, len(nodes))
	for i, node := range nodes {
		cloned, err := prepareASTNodeForRendering(dialect, caps, node)
		if err != nil {
			return nil, err
		}
		prepared[i] = cloned
	}
	return prepared, nil
}

func prepareASTNodeForRendering(
	dialect string,
	caps capability.Capabilities,
	node ast.Node,
) (ast.Node, error) {
	if node == nil {
		return nil, invalidASTForeignKeyError(dialect, "AST node is nil")
	}
	switch typed := node.(type) {
	case *ast.StatementList:
		return prepareStatementListNode(dialect, caps, typed)
	case *ast.CreateTableNode:
		return prepareCreateTableNode(dialect, caps, typed)
	case *ast.AlterTableNode:
		return prepareAlterTableNode(dialect, caps, typed)
	case *ast.ConstraintNode:
		return prepareConstraintNode(dialect, caps, typed)
	case *ast.ColumnNode:
		return prepareColumnNode(dialect, caps, typed)
	case *ast.IndexNode:
		return prepareIndexNode(dialect, caps, typed)
	case *ast.ExtensionNode:
		return prepareExtensionNode(dialect, typed)
	case *ast.CreateMaterializedViewNode:
		return prepareCreateMaterializedViewNode(dialect, typed)
	default:
		if isNilInterface(node) {
			return nil, invalidASTForeignKeyError(dialect, "AST node is nil")
		}
		return node, nil
	}
}

func prepareCreateMaterializedViewNode(
	dialect string,
	node *ast.CreateMaterializedViewNode,
) (*ast.CreateMaterializedViewNode, error) {
	if node == nil {
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "materialized view node is nil",
		}
	}
	if err := matviewrefresh.Validate(dialect, node.Name, node.RefreshStrategy); err != nil {
		return nil, err
	}
	return new(*node), nil
}

func prepareExtensionNode(dialect string, node *ast.ExtensionNode) (*ast.ExtensionNode, error) {
	if node == nil {
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "extension node is nil",
		}
	}
	if !extensionInstallationSchemaRejected(dialect) || node.Schema == "" {
		return new(*node), nil
	}
	return nil, unsupportedExtensionInstallationSchema(dialect, node.Name, node.Schema)
}

func extensionInstallationSchemaRejected(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.CockroachDB, platform.Spanner:
		return true
	default:
		return false
	}
}

func unsupportedExtensionInstallationSchema(dialect, name, schema string) error {
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: "PostgreSQL extension installation schemas",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s does not support PostgreSQL extension installation schema %q for extension %q",
			normalized,
			schema,
			name,
		),
	}
}

func prepareIndexNode(dialect string, caps capability.Capabilities, node *ast.IndexNode) (*ast.IndexNode, error) {
	if node == nil {
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "index node is nil",
		}
	}
	if err := validateIndexInclude(dialect, caps, node.Name, node.Type, node.IncludeColumns); err != nil {
		return nil, err
	}
	return node, nil
}

func prepareStatementListNode(
	dialect string,
	caps capability.Capabilities,
	list *ast.StatementList,
) (*ast.StatementList, error) {
	if list == nil {
		return nil, invalidASTForeignKeyError(dialect, "statement list is nil")
	}
	prepared, err := prepareASTNodesForRendering(dialect, caps, list.Statements)
	if err != nil {
		return nil, err
	}
	return &ast.StatementList{Statements: prepared}, nil
}

func prepareCreateTableNode(
	dialect string,
	caps capability.Capabilities,
	node *ast.CreateTableNode,
) (*ast.CreateTableNode, error) {
	if node == nil {
		return nil, invalidASTForeignKeyError(dialect, "create-table node is nil")
	}
	cloned := *node
	cloned.Columns = slices.Clone(node.Columns)
	cloned.Constraints = slices.Clone(node.Constraints)
	cloned.Options = maps.Clone(node.Options)

	for i, column := range cloned.Columns {
		prepared, err := prepareColumnNode(dialect, caps, column)
		if err != nil {
			return nil, err
		}
		cloned.Columns[i] = prepared
	}
	for i, constraint := range cloned.Constraints {
		prepared, err := prepareConstraintNode(dialect, caps, constraint)
		if err != nil {
			return nil, err
		}
		cloned.Constraints[i] = prepared
	}
	if err := validateCreateTableForeignKeyColumns(dialect, &cloned); err != nil {
		return nil, err
	}
	if err := ensureASTForeignKeyTableEngine(dialect, &cloned); err != nil {
		return nil, err
	}
	return &cloned, nil
}

func ensureASTForeignKeyTableEngine(dialect string, node *ast.CreateTableNode) error {
	normalizedDialect := platform.NormalizeDialect(dialect)
	if normalizedDialect != platform.MySQL && normalizedDialect != platform.MariaDB {
		return nil
	}
	if !createTableContainsForeignKey(node) {
		return nil
	}

	optionKey, engine := tableOption(node.Options, "ENGINE")
	if strings.TrimSpace(engine) == "" {
		if optionKey == "" {
			optionKey = "ENGINE"
		}
		if node.Options == nil {
			node.Options = make(map[string]string)
		}
		node.Options[optionKey] = "InnoDB"
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(engine), "InnoDB") {
		return invalidASTForeignKeyError(
			dialect,
			fmt.Sprintf(
				"table %q uses storage engine %q; %s foreign keys require InnoDB",
				node.Name,
				strings.TrimSpace(engine),
				normalizedDialect,
			),
		)
	}
	return nil
}

func createTableContainsForeignKey(node *ast.CreateTableNode) bool {
	for _, column := range node.Columns {
		if column.ForeignKey != nil {
			return true
		}
	}
	for _, constraint := range node.Constraints {
		if constraint.Type == ast.ForeignKeyConstraint {
			return true
		}
	}
	return false
}

func tableOption(options map[string]string, target string) (optionKey, optionValue string) {
	for key, value := range options {
		if strings.EqualFold(key, target) {
			return key, value
		}
	}
	return "", ""
}

func prepareAlterTableNode(
	dialect string,
	caps capability.Capabilities,
	node *ast.AlterTableNode,
) (*ast.AlterTableNode, error) {
	if node == nil {
		return nil, invalidASTForeignKeyError(dialect, "alter-table node is nil")
	}
	cloned := *node
	cloned.Operations = slices.Clone(node.Operations)
	for i, operation := range cloned.Operations {
		prepared, err := prepareAlterOperation(dialect, caps, operation)
		if err != nil {
			return nil, err
		}
		cloned.Operations[i] = prepared
	}
	return &cloned, nil
}

func prepareAlterOperation(
	dialect string,
	caps capability.Capabilities,
	operation ast.AlterOperation,
) (ast.AlterOperation, error) {
	if operation == nil {
		return nil, invalidASTForeignKeyError(dialect, "alter-table operation is nil")
	}
	switch typed := operation.(type) {
	case *ast.AddConstraintOperation:
		if typed == nil {
			return nil, invalidASTForeignKeyError(dialect, "add-constraint operation is nil")
		}
		cloned := *typed
		constraint, err := prepareConstraintNode(dialect, caps, typed.Constraint)
		if err != nil {
			return nil, err
		}
		cloned.Constraint = constraint
		return &cloned, nil
	case *ast.AddColumnOperation:
		if typed == nil {
			return nil, invalidASTForeignKeyError(dialect, "add-column operation is nil")
		}
		cloned := *typed
		column, err := prepareColumnNode(dialect, caps, typed.Column)
		if err != nil {
			return nil, err
		}
		cloned.Column = column
		return &cloned, nil
	case *ast.ModifyColumnOperation:
		if typed == nil {
			return nil, invalidASTForeignKeyError(dialect, "modify-column operation is nil")
		}
		cloned := *typed
		column, err := prepareColumnNode(dialect, caps, typed.Column)
		if err != nil {
			return nil, err
		}
		cloned.Column = column
		return &cloned, nil
	default:
		if isNilInterface(operation) {
			return nil, invalidASTForeignKeyError(dialect, "alter-table operation is nil")
		}
		return operation, nil
	}
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}

func prepareColumnNode(
	dialect string,
	caps capability.Capabilities,
	node *ast.ColumnNode,
) (*ast.ColumnNode, error) {
	if node == nil {
		return nil, invalidASTForeignKeyError(dialect, "column node is nil")
	}
	if node.ForeignKey == nil {
		return node, nil
	}
	if !caps.Has(capability.ForeignKeys) {
		return nil, foreignKeysUnsupportedError(dialect)
	}

	cloned := *node
	cloned.ForeignKey = cloneForeignKeyRef(node.ForeignKey)
	if err := validateASTForeignKey(dialect, []string{node.Name}, cloned.ForeignKey); err != nil {
		return nil, err
	}
	var err error
	cloned.ForeignKey.OnDelete, cloned.ForeignKey.OnUpdate, err = normalizeReferentialActions(
		dialect,
		cloned.ForeignKey.OnDelete,
		cloned.ForeignKey.OnUpdate,
	)
	if err != nil {
		return nil, err
	}
	if !cloned.Nullable && foreignKeyUsesSetNull(cloned.ForeignKey) {
		return nil, invalidASTForeignKeyError(
			dialect,
			fmt.Sprintf("column %q uses SET NULL but is NOT NULL", cloned.Name),
		)
	}
	return &cloned, nil
}

func validateCreateTableForeignKeyColumns(dialect string, node *ast.CreateTableNode) error {
	for _, constraint := range node.Constraints {
		if constraint.Type != ast.ForeignKeyConstraint {
			continue
		}
		for _, columnName := range constraint.Columns {
			column := createTableColumn(node.Columns, columnName)
			if column == nil {
				return invalidASTForeignKeyError(
					dialect,
					fmt.Sprintf("table %q has no local foreign-key column %q", node.Name, columnName),
				)
			}
			if !column.Nullable && foreignKeyUsesSetNull(constraint.Reference) {
				return invalidASTForeignKeyError(
					dialect,
					fmt.Sprintf(
						"foreign key on %q.%q uses SET NULL but the local column is NOT NULL",
						node.Name,
						columnName,
					),
				)
			}
		}
	}
	return nil
}

func createTableColumn(columns []*ast.ColumnNode, name string) *ast.ColumnNode {
	for _, column := range columns {
		if column != nil && column.Name == name {
			return column
		}
	}
	return nil
}

func foreignKeyUsesSetNull(reference *ast.ForeignKeyRef) bool {
	return reference != nil && (reference.OnDelete == "SET NULL" || reference.OnUpdate == "SET NULL")
}

func prepareConstraintNode(
	dialect string,
	caps capability.Capabilities,
	node *ast.ConstraintNode,
) (*ast.ConstraintNode, error) {
	if node == nil {
		return nil, invalidASTForeignKeyError(dialect, "constraint node is nil")
	}
	if node.Type != ast.ForeignKeyConstraint {
		return node, nil
	}
	if !caps.Has(capability.ForeignKeys) {
		return nil, foreignKeysUnsupportedError(dialect)
	}

	cloned := *node
	cloned.Columns = slices.Clone(node.Columns)
	cloned.Reference = cloneForeignKeyRef(node.Reference)
	if err := validateASTForeignKey(dialect, cloned.Columns, cloned.Reference); err != nil {
		return nil, err
	}
	var err error
	cloned.Reference.OnDelete, cloned.Reference.OnUpdate, err = normalizeReferentialActions(
		dialect,
		cloned.Reference.OnDelete,
		cloned.Reference.OnUpdate,
	)
	if err != nil {
		return nil, err
	}
	return &cloned, nil
}

func cloneForeignKeyRef(reference *ast.ForeignKeyRef) *ast.ForeignKeyRef {
	if reference == nil {
		return nil
	}
	cloned := *reference
	cloned.Columns = slices.Clone(reference.Columns)
	return &cloned
}

func validateASTForeignKey(
	dialect string,
	localColumns []string,
	reference *ast.ForeignKeyRef,
) error {
	if reference == nil || strings.TrimSpace(reference.Table) == "" {
		return invalidASTForeignKeyError(dialect, "referenced table is empty")
	}
	referencedColumns := reference.ReferencedColumns()
	if err := validateForeignKeyColumnLists(localColumns, referencedColumns); err != nil {
		return invalidASTForeignKeyError(dialect, err.Error())
	}
	return nil
}

func validateForeignKeyColumnLists(localColumns, referencedColumns []string) error {
	if len(localColumns) == 0 || len(referencedColumns) == 0 || len(localColumns) != len(referencedColumns) {
		return fmt.Errorf(
			"foreign key has %d local columns and %d referenced columns",
			len(localColumns),
			len(referencedColumns),
		)
	}
	if err := validateForeignKeyColumns("local", localColumns); err != nil {
		return err
	}
	return validateForeignKeyColumns("referenced", referencedColumns)
}

func validateForeignKeyColumns(kind string, columns []string) error {
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("%s foreign-key column is empty", kind)
		}
		if _, duplicate := seen[column]; duplicate {
			return fmt.Errorf("%s foreign-key column %q is duplicated", kind, column)
		}
		seen[column] = struct{}{}
	}
	return nil
}

func invalidASTForeignKeyError(dialect, message string) error {
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrInvalidSchemaDiff,
		Message: "invalid foreign key: " + message,
	}
}

func foreignKeysUnsupportedError(dialect string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign keys",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s does not support foreign keys", platform.NormalizeDialect(dialect)),
	}
}

// GetOrderedCreateStatements renders a complete schema for the default
// capability preset of dialect. Non-SQLite targets emit all tables before
// phase-two foreign key constraints, so mutually dependent tables remain
// executable. SQLite keeps foreign keys inline because it cannot add them
// after table creation.
//
// The function validates foreign key shape, actions, and target capabilities
// before rendering. Unsupported or malformed constraints return an error and
// no partial statement list.
func GetOrderedCreateStatements(r *goschema.Database, dialect string) ([]string, error) {
	return GetOrderedCreateStatementsWithCapabilities(r, dialect, capability.ForDialect(dialect))
}

// ValidateSchema validates a complete schema against the default capability
// preset for dialect without rendering SQL.
func ValidateSchema(r *goschema.Database, dialect string) error {
	return ValidateSchemaWithCapabilities(r, dialect, capability.ForDialect(dialect))
}

// ValidateSchemaWithCapabilities validates a complete schema against a
// concrete server capability set without rendering SQL.
func ValidateSchemaWithCapabilities(
	r *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) error {
	if _, err := NewRendererWithCapabilities(dialect, caps); err != nil {
		return err
	}
	if r == nil {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "cannot validate a nil database schema",
		}
	}
	_, err := prepareDatabaseForRendering(r, dialect, caps)
	return err
}

// GetOrderedCreateStatementsWithCapabilities renders ordered create statements
// for a concrete server capability set. It has the same two-phase and
// fail-closed guarantees as GetOrderedCreateStatements.
func GetOrderedCreateStatementsWithCapabilities(
	r *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) ([]string, error) {
	var statements []string

	if _, err := NewRendererWithCapabilities(dialect, caps); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: "cannot render a nil database schema",
		}
	}
	database, err := prepareDatabaseForRendering(r, dialect, caps)
	if err != nil {
		return nil, err
	}
	astNodes := fromschema.FromDatabase(database, dialect)
	for _, node := range astNodes.Statements {
		sql, err := RenderSQLWithCapabilities(dialect, caps, node)
		if err != nil {
			return nil, err
		}
		// A node a dialect renders as nothing is not a statement. Keeping it
		// put a bare `;` in front of the script — SQLite renders no statement
		// for its `main` namespace, which an introspected database now
		// describes as a schema (stokaro/ptah#1264).
		if strings.TrimSpace(sql) == "" {
			continue
		}
		statements = append(statements, sql)
	}

	return statements, nil
}

func prepareDatabaseForRendering(
	database *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) (goschema.Database, error) {
	// The declared scope is resolved before anything else looks at the schema.
	// An object this target was not declared for is not part of the desired
	// state here, so it must not be validated against this target's
	// capabilities either: refusing a declaration the operator already excluded
	// from this dialect is the refusal the scope exists to remove.
	//
	// This is the render half of the seam. The compare half is in
	// [go.5x5.cz/ptah/migration/schemadiff.CompareReportingUndecidedAdditions],
	// and both go through [goschema.ScopeToDialect] so `schema render` and
	// `schema apply` cannot disagree about which objects a target has.
	database = goschema.ScopeToDialect(database, dialect)
	if err := validateDatabaseDeclarations(dialect, caps, database); err != nil {
		return goschema.Database{}, err
	}

	prepared := *database
	prepared.Tables = slices.Clone(database.Tables)
	prepared.Fields = slices.Clone(database.Fields)
	prepared.EmbeddedFields = slices.Clone(database.EmbeddedFields)
	prepared.Constraints = slices.Clone(database.Constraints)

	hasForeignKeys := false
	for i := range prepared.Fields {
		field := &prepared.Fields[i]
		if field.Foreign == "" {
			continue
		}
		hasForeignKeys = true
		if err := validateFieldForeignKey(*field, dialect); err != nil {
			return goschema.Database{}, err
		}
		var err error
		field.OnDelete, field.OnUpdate, err = normalizeReferentialActions(dialect, field.OnDelete, field.OnUpdate)
		if err != nil {
			return goschema.Database{}, err
		}
	}
	for i := range prepared.EmbeddedFields {
		embedded := &prepared.EmbeddedFields[i]
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}
		hasForeignKeys = true
		if err := validateForeignKeyReference(embedded.Ref); err != nil {
			return goschema.Database{}, &ptaherr.RenderError{
				Dialect: dialect,
				Err:     ptaherr.ErrInvalidSchemaDiff,
				Message: fmt.Sprintf("invalid embedded foreign key on field %q: %s", embedded.Field, err),
			}
		}
		var err error
		embedded.OnDelete, embedded.OnUpdate, err = normalizeReferentialActions(dialect, embedded.OnDelete, embedded.OnUpdate)
		if err != nil {
			return goschema.Database{}, err
		}
	}
	for i := range prepared.Constraints {
		constraint := &prepared.Constraints[i]
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") {
			continue
		}
		hasForeignKeys = true
		if err := validateTableForeignKey(*constraint, dialect); err != nil {
			return goschema.Database{}, err
		}
		var err error
		constraint.OnDelete, constraint.OnUpdate, err = normalizeReferentialActions(dialect, constraint.OnDelete, constraint.OnUpdate)
		if err != nil {
			return goschema.Database{}, err
		}
	}
	if hasForeignKeys && !caps.Has(capability.ForeignKeys) {
		return goschema.Database{}, foreignKeysUnsupportedError(dialect)
	}
	if err := validateSchemaForeignKeys(prepared, dialect, caps); err != nil {
		return goschema.Database{}, err
	}
	if hasForeignKeys {
		makeMySQLForeignKeyTableEnginesExplicit(&prepared, dialect)
	}
	return prepared, nil
}

func validateDatabaseDeclarations(
	dialect string,
	caps capability.Capabilities,
	database *goschema.Database,
) error {
	if err := matviewrefresh.ValidateDeclared(dialect, database.MaterializedViews); err != nil {
		return err
	}
	if err := schemaselection.ValidateDeclaredPostgresSystemSchemas(dialect, database.Schemas); err != nil {
		return err
	}
	if err := validateExtensionInstallationSchemas(dialect, database.Extensions); err != nil {
		return err
	}
	// A reserved PostgreSQL role name renders into a CREATE ROLE the server is
	// guaranteed to reject, so it is refused here, in the validation phase both
	// whole-schema rendering and migration planning run before they emit
	// anything (stokaro/ptah#1312).
	if err := reservedrole.ValidateDeclared(dialect, database.Roles); err != nil {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     err,
			Message: err.Error(),
		}
	}
	if err := mysqllike.ValidateDeclaredRoles(dialect, database.Roles); err != nil {
		return err
	}
	// A domain, composite or range type the target cannot create is refused
	// here rather than skipped, because skipping it leaves the declaration's
	// own columns naming a type the server has no definition of
	// (stokaro/ptah#1717).
	if err := usertypescope.ValidateDeclared(dialect, caps, database); err != nil {
		return err
	}
	// ClickHouse roles and grants are real, and a narrower set of declarations
	// is representable there than in PostgreSQL: a role carries no attributes
	// at all, and the server absorbs a narrower grant into a broader one, so a
	// schema declaring both can never converge. The empty default database is
	// deliberate — a render is offline and has no current database, so an
	// unqualified on_table is refused rather than attached to a database
	// nobody named. See internal/clickhouserbac (stokaro/ptah#1025).
	if err := clickhouserbac.ValidateDeclared(dialect, database.Roles, database.Grants, ""); err != nil {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     err,
			Message: err.Error(),
		}
	}
	// Row-level TTL is refused here as well as at the table it belongs to,
	// because these are the refusals that must arrive before ANY statement is
	// emitted: a knob without an enabler, or a value the server stores
	// differently from how it was written, is a property of the declaration
	// rather than of the one CREATE TABLE that carries it. The per-table gate
	// in the PostgreSQL renderer catches the dialect case; this catches the
	// rest, whole-schema, before the first statement (stokaro/ptah#1027).
	if err := crdbttl.ValidateDeclared(dialect, caps, crdbttl.DeclaredIn(rowTTLTables(database))); err != nil {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     err,
			Message: err.Error(),
		}
	}
	if err := validateRoutineIdentityCollisions(dialect, database.Functions); err != nil {
		return err
	}
	return validateDeclaredIndexIncludes(dialect, caps, database.Indexes)
}

// validateRoutineIdentityCollisions refuses two function declarations the
// target cannot tell apart.
//
// The duplicate-definition check in core/goschema keys functions by their exact
// name, which is right for PostgreSQL, where routine names ARE case-sensitive
// and `Foo` and `foo` are two functions. On MySQL and MariaDB they are one
// routine, and the comparator folds them accordingly -- so the two keyings
// disagreed, and the disagreement lost a declaration rather than reporting it:
// both names passed validation, the comparator's map kept whichever came last,
// and an apply against an empty database created ONE function from TWO
// declarations and exited 0. Measured on MySQL 26.7.0 and MariaDB 12.3.2:
//
//	declared 2 functions -> diff.FunctionsAdded = [ptah_dup_fn]
//	                     -> 1 statement planned
//	                     -> 1 row in information_schema.ROUTINES
//
// The identity is [mysqlroutine.IdentityKey], the same function the comparator
// folds with, so the check and the behavior it guards cannot drift apart. This
// lives in the dialect-aware validation seam rather than in the dialect-blind
// duplicate check because the collision only exists on targets that fold; both
// `schema render` and the migration planner pass through here.
//
// Only names that differ in spelling are reported. Two declarations of the
// SAME name are the existing duplicate-definition case, which
// core/goschema already answers -- and answers better, because it allows
// byte-identical repeats.
func validateRoutineIdentityCollisions(dialect string, functions []goschema.Function) error {
	if !routineNamesAreCaseInsensitive(dialect) {
		return nil
	}
	seen := make(map[objectidentity.Key]string, len(functions))
	for _, function := range functions {
		ref, ok := tableref.Parse(function.Name)
		if !ok {
			continue
		}
		// The shared identity model rather than a private struct: a private one
		// is what let this check and the comparator key the same routine two
		// ways in the first place. The name is folded by mysqlroutine before it
		// arrives, so the builder is asked to fold nothing on top of it.
		key := routineIdentities.
			SchemaScopedParts(objectidentity.KindFunction, ref.Schema, mysqlroutine.IdentityKey(ref.Name)).
			Key()
		previous, collides := seen[key]
		if collides && previous != function.Name {
			return &ptaherr.RenderError{
				Dialect: dialect,
				Err:     ptaherr.ErrUnsupportedFeature,
				Message: fmt.Sprintf(
					"functions %q and %q differ only by case, and stored-routine names are "+
						"case-insensitive on %s, so the target cannot hold both: creating the "+
						"second is Error 1304 on the first. Rename one of them",
					previous, function.Name, dialect),
			}
		}
		seen[key] = function.Name
	}
	return nil
}

// routineIdentities folds nothing of its own: the routine name arrives already
// folded by [mysqlroutine.IdentityKey], which is the rule the comparator uses,
// and folding again here would be the second application invariant 4 of
// docs/object_identity.md refuses.
var routineIdentities = objectidentity.NewBuilder(identifier.Semantics{})

// routineNamesAreCaseInsensitive reports whether dialect folds stored-routine
// names. PostgreSQL and its family do not, which is why this is not applied
// everywhere.
func routineNamesAreCaseInsensitive(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func validateExtensionInstallationSchemas(dialect string, extensions []goschema.Extension) error {
	if !extensionInstallationSchemaRejected(dialect) {
		return nil
	}
	for _, extension := range extensions {
		if extension.Schema != "" {
			return unsupportedExtensionInstallationSchema(dialect, extension.Name, extension.Schema)
		}
	}
	return nil
}

func validateDeclaredIndexIncludes(
	dialect string,
	caps capability.Capabilities,
	indexes []goschema.Index,
) error {
	for _, index := range indexes {
		if err := validateIndexInclude(dialect, caps, index.Name, index.Type, index.IncludeColumns); err != nil {
			return err
		}
	}
	return nil
}

func validateIndexInclude(
	dialect string,
	caps capability.Capabilities,
	indexName, indexType string,
	includeColumns []string,
) error {
	if len(includeColumns) == 0 {
		return nil
	}
	for i, column := range includeColumns {
		if strings.TrimSpace(column) != "" {
			continue
		}
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf("index %q has an empty INCLUDE column at position %d", indexName, i+1),
		}
	}
	trimmedIndexType := strings.TrimSpace(indexType)
	if trimmedIndexType != indexType {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf(
				"index %q access method %q has leading or trailing whitespace",
				indexName,
				indexType,
			),
		}
	}

	normalizedDialect := platform.NormalizeDialect(dialect)
	switch normalizedDialect {
	case platform.Postgres, platform.YugabyteDB, platform.Spanner:
	default:
		return &ptaherr.CapabilityError{
			Dialect: dialect,
			Feature: "index INCLUDE columns",
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf(
				"%s does not support INCLUDE columns on index %q; target postgres, yugabytedb, or spanner",
				normalizedDialect,
				indexName,
			),
		}
	}

	method := strings.ToUpper(trimmedIndexType)
	var allowed bool
	var supportedMethods string
	switch normalizedDialect {
	case platform.Postgres:
		allowed = method == "" || method == "BTREE" || method == "GIST" ||
			(method == "SPGIST" && caps.Has(capability.IndexIncludeSPGiST))
		supportedMethods = "the default, BTREE, or GIST access method"
		if caps.Has(capability.IndexIncludeSPGiST) {
			supportedMethods = "the default, BTREE, GIST, or SPGIST access method"
		}
	case platform.YugabyteDB:
		allowed = method == "" || method == "LSM" || method == "BTREE"
		supportedMethods = "the default, LSM, or BTREE access method"
	case platform.Spanner:
		allowed = method == ""
		supportedMethods = "the default access method"
	}
	if allowed {
		return nil
	}
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "index INCLUDE access method",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s INCLUDE columns on index %q require %s; access method %q is not supported",
			normalizedDialect,
			indexName,
			supportedMethods,
			method,
		),
	}
}

func makeMySQLForeignKeyTableEnginesExplicit(database *goschema.Database, dialect string) {
	normalizedDialect := platform.NormalizeDialect(dialect)
	if normalizedDialect != platform.MySQL && normalizedDialect != platform.MariaDB {
		return
	}

	participants := mysqlForeignKeyTableParticipants(*database)
	for i := range database.Tables {
		table := &database.Tables[i]
		if _, participates := participants[table.QualifiedName()]; !participates {
			continue
		}
		engine, overridden := configuredTableEngine(*table, normalizedDialect)
		if strings.TrimSpace(engine) != "" {
			continue
		}
		if overridden {
			table.Overrides = maps.Clone(table.Overrides)
			table.Overrides[normalizedDialect] = maps.Clone(table.Overrides[normalizedDialect])
			table.Overrides[normalizedDialect]["engine"] = "InnoDB"
			continue
		}
		table.Engine = "InnoDB"
	}
}

func mysqlForeignKeyTableParticipants(database goschema.Database) map[string]struct{} {
	participants := make(map[string]struct{})
	fields := fromschema.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
	for _, field := range fields {
		if field.Foreign == "" {
			continue
		}
		owner := tableByStructName(database.Tables, field.StructName)
		if owner == nil {
			continue
		}
		target := referencedTable(
			database.Tables,
			*owner,
			fromschema.ParseForeignKeyReference(field.Foreign).Table,
		)
		participants[owner.QualifiedName()] = struct{}{}
		participants[target.QualifiedName()] = struct{}{}
	}
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") {
			continue
		}
		owner := constraintOwnerTable(database.Tables, constraint)
		target := referencedTable(database.Tables, *owner, constraint.ForeignTable)
		participants[owner.QualifiedName()] = struct{}{}
		participants[target.QualifiedName()] = struct{}{}
	}
	return participants
}

func configuredTableEngine(table goschema.Table, dialect string) (string, bool) {
	if overrides := table.Overrides[dialect]; overrides != nil {
		if engine, found := overrides["engine"]; found {
			return engine, true
		}
	}
	return table.Engine, false
}

func validateFieldForeignKey(field goschema.Field, dialect string) error {
	if err := validateForeignKeyReference(field.Foreign); err != nil {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf("invalid foreign key on field %q: %s", field.Name, err),
		}
	}
	return nil
}

func validateForeignKeyReference(reference string) error {
	parsed := fromschema.ParseForeignKeyReference(reference)
	if parsed == nil || strings.TrimSpace(parsed.Table) == "" {
		return fmt.Errorf("malformed reference %q", reference)
	}
	columns := parsed.ReferencedColumns()
	if len(columns) != 1 || strings.TrimSpace(columns[0]) == "" || strings.Contains(columns[0], ",") {
		return fmt.Errorf("reference %q must name exactly one column", reference)
	}
	return nil
}

func validateTableForeignKey(constraint goschema.Constraint, dialect string) error {
	localColumns := constraint.Columns
	foreignColumns := constraint.ForeignColumnsOrDefault()
	if strings.TrimSpace(constraint.ForeignTable) == "" || len(localColumns) == 0 || len(foreignColumns) == 0 || len(localColumns) != len(foreignColumns) {
		return &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf(
				"invalid foreign key constraint %q: %d local columns and %d referenced columns",
				constraint.Name,
				len(localColumns),
				len(foreignColumns),
			),
		}
	}
	return nil
}

func normalizeReferentialActions(
	dialect, onDelete, onUpdate string,
) (normalizedDelete, normalizedUpdate string, err error) {
	normalizedDelete, err = normalizeReferentialAction(dialect, "ON DELETE", onDelete)
	if err != nil {
		return "", "", err
	}
	normalizedUpdate, err = normalizeReferentialAction(dialect, "ON UPDATE", onUpdate)
	if err != nil {
		return "", "", err
	}
	return normalizedDelete, normalizedUpdate, nil
}

func normalizeReferentialAction(dialect, clause, action string) (string, error) {
	action = strings.ReplaceAll(action, "_", " ")
	action = strings.ToUpper(strings.Join(strings.Fields(action), " "))
	if action == "" {
		return "", nil
	}
	if !slices.Contains([]string{"NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT"}, action) {
		return "", invalidReferentialActionError(dialect, clause, action)
	}

	normalizedDialect := platform.NormalizeDialect(dialect)
	switch normalizedDialect {
	case platform.MySQL, platform.MariaDB:
		if action == "SET DEFAULT" {
			return "", invalidReferentialActionError(dialect, clause, action)
		}
	case platform.SQLServer:
		if action == "RESTRICT" {
			return "NO ACTION", nil
		}
	case platform.Spanner:
		if clause == "ON UPDATE" || !slices.Contains([]string{"NO ACTION", "CASCADE"}, action) {
			return "", invalidReferentialActionError(dialect, clause, action)
		}
	}
	return action, nil
}

func invalidReferentialActionError(dialect, clause, action string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign key referential actions",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s does not support %s %s", platform.NormalizeDialect(dialect), clause, action),
	}
}

func validateSchemaForeignKeys(
	database goschema.Database,
	dialect string,
	caps capability.Capabilities,
) error {
	fields := fromschema.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
	bindings := make([]foreignKeyBinding, 0)
	explicitNames := make(map[string]map[string]struct{})
	for _, field := range fields {
		if field.Foreign == "" {
			continue
		}
		owner := tableByStructName(database.Tables, field.StructName)
		if owner == nil {
			if isEmbeddedHelperStruct(database.EmbeddedFields, field.StructName) {
				continue
			}
			return invalidSchemaForeignKeyError(
				dialect,
				fmt.Sprintf("field %q has no owning table for struct %q", field.Name, field.StructName),
			)
		}
		if err := reserveExplicitForeignKeyName(explicitNames, dialect, *owner, field.ForeignKeyName); err != nil {
			return invalidSchemaForeignKeyError(dialect, err.Error())
		}
		reference := fromschema.ParseForeignKeyReference(field.Foreign)
		target := referencedTable(database.Tables, *owner, reference.Table)
		if target == nil {
			return invalidSchemaForeignKeyError(
				dialect,
				fmt.Sprintf("field %q references unknown table %q", field.Name, reference.Table),
			)
		}
		referencedColumns := reference.ReferencedColumns()
		if err := validateSchemaForeignKeyColumns(
			fields,
			dialect,
			*owner,
			[]string{field.Name},
			*target,
			referencedColumns,
			field.OnDelete,
			field.OnUpdate,
		); err != nil {
			return err
		}
		if err := validateReferencedKeyPolicy(database, dialect, caps, *target, referencedColumns); err != nil {
			return err
		}
		bindings = append(bindings, foreignKeyBinding{
			owner:    owner.QualifiedName(),
			target:   target.QualifiedName(),
			onDelete: field.OnDelete,
			onUpdate: field.OnUpdate,
		})
	}
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") {
			continue
		}
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner == nil {
			return invalidSchemaForeignKeyError(
				dialect,
				fmt.Sprintf("constraint %q has no owning table", constraint.Name),
			)
		}
		if err := reserveExplicitForeignKeyName(explicitNames, dialect, *owner, constraint.Name); err != nil {
			return invalidSchemaForeignKeyError(dialect, err.Error())
		}
		columns := constraint.ForeignColumnsOrDefault()
		target := referencedTable(database.Tables, *owner, constraint.ForeignTable)
		if target == nil {
			return invalidSchemaForeignKeyError(
				dialect,
				fmt.Sprintf("constraint %q references unknown table %q", constraint.Name, constraint.ForeignTable),
			)
		}
		if err := validateSchemaForeignKeyColumns(
			fields,
			dialect,
			*owner,
			constraint.Columns,
			*target,
			columns,
			constraint.OnDelete,
			constraint.OnUpdate,
		); err != nil {
			return err
		}
		if err := validateReferencedKeyPolicy(database, dialect, caps, *target, columns); err != nil {
			return err
		}
		bindings = append(bindings, foreignKeyBinding{
			owner:    owner.QualifiedName(),
			target:   target.QualifiedName(),
			onDelete: constraint.OnDelete,
			onUpdate: constraint.OnUpdate,
		})
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		return validateSQLServerCascadeGraph(dialect, bindings)
	}
	return nil
}

func isEmbeddedHelperStruct(embeddedFields []goschema.EmbeddedField, structName string) bool {
	for _, embedded := range embeddedFields {
		if embedded.StructName == structName || embedded.EmbeddedTypeName == structName {
			return true
		}
	}
	return false
}

func reserveExplicitForeignKeyName(
	reserved map[string]map[string]struct{},
	dialect string,
	table goschema.Table,
	name string,
) error {
	if name == "" {
		return nil
	}
	if err := validateExplicitForeignKeyName(dialect, name); err != nil {
		return err
	}
	scope, normalizedName := foreignKeyNameScope(dialect, table, name)
	if reserved[scope] == nil {
		reserved[scope] = make(map[string]struct{})
	}
	if _, duplicate := reserved[scope][normalizedName]; duplicate {
		return fmt.Errorf("foreign-key name %q is duplicated in %s", name, scope)
	}
	reserved[scope][normalizedName] = struct{}{}
	return nil
}

// validateExplicitForeignKeyName refuses a name the target would reject or
// truncate.
//
// The limit and its unit come from [capability.Identifiers] rather than from a
// dialect switch here. The switch this replaced carried the numbers 63, 64 and
// 128 and, more importantly, carried the byte-versus-character rule that
// decides whether a multibyte name fits.
//
// That removes this file's copy of the rule. One other remains:
// internal/convert/fromschema carries its own three-arm switch because it
// truncates a generated name to fit rather than refusing it, and the truncation
// needs a budget in the limit's unit that IdentifierLimit does not expose. Its
// predicate agrees with capability.Identifiers today — 144 verdicts across
// every boundary shape, zero disagreements — so what remains is a drift hazard
// rather than a wrong answer. Do not add a third copy.
func validateExplicitForeignKeyName(dialect, name string) error {
	normalizedDialect := platform.NormalizeDialect(dialect)
	limit := capability.Identifiers(normalizedDialect)
	if !limit.Exceeds(name) {
		return nil
	}
	return fmt.Errorf("foreign-key name %q exceeds the %s identifier limit of %s", name, normalizedDialect, limit)
}

func foreignKeyNameScope(dialect string, table goschema.Table, name string) (scope, normalizedName string) {
	normalizedDialect := platform.NormalizeDialect(dialect)
	switch normalizedDialect {
	case platform.MySQL, platform.MariaDB:
		return "database constraint namespace", strings.ToLower(name)
	case platform.SQLServer, platform.Spanner:
		schema := strings.TrimSpace(table.Schema)
		if schema == "" {
			schema = identifier.ForDialect(dialect).DefaultSchema
		}
		schema = strings.ToLower(schema)
		return fmt.Sprintf("schema %q constraint namespace", schema), strings.ToLower(name)
	default:
		return fmt.Sprintf("table %q constraint namespace", table.QualifiedName()), name
	}
}

type foreignKeyBinding struct {
	owner    string
	target   string
	onDelete string
	onUpdate string
}

func validateSchemaForeignKeyColumns(
	fields []goschema.Field,
	dialect string,
	owner goschema.Table,
	localColumns []string,
	target goschema.Table,
	referencedColumns []string,
	onDelete,
	onUpdate string,
) error {
	if err := validateForeignKeyColumnLists(localColumns, referencedColumns); err != nil {
		return invalidSchemaForeignKeyError(dialect, err.Error())
	}
	if missing := firstMissingTableColumn(fields, owner, localColumns); missing != "" {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf("table %q has no local foreign-key column %q", owner.QualifiedName(), missing),
		)
	}
	if missing := firstMissingTableColumn(fields, target, referencedColumns); missing != "" {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf("referenced table %q has no column %q", target.QualifiedName(), missing),
		)
	}
	if err := validateForeignKeyTableStorage(dialect, owner, target); err != nil {
		return err
	}
	for i, localColumn := range localColumns {
		localField := tableColumn(fields, owner, localColumn)
		referencedField := tableColumn(fields, target, referencedColumns[i])
		if err := validateForeignKeyColumnCompatibility(
			dialect,
			owner,
			localField,
			target,
			referencedField,
			onDelete,
			onUpdate,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateForeignKeyTableStorage(dialect string, owner, target goschema.Table) error {
	normalizedDialect := platform.NormalizeDialect(dialect)
	if normalizedDialect != platform.MySQL && normalizedDialect != platform.MariaDB {
		return nil
	}
	for _, table := range []goschema.Table{owner, target} {
		engine := effectiveTableEngine(table, normalizedDialect)
		if !strings.EqualFold(engine, "InnoDB") {
			return invalidSchemaForeignKeyError(
				dialect,
				fmt.Sprintf(
					"table %q uses storage engine %q; %s foreign keys require InnoDB",
					table.QualifiedName(),
					engine,
					normalizedDialect,
				),
			)
		}
	}
	return nil
}

func effectiveTableEngine(table goschema.Table, dialect string) string {
	configured, _ := configuredTableEngine(table, dialect)
	engine := strings.TrimSpace(configured)
	if engine == "" {
		return "InnoDB"
	}
	return engine
}

func validateForeignKeyColumnCompatibility(
	dialect string,
	owner goschema.Table,
	local goschema.Field,
	target goschema.Table,
	referenced goschema.Field,
	onDelete,
	onUpdate string,
) error {
	normalizedDialect := platform.NormalizeDialect(dialect)
	local = fromschema.EffectiveFieldForPlatform(local, normalizedDialect)
	referenced = fromschema.EffectiveFieldForPlatform(referenced, normalizedDialect)
	if (onDelete == "SET NULL" || onUpdate == "SET NULL") && !local.Nullable {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf(
				"foreign key on %q.%q uses SET NULL but the local column is NOT NULL",
				owner.QualifiedName(),
				local.Name,
			),
		)
	}
	if normalizedDialect == platform.SQLite {
		return nil
	}
	if normalizedDialect == platform.MySQL || normalizedDialect == platform.MariaDB {
		if err := validateMySQLFamilyGeneratedForeignKey(
			dialect,
			owner,
			local,
			target,
			referenced,
			onDelete,
			onUpdate,
		); err != nil {
			return err
		}
	}
	localType := normalizeForeignKeyColumnType(local.Type, normalizedDialect)
	referencedType := normalizeForeignKeyColumnType(referenced.Type, normalizedDialect)
	if localType == "" || referencedType == "" ||
		!foreignKeyColumnTypesCompatible(localType, referencedType, normalizedDialect) {
		return incompatibleForeignKeyColumnTypesError(dialect, owner, local, target, referenced)
	}
	if normalizedDialect == platform.MySQL || normalizedDialect == platform.MariaDB {
		if mysqlForeignKeyTypeUsesCharset(localType) || mysqlForeignKeyTypeUsesCharset(referencedType) {
			if err := validateMySQLForeignKeyTextMetadata(
				dialect,
				owner,
				local,
				target,
				referenced,
			); err != nil {
				return err
			}
		}
		if mysqlForeignKeyTypeBase(localType) == "ENUM" && !slices.Equal(local.Enum, referenced.Enum) {
			return incompatibleForeignKeyColumnTypesError(dialect, owner, local, target, referenced)
		}
	}
	if normalizedDialect == platform.Spanner && isSpannerNonKeyType(localType) {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf(
				"Spanner type %s cannot participate in foreign keys: %q.%q references %q.%q",
				local.Type,
				owner.QualifiedName(),
				local.Name,
				target.QualifiedName(),
				referenced.Name,
			),
		)
	}
	return nil
}

func incompatibleForeignKeyColumnTypesError(
	dialect string,
	owner goschema.Table,
	local goschema.Field,
	target goschema.Table,
	referenced goschema.Field,
) error {
	return invalidSchemaForeignKeyError(
		dialect,
		fmt.Sprintf(
			"foreign-key columns %q.%q (%s) and %q.%q (%s) have incompatible types",
			owner.QualifiedName(),
			local.Name,
			local.Type,
			target.QualifiedName(),
			referenced.Name,
			referenced.Type,
		),
	)
}

func validateMySQLFamilyGeneratedForeignKey(
	dialect string,
	owner goschema.Table,
	local goschema.Field,
	target goschema.Table,
	referenced goschema.Field,
	onDelete,
	onUpdate string,
) error {
	generated := local.GeneratedExpression != "" || referenced.GeneratedExpression != ""
	if !generated {
		return nil
	}
	normalizedDialect := platform.NormalizeDialect(dialect)
	if normalizedDialect == platform.MariaDB ||
		!generatedForeignKeyColumnsAreStored(local, referenced) {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf(
				"generated columns cannot participate in portable %s foreign keys: %q.%q references %q.%q",
				normalizedDialect,
				owner.QualifiedName(),
				local.Name,
				target.QualifiedName(),
				referenced.Name,
			),
		)
	}
	if slices.Contains([]string{"CASCADE", "SET NULL", "SET DEFAULT"}, onUpdate) ||
		slices.Contains([]string{"SET NULL", "SET DEFAULT"}, onDelete) {
		return invalidSchemaForeignKeyError(
			dialect,
			fmt.Sprintf(
				"stored generated foreign-key columns do not support ON DELETE %s ON UPDATE %s",
				referentialActionOrDefault(onDelete),
				referentialActionOrDefault(onUpdate),
			),
		)
	}
	return nil
}

func generatedForeignKeyColumnsAreStored(fields ...goschema.Field) bool {
	for _, field := range fields {
		if field.GeneratedExpression == "" {
			continue
		}
		kind := strings.ToUpper(strings.TrimSpace(field.GeneratedKind))
		if kind != "STORED" {
			return false
		}
	}
	return true
}

func referentialActionOrDefault(action string) string {
	if action == "" {
		return "NO ACTION"
	}
	return action
}

func validateMySQLForeignKeyTextMetadata(
	dialect string,
	owner goschema.Table,
	local goschema.Field,
	target goschema.Table,
	referenced goschema.Field,
) error {
	localCharset := effectiveColumnMetadata(local.Charset, effectiveTableMetadata(owner, dialect, "charset"))
	referencedCharset := effectiveColumnMetadata(
		referenced.Charset,
		effectiveTableMetadata(target, dialect, "charset"),
	)
	if localCharset != referencedCharset {
		return incompatibleForeignKeyTextMetadataError(
			dialect,
			"character sets",
			owner,
			local,
			localCharset,
			target,
			referenced,
			referencedCharset,
		)
	}
	localCollation := effectiveColumnMetadata(local.Collate, effectiveTableMetadata(owner, dialect, "collate"))
	referencedCollation := effectiveColumnMetadata(
		referenced.Collate,
		effectiveTableMetadata(target, dialect, "collate"),
	)
	if localCollation != referencedCollation {
		return incompatibleForeignKeyTextMetadataError(
			dialect,
			"collations",
			owner,
			local,
			localCollation,
			target,
			referenced,
			referencedCollation,
		)
	}
	return nil
}

func effectiveTableMetadata(table goschema.Table, dialect, key string) string {
	value := table.Charset
	if key == "collate" {
		value = table.Collate
	}
	if overrides := table.Overrides[platform.NormalizeDialect(dialect)]; overrides != nil {
		if override, found := overrides[key]; found {
			value = override
		}
	}
	return value
}

func effectiveColumnMetadata(columnValue, tableValue string) string {
	value := strings.TrimSpace(columnValue)
	if value == "" {
		value = strings.TrimSpace(tableValue)
	}
	return strings.ToLower(value)
}

func incompatibleForeignKeyTextMetadataError(
	dialect,
	metadata string,
	owner goschema.Table,
	local goschema.Field,
	localValue string,
	target goschema.Table,
	referenced goschema.Field,
	referencedValue string,
) error {
	return invalidSchemaForeignKeyError(
		dialect,
		fmt.Sprintf(
			"foreign-key columns %q.%q and %q.%q have incompatible %s %q and %q",
			owner.QualifiedName(),
			local.Name,
			target.QualifiedName(),
			referenced.Name,
			metadata,
			localValue,
			referencedValue,
		),
	)
}

func normalizeForeignKeyColumnType(fieldType, dialect string) string {
	normalized := strings.ToUpper(strings.Join(strings.Fields(fieldType), " "))
	normalized = strings.TrimSpace(strings.ReplaceAll(normalized, " AUTO_INCREMENT", ""))
	unsigned := strings.Contains(normalized, " UNSIGNED") || strings.Contains(normalized, " ZEROFILL")
	normalized = strings.TrimSpace(strings.ReplaceAll(normalized, " UNSIGNED", ""))
	normalized = strings.TrimSpace(strings.ReplaceAll(normalized, " ZEROFILL", ""))
	normalized = normalizeForeignKeyColumnTypeAlias(normalized, dialect)
	if unsigned {
		normalized += " UNSIGNED"
	}
	return normalized
}

func normalizeForeignKeyColumnTypeAlias(fieldType, dialect string) string {
	if dialect == platform.MySQL || dialect == platform.MariaDB {
		fieldType = stripMySQLIntegerDisplayWidth(fieldType)
	}
	switch {
	case strings.HasPrefix(fieldType, "CHARACTER VARYING("):
		return "VARCHAR" + strings.TrimPrefix(fieldType, "CHARACTER VARYING")
	case strings.HasPrefix(fieldType, "CHARACTER("):
		return "CHAR" + strings.TrimPrefix(fieldType, "CHARACTER")
	case strings.HasPrefix(fieldType, "DECIMAL("):
		return "NUMERIC" + strings.TrimPrefix(fieldType, "DECIMAL")
	}
	switch fieldType {
	case "SMALLSERIAL", "SERIAL2", "INT2":
		return "SMALLINT"
	case "SERIAL", "SERIAL4", "INT", "INT4":
		return "INTEGER"
	case "BIGSERIAL", "SERIAL8", "INT8":
		return "BIGINT"
	case "CHARACTER VARYING":
		return "VARCHAR"
	case "CHARACTER":
		return "CHAR"
	case "DECIMAL":
		return "NUMERIC"
	default:
		return fieldType
	}
}

func foreignKeyColumnTypesCompatible(localType, referencedType, dialect string) bool {
	if localType == referencedType {
		return !mysqlFamilyForeignKeyTypeUnsupported(localType, dialect)
	}
	if dialect != platform.MySQL && dialect != platform.MariaDB {
		return false
	}
	if mysqlFamilyForeignKeyTypeUnsupported(localType, dialect) ||
		mysqlFamilyForeignKeyTypeUnsupported(referencedType, dialect) {
		return false
	}
	return mysqlStringTypeFamily(localType) != "" &&
		mysqlStringTypeFamily(localType) == mysqlStringTypeFamily(referencedType)
}

func mysqlStringTypeFamily(fieldType string) string {
	base := mysqlForeignKeyTypeBase(fieldType)
	if slices.Contains([]string{"CHAR", "VARCHAR", "BINARY", "VARBINARY"}, base) {
		return base
	}
	return ""
}

func mysqlForeignKeyTypeUsesCharset(fieldType string) bool {
	return slices.Contains([]string{
		"CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET",
	}, mysqlForeignKeyTypeBase(fieldType))
}

func mysqlForeignKeyTypeBase(fieldType string) string {
	base, _, _ := strings.Cut(fieldType, "(")
	return strings.TrimSuffix(base, " UNSIGNED")
}

func mysqlFamilyForeignKeyTypeUnsupported(fieldType, dialect string) bool {
	if dialect != platform.MySQL && dialect != platform.MariaDB {
		return false
	}
	base := mysqlForeignKeyTypeBase(fieldType)
	return slices.Contains([]string{
		"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB", "JSON",
	}, base)
}

func stripMySQLIntegerDisplayWidth(fieldType string) string {
	for _, integerType := range []string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"} {
		prefix := integerType + "("
		if strings.HasPrefix(fieldType, prefix) && strings.HasSuffix(fieldType, ")") {
			return integerType
		}
	}
	return fieldType
}

func isSpannerNonKeyType(fieldType string) bool {
	return slices.Contains([]string{"FLOAT32", "FLOAT4", "REAL", "NUMERIC", "JSON", "JSONB", "STRUCT"}, fieldType) ||
		strings.HasPrefix(fieldType, "ARRAY<") || strings.HasSuffix(fieldType, "[]")
}

func tableColumn(fields []goschema.Field, table goschema.Table, column string) goschema.Field {
	for _, field := range fields {
		if field.StructName == table.StructName && field.Name == column {
			return field
		}
	}
	return goschema.Field{}
}

func firstMissingTableColumn(fields []goschema.Field, table goschema.Table, columns []string) string {
	available := make(map[string]struct{})
	for _, field := range fields {
		if field.StructName == table.StructName {
			available[field.Name] = struct{}{}
		}
	}
	for _, column := range columns {
		if _, found := available[column]; !found {
			return column
		}
	}
	return ""
}

func validateReferencedKeyPolicy(
	database goschema.Database,
	dialect string,
	caps capability.Capabilities,
	target goschema.Table,
	columns []string,
) error {
	switch {
	case caps.Has(capability.ForeignKeysRequireUniqueReference):
		if !tableHasUniqueKey(database, dialect, target, columns) {
			return uniqueReferenceError(dialect, target, columns)
		}
	case caps.Has(capability.ForeignKeysRequireIndexedReference):
		if !tableHasIndexedKey(database, target, columns) {
			return indexedReferenceError(dialect, target, columns)
		}
	case caps.Has(capability.ForeignKeysCreateBackingIndex):
		return nil
	}
	return nil
}

func tableByStructName(tables []goschema.Table, structName string) *goschema.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func constraintOwnerTable(tables []goschema.Table, constraint goschema.Constraint) *goschema.Table {
	if constraint.Table != "" {
		for i := range tables {
			if tables[i].QualifiedName() == constraint.Table || tables[i].Name == constraint.Table {
				return &tables[i]
			}
		}
	}
	return tableByStructName(tables, constraint.StructName)
}

func referencedTable(tables []goschema.Table, owner goschema.Table, reference string) *goschema.Table {
	resolved := tablelookup.ResolveReference(tables, owner, reference)
	for i := range tables {
		if tables[i].QualifiedName() == resolved {
			return &tables[i]
		}
	}
	return nil
}

func tableHasUniqueKey(database goschema.Database, dialect string, table goschema.Table, columns []string) bool {
	return tablePrimaryKeyEquals(table, columns) ||
		tableFieldsHaveUniqueKey(allDatabaseFields(database), table, columns) ||
		tableConstraintsHaveUniqueKey(database, table, columns) ||
		tableIndexesHaveUniqueKey(database, dialect, table, columns)
}

func tablePrimaryKeyEquals(table goschema.Table, columns []string) bool {
	return slices.Equal(table.PrimaryKey, columns) ||
		(primaryKeyPartsAreFullColumns(table.PrimaryKeyParts) &&
			slices.Equal(primaryKeyPartNames(table.PrimaryKeyParts), columns))
}

func tableFieldsHaveUniqueKey(fields []goschema.Field, table goschema.Table, columns []string) bool {
	var primaryFields []string
	for _, field := range fields {
		if field.StructName != table.StructName {
			continue
		}
		if field.Primary {
			primaryFields = append(primaryFields, field.Name)
		}
		if len(columns) == 1 && field.Name == columns[0] && field.Unique {
			return true
		}
	}
	return slices.Equal(primaryFields, columns)
}

func tableConstraintsHaveUniqueKey(
	database goschema.Database,
	table goschema.Table,
	columns []string,
) bool {
	for _, constraint := range database.Constraints {
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner != nil && owner.QualifiedName() == table.QualifiedName() &&
			(strings.EqualFold(constraint.Type, "PRIMARY KEY") || strings.EqualFold(constraint.Type, "UNIQUE")) &&
			slices.Equal(constraint.Columns, columns) {
			return true
		}
	}
	return false
}

func tableIndexesHaveUniqueKey(
	database goschema.Database,
	dialect string,
	table goschema.Table,
	columns []string,
) bool {
	// SQLite's IR does not preserve per-index-part collation. Accepting a
	// standalone unique index could therefore produce a deferred
	// "foreign key mismatch" at DML time. Inline keys remain verifiable.
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		return false
	}
	indexOwners := goschema.ResolveIndexTableNames(database.Indexes, database.Tables)
	for i, index := range database.Indexes {
		indexColumns, valid := fullIndexColumnNames(index)
		if index.Unique && valid && strings.TrimSpace(index.Condition) == "" &&
			indexOwners[i] == table.QualifiedName() && slices.Equal(indexColumns, columns) {
			return true
		}
	}
	return false
}

func tableHasIndexedKey(database goschema.Database, table goschema.Table, columns []string) bool {
	if isColumnPrefix(table.PrimaryKey, columns) ||
		(primaryKeyPartsAreFullColumns(table.PrimaryKeyParts) &&
			isColumnPrefix(primaryKeyPartNames(table.PrimaryKeyParts), columns)) {
		return true
	}

	var primaryFields []string
	for _, field := range allDatabaseFields(database) {
		if field.StructName != table.StructName {
			continue
		}
		if field.Primary {
			primaryFields = append(primaryFields, field.Name)
		}
		if len(columns) == 1 && field.Name == columns[0] && field.Unique {
			return true
		}
	}
	if isColumnPrefix(primaryFields, columns) {
		return true
	}

	for _, constraint := range database.Constraints {
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner != nil && owner.QualifiedName() == table.QualifiedName() &&
			(strings.EqualFold(constraint.Type, "PRIMARY KEY") || strings.EqualFold(constraint.Type, "UNIQUE")) &&
			isColumnPrefix(constraint.Columns, columns) {
			return true
		}
	}
	indexOwners := goschema.ResolveIndexTableNames(database.Indexes, database.Tables)
	for i, index := range database.Indexes {
		if indexOwners[i] == table.QualifiedName() && indexHasFullColumnPrefix(index, columns) {
			return true
		}
	}
	return false
}

func allDatabaseFields(database goschema.Database) []goschema.Field {
	return fromschema.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
}

func isColumnPrefix(keyColumns, referencedColumns []string) bool {
	return len(referencedColumns) > 0 && len(keyColumns) >= len(referencedColumns) &&
		slices.Equal(keyColumns[:len(referencedColumns)], referencedColumns)
}

func primaryKeyPartsAreFullColumns(parts []goschema.PrimaryKeyPart) bool {
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if strings.TrimSpace(part.Name) == "" || strings.TrimSpace(part.Prefix) != "" {
			return false
		}
	}
	return true
}

func primaryKeyPartNames(parts []goschema.PrimaryKeyPart) []string {
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		names = append(names, part.Name)
	}
	return names
}

func fullIndexColumnNames(index goschema.Index) ([]string, bool) {
	if len(index.Parts) == 0 {
		return index.Fields, len(index.Fields) > 0
	}
	names := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		if strings.TrimSpace(part.Name) == "" || strings.TrimSpace(part.Expr) != "" ||
			strings.TrimSpace(part.Prefix) != "" || strings.TrimSpace(part.Operator) != "" {
			return nil, false
		}
		names = append(names, part.Name)
	}
	return names, true
}

func indexHasFullColumnPrefix(index goschema.Index, columns []string) bool {
	if strings.TrimSpace(index.Condition) != "" || len(columns) == 0 {
		return false
	}
	indexType := strings.ToUpper(strings.TrimSpace(index.Type))
	if (indexType != "" && indexType != "BTREE") || strings.TrimSpace(index.Parser) != "" {
		return false
	}
	if len(index.Parts) == 0 {
		return isColumnPrefix(index.Fields, columns)
	}
	if len(index.Parts) < len(columns) {
		return false
	}
	for i, column := range columns {
		part := index.Parts[i]
		if part.Name != column || strings.TrimSpace(part.Expr) != "" ||
			strings.TrimSpace(part.Prefix) != "" || strings.TrimSpace(part.Operator) != "" {
			return false
		}
	}
	return true
}

func uniqueReferenceError(dialect string, table goschema.Table, columns []string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign key referenced key uniqueness",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s requires referenced columns %s on table %q to be declared unique",
			platform.NormalizeDialect(dialect),
			strings.Join(columns, ", "),
			table.QualifiedName(),
		),
	}
}

func indexedReferenceError(dialect string, table goschema.Table, columns []string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign key referenced key index",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s requires referenced columns %s on table %q to be the full leftmost prefix of an index",
			platform.NormalizeDialect(dialect),
			strings.Join(columns, ", "),
			table.QualifiedName(),
		),
	}
}

func invalidSchemaForeignKeyError(dialect, message string) error {
	return &ptaherr.RenderError{
		Dialect: dialect,
		Err:     ptaherr.ErrInvalidSchemaDiff,
		Message: "invalid foreign key: " + message,
	}
}

func validateSQLServerCascadeGraph(dialect string, bindings []foreignKeyBinding) error {
	deleteGraph := make(map[string][]string)
	updateGraph := make(map[string][]string)
	for _, binding := range bindings {
		if isCascadingReferentialAction(binding.onDelete) {
			deleteGraph[binding.target] = append(deleteGraph[binding.target], binding.owner)
		}
		if isCascadingReferentialAction(binding.onUpdate) {
			updateGraph[binding.target] = append(updateGraph[binding.target], binding.owner)
		}
	}
	if err := validateSQLServerCascadeActionGraph(dialect, "ON DELETE", deleteGraph); err != nil {
		return err
	}
	return validateSQLServerCascadeActionGraph(dialect, "ON UPDATE", updateGraph)
}

func isCascadingReferentialAction(action string) bool {
	return action != "" && action != "NO ACTION"
}

func validateSQLServerCascadeActionGraph(
	dialect,
	clause string,
	graph map[string][]string,
) error {
	for start := range graph {
		seen := map[string]struct{}{start: {}}
		queue := []string{start}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, next := range graph[current] {
				if _, duplicatePath := seen[next]; duplicatePath {
					return &ptaherr.CapabilityError{
						Dialect: dialect,
						Feature: "SQL Server cascading referential actions",
						Err:     ptaherr.ErrUnsupportedFeature,
						Message: fmt.Sprintf(
							"sqlserver does not allow %s cycles or multiple cascade paths reaching table %q",
							clause,
							next,
						),
					}
				}
				seen[next] = struct{}{}
				queue = append(queue, next)
			}
		}
	}
	return nil
}

// rowTTLTables projects the schema's tables into the pairs internal/crdbttl
// validates, so that package needs no knowledge of goschema.
func rowTTLTables(database *goschema.Database) []crdbttl.TableTTL {
	tables := make([]crdbttl.TableTTL, 0, len(database.Tables))
	for _, table := range database.Tables {
		tables = append(tables, crdbttl.TableTTL{Name: table.Name, RowTTL: table.RowTTL})
	}
	return tables
}
