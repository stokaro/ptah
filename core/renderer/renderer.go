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

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/clickhouse"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/mariadb"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/mssql"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/mysql"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/postgres"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/sqlite"
	"github.com/stokaro/ptah/internal/convert/fromschema"
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
	normalizedDialect := platform.NormalizeDialect(dialect)

	switch normalizedDialect {
	case platform.Postgres:
		return postgres.NewWithCapabilities(caps, normalizedDialect), nil
	case platform.MySQL:
		return mysql.NewWithCapabilities(caps), nil
	case platform.MariaDB:
		return mariadb.NewWithCapabilities(caps), nil
	case platform.ClickHouse:
		return clickhouse.New(), nil
	case platform.SQLite:
		return sqlite.New(), nil
	case platform.SQLServer:
		return mssql.New(), nil
	case platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return postgres.NewWithCapabilities(caps, normalizedDialect), nil
	default:
		return nil, &ptaherr.RenderError{
			Dialect: dialect,
			Err:     ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("unsupported database dialect: %s", dialect),
		}
	}
}

// RenderSQL is a convenience function that creates a renderer and renders an AST node in one call.
//
// This function is useful for one-off SQL generation where you don't need to reuse the renderer.
// For multiple operations, it's more efficient to create a renderer once and reuse it.
func RenderSQL(dialect string, nodes ...ast.Node) (string, error) {
	r, err := NewRenderer(dialect)
	if err != nil {
		return "", err
	}
	return VisitorRenderSQL(r, nodes...)
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
	for _, node := range nodes {
		if err := node.Accept(r); err != nil {
			var renderErr *ptaherr.RenderError
			if errors.As(err, &renderErr) {
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

func GetOrderedCreateStatements(r *goschema.Database, dialect string) ([]string, error) {
	return GetOrderedCreateStatementsWithCapabilities(r, dialect, capability.ForDialect(dialect))
}

// GetOrderedCreateStatementsWithCapabilities renders ordered create statements
// for a concrete server capability set.
func GetOrderedCreateStatementsWithCapabilities(
	r *goschema.Database,
	dialect string,
	caps capability.Capabilities,
) ([]string, error) {
	var statements []string

	astNodes := fromschema.FromDatabase(*r, dialect)
	for _, node := range astNodes.Statements {
		sql, err := RenderSQLWithCapabilities(dialect, caps, node)
		if err != nil {
			return nil, err
		}
		statements = append(statements, sql)
	}

	return statements, nil
}
