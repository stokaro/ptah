package mysql

import (
	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/internal/bufwriter"
	"github.com/stokaro/ptah/core/renderer/dialects/mysqllike"
	"github.com/stokaro/ptah/core/renderer/types"
)

var (
	_ types.RenderVisitor = (*Renderer)(nil)
)

// Renderer provides MySQL-specific SQL rendering
type Renderer struct {
	r *mysqllike.Renderer
	w bufwriter.Writer
}

// New creates a new MySQL renderer
func New() *Renderer {
	var w bufwriter.Writer
	return &Renderer{
		r: mysqllike.New("mysql", &w),
		w: w,
	}
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	return r.r.VisitDropIndex(node)
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	return r.r.VisitCreateType(node)
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	return r.r.VisitAlterType(node)
}

func (r *Renderer) Dialect() string {
	return r.r.Dialect()
}

func (r *Renderer) Reset() {
	r.r.Reset()
}

func (r *Renderer) Output() string {
	return r.r.Output()
}

// Render renders an AST node to SQL and returns the result
func (r *Renderer) Render(node ast.Node) (string, error) {
	return r.r.Render(node)
}

// GetDialect returns the database dialect (alias for Dialect for compatibility)
func (r *Renderer) GetDialect() string {
	return r.r.GetDialect()
}

// GetOutput returns the current generated SQL output (alias for Output for compatibility)
func (r *Renderer) GetOutput() string {
	return r.r.GetOutput()
}

// VisitCreateTable renders MySQL-specific CREATE TABLE statements
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	return r.r.VisitCreateTable(node)
}

// VisitAlterTable renders MySQL-specific ALTER TABLE statements
func (r *Renderer) VisitAlterTable(node *ast.AlterTableNode) error {
	return r.r.VisitAlterTable(node)
}

// VisitColumn is called when visiting individual columns (used by other visitors)
func (r *Renderer) VisitColumn(node *ast.ColumnNode) error {
	return r.r.VisitColumn(node)
}

// VisitConstraint is called when visiting individual constraints (used by other visitors)
func (r *Renderer) VisitConstraint(node *ast.ConstraintNode) error {
	return r.r.VisitConstraint(node)
}

// VisitIndex renders a CREATE INDEX statement for MySQL
func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	return r.r.VisitIndex(node)
}

// VisitEnum renders enum handling for MySQL (inline ENUM types like MySQL)
func (r *Renderer) VisitEnum(node *ast.EnumNode) error {
	return r.r.VisitEnum(node)
}

// VisitComment renders a comment
func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	return r.r.VisitComment(node)
}

// VisitDropTable renders MySQL-specific DROP TABLE statements
func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	return r.r.VisitDropTable(node)
}

// VisitDropType renders DROP TYPE statements for MySQL
func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	return r.r.VisitDropType(node)
}

// VisitExtension renders CREATE EXTENSION statements for MySQL (no-op)
func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	// MySQL doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- Extension %s not supported in MySQL: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- Extension %s not supported in MySQL", node.Name)
	}
	return nil
}

// VisitDropExtension renders DROP EXTENSION statements for MySQL (no-op)
func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	// MySQL doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MySQL: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MySQL", node.Name)
	}
	return nil
}

// VisitCreateFunction renders CREATE FUNCTION statements for MySQL (no-op)
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	// MySQL doesn't support PostgreSQL-style functions
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in MySQL: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in MySQL", node.Name)
	}
	return nil
}

// VisitCreatePolicy renders CREATE POLICY statements for MySQL (no-op)
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	// MySQL doesn't support Row-Level Security policies
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MySQL: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MySQL", node.Name)
	}
	return nil
}

// VisitAlterTableEnableRLS renders ALTER TABLE ENABLE RLS statements for MySQL (no-op)
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	// MySQL doesn't support Row-Level Security
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MySQL: %s", node.Table, node.Comment)
	} else {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MySQL", node.Table)
	}
	return nil
}

// VisitDropFunction delegates to the mysqllike renderer
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	return r.r.VisitDropFunction(node)
}

// VisitDropPolicy delegates to the mysqllike renderer
func (r *Renderer) VisitDropPolicy(node *ast.DropPolicyNode) error {
	return r.r.VisitDropPolicy(node)
}

// VisitAlterTableDisableRLS delegates to the mysqllike renderer
func (r *Renderer) VisitAlterTableDisableRLS(node *ast.AlterTableDisableRLSNode) error {
	return r.r.VisitAlterTableDisableRLS(node)
}

// VisitCreateRole delegates to the mysqllike renderer
func (r *Renderer) VisitCreateRole(node *ast.CreateRoleNode) error {
	return r.r.VisitCreateRole(node)
}

// VisitDropRole delegates to the mysqllike renderer
func (r *Renderer) VisitDropRole(node *ast.DropRoleNode) error {
	return r.r.VisitDropRole(node)
}

// VisitAlterRole delegates to the mysqllike renderer
func (r *Renderer) VisitAlterRole(node *ast.AlterRoleNode) error {
	return r.r.VisitAlterRole(node)
}
