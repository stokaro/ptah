package mariadb

import (
	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"github.com/stokaro/ptah/core/renderer/internal/dialects/mysqllike"
)

// Renderer provides MariaDB-specific SQL rendering
type Renderer struct {
	r *mysqllike.Renderer
	w bufwriter.Writer
}

// New creates a new MariaDB renderer
func New() *Renderer {
	return NewWithCapabilities(capability.MariaDB1011())
}

// NewWithCapabilities creates a MariaDB renderer for a concrete server
// capability set. Use New for offline/default rendering.
func NewWithCapabilities(caps capability.Capabilities) *Renderer {
	var w bufwriter.Writer
	return &Renderer{
		r: mysqllike.NewWithCapabilities("mariadb", &w, caps),
		w: w,
	}
}

func (r *Renderer) VisitDropIndex(node *ast.DropIndexNode) error {
	return r.r.VisitDropIndex(node)
}

func (r *Renderer) VisitCreateType(node *ast.CreateTypeNode) error {
	return r.r.VisitCreateType(node)
}

// VisitCreateSchema delegates to the mysqllike renderer
func (r *Renderer) VisitCreateSchema(node *ast.CreateSchemaNode) error {
	return r.r.VisitCreateSchema(node)
}

// VisitCreateDatabase delegates to the mysqllike renderer
func (r *Renderer) VisitCreateDatabase(node *ast.CreateDatabaseNode) error {
	return r.r.VisitCreateDatabase(node)
}

func (r *Renderer) VisitAlterType(node *ast.AlterTypeNode) error {
	return r.r.VisitAlterType(node)
}

func (r *Renderer) VisitUpsert(node *ast.UpsertNode) error {
	return r.r.VisitUpsert(node)
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

// VisitCreateTable renders MariaDB-specific CREATE TABLE statements
func (r *Renderer) VisitCreateTable(node *ast.CreateTableNode) error {
	return r.r.VisitCreateTable(node)
}

// VisitAlterTable renders MariaDB-specific ALTER TABLE statements
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

// VisitIndex renders a CREATE INDEX statement for MariaDB
func (r *Renderer) VisitIndex(node *ast.IndexNode) error {
	return r.r.VisitIndex(node)
}

// VisitEnum renders enum handling for MariaDB (inline ENUM types like MySQL)
func (r *Renderer) VisitEnum(node *ast.EnumNode) error {
	return r.r.VisitEnum(node)
}

// VisitComment renders a comment
func (r *Renderer) VisitComment(node *ast.CommentNode) error {
	return r.r.VisitComment(node)
}

// VisitDropTable renders MariaDB-specific DROP TABLE statements
func (r *Renderer) VisitDropTable(node *ast.DropTableNode) error {
	return r.r.VisitDropTable(node)
}

// VisitDropType renders DROP TYPE statements for MariaDB
func (r *Renderer) VisitDropType(node *ast.DropTypeNode) error {
	return r.r.VisitDropType(node)
}

// VisitExtension renders CREATE EXTENSION statements for MariaDB (no-op)
func (r *Renderer) VisitExtension(node *ast.ExtensionNode) error {
	// MariaDB doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- Extension %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- Extension %s not supported in MariaDB", node.Name)
	}
	return nil
}

// VisitDropExtension renders DROP EXTENSION statements for MariaDB (no-op)
func (r *Renderer) VisitDropExtension(node *ast.DropExtensionNode) error {
	// MariaDB doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MariaDB", node.Name)
	}
	return nil
}

// VisitCreateFunction renders CREATE FUNCTION statements for MariaDB (no-op)
func (r *Renderer) VisitCreateFunction(node *ast.CreateFunctionNode) error {
	// MariaDB doesn't support PostgreSQL-style functions
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE FUNCTION %s not supported in MariaDB", node.Name)
	}
	return nil
}

// VisitCreatePolicy renders CREATE POLICY statements for MariaDB (no-op)
func (r *Renderer) VisitCreatePolicy(node *ast.CreatePolicyNode) error {
	// MariaDB doesn't support Row-Level Security policies
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MariaDB", node.Name)
	}
	return nil
}

// VisitAlterTableEnableRLS renders ALTER TABLE ENABLE RLS statements for MariaDB (no-op)
func (r *Renderer) VisitAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	// MariaDB doesn't support Row-Level Security
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MariaDB: %s", node.Table, node.Comment)
	} else {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MariaDB", node.Table)
	}
	return nil
}

// VisitDropFunction delegates to the mysqllike renderer
func (r *Renderer) VisitDropFunction(node *ast.DropFunctionNode) error {
	return r.r.VisitDropFunction(node)
}

func (r *Renderer) VisitCreateSequence(node *ast.CreateSequenceNode) error {
	return r.r.VisitCreateSequence(node)
}

func (r *Renderer) VisitAlterSequence(node *ast.AlterSequenceNode) error {
	return r.r.VisitAlterSequence(node)
}

func (r *Renderer) VisitDropSequence(node *ast.DropSequenceNode) error {
	return r.r.VisitDropSequence(node)
}

func (r *Renderer) VisitCreateView(node *ast.CreateViewNode) error {
	return r.r.VisitCreateView(node)
}

func (r *Renderer) VisitDropView(node *ast.DropViewNode) error {
	return r.r.VisitDropView(node)
}

func (r *Renderer) VisitCreateMaterializedView(node *ast.CreateMaterializedViewNode) error {
	return r.r.VisitCreateMaterializedView(node)
}

func (r *Renderer) VisitDropMaterializedView(node *ast.DropMaterializedViewNode) error {
	return r.r.VisitDropMaterializedView(node)
}

func (r *Renderer) VisitRefreshMaterializedView(node *ast.RefreshMaterializedViewNode) error {
	return r.r.VisitRefreshMaterializedView(node)
}

func (r *Renderer) VisitCreateTrigger(node *ast.CreateTriggerNode) error {
	return r.r.VisitCreateTrigger(node)
}

func (r *Renderer) VisitDropTrigger(node *ast.DropTriggerNode) error {
	return r.r.VisitDropTrigger(node)
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

// VisitGrantPrivilege delegates to the mysqllike renderer
func (r *Renderer) VisitGrantPrivilege(node *ast.GrantPrivilegeNode) error {
	return r.r.VisitGrantPrivilege(node)
}

// VisitRevokePrivilege delegates to the mysqllike renderer
func (r *Renderer) VisitRevokePrivilege(node *ast.RevokePrivilegeNode) error {
	return r.r.VisitRevokePrivilege(node)
}

// VisitRawSQL delegates to the mysqllike renderer
func (r *Renderer) VisitRawSQL(node *ast.RawSQLNode) error {
	return r.r.VisitRawSQL(node)
}
