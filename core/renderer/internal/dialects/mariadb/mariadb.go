// Package mariadb provides the MariaDB SQL renderer: a thin wrapper over the
// shared mysqllike renderer configured with MariaDB capabilities.
package mariadb

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/internal/bufwriter"
	"ptah.run/core/renderer/internal/dialects/internal/nodedispatch"
	"ptah.run/core/renderer/internal/dialects/mysqllike"
	"ptah.run/internal/renderdiag"
)

// Renderer provides MariaDB-specific SQL rendering
type Renderer struct {
	r *mysqllike.Renderer

	// w is the SAME buffer the embedded mysqllike renderer writes into, held by
	// pointer on purpose. Output and Reset delegate to r, so a handler defined
	// on this wrapper is only observable if it appends to r's buffer. Storing a
	// bufwriter.Writer by value here gives the wrapper a copy: its own handlers
	// then write into a buffer nothing reads, and the render exits 0 with the
	// refusal lines missing.
	w *bufwriter.Writer
}

// New creates a new MariaDB renderer
func New() *Renderer {
	return NewWithCapabilities(capability.MariaDB1011())
}

// NewWithCapabilities creates a MariaDB renderer for a concrete server
// capability set. Use New for offline/default rendering.
func NewWithCapabilities(caps capability.Capabilities) *Renderer {
	w := &bufwriter.Writer{}
	return &Renderer{
		r: mysqllike.NewWithCapabilities("mariadb", w, caps),
		w: w,
	}
}

// ReportOmissionsTo forwards the sink to the embedded renderer, which is where
// every skip this wrapper can produce is written.
func (r *Renderer) ReportOmissionsTo(sink *renderdiag.Sink) {
	r.r.ReportOmissionsTo(sink)
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

// Render renders an AST node to SQL and returns the result.
//
// The node is accepted onto this renderer, so it reaches VisitNode below.
// Handing it to the shared renderer instead would route around the four
// refusals this wrapper writes itself, and the refusal a caller reads would
// then depend on which entry point produced it.
func (r *Renderer) Render(node ast.Node) (string, error) {
	r.Reset()
	if err := node.Accept(r); err != nil {
		return "", err
	}
	return r.Output(), nil
}

// GetDialect returns the database dialect (alias for Dialect for compatibility)
func (r *Renderer) GetDialect() string {
	return r.r.GetDialect()
}

// GetOutput returns the current generated SQL output (alias for Output for compatibility)
func (r *Renderer) GetOutput() string {
	return r.r.GetOutput()
}

// VisitNode renders node.
//
// The switch names the node kinds this wrapper writes itself. Every other kind
// reaches the shared MySQL-family renderer, whose own switch decides about the
// rest; [ptah.run/internal/astrouteguard] follows that forward rather than
// counting it as an answer.
//
// CreateFunctionNode is deliberately not named here. The engine has CREATE
// FUNCTION, so a `-- CREATE FUNCTION <name> not supported in MariaDB` line
// would be a claim about the server that the server contradicts
// (stokaro/ptah#929).
//
// A nil node and a non-nil interface holding a nil pointer both go to the
// shared renderer, which refuses a node that is not there. Without the check a
// typed nil of one of the four kinds below would reach a handler and be
// dereferenced, so the two spellings of "no node" would get one answer and one
// panic.
func (r *Renderer) VisitNode(node ast.Node) error {
	if nodedispatch.IsAbsent(node) {
		return r.r.VisitNode(node)
	}
	switch n := node.(type) {
	case *ast.ExtensionNode:
		return r.renderExtension(n)
	case *ast.DropExtensionNode:
		return r.renderDropExtension(n)
	case *ast.CreatePolicyNode:
		return r.renderCreatePolicy(n)
	case *ast.AlterTableEnableRLSNode:
		return r.renderAlterTableEnableRLS(n)
	default:
		return r.r.VisitNode(node)
	}
}

// renderExtension renders CREATE EXTENSION statements for MariaDB (no-op)
func (r *Renderer) renderExtension(node *ast.ExtensionNode) error {
	// MariaDB doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- Extension %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- Extension %s not supported in MariaDB", node.Name)
	}
	return nil
}

// renderDropExtension renders DROP EXTENSION statements for MariaDB (no-op)
func (r *Renderer) renderDropExtension(node *ast.DropExtensionNode) error {
	// MariaDB doesn't support extensions like PostgreSQL
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- DROP EXTENSION %s not supported in MariaDB", node.Name)
	}
	return nil
}

// renderCreatePolicy renders CREATE POLICY statements for MariaDB (no-op).
//
// DropPolicyNode is not answered here. It reaches the shared renderer, which
// writes a differently worded line and records a renderdiag omission, so the UP
// and DOWN halves of one policy are reported in two shapes.
func (r *Renderer) renderCreatePolicy(node *ast.CreatePolicyNode) error {
	// MariaDB doesn't support Row-Level Security policies
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MariaDB: %s", node.Name, node.Comment)
	} else {
		r.w.WriteLinef("-- CREATE POLICY %s not supported in MariaDB", node.Name)
	}
	return nil
}

// renderAlterTableEnableRLS renders ALTER TABLE ENABLE RLS statements for
// MariaDB (no-op).
//
// AlterTableDisableRLSNode is not answered here, for the reason
// renderCreatePolicy gives about the policy pair.
func (r *Renderer) renderAlterTableEnableRLS(node *ast.AlterTableEnableRLSNode) error {
	// MariaDB doesn't support Row-Level Security
	// Add a comment to indicate this feature is not supported
	if node.Comment != "" {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MariaDB: %s", node.Table, node.Comment)
	} else {
		r.w.WriteLinef("-- ALTER TABLE %s ENABLE ROW LEVEL SECURITY not supported in MariaDB", node.Table)
	}
	return nil
}
