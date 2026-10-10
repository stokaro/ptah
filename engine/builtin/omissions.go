package builtin

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/renderdiag"
)

// GetOrderedCreateStatementsReportingOmissions renders ordered create
// statements and reports every declaration the target could not carry.
//
// The statements are exactly what GetOrderedCreateStatementsWithCapabilities
// produces for the same arguments, from the same pipeline: this is that
// function with a diagnostic sink attached, not a second rendering path. A
// caller that only needs the SQL should keep using that function.
//
// The omissions are ordered deterministically and do not depend on the walk
// order of the schema. A refusal is an error rather than an omission, so a
// non-nil error means no statement list and no omission list: the target
// refused the schema outright and nothing was rendered to have a loss.
//
// Reporting is not exhaustive over every property every dialect drops. It
// covers the declarations a renderer names as skipped, the table options a
// target cannot carry, the comments a target does not store, an index's partial
// condition and operator class, a column's identity clauses, and the length or
// precision a YDB column type does not keep; stokaro/ptah#2983 records what
// remains.
func GetOrderedCreateStatementsReportingOmissions(
	r *schemamodel.Database,
	dialect string,
	caps capability.Capabilities,
) ([]string, []renderer.Omission, error) {
	runtime, err := bundled()
	if err != nil {
		return nil, nil, err
	}
	result, err := runtime.RenderSchema(context.Background(), renderer.SchemaRequest{
		Target: renderTarget(dialect), Schema: r, Capabilities: caps,
	})
	if err != nil {
		return nil, nil, err
	}
	return result.Statements, result.Omissions, nil
}

// RenderSQLReportingOmissions renders nodes the way
// [RenderSQLWithCapabilities] does and reports every declaration the target
// could not carry.
//
// It is the node-level counterpart of
// [GetOrderedCreateStatementsReportingOmissions]: the same renderer and the
// same preparation, with a diagnostic sink attached. A caller that converts a
// description written for another tool reads the omissions to refuse a
// conversion that would lose something, rather than write SQL that is quietly
// smaller than its input.
//
// The SQL is exactly what RenderSQLWithCapabilities returns for the same
// arguments. The omissions are ordered deterministically, and their coverage is
// the one that function documents: it is not exhaustive. A non-nil error means
// an empty string and no omissions.
func RenderSQLReportingOmissions(
	dialect string,
	caps capability.Capabilities,
	nodes ...ast.Node,
) (string, []renderer.Omission, error) {
	runtime, err := bundled()
	if err != nil {
		return "", nil, err
	}
	output, err := runtime.Render(context.Background(), renderer.Request{
		Target: renderTarget(dialect), Capabilities: caps, Nodes: nodes,
	})
	if err != nil {
		return "", nil, err
	}
	return output.SQL(), output.Omissions, nil
}

// publicOmissions stamps the target onto each record and converts it.
//
// The target is stamped here because this is the one place that holds the
// normalized dialect name; a renderer that spelled it again would be a second
// answer to what dialect a render was for.
func publicOmissions(dialect string, sink *renderdiag.Sink) []renderer.Omission {
	recorded := sink.Omissions()
	if len(recorded) == 0 {
		return nil
	}
	out := make([]renderer.Omission, 0, len(recorded))
	for _, omission := range recorded {
		out = append(out, renderer.Omission{
			Dialect:  dialect,
			Reason:   string(omission.Reason),
			Kind:     omission.Kind,
			Name:     omission.Name,
			Property: omission.Property,
			Detail:   omission.Detail,
			Remedy:   omission.Remedy,
		})
	}
	return out
}

// omissionReporter is a dialect renderer that can name what it did not emit.
//
// It is deliberately not part of [ptah.run/core/renderer.RenderVisitor]: that interface is exported
// and an embedder may implement it, so a method added there is a breaking
// change for a capability no embedder has to provide.
type omissionReporter interface {
	ReportOmissionsTo(sink *renderdiag.Sink)
}

// ReportOmissionsTo passes the sink to the wrapped dialect renderer.
//
// The wrapper holds [ptah.run/core/renderer.RenderVisitor] as a named field, so a method the interface
// does not declare is not promoted; without this the assertion below would
// always fail and every render would report nothing.
func (r *validatingRenderer) ReportOmissionsTo(sink *renderdiag.Sink) {
	if reporter, ok := r.inner.(omissionReporter); ok {
		reporter.ReportOmissionsTo(sink)
	}
}

// renderNodeReporting renders one node, attaching sink to the renderer built
// for it.
//
// The ordered path builds a renderer per node and discards it, so a sink held
// by the renderer would never span a schema. Passing it in per node is what
// makes one sink collect the whole render, and it is also why no state leaks
// between statements: the renderer that could leak does not outlive the node.
func renderNodeReporting(
	ctx context.Context,
	dialect string,
	caps capability.Capabilities,
	sink *renderdiag.Sink,
	node ast.Node,
) (string, error) {
	r, err := NewRendererWithCapabilities(dialect, caps)
	if err != nil {
		return "", err
	}
	if reporter, ok := r.(omissionReporter); ok {
		reporter.ReportOmissionsTo(sink)
	}
	result, err := renderNodes(ctx, r, node)
	return result.SQL(), err
}
