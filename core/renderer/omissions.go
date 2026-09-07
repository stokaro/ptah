package renderer

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/renderdiag"
)

// Omission is one declaration a target did not render.
//
// A render answers a declaration by emitting it, by refusing the whole render
// with an error, or by continuing without it. Only the third answer produces an
// Omission, and it is the answer nothing could previously observe: a
// PostgreSQL-family target writes a `skipped` comment beside the statement
// while SQLite, SQL Server and Oracle drop the same table options without a
// word, and both spellings exit 0 (stokaro/ptah#2976).
//
// Kind and Name identify the object that owned the declaration. Property names
// what the object lost and is empty when the object itself was lost. Detail
// carries the declared value where repeating it helps a reader recognize what
// went missing, and Remedy is set only where a remedy exists on this target.
//
// An Omission is not a portability verdict. A declaration excluded from a
// target by a `dialects=` scope is not part of that target's desired state at
// all, so it is absent rather than omitted and produces nothing here.
type Omission struct {
	// Dialect is the normalized target the render was for.
	Dialect string
	// Reason is a stable token for why the declaration did not reach the
	// output. Branch on it rather than on Message, whose wording is free to
	// change.
	Reason string
	// Kind is the owning object's kind, such as "table".
	Kind string
	// Name is the owning object's name as the declaration spells it.
	Name string
	// Property names the lost property, empty when the whole object was lost.
	Property string
	// Detail is the declared value, empty when there is nothing to repeat.
	Detail string
	// Remedy states how to keep the declaration on this target, empty when
	// there is no remedy that works here.
	Remedy string
}

// Message states the loss in one sentence, without naming the object.
//
// The object's identity is in Kind and Name, so a caller that already prints
// those does not repeat them. The wording is presentation and may change; a
// caller deciding anything reads Reason.
func (o Omission) Message() string {
	subject := o.Property
	if subject == "" {
		subject = o.Kind + " " + o.Name
	}
	if o.Detail != "" {
		subject += "=" + o.Detail
	}
	return subject + " would be skipped"
}

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
// condition and operator class, and a column's identity clauses;
// stokaro/ptah#2983 records what remains.
func GetOrderedCreateStatementsReportingOmissions(
	r *schemamodel.Database,
	dialect string,
	caps capability.Capabilities,
) ([]string, []Omission, error) {
	sink := &renderdiag.Sink{}
	statements, err := orderedCreateStatements(r, dialect, caps, sink)
	if err != nil {
		return nil, nil, err
	}
	return statements, publicOmissions(platform.NormalizeDialect(dialect), sink), nil
}

// publicOmissions stamps the target onto each record and converts it.
//
// The target is stamped here because this is the one place that holds the
// normalized dialect name; a renderer that spelled it again would be a second
// answer to what dialect a render was for.
func publicOmissions(dialect string, sink *renderdiag.Sink) []Omission {
	recorded := sink.Omissions()
	if len(recorded) == 0 {
		return nil
	}
	out := make([]Omission, 0, len(recorded))
	for _, omission := range recorded {
		out = append(out, Omission{
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
// It is deliberately not part of [RenderVisitor]: that interface is exported
// and an embedder may implement it, so a method added there is a breaking
// change for a capability no embedder has to provide.
type omissionReporter interface {
	ReportOmissionsTo(sink *renderdiag.Sink)
}

// ReportOmissionsTo passes the sink to the wrapped dialect renderer.
//
// The wrapper embeds [RenderVisitor] as an interface, so a method the interface
// does not declare is not promoted; without this the assertion below would
// always fail and every render would report nothing.
func (r *validatingRenderer) ReportOmissionsTo(sink *renderdiag.Sink) {
	if reporter, ok := r.RenderVisitor.(omissionReporter); ok {
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
	return visitorRenderSQL(r, node)
}
