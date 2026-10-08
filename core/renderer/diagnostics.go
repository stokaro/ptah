package renderer

import (
	"errors"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemavalidation"
)

// Diagnostic identifies a batch refusal using data that a transport can encode.
// Input is the zero-based request-node index, or nil for a batch-wide problem.
// A diagnostic never embeds a provider-owned Go error or AST node.
type Diagnostic struct {
	Problem schemavalidation.Diagnostic
	Input   *int
}

// BatchRefusalError is a completed rendering refusal, distinct from failure to
// perform rendering. Diagnostics preserves the provider's data. Unwrap exposes
// schema/capability sentinels and the caller's input node when one was identified.
// The error itself is local; transport adapters encode the diagnostics instead.
type BatchRefusalError struct {
	Target      string
	Diagnostics []Diagnostic
	nodes       []ast.Node
}

func newBatchRefusal(target string, nodes []ast.Node, diagnostics []Diagnostic) *BatchRefusalError {
	cloned := slices.Clone(diagnostics)
	for i := range cloned {
		if cloned[i].Input != nil {
			cloned[i].Input = new(*cloned[i].Input)
		}
	}
	return &BatchRefusalError{Target: target, Diagnostics: cloned, nodes: slices.Clone(nodes)}
}

// Error describes the completed refusal without exposing SQL.
func (e *BatchRefusalError) Error() string {
	if err := e.Unwrap(); err != nil {
		return err.Error()
	}
	return "batch rendering refused"
}

// Unwrap provides typed causes and input-node provenance for local consumers.
func (e *BatchRefusalError) Unwrap() error {
	if e == nil {
		return nil
	}
	failures := make([]error, 0, len(e.Diagnostics))
	for _, diagnostic := range e.Diagnostics {
		failure := (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{diagnostic.Problem}}).Err(e.Target)
		if diagnostic.Input != nil && *diagnostic.Input >= 0 && *diagnostic.Input < len(e.nodes) {
			failure = &ptaherr.RenderError{Dialect: e.Target, Node: e.nodes[*diagnostic.Input], Err: failure, Message: diagnostic.Problem.Message}
		}
		failures = append(failures, failure)
	}
	return errors.Join(failures...)
}
