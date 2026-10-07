package renderer

import (
	"context"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
)

// Request describes an offline rendering batch for one target. Capabilities
// are the caller's resolved server facts or explicit offline profile. Nodes
// remain in semantic order. A service must not mutate either input.
type Request struct {
	// Target identifies the selected database semantics.
	Target string
	// Capabilities describes the target facts used to validate this request.
	Capabilities capability.Capabilities
	// Nodes is the complete ordered batch to render.
	Nodes []ast.Node
}

// Result contains a complete rendered batch. A failed batch has no SQL;
// callers must never execute a prefix returned alongside an error.
type Result struct {
	// SQL contains the complete script, or is empty when there is no work.
	SQL string
}

// Service renders a batch without database access or schema mutations. It must
// validate the complete batch and return an error for unsupported semantics.
// Implementations honor cancellation and are safe for concurrent calls.
//
// Unlike RenderVisitor, this is a coarse, error-returning provider boundary.
// An in-process service works on typed values directly. A transport adapter
// can encode a whole request without turning each AST visit into an RPC.
type Service interface {
	Render(context.Context, Request) (Result, error)
}
