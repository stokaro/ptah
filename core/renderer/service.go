package renderer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

// ErrInvalidResult identifies an incomplete rendering reply. No fragment from
// such a reply is usable, even if some of its SQL could execute independently.
var ErrInvalidResult = errors.New("invalid rendering result")

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

// Result contains a complete rendered batch, preserving input-node provenance.
// A failed batch has no fragments or omissions; callers must never execute a
// prefix returned alongside an error. Rendering units imply neither statements
// nor transactions.
type Result struct {
	// Complete confirms that the owner finished deciding the whole batch.
	Complete bool
	// Fragments has exactly one entry per input node, in input order. Empty SQL
	// accounts for a node that emits no work. SQL can contain several statements;
	// native operations occupy a whole execution unit. SQL separators belong to
	// the fragments, so a SQL-only batch joins into a complete script.
	Fragments []Fragment
	// ExecutableUnits means each nonempty fragment is already one exact
	// execution unit. Consumers must preserve its bytes and boundary rather
	// than split it with a host SQL grammar. Script concatenation must still
	// be valid; separators remain the rendering owner's responsibility.
	ExecutableUnits bool
	// Omissions records declarations the target did not emit, in the provider's
	// deterministic report order. Reporting need not cover every unsupported
	// property; an empty list does not establish lossless rendering.
	Omissions []Omission
	// Diagnostics records a completed refusal, which contains no SQL or omissions.
	Diagnostics []Diagnostic
}

// SQL joins the ordered fragments without adding separators or changing bytes.
// An invalid result or any native operation returns no SQL. Native consumers
// use Fragments and preserve their operation payloads.
func (r Result) SQL() (string, error) {
	fragments, err := r.SQLFragments()
	if err != nil {
		return "", err
	}
	return strings.Join(fragments, ""), nil
}

// SQLFragments returns the complete SQL projection, preserving node boundaries.
// A native operation refuses the whole projection; no SQL prefix is returned.
func (r Result) SQLFragments() ([]string, error) {
	if err := r.Validate(len(r.Fragments)); err != nil {
		return nil, err
	}
	if len(r.Diagnostics) != 0 {
		return nil, fmt.Errorf("%w: refused batch has no SQL projection", ErrInvalidResult)
	}
	fragments := make([]string, len(r.Fragments))
	for i, fragment := range r.Fragments {
		if fragment.Native != nil {
			return nil, fmt.Errorf("%w: input %d", ErrNativeSQL, i)
		}
		fragments[i] = fragment.SQL
	}
	return fragments, nil
}

// Validate checks that the reply accounts for every requested input node and
// that omission records are well formed. Invalid replies satisfy
// errors.Is(err, ErrInvalidResult). It cannot establish semantic equivalence
// between SQL and the owner's operations.
func (r Result) Validate(nodes int) error {
	if !r.Complete {
		return fmt.Errorf("%w: batch rendering did not complete", ErrInvalidResult)
	}
	if nodes < 0 {
		return fmt.Errorf("%w: negative input count", ErrInvalidResult)
	}
	for _, diagnostic := range r.Diagnostics {
		if err := (schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{diagnostic.Problem}}).Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidResult, err)
		}
		if diagnostic.Input != nil && (*diagnostic.Input < 0 || *diagnostic.Input >= nodes) {
			return fmt.Errorf("%w: diagnostic input is outside the batch", ErrInvalidResult)
		}
	}
	if len(r.Diagnostics) != 0 {
		if len(r.Fragments) != 0 || len(r.Omissions) != 0 {
			return fmt.Errorf("%w: refused batch contains output", ErrInvalidResult)
		}
		return nil
	}
	if len(r.Fragments) != nodes {
		return fmt.Errorf("%w: expected %d node fragments, received %d", ErrInvalidResult, nodes, len(r.Fragments))
	}
	for i, fragment := range r.Fragments {
		if err := fragment.Validate(); err != nil {
			return fmt.Errorf("%w: input %d: %w", ErrInvalidResult, i, err)
		}
		if fragment.Native != nil && !r.ExecutableUnits {
			return fmt.Errorf("%w: native input %d is not an execution unit", ErrInvalidResult, i)
		}
	}
	return validateOmissions(r.Omissions)
}

// Service renders a batch without database access or schema mutations. It must
// validate the complete batch. Completed refusals are returned as diagnostic
// data; errors mean that rendering could not be performed. Permitted omissions
// are returned in Result.Omissions. Implementations honor cancellation and are
// safe for concurrent calls.
//
// Unlike RenderVisitor, this is a coarse, error-returning provider boundary.
// An in-process service works on typed values directly. A transport adapter
// can encode a whole request without turning each AST visit into an RPC.
type Service interface {
	Render(context.Context, Request) (Result, error)
}

// Render invokes the explicitly selected service once for the complete batch.
// It isolates the node slice, capability map, and returned report slices. Nodes
// themselves remain read-only under the Service contract. Nil services, failed
// or malformed replies, and cancellation return no result. No built-in service
// is selected when the caller supplies none.
func Render(ctx context.Context, service Service, request Request) (Result, error) {
	if err := schemaext.RequireRuntime(ctx, service); err != nil {
		return Result{}, err
	}
	inputs := slices.Clone(request.Nodes)
	request.Nodes = slices.Clone(inputs)
	nodes := len(request.Nodes)
	result, err := service.Render(ctx, request)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := result.Validate(nodes); err != nil {
		return Result{}, err
	}
	if len(result.Diagnostics) != 0 {
		return Result{}, newBatchRefusal(request.Target, inputs, result.Diagnostics)
	}
	result.Fragments = slices.Clone(result.Fragments)
	for i := range result.Fragments {
		result.Fragments[i], err = result.Fragments[i].Clone()
		if err != nil {
			return Result{}, err
		}
	}
	result.Omissions = slices.Clone(result.Omissions)
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return result, nil
}
