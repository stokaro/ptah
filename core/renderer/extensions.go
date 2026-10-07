package renderer

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ExtensionContext carries the actual statement context and target facts.
// Parent is nil for a standalone node. Handlers must not mutate the context.
type ExtensionContext struct {
	Target       string
	Capabilities capability.Capabilities
	Parent       *ast.AlterTableNode
}

// ExtensionHandler describes an in-process payload handler. Prototype binds a
// semantic kind to one concrete type and is never used as request state.
// Validate checks target support and payload shape without requiring a parent
// when Parent is nil. Render runs only after validation and parent checks.
//
// Handlers are local implementation details of a batched Service; a subprocess
// adapter must use the Service boundary rather than an RPC per handler call.
type ExtensionHandler struct {
	Prototype ast.ExtensionPayload
	Role      ast.ExtensionRole
	Validate  func(ExtensionContext, ast.ExtensionPayload) error
	Render    func(ExtensionContext, ast.ExtensionPayload) ([]string, error)
}

// TypedHandler adapts typed local owner functions to a registration. It keeps
// concrete assertions out of owner functions and refuses an incorrect type if
// the descriptor is called outside its registry. NewExtensions still validates
// the prototype, ownership, role, and completeness.
func TypedHandler[P ast.ExtensionPayload](prototype P, role ast.ExtensionRole,
	validate func(ExtensionContext, P) error,
	render func(ExtensionContext, P) ([]string, error),
) ExtensionHandler {
	handler := ExtensionHandler{Prototype: prototype, Role: role}
	if validate != nil {
		handler.Validate = func(ctx ExtensionContext, payload ast.ExtensionPayload) error {
			typed, ok := payload.(P)
			if !ok {
				return fmt.Errorf("%w: unexpected extension payload type %T", ptaherr.ErrInvalidSchemaDiff, payload)
			}
			return validate(ctx, typed)
		}
	}
	if render != nil {
		handler.Render = func(ctx ExtensionContext, payload ast.ExtensionPayload) ([]string, error) {
			typed, ok := payload.(P)
			if !ok {
				return nil, fmt.Errorf("%w: unexpected extension payload type %T", ptaherr.ErrInvalidSchemaDiff, payload)
			}
			return render(ctx, typed)
		}
	}
	return handler
}

type extensionKey struct {
	kind schemaext.Kind
	role ast.ExtensionRole
}

type extensionHandler struct {
	typeOf   reflect.Type
	validate func(ExtensionContext, ast.ExtensionPayload) error
	render   func(ExtensionContext, ast.ExtensionPayload) ([]string, error)
}

// Extensions is a frozen local dispatch table for a selected target's
// rendering service. Its zero value refuses every payload. It is safe to share
// when registered handlers honor their concurrency and no-mutation contracts.
type Extensions struct {
	handlers map[extensionKey]extensionHandler
}

// NewExtensions validates registrations and copies their metadata. Duplicate
// kind/role ownership, nil payloads, and incomplete handlers are refused.
// A payload type may occupy both roles only when each has its own handler.
func NewExtensions(handlers ...ExtensionHandler) (Extensions, error) {
	result := Extensions{handlers: make(map[extensionKey]extensionHandler, len(handlers))}
	types := make(map[schemaext.Kind]reflect.Type)
	for _, handler := range handlers {
		prototype, err := ast.CloneExtensionPayload(handler.Prototype)
		if err != nil {
			return Extensions{}, fmt.Errorf("register extension: %w", err)
		}
		if handler.Role != ast.StatementExtension && handler.Role != ast.AlterExtension {
			return Extensions{}, fmt.Errorf("extension %q has invalid role %q", prototype.Kind(), handler.Role)
		}
		if handler.Validate == nil || handler.Render == nil {
			return Extensions{}, fmt.Errorf("extension %q has incomplete handlers", prototype.Kind())
		}
		key := extensionKey{kind: prototype.Kind(), role: handler.Role}
		typeOf := reflect.TypeOf(prototype)
		if previous, ok := types[key.kind]; ok && previous != typeOf {
			return Extensions{}, fmt.Errorf("extension %q has conflicting payload types %v and %v", key.kind, previous, typeOf)
		}
		if _, duplicate := result.handlers[key]; duplicate {
			return Extensions{}, fmt.Errorf("duplicate extension %q for role %q", key.kind, key.role)
		}
		result.handlers[key] = extensionHandler{
			typeOf: typeOf, validate: handler.Validate, render: handler.Render,
		}
		types[key.kind] = typeOf
	}
	return result, nil
}

// Prepare clones a payload and validates its registered type, role, target
// capabilities, and shape. It deliberately permits a missing parent so target
// refusals can be reported for standalone ALTER fragments without a fake table.
func (e Extensions) Prepare(ctx ExtensionContext, role ast.ExtensionRole, payload ast.ExtensionPayload) (ast.ExtensionPayload, error) {
	cloned, err := ast.CloneExtensionPayload(payload)
	if err != nil {
		return nil, &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	kind := cloned.Kind()
	handler, found := e.handlers[extensionKey{kind: kind, role: role}]
	if !found {
		return nil, UnsupportedExtension(ctx.Target, cloned.Kind(), role)
	}
	if reflect.TypeOf(cloned) != handler.typeOf {
		return nil, &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf("extension %q has an unregistered payload type %T", cloned.Kind(), cloned)}
	}
	ctx.Capabilities = ctx.Capabilities.Clone()
	if err := handler.validate(ctx, cloned); err != nil {
		return nil, err
	}
	if cloned.Kind() != kind {
		return nil, &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf("extension %q changed kind during validation", kind)}
	}
	return cloned, nil
}

// Render validates and renders one local payload. ALTER fragments require the
// real parent. Any error returns no statements, including an owner failure
// after producing a partial result. Statements retain the owner's order.
func (e Extensions) Render(ctx ExtensionContext, role ast.ExtensionRole, payload ast.ExtensionPayload) ([]string, error) {
	prepared, err := e.Prepare(ctx, role, payload)
	if err != nil {
		return nil, err
	}
	if role == ast.AlterExtension && ctx.Parent == nil {
		return nil, &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff,
			Message: fmt.Sprintf("extension %q requires an ALTER TABLE parent", prepared.Kind())}
	}
	handler := e.handlers[extensionKey{kind: prepared.Kind(), role: role}]
	ctx.Capabilities = ctx.Capabilities.Clone()
	statements, err := handler.render(ctx, prepared)
	if err != nil {
		return nil, err
	}
	return slices.Clone(statements), nil
}

// UnsupportedExtension reports an unregistered target/kind/role combination.
// It is the common refusal for non-owning backends and needs no concrete
// feature import. errors.Is matches ptaherr.ErrUnsupportedFeature.
func UnsupportedExtension(target string, kind schemaext.Kind, role ast.ExtensionRole) error {
	return &ptaherr.CapabilityError{Dialect: target, Feature: string(kind), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("target %q does not support extension %q in role %q", target, kind, role)}
}

// PayloadTypes returns registered payload types, once each. It supports
// conformance against a source-derived inventory; registration alone does not
// establish that the inventory contains every owner-defined payload.
func (e Extensions) PayloadTypes() []reflect.Type {
	var result []reflect.Type
	for _, handler := range e.handlers {
		if !slices.Contains(result, handler.typeOf) {
			result = append(result, handler.typeOf)
		}
	}
	slices.SortFunc(result, func(a, b reflect.Type) int { return strings.Compare(a.String(), b.String()) })
	return result
}
