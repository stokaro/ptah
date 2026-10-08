package renderer_test

import (
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

type samplePayload struct {
	ID     schemaext.Kind
	Values []string
}

func (p *samplePayload) Kind() schemaext.Kind { return p.ID }

func (p *samplePayload) CloneExtension() ast.ExtensionPayload {
	return &samplePayload{ID: p.ID, Values: slices.Clone(p.Values)}
}

type impostorPayload struct{ samplePayload }

func (p *impostorPayload) CloneExtension() ast.ExtensionPayload {
	return &impostorPayload{samplePayload: samplePayload{ID: p.ID, Values: slices.Clone(p.Values)}}
}

const sampleKind schemaext.Kind = "example.org/widget/add"

func sampleHandler() renderer.ExtensionHandler {
	return renderer.ExtensionHandler{
		Prototype: &samplePayload{ID: sampleKind}, Role: ast.AlterExtension,
		Validate: func(renderer.ExtensionContext, ast.ExtensionPayload) error { return nil },
		Render: func(ctx renderer.ExtensionContext, payload ast.ExtensionPayload) ([]string, error) {
			return []string{ctx.Parent.Name + ": " + payload.(*samplePayload).Values[0]}, nil
		},
	}
}

func TestExtensions_RefuseInvalidRegistration(t *testing.T) {
	valid := sampleHandler()
	wrongRole := valid
	wrongRole.Role = "unrecognized"
	incomplete := valid
	incomplete.Render = nil
	conflicting := valid
	conflicting.Role = ast.StatementExtension
	conflicting.Prototype = &impostorPayload{samplePayload: samplePayload{ID: sampleKind}}
	tests := []struct {
		name     string
		handlers []renderer.ExtensionHandler
		message  string
	}{
		{name: "duplicate ownership", handlers: []renderer.ExtensionHandler{valid, valid}, message: `duplicate extension .*`},
		{name: "nil payload", handlers: []renderer.ExtensionHandler{{}}, message: `register extension: extension payload is nil`},
		{name: "unknown role", handlers: []renderer.ExtensionHandler{wrongRole}, message: `extension .* has invalid role .*`},
		{name: "missing implementation", handlers: []renderer.ExtensionHandler{incomplete}, message: `extension .* has incomplete handlers`},
		{name: "conflicting types across roles", handlers: []renderer.ExtensionHandler{valid, conflicting}, message: `extension .* has conflicting payload types .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := renderer.NewExtensions(test.handlers...)
			c.Assert(err, qt.ErrorMatches, test.message)
		})
	}
}

func TestExtensions_ValidateBoundaryBeforeRendering(t *testing.T) {
	c := qt.New(t)
	registry, err := renderer.NewExtensions(sampleHandler())
	c.Assert(err, qt.IsNil)
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		role    ast.ExtensionRole
		parent  *ast.AlterTableNode
		want    error
		message string
	}{
		{name: "nil", role: ast.AlterExtension, want: ptaherr.ErrInvalidSchemaDiff, message: `extension payload is nil`},
		{name: "typed nil", payload: (*samplePayload)(nil), role: ast.AlterExtension, want: ptaherr.ErrInvalidSchemaDiff, message: `extension payload is nil`},
		{name: "invalid kind", payload: &samplePayload{ID: "plain"}, role: ast.AlterExtension, want: ptaherr.ErrInvalidSchemaDiff, message: `invalid extension kind .*`},
		{name: "unregistered kind", payload: &samplePayload{ID: "example.org/unknown"}, role: ast.AlterExtension, want: ptaherr.ErrUnsupportedFeature, message: `target .* does not support extension .*`},
		{name: "wrong role", payload: &samplePayload{ID: sampleKind}, role: ast.StatementExtension, want: ptaherr.ErrUnsupportedFeature, message: `target .* does not support extension .*`},
		{name: "type impersonation", payload: &impostorPayload{samplePayload: samplePayload{ID: sampleKind}}, role: ast.AlterExtension, want: ptaherr.ErrInvalidSchemaDiff, message: `extension .* has an unregistered payload type .*`},
		{name: "actual parent required", payload: &samplePayload{ID: sampleKind}, role: ast.AlterExtension, want: ptaherr.ErrInvalidSchemaDiff, message: `extension .* requires an ALTER TABLE parent`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := registry.Render(renderer.ExtensionContext{Target: "sample", Parent: test.parent}, test.role, test.payload)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(err, qt.ErrorMatches, test.message)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestExtensions_PreserveInputAndRealContext(t *testing.T) {
	c := qt.New(t)
	handler := sampleHandler()
	var seenParent *ast.AlterTableNode
	handler.Validate = func(ctx renderer.ExtensionContext, payload ast.ExtensionPayload) error {
		seenParent = ctx.Parent
		payload.(*samplePayload).Values[0] = "prepared"
		return nil
	}
	registry, err := renderer.NewExtensions(handler)
	c.Assert(err, qt.IsNil)
	payload := &samplePayload{ID: sampleKind, Values: []string{"original"}}
	parent := &ast.AlterTableNode{Name: "actual_table"}
	statements, err := registry.Render(renderer.ExtensionContext{Target: "sample", Parent: parent}, ast.AlterExtension, payload)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"actual_table: prepared"})
	c.Assert(seenParent, qt.Equals, parent)
	c.Assert(payload.Values, qt.DeepEquals, []string{"original"})
}

func TestExtensions_RefusalPrecedesParentRequirement(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("target lacks widget support")
	handler := sampleHandler()
	handler.Validate = func(renderer.ExtensionContext, ast.ExtensionPayload) error { return refused }
	registry, err := renderer.NewExtensions(handler)
	c.Assert(err, qt.IsNil)
	statements, err := registry.Render(renderer.ExtensionContext{Target: "sample"}, ast.AlterExtension, &samplePayload{ID: sampleKind})
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(statements, qt.IsNil)
}

func TestExtensions_DiscardPartialOwnerResult(t *testing.T) {
	c := qt.New(t)
	failed := errors.New("owner failed")
	handler := sampleHandler()
	handler.Render = func(renderer.ExtensionContext, ast.ExtensionPayload) ([]string, error) {
		return []string{"partial"}, failed
	}
	registry, err := renderer.NewExtensions(handler)
	c.Assert(err, qt.IsNil)
	statements, err := registry.Render(renderer.ExtensionContext{Parent: &ast.AlterTableNode{Name: "actual"}}, ast.AlterExtension, &samplePayload{ID: sampleKind})
	c.Assert(err, qt.ErrorIs, failed)
	c.Assert(statements, qt.IsNil)
}

func TestExtensions_RefuseIdentityMutation(t *testing.T) {
	c := qt.New(t)
	handler := sampleHandler()
	handler.Validate = func(_ renderer.ExtensionContext, payload ast.ExtensionPayload) error {
		payload.(*samplePayload).ID = "example.org/changed"
		return nil
	}
	registry, err := renderer.NewExtensions(handler)
	c.Assert(err, qt.IsNil)
	statements, err := registry.Render(renderer.ExtensionContext{Parent: &ast.AlterTableNode{Name: "actual"}},
		ast.AlterExtension, &samplePayload{ID: sampleKind})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `extension .* changed kind during validation`)
	c.Assert(statements, qt.IsNil)
}

func TestExtensions_EmptyRegistryRefusesClaimedCapabilities(t *testing.T) {
	c := qt.New(t)
	_, err := (renderer.Extensions{}).Render(renderer.ExtensionContext{Target: "other", Capabilities: capability.YDB262()}, ast.AlterExtension, &samplePayload{ID: sampleKind})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

func TestTypedHandler_StatementAndTypeRefusal(t *testing.T) {
	c := qt.New(t)
	handler := renderer.TypedHandler(&samplePayload{ID: sampleKind}, ast.StatementExtension,
		func(renderer.ExtensionContext, *samplePayload) error { return nil },
		func(_ renderer.ExtensionContext, payload *samplePayload) ([]string, error) {
			return slices.Clone(payload.Values), nil
		},
	)
	registry, err := renderer.NewExtensions(handler)
	c.Assert(err, qt.IsNil)
	statements, err := registry.Render(renderer.ExtensionContext{Target: "sample"}, ast.StatementExtension,
		&samplePayload{ID: sampleKind, Values: []string{"first", "second"}})
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"first", "second"})
	impostor := &impostorPayload{samplePayload: samplePayload{ID: sampleKind}}
	c.Assert(handler.Validate(renderer.ExtensionContext{}, impostor), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	statements, err = handler.Render(renderer.ExtensionContext{}, impostor)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(statements, qt.IsNil)
}
