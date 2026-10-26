package renderer_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

type nativeFragment struct{ Data []string }

func (*nativeFragment) Kind() schemaext.Kind { return "example.org/native/operation" }
func (p *nativeFragment) CloneExtension() ast.ExtensionPayload {
	return &nativeFragment{Data: slices.Clone(p.Data)}
}

func TestRenderNativeFragmentsPreservePayloadAndRefuseSQLPrefix(t *testing.T) {
	c := qt.New(t)
	payload := &nativeFragment{Data: []string{"original"}}
	returned := renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{
		{SQL: "SELECT 1;"}, {Native: payload}, {SQL: "SELECT 2;"},
	}}
	service := renderFunc(func(_ context.Context, _ renderer.Request) (renderer.Result, error) { return returned, nil })
	result, err := renderer.Render(c.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("first"), &ast.ExtensionStatement{Payload: payload}, ast.NewRawSQL("last")}})
	c.Assert(err, qt.IsNil)
	payload.Data[0] = "changed"
	returned.Fragments[0].SQL = "changed"
	c.Assert(result.Fragments[0].SQL, qt.Equals, "SELECT 1;")
	c.Assert(result.Fragments[1].Native.(*nativeFragment).Data, qt.DeepEquals, []string{"original"})
	text, err := result.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(text, qt.Equals, "")
	fragments, err := result.SQLFragments()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(fragments, qt.IsNil)
	data, err := json.Marshal(result)
	c.Assert(err, qt.ErrorIs, schemaext.ErrExplicitCodec)
	c.Assert(data, qt.IsNil)
}

func TestRenderRefusesAmbiguousAndInvalidNativeFragments(t *testing.T) {
	var absent *nativeFragment
	for _, test := range []struct {
		name     string
		fragment renderer.Fragment
		exact    bool
	}{
		{"SQL beside native", renderer.Fragment{SQL: "SELECT 1;", Native: &nativeFragment{}}, true},
		{"typed nil", renderer.Fragment{Native: absent}, true},
		{"script native", renderer.Fragment{Native: &nativeFragment{}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := renderFunc(func(_ context.Context, _ renderer.Request) (renderer.Result, error) {
				return renderer.Result{Complete: true, ExecutableUnits: test.exact, Fragments: []renderer.Fragment{{SQL: "SELECT 1;"}, test.fragment}}, nil
			})
			result, err := renderer.Render(c.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("first"), ast.NewRawSQL("last")}})
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
		})
	}
}

func TestSQLProjectionRefusesIncompleteBatch(t *testing.T) {
	c := qt.New(t)
	result := renderer.Result{Fragments: []renderer.Fragment{{SQL: "prefix"}}}
	sql, err := result.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(sql, qt.Equals, "")
}

// TestRenderInTheTextFormRefusesANativeFragment returns no result for a reply
// that holds a native operation when the caller asked for SQL text, so a text
// consumer never receives a prefix of a plan it cannot write. The same reply
// is a valid result in the form that takes native operations.
func TestRenderInTheTextFormRefusesANativeFragment(t *testing.T) {
	payload := &nativeFragment{Data: []string{"original"}}
	returned := renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: "SELECT 1;"}, {Native: payload}}}
	service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) { return returned, nil })
	nodes := []ast.Node{ast.NewRawSQL("first"), &ast.ExtensionStatement{Payload: payload}}

	t.Run("text", func(t *testing.T) {
		c := qt.New(t)
		result, err := renderer.Render(c.Context(), service, renderer.Request{Nodes: nodes, Form: renderer.FormText})
		c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
		c.Assert(err, qt.ErrorMatches, `.*a text rendering of input 1 returned a native operation`)
		c.Assert(result, qt.DeepEquals, renderer.Result{})
	})
	t.Run("any", func(t *testing.T) {
		c := qt.New(t)
		result, err := renderer.Render(c.Context(), service, renderer.Request{Nodes: nodes, Form: renderer.FormAny})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Fragments, qt.HasLen, 2)
	})
}

// TestRenderPassesTheFormToTheService hands the service the form the caller
// asked for.
func TestRenderPassesTheFormToTheService(t *testing.T) {
	c := qt.New(t)
	var received renderer.Form
	service := renderFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		received = request.Form
		return renderer.Result{Complete: true, Fragments: []renderer.Fragment{{SQL: "SELECT 1;"}}}, nil
	})

	_, err := renderer.Render(c.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("first")}, Form: renderer.FormText})

	c.Assert(err, qt.IsNil)
	c.Assert(received, qt.Equals, renderer.FormText)
}
