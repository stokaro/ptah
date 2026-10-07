// This program is copied into an independent module by the external-provider
// test. All Ptah imports are public contracts, and no built-in target is linked.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type widget struct {
	Name   string   `json:"name"`
	Levels []string `json:"levels"`
}

func (*widget) Kind() schemaext.Kind { return "example.org/widget/model" }
func (v *widget) Clone() schemaext.Value {
	return &widget{Name: v.Name, Levels: slices.Clone(v.Levels)}
}
func (v *widget) Equal(other schemaext.Value) bool {
	w, ok := other.(*widget)
	return ok && v.Name == w.Name && slices.Equal(v.Levels, w.Levels)
}

type addWidget struct{ Name string }

func (*addWidget) Kind() schemaext.Kind                   { return "example.org/widget/add" }
func (p *addWidget) CloneExtension() ast.ExtensionPayload { return &addWidget{Name: p.Name} }

type service struct{ extensions renderer.Extensions }

func (s service) Render(_ context.Context, request renderer.Request) (renderer.Result, error) {
	var result renderer.Result
	for _, node := range request.Nodes {
		statement, ok := node.(*ast.ExtensionStatement)
		if !ok {
			return renderer.Result{}, fmt.Errorf("unsupported node %T", node)
		}
		sql, err := s.extensions.Render(renderer.ExtensionContext{Target: request.Target}, ast.StatementExtension, statement.Payload)
		if err != nil {
			return renderer.Result{}, err
		}
		for _, text := range sql {
			result.SQL += text + "\n"
		}
	}
	return result, nil
}

func codec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: &widget{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"name":"string","levels":"ordered string list"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, ok := payload.(*widget)
			if !ok {
				return nil, fmt.Errorf("unexpected model %T", payload)
			}
			return value.Clone(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*widget](data) },
	}
}

func main() {
	if err := verify(); err != nil {
		panic(err)
	}
	fmt.Println("external provider conformance passed")
}

func verify() error {
	handlers, err := renderer.NewExtensions(renderer.TypedHandler(&addWidget{}, ast.StatementExtension,
		func(renderer.ExtensionContext, *addWidget) error { return nil },
		func(_ renderer.ExtensionContext, operation *addWidget) ([]string, error) {
			return []string{"CREATE WIDGET " + operation.Name}, nil
		}))
	if err != nil {
		return err
	}
	runtime, err := engine.New(engine.Provider{ID: "example.org/widget", Codecs: []schemaext.Codec{codec()},
		Targets: []engine.Target{{Name: "widget", Rendering: service{extensions: handlers}}}})
	if err != nil {
		return err
	}
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "widget", Nodes: []ast.Node{
		&ast.ExtensionStatement{Payload: &addWidget{Name: "sample"}},
	}})
	if err != nil {
		return err
	}
	if result.SQL != "CREATE WIDGET sample\n" {
		return fmt.Errorf("wrong custom render: %q", result.SQL)
	}
	_, err = runtime.Render(context.Background(), renderer.Request{Target: "postgres"})
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected target was not refused: %v", err)
	}
	original := &widget{Name: "sample", Levels: []string{"two", "one"}}
	facets, err := schemaext.NewFacets(original)
	if err != nil {
		return err
	}
	encoded, err := runtime.Codecs().EncodeFacets(context.Background(), schemaext.Desired, facets)
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().DecodeFacets(context.Background(), schemaext.Desired, encoded)
	if err != nil {
		return err
	}
	value, found, err := schemaext.FacetAs[*widget](decoded, original.Kind())
	if err != nil {
		return err
	}
	if !found || !value.Equal(original) {
		return fmt.Errorf("typed round trip lost the value")
	}
	value.Levels[0] = "changed"
	if original.Levels[0] != "two" {
		return fmt.Errorf("round trip exposed aliased state")
	}
	return nil
}
