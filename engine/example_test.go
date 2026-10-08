package engine_test

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/engine"
)

// ExampleNew demonstrates an application that registers its own rendering
// service without importing the built-in composition or any database driver.
func ExampleNew() {
	service := renderingFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"rendered for " + request.Target}}, nil
	})
	runtime, err := engine.New(engine.Provider{
		ID:      "example.org/my-provider",
		Targets: []engine.Target{{Name: "custom", Rendering: service}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "custom", Nodes: []ast.Node{&ast.StatementList{}}})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result.SQL())
	fmt.Println(runtime.Targets())
	// Output:
	// rendered for custom
	// [custom]
}
