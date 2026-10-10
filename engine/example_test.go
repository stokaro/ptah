package engine_test

import (
	"context"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
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

// ExampleRuntime_NormalizeObjects demonstrates a provider that attaches a
// target's own spelling to the declared objects of one kind before a
// comparison. Objects of a kind no owner normalizes come back as they went in.
// The normalizer here doubles a number instead of asking a server, so the
// example runs offline.
func ExampleRuntime_NormalizeObjects() {
	provider := conversionProvider(nil)
	provider.Conversions = nil
	provider.Normalizations = []engine.Normalization{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst},
		Service: normalizationFunc(func(_ context.Context, request schemaext.NormalizationRequest) (schemaext.NormalizationResult, error) {
			objects, err := request.Desired.Objects.All()
			if err != nil {
				return schemaext.NormalizationResult{}, err
			}
			result := request.Desired
			for _, object := range objects {
				spelled := &conversionValue{ID: conversionFirst, Number: object.Value.(*conversionValue).Number * 2}
				if result.Objects, err = result.Objects.Replace(schemaext.Object{Ref: object.Ref, Value: spelled}); err != nil {
					return schemaext.NormalizationResult{}, err
				}
			}
			return schemaext.NormalizationResult{Complete: true, Desired: result}, nil
		})}}
	runtime, err := engine.New(provider)
	if err != nil {
		fmt.Println(err)
		return
	}
	declared, err := schemaext.NewObjects(
		schemaext.Object{Ref: normalizationRef(conversionFirst, "a"), Value: &conversionValue{ID: conversionFirst, Number: 3}},
		schemaext.Object{Ref: normalizationRef(conversionSecond, "b"), Value: &conversionValue{ID: conversionSecond, Number: 5}},
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	result, err := runtime.NormalizeObjects(context.Background(), schemaext.NormalizationRequest{
		Target: "custom", Desired: schemaext.ObjectState{Objects: declared},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	objects, err := result.Desired.Objects.All()
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, object := range objects {
		fmt.Println(object.Ref.Name.Source, object.Value.(*conversionValue).Number)
	}
	// Output:
	// a 6
	// b 5
}
