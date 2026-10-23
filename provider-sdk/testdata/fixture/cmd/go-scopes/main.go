// Package main runs the fixture with Go directives beyond table scope: a column
// directive, an index directive, and attributes on the frontend's own table,
// field and index directives.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-go-scopes"
	engine := description.GetEngines()[0]
	engine.GoSourceDirectives = append(engine.GoSourceDirectives, backend.ScopedGoSourceDirectives()...)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
