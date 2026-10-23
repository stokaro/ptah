// Package main runs the fixture with retention settings in the platform group
// of the common syntax and a table directive that declares a setting of one of
// its columns.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-platform-claims"
	engine := description.GetEngines()[0]
	keys, directives := backend.PlatformSourceClaims()
	engine.YamlSourceKeys = append(engine.YamlSourceKeys, keys...)
	engine.GoSourceDirectives = append(engine.GoSourceDirectives, directives...)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
