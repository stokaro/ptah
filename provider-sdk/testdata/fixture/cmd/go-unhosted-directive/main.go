// Package main runs the fixture with attributes claimed on a frontend directive
// the host does not read for providers, to show that the host refuses the
// provider at start instead of dropping the attributes.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-go-unhosted-directive"
	engine := description.GetEngines()[0]
	for _, claim := range engine.GetGoSourceDirectives() {
		claim.Directive, claim.Scope = "ptah:schema:constraint", protocol.GoSourceDirective_TABLE
	}
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
