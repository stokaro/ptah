// Package main runs the fixture with a Go directive at column scope, which the
// host does not read, to show that the host refuses the provider at start.
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
	description.SemanticIdentity += "-column-go-directive"
	for _, claim := range description.GetEngines()[0].GetGoSourceDirectives() {
		claim.Scope = protocol.GoSourceDirective_COLUMN
	}
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
