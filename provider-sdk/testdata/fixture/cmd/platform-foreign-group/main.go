// Package main runs the fixture with a claim on the platform group of an engine
// the provider does not serve, which the host refuses when the provider starts.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-platform-foreign-group"
	engine := description.GetEngines()[0]
	keys, _ := backend.PlatformSourceClaims()
	keys[0].Key = "platform.mysql"
	engine.YamlSourceKeys = append(engine.YamlSourceKeys, keys...)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
