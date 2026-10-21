// Package main runs the fixture with a second engine whose model has no YAML
// syntax, to exercise format-specific coverage without opening a session.
package main

import (
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-mixed-yaml"
	other := proto.CloneOf(description.GetEngines()[0])
	other.Id = "other-fixture"
	other.YamlSourceKeys = nil
	for _, model := range other.GetValueModels() {
		if model.GetRepresentation() == protocol.ValueInput_DESIRED {
			model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_HCL}
		}
	}
	description.Engines = append(description.Engines, other)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
