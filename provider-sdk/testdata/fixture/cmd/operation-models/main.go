// Package main exercises captured executable operations in a separate provider.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"example.com/ptah-fixture-provider/backend"
	"fmt"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"os"
)

const operationKind = "example.org/fixture/table-operation"

type owner struct{ *backend.Provider }

func main() {
	d := backend.Description()
	d.SemanticIdentity += "-native-operations"
	definition, err := modeljson.Canonical([]byte(`{"additionalProperties":false,"properties":{"action":{"enum":["create","rename","drop"]},"to":{"type":"string"}},"required":["action"],"type":"object"}`))
	if err != nil {
		panic(err)
	} // A malformed static definition is a fixture programming error.
	hash := sha256.Sum256(definition)
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, &protocol.ValueModel{Kind: operationKind, Representation: protocol.ValueInput_OPERATION, SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(hash[:]), CanonicalFormat: "table-operation-v1"})
	p := &owner{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: d, Sessions: p, Inspector: p, Renderer: p, Executor: p, SQLAnalyzer: p, ValueNormalizer: p, SourceDecoder: p, ValueConverter: p}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
