// Package main exercises captured executable operations in a separate provider.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

const operationKind = "example.org/fixture/table-operation"
const changeKind = "example.org/fixture/table-change"

type owner struct {
	*backend.Provider
	operationEnvelope *protocol.Envelope
}

func main() {
	d := backend.Description()
	d.SemanticIdentity += "-native-operations"
	definition, err := modeljson.Canonical([]byte(`{
		"additionalProperties": false,
		"properties": {
			"action": {"enum": ["create", "rename", "drop", "add-column"]},
			"column": {
				"additionalProperties": false,
				"properties": {
					"name": {"type": "string"},
					"type": {"enum": ["INTEGER", "TEXT", "REAL", "BLOB"]},
					"nullable": {"type": "boolean"}
				},
				"required": ["name", "type", "nullable"],
				"type": "object"
			},
			"to": {"type": "string"},
			"transaction": {"enum": ["required"]}
		},
		"required": ["action"],
		"type": "object"
	}`))
	if err != nil {
		panic(err)
	} // A malformed static definition is a fixture programming error.
	hash := sha256.Sum256(definition)
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, &protocol.ValueModel{Kind: operationKind, Representation: protocol.ValueInput_OPERATION,
		SchemaVersion: 1, Encoding: "json", DefinitionJson: definition,
		DefinitionSha256: hex.EncodeToString(hash[:]), CanonicalFormat: "table-operation-v1"})
	change := proto.CloneOf(d.Engines[0].ValueModels[len(d.Engines[0].ValueModels)-1])
	change.Kind, change.Representation = changeKind, protocol.ValueInput_CHANGE
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, change)
	d.Features = append(d.Features, provider.PlanFeaturesFeature)
	d.Engines[0].Planning = []*protocol.ModelPlanning{{Id: "table-actions", ChangeKinds: []string{changeKind}, ParentKinds: []string{"example.org/fixture/retention"}, OperationKinds: []string{operationKind}}}
	p := &owner{Provider: &backend.Provider{}, operationEnvelope: &protocol.Envelope{
		ProviderId: d.GetProviderId(), Kind: operationKind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: hex.EncodeToString(hash[:]),
	}}
	if err := runtime.Serve(runtime.Server{Description: d, Sessions: p, Inspector: p, Renderer: p, Executor: p, SQLAnalyzer: p, ValueNormalizer: p, SourceDecoder: p, ValueConverter: p, FeaturePlanner: p}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
