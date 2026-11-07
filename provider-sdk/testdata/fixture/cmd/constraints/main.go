// Package main serves the fixture engine as a provider that owns its tables
// and reports, compares and plans their foreign keys, CHECKs and index NULLS
// orders (contract 25.28), the predicates of their partial indexes (contract
// 25.29) and their UNIQUE constraints (contract 25.31), and states how it
// builds an enum column where FIXTURE_ENUM_COLUMNS names a form (contract
// 25.31). It reads the constraints back from the SQLite tables it writes, in
// the table text it writes itself, and changes a constraint or an index by
// rebuilding its table, since SQLite adds no constraint to a table in place.
// It establishes no support for an official engine.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	"github.com/stokaro/ptah/provider-sdk/modeljson"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// statementsKind is the operation that runs the statements a rebuild writes.
const statementsKind = "example.org/fixture/constraint-statements"

// retentionKind is the backend's table facet, which the planning service
// assesses as the parent of the tables it owns.
const retentionKind = "example.org/fixture/retention"

// engineFeatures are the features of 25.28, 25.29 and 25.31 the fixture
// serves and the ones they need. The mode FIXTURE_CONSTRAINT_MODE=unplanned leaves out
// the constraint changes, so a host refuses a change of a constraint the read
// reports, unpredicated leaves out the index predicates, so a host compares
// them by its own rules, and the mode text adds text units, so a migration
// file holds the statements of a rebuild.
func engineFeatures(mode string) []string {
	features := []string{provider.ObservedColumnsFeature, provider.ObservedConstraintsFeature, provider.ObservedUniqueConstraintsFeature, provider.ConstraintChangesFeature,
		provider.CheckEquivalenceFeature, provider.ObservedIndexesFeature, provider.IndexNullsOrderFeature, provider.IndexPredicatesFeature,
		provider.PrepareTablesFeature, provider.ProjectTableCreationsFeature, provider.ValidateSchemaFeature, provider.PlanningPolicyFeature,
		provider.PlanningStatementStyleFeature, provider.PlanningLifecycleFeature, provider.LocksFeature}
	switch mode {
	case "unplanned":
		features = slices.DeleteFunc(features, func(feature string) bool { return feature == provider.ConstraintChangesFeature })
	case "unpredicated":
		features = slices.DeleteFunc(features, func(feature string) bool { return feature == provider.IndexPredicatesFeature })
	case "text":
		features = append(features, provider.TextUnitsFeature)
	}
	return features
}

// enumForms are the forms FIXTURE_ENUM_COLUMNS names. The empty name states
// none, so a host keeps the enum a type of its own.
var enumForms = map[string]*protocol.EnumColumns{
	"":        nil,
	"check":   {Form: protocol.EnumColumns_CHECK, CheckType: "TEXT"},
	"values":  {Form: protocol.EnumColumns_VALUES_TYPE},
	"refused": {Form: protocol.EnumColumns_REFUSED},
}

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	mode := os.Getenv("FIXTURE_CONSTRAINT_MODE")
	if mode != "" && mode != "unplanned" && mode != "unpredicated" && mode != "text" {
		fmt.Fprintln(os.Stderr, "unknown fixture constraint mode")
		os.Exit(1)
	}
	enumForm := os.Getenv("FIXTURE_ENUM_COLUMNS")
	enums, known := enumForms[enumForm]
	if !known {
		fmt.Fprintln(os.Stderr, "unknown fixture enum form")
		os.Exit(1)
	}
	description := backend.Description()
	description.SemanticIdentity += "-constraints"
	if mode != "" {
		description.SemanticIdentity += "-" + mode
	}
	engine := description.Engines[0]
	engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	engine.Features = append(engine.Features, engineFeatures(mode)...)
	description.Features = append(description.Features, engineFeatures(mode)...)
	description.Features = append(description.Features, provider.PlanFeaturesFeature)
	if enums != nil {
		description.SemanticIdentity += "-enums-" + enumForm
		description.Features = append(description.Features, provider.EnumColumnsFeature)
		engine.Features = append(engine.Features, provider.EnumColumnsFeature)
		engine.EnumColumns = enums
	}
	definition, err := modeljson.Canonical([]byte(`{
		"additionalProperties": false,
		"properties": {"statements": {"items": {"type": "string"}, "minItems": 1, "type": "array"}},
		"required": ["statements"],
		"type": "object"
	}`))
	if err != nil {
		panic(err)
	} // A malformed static definition is a fixture programming error.
	digest := sha256.Sum256(definition)
	engine.ValueModels = append(engine.ValueModels, &protocol.ValueModel{Kind: statementsKind, Representation: protocol.ValueInput_OPERATION,
		SchemaVersion: 1, Encoding: "json", DefinitionJson: definition, DefinitionSha256: hex.EncodeToString(digest[:]), CanonicalFormat: "constraint-statements-v1"})
	engine.Planning = []*protocol.ModelPlanning{{Id: "tables", ParentKinds: []string{retentionKind}, OperationKinds: []string{statementsKind}}}
	owner := &owner{Provider: &backend.Provider{}, description: description, text: mode == "text",
		operation: &protocol.Envelope{ProviderId: description.GetProviderId(), Kind: statementsKind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: hex.EncodeToString(digest[:])}}
	err = runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, FeaturePlanner: owner, TablePreparer: owner, TableCreations: owner, SchemaValidator: owner,
		Locks: owner, CheckComparer: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// owner serves the fixture engine's sessions, reads and SQL, and owns the
// lifecycle of its tables.
type owner struct {
	*backend.Provider
	description *protocol.Description
	// operation is the envelope of a statements operation, without its owner
	// and payload.
	operation *protocol.Envelope
	// text renders a statements operation as text where the host asks for
	// it, and reads the statements of a rebuild back.
	text bool
}
