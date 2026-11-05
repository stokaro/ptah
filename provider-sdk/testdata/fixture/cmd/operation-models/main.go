// Package main exercises captured executable operations in a separate provider.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync/atomic"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/devdb"
	"example.com/ptah-fixture-provider/statements"
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
	nativeUnits       bool
	transactionalDDL  bool
	// refusesDDLTransactions states that the server refuses a schema
	// statement inside an explicit transaction.
	refusesDDLTransactions bool
	ownedLifecycle         bool
	ownedEdits             bool
	// comments carries comments on the common-object wire beside the owned
	// table lifecycle and reports the indexes a table holds, as the YDB
	// provider does.
	comments bool
	// renames claims the host's index rename steps, and its sessions report
	// index_rename to a host that lists planning.index-renames.
	renames bool
	// reportsRenames reports index_rename whatever the host lists, as a
	// provider that breaks the contract would.
	reportsRenames bool
	statementStyle bool
	// rebuilds plans an added NOT NULL column, which SQLite cannot add in
	// place, as a rebuild of its table when the host's planning policy asks
	// for one, and refuses it in the words of that policy when it does not.
	rebuilds   bool
	inputMode  string
	inputCalls atomic.Int64
}

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	d := backend.Description()
	d.SemanticIdentity += "-native-operations"
	mode := os.Getenv("FIXTURE_RENDER_MODE")
	inputMode := os.Getenv("FIXTURE_INPUT_MODE")
	transactional := mode == "native-transactional" || inputMode == "transactional"
	if inputMode != "" {
		d.MinContract = &protocol.ContractVersion{Major: provider.Revision, Minor: 5}
		d.Features = append(d.Features, provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature)
	}
	modes := []string{"", "native", "native-transactional", "native-unwrapped", "native-cli", "native-text", "owned", "policy", "declarations", "owned-edit", "object-context", "statement-style",
		"owned-comments", "owned-renames"}
	if !slices.Contains(modes, mode) {
		panic("unknown fixture render mode")
	}
	if mode != "" {
		d.SemanticIdentity += "-" + mode
	}
	if transactional {
		d.Features = append(d.Features, provider.TransactionsFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.TransactionsFeature)
	}
	if mode == "native-cli" || mode == "native-text" || mode == "policy" {
		d.Features = append(d.Features, provider.LocksFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.LocksFeature)
	}
	if mode == "native-text" || mode == "policy" {
		// The provider renders a native operation as SQL text when the host
		// asks for text, and refuses a rebuild, which only its native
		// operation runs as one unit.
		d.Features = append(d.Features, provider.TextUnitsFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.TextUnitsFeature)
	}
	commented := mode == "owned-comments" || mode == "owned-renames"
	if mode == "owned" || mode == "declarations" || mode == "owned-edit" || commented {
		// The provider owns the table lifecycle: the host never interprets a
		// table operation, and the provider claims each column step itself.
		d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(feature string) bool { return feature == provider.CommonTableIdentityFeature })
	}
	if commented {
		// The provider also reports the columns and the indexes its tables
		// hold and carries comments on the common-object wire, so the host
		// sends a comment as a step of its own. It creates a table in the
		// lifecycle phase.
		added := []string{provider.CommonObjectsFeature, provider.ObservedColumnsFeature, provider.ObservedIndexesFeature, provider.PlanningLifecycleFeature}
		d.Features = append(d.Features, added...)
		d.Engines[0].Features = append(d.Engines[0].Features, added...)
		d.Engines[0].CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_COMMENT, protocol.CommonObjectSupport_GRANT}}
	}
	indexProperty := ""
	if mode == "owned-renames" {
		// The provider renames an index of a table it owns in place.
		d.Features = append(d.Features, provider.IndexRenamesFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.IndexRenamesFeature)
		indexProperty = `"index": {"type": "string"},`
	}
	actions, rebuild := `"create", "rename", "drop", "add-column", "drop-column"`, ""
	if mode == "owned-renames" {
		actions += `, "rename-index"`
	}
	if mode == "policy" {
		// The provider honors the planning policy: it replaces the common
		// step that adds a NOT NULL column with a rebuild of its table when
		// the operator asks for one. The native commands apply it under the
		// session lock native-cli also takes.
		d.Features = append(d.Features, provider.PlanningPolicyFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningPolicyFeature)
		actions += `, "rebuild"`
		rebuild = `"rebuild": {
				"additionalProperties": false,
				"properties": {
					"columns": {"items": {
						"additionalProperties": false,
						"properties": {
							"name": {"type": "string"},
							"type": {"enum": ["INTEGER", "TEXT", "REAL", "BLOB"]},
							"nullable": {"type": "boolean"},
							"primaryKey": {"type": "boolean"}
						},
						"required": ["name", "type", "nullable", "primaryKey"],
						"type": "object"
					}, "type": "array"},
					"copy": {"items": {"type": "string"}, "type": "array"}
				},
				"required": ["columns", "copy"],
				"type": "object"
			},`
	}
	if mode == "statement-style" {
		d.MinContract = provider.Contract()
		d.Features = append(d.Features, provider.PlanningPolicyFeature, provider.PlanningStatementStyleFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.PlanningPolicyFeature, provider.PlanningStatementStyleFeature)
	}
	definition, err := modeljson.Canonical([]byte(`{
		"additionalProperties": false,
		"properties": {
			` + rebuild + indexProperty + `
			"action": {"enum": [` + actions + `]},
			"dropColumn": {"type": "string"},
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
			"ifExists": {"type": "boolean"},
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
	envelope := &protocol.Envelope{ProviderId: d.GetProviderId(), Kind: operationKind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: hex.EncodeToString(hash[:])}
	p := &owner{Provider: &backend.Provider{},
		nativeUnits: mode != "" && mode != "owned" && mode != "policy" && mode != "declarations" && mode != "owned-edit" && mode != "object-context" && mode != "statement-style" &&
			!commented,
		ownedLifecycle: mode == "owned" || mode == "declarations" || mode == "owned-edit" || commented, ownedEdits: mode == "owned-edit",
		comments: commented, renames: mode == "owned-renames", reportsRenames: os.Getenv("FIXTURE_INDEX_RENAME") == "always",
		rebuilds: mode == "policy", statementStyle: mode == "statement-style", transactionalDDL: transactional, refusesDDLTransactions: mode == "native-unwrapped",
		operationEnvelope: envelope, inputMode: inputMode}
	server := runtime.Server{Description: d, Sessions: p, Inspector: p, Renderer: p, Executor: p, SQLAnalyzer: p, ValueNormalizer: p, SourceDecoder: p, ValueConverter: p, FeaturePlanner: p}
	if inputMode != "" {
		server.ExecutionInputs = p
		server.History = p
		server.Statements = &statements.Service{Sessions: p}
	}
	if mode == "object-context" {
		configureObjectContext(d)
	}
	if p.comments {
		server.CommonObjects = p
	}
	if mode == "declarations" {
		configureDeclarations(d)
		server.Declarations, server.SchemaValidator = p, p
	}
	if os.Getenv("FIXTURE_SCHEMA_VALIDATION") != "" {
		// The provider validates a whole schema, which a rollback plan asks
		// of its target before it plans the rollback.
		d.SemanticIdentity += "-schema-validation"
		d.Features = append(d.Features, provider.ValidateSchemaFeature)
		d.Engines[0].Features = append(d.Engines[0].Features, provider.ValidateSchemaFeature)
		server.SchemaValidator = p
	}
	if p.transactionalDDL {
		server.Transactions = p
	}
	if mode == "native-cli" || mode == "native-text" || mode == "policy" {
		server.Locks = p
	}
	if os.Getenv("FIXTURE_DEV_SERVICES") != "" {
		// The provider cleans and guards a dev database, so a rehearsal of a
		// native plan can claim one, reset it and run the plan there.
		d.SemanticIdentity += "-dev-services"
		added := []string{provider.DevCleanupFeature, provider.ReplayGuardFeature}
		d.Features = append(d.Features, added...)
		d.Engines[0].Features = append(d.Engines[0].Features, added...)
		cleaner := devdb.Cleaner{Sessions: p.Provider}
		server.DevCleaner, server.ReplayGuard = cleaner, cleaner
	}
	if err := runtime.Serve(server); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
