// Package main serves the fixture engine as a provider that owns its tables
// and reports, compares and plans their foreign keys, CHECKs and index NULLS
// orders (contract 25.28), the predicates of their partial indexes (contract
// 25.29) and their UNIQUE constraints (contract 25.31), and states how it
// builds an enum column where FIXTURE_ENUM_COLUMNS names a form (contract
// 25.31). Where FIXTURE_TABLE_OWNERSHIP=service its planning service owns the
// tables with no table setting (contract 25.32), in place of the backend's
// retention facet. Where FIXTURE_TABLE_GRANTS names a mode its read reports one
// grant on every table, and with the mode listed it lists planning.table-grants
// and gives a rebuilt table the grants the request carries (contract 25.33).
// Where FIXTURE_REMOVAL_BLOCKERS names some, its read reports removal blockers
// of the tables they name, and its planning service refuses a plan that drops
// a blocked table or column (contract 25.39). Where FIXTURE_NOT_DESCRIBED names
// a kind, the engine declares it and every read records it as not described
// (contract 25.41).
// It reads the constraints back from the SQLite tables it writes, in
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

	"ptah.run/provider-sdk/modeljson"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
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
// file holds the statements of a rebuild. The mode migrations adds revision
// history and transactions to the text mode, so `migrations up` and `down`
// run such a file, each in a transaction. The mode schemas changes no feature:
// a read of a schema other than main finds nothing in it, as the read of a
// provider whose engine has schemas and that carries no namespace finds no
// object in a schema that does not exist.
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
	case "migrations":
		features = append(features, provider.TextUnitsFeature, provider.TransactionsFeature, provider.HistoryFeature)
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
	if mode != "" && mode != "unplanned" && mode != "unpredicated" && mode != "text" && mode != "migrations" && mode != "schemas" {
		fmt.Fprintln(os.Stderr, "unknown fixture constraint mode")
		os.Exit(1)
	}
	ownership := os.Getenv("FIXTURE_TABLE_OWNERSHIP")
	if ownership != "" && ownership != "service" {
		fmt.Fprintln(os.Stderr, "unknown fixture table ownership")
		os.Exit(1)
	}
	grants := os.Getenv("FIXTURE_TABLE_GRANTS")
	if grants != "" && grants != "listed" && grants != "unlisted" {
		fmt.Fprintln(os.Stderr, "unknown fixture table grants mode")
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
	if ownership == "service" {
		// The service then owns the tables through owns_tables alone, with no
		// table setting model, and stays the owner constraint changes need.
		description.SemanticIdentity += "-owned"
		description.Features = append(description.Features, provider.TableOwnershipFeature)
		engine.Features = append(engine.Features, provider.TableOwnershipFeature)
		engine.Planning = []*protocol.ModelPlanning{{Id: "tables", OwnsTables: true, OperationKinds: []string{statementsKind}}}
	}
	if grants != "" {
		// The read reports a grant on every table, which SQLite cannot hold:
		// GRANT is the only common object the engine carries. Unlisted, the
		// feature is left out, so a rebuild gives the new table no grant.
		description.SemanticIdentity += "-grants-" + grants
		added := []string{provider.CommonObjectsFeature}
		if grants == "listed" {
			added = append(added, provider.TableGrantsFeature)
		}
		description.Features = append(description.Features, added...)
		engine.Features = append(engine.Features, added...)
		engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: []protocol.CommonObjectSupport_Kind{protocol.CommonObjectSupport_GRANT}}
	}
	var blockers map[string][]*protocol.RemovalBlocker
	if listed := os.Getenv("FIXTURE_REMOVAL_BLOCKERS"); listed != "" {
		// The read reports the blockers the variable names, and the planning
		// service refuses to drop what they block (contract 25.39).
		if blockers, err = parseBlockers(listed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		description.SemanticIdentity += "-blockers"
		description.Features = append(description.Features, provider.RemovalBlockersFeature)
		engine.Features = append(engine.Features, provider.RemovalBlockersFeature)
	}
	notDescribed := os.Getenv("FIXTURE_NOT_DESCRIBED")
	if notDescribed != "" {
		// The engine declares the kind and every read records it, whatever
		// the database holds, as a provider records a kind its engine has
		// and it does not model (contract 25.41).
		description.SemanticIdentity += "-not-described-" + notDescribed
		description.Features = append(description.Features, provider.NotDescribedFeature)
		engine.Features = append(engine.Features, provider.NotDescribedFeature)
		engine.NotDescribedKinds = []string{notDescribed}
	}
	owner := &owner{Provider: &backend.Provider{}, description: description, text: mode == "text" || mode == "migrations", transactional: mode == "migrations", ownsTables: ownership == "service",
		otherSchemas: mode == "schemas", grants: grants != "", blockers: blockers, notDescribed: notDescribed,
		operation: &protocol.Envelope{ProviderId: description.GetProviderId(), Kind: statementsKind, SchemaVersion: 1, Encoding: "json", DefinitionSha256: hex.EncodeToString(digest[:])}}
	server := runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, FeaturePlanner: owner, TablePreparer: owner, TableCreations: owner, SchemaValidator: owner,
		Locks: owner, CheckComparer: owner}
	if grants != "" {
		// An engine that carries common objects serves their preparation.
		server.CommonObjects = owner
	}
	if mode == "migrations" {
		server.Transactions, server.History = owner, owner
	}
	if err := runtime.Serve(server); err != nil {
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
	// transactional states the schema changes of a session as ones a
	// transaction holds, which SQLite's are.
	transactional bool
	// ownsTables answers a receipt of kind provider.TableReceiptKind for each
	// table, as a planning service that owns the tables does.
	ownsTables bool
	// otherSchemas reads a schema other than main as one that holds nothing,
	// where the fixture otherwise refuses the read.
	otherSchemas bool
	// grants reports heldGrant on every table the read finds.
	grants bool
	// blockers are the removal blockers the read reports for each table, by
	// the table's name, where FIXTURE_REMOVAL_BLOCKERS names some.
	blockers map[string][]*protocol.RemovalBlocker
	// notDescribed is the kind every read records as not described, where
	// FIXTURE_NOT_DESCRIBED names one.
	notDescribed string
}
