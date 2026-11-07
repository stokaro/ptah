// Package providerhost adapts explicitly trusted subprocess providers to the
// application pipeline. It owns processes, not comparison or migration policy.
package providerhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	providerruntime "github.com/stokaro/ptah/provider-sdk/runtime"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providersource"
)

// Spec selects executable bytes already trusted by the caller. Environment is
// an explicit non-secret allowlist. Addresses belong to connection requests.
type Spec struct {
	Executable  string   `json:"executable"`
	SHA256      string   `json:"sha256"`
	ProviderID  string   `json:"provider_id"`
	Environment []string `json:"environment,omitempty"`
}

// Host owns one provider process and its frozen target declarations. Close it
// after all application commands and their sessions have finished.
type Host struct {
	client      *providerruntime.Client
	description *protocol.Description
	artifact    string
	targets     map[string]*target
	models      map[modelKey]*modelContract
	modelCodecs []schemaext.Codec
	goSource    *goSource
	selections  *scopeSelections
	// splits keeps the provider's split of each migration body it was asked
	// for; see session.SplitMigration.
	splits migrationSplits
	// releases end the registrations the host made for its engines, such as
	// their scanners; see provideScanners.
	releases []func()
}

type target struct {
	host   *Host
	engine *protocol.Engine
}

// hostFeatures are the two-way features of the contract
// (provider.TwoWayFeatures) this host acts on, which it lists when it starts a
// provider. A feature joins the list in the change that adds the adapter
// acting on it: an addition the list does not name stays out of every
// exchange with the provider.
var hostFeatures = []string{
	// Rendered units retain approved input references; execution discovers the
	// same references again and binds values only for the dispatch RPC.
	provider.SensitiveInputsFeature,
	// The migrator hands a migration's statement timeout to the provider with
	// each unit; see session.BoundsStatements.
	provider.StatementTimeoutFeature,
	// A dev URL of an engine whose provider creates dev realms enters a
	// realm; see DevDatabaseOwner.EnterRealm.
	provider.DevRealmsFeature,
	// `ptah sql lint` reads a provider's statement facts; see
	// sqllint.Options.Owner.
	provider.LintStatementFactsFeature,
	// A provider's lint rules learn who opens each file's transaction, as the
	// bundled owners do; see lintService.CheckMigrations.
	provider.LintTransactionOwnerFeature,
	// The query builder renders an engine from the spelling its provider
	// declares; see registerQueryDialects.
	provider.QueryDialectFeature,
	// The note that counts the roles a read leaves out names the reason the
	// provider gives; see rolescope.ReportUndescribed.
	provider.RoleScopeReasonsFeature,
	// A provider's Go directive of the document stands in the group its
	// encoded part names; see goEncoding.fragment.
	provider.GoPlacementsFeature,
	// `ptah inference` keeps a generation's run state and vectors in a
	// provider's embedding store; see EmbeddingSession.
	provider.EmbeddingStoreFeature,
	// A provider's planner receives the operator's planning policy, and its
	// comparisons resolve the requests the operator spells, such as
	// --rotate-secret; see target.planningPolicy and writtenRequests.
	provider.PlanningPolicyFeature,
	provider.PlanningStatementStyleFeature,
	// Lifecycle operations may surround common changes; see commonPlanner.schedule.
	provider.PlanningLifecycleFeature,
	provider.PlanningObjectContextFeature,
	// A filtered read carries the provider's planning context outside its
	// scope, which the planner receives as context and nothing compares; see
	// reader.planningContext.
	provider.PlanningContextInventoryFeature,
	// The reverse plan of a provider's change is the provider's reversal of
	// it, which the engine runtime dispatches as it dispatches a built-in
	// owner's; see modelReverser.
	provider.ReverseChangesFeature,
	// A read's coverage region may cover a schema and every schema below it,
	// under the separator the engine's identifier rules declare; see
	// namespaceScopes and comparisonNamespace (ADR 0064).
	provider.SchemaSubtreeCoverageFeature,
	// A migration file, a printed diff and a rendered schema ask for SQL
	// text units, and a provider refuses a change it can make only natively;
	// see target.Render and rowWriter.RenderWrites.
	provider.TextUnitsFeature,
	// Views, materialized views, sequences, roles, grants and comments reach
	// a provider on the common-object wire: read, validated, spelled for the
	// comparison and rendered by it; see target.declaredCommonObjects and
	// target.commonObjectChange.
	provider.CommonObjectsFeature,
	provider.CommonAccessFeature,
	// A provider's read lists the namespaces of its scope, which the catalog
	// keeps as its schemas, and a plan creates a declared schema the read
	// does not hold, and the schema of a created object, through the
	// provider; see reader.commonObjects and commonPlanner.namespaceSteps.
	provider.CommonNamespacesFeature,
	// A declared role password reaches a provider as an execution input it
	// renders a reference to, and its value only as an execution binding; see
	// target.passwordInput and session.bindExecutionInputs.
	provider.PasswordInputsFeature,
	// A provider's YAML keys of column and index entries, and the common
	// keys and attributes of an entry its claim reads with its own; see
	// Host.yamlExtension and claimedExtension.
	provider.EntryKeysFeature,
	// A provider's HCL blocks and the attributes it adds to the frontend's
	// blocks, read by the frontend and decoded by the provider; see
	// Host.hclExtensions.
	provider.HCLSourceFeature,
	// A provider's claim of a table's platform group of its engine, whose
	// models an HCL document describes only for a table that writes the
	// group, and the loss of a value such a group writes in part; see
	// Host.hclExtensions and hclEncoding.fragment.
	provider.HCLPlatformFeature,
	// A provider's claim of the index types it reads in place of the
	// frontend's common index: the Go, YAML and HCL frontends build no index
	// for such a declaration and send it whole, and export writes the
	// provider's index back as the same declaration; see withIndexKinds,
	// Host.yamlExtension, Host.hclExtensions and the encoders.
	provider.IndexKindsFeature,
	// A claim a document captured under an identity a provider's model had
	// before, such as an HCL export's header written by a former built-in
	// engine, or by a release of the provider before a definition text
	// changed, is read as the same claim of the provider's model; see
	// Host.headerCodecs.
	provider.ModelPredecessorsFeature,
	provider.OwnPredecessorsFeature,
	// Source groups retain exact value accounting across ordered syntax parts;
	// claimed headers retain the owner's narrowed source coverage.
	provider.EncodeGroupsFeature,
	provider.SourceLimitsFeature,
	provider.ColumnsFeature,
	provider.ObservedColumnsFeature,
	// Inspection reports a provider table's common indexes, and the planner
	// hands their changes to the table's owner; see reader.ReadSchemaContext
	// and commonPlanner.indexChanges.
	provider.ObservedIndexesFeature,
	// Inspection reports a provider table's primary key in key order, which
	// the catalog keeps as the table's PRIMARY KEY constraint, and the
	// planner hands a change of it to the table's owner as an alteration of
	// the table; see reader.tables and commonPlanner.constraintChanges.
	provider.ObservedPrimaryKeysFeature,
	provider.PrimaryKeyChangesFeature,
	// Inspection reports the foreign keys and CHECKs a provider table holds,
	// which the catalog keeps as the table's constraints, and the planner
	// hands a change of one to the table's owner as an alteration of the
	// table; see reader.constraints and commonPlanner.constraintChanges.
	provider.ObservedConstraintsFeature,
	provider.ConstraintChangesFeature,
	// A read reports the ledger the engine's seed service keeps as reserved
	// storage, which every comparison leaves out, so a plan never drops it;
	// see reader.reservedNames.
	provider.ReservedSeedsFeature,
	// A clause of CREATE TABLE an engine that reads the shared SQL grammar
	// claims reaches the provider whole, and the facets it decodes are the
	// table's; see Host.tableClauseExtension.
	provider.SQLTableClausesFeature,
	// A release line of an engine linked into its provider names the version
	// it links as its evidence, which `ptah providers check` reports; see
	// Host.ReleaseLines.
	provider.LinkedReleaseLinesFeature,
	// Table preparation states the kind of each declared index, and the
	// index rules compare it with the held index's method; see
	// observedIndexRules.
	provider.IndexMethodsFeature,
	// Inspection states the NULLS order of every key part of a held index,
	// and table preparation states it for every key part of a declared one,
	// which the index rules compare exactly; see observedIndexRules.
	provider.IndexNullsOrderFeature,
	// Table preparation states the predicate of each declared partial index
	// as the engine holds it, and inspection states a held one in the same
	// spelling, which the index comparison compares exactly; see
	// preparedIndexConditions.
	provider.IndexPredicatesFeature,
	provider.DatabaseContextFeature,
	provider.ValueContextFeature,
	// A command's scope names a provider's objects by the selector types its
	// engines declare, and a grant rides the object it names; see
	// newScopeSelections.
	provider.ScopeSelectionsFeature,
	// Offline schema rendering plans authored objects without an observed
	// catalog; see target.renderDeclarations.
	provider.SchemaDeclarationsFeature,
	// The migrator runs a migration body as the units its provider splits
	// it into, and runs a unit again that the engine aborted for a
	// conflict; see session.SplitMigration and session.BeginUnitTransaction.
	provider.MigrationSplitFeature,
	// An operator statement's arguments reach the provider with the types
	// their caller bound them at; see statementConn.CheckNamedValue.
	provider.TypedArgumentsFeature,
	// A failed provider call, and a lock outcome other than the one asked
	// for, read with the reason the provider wrote after the code. The SDK
	// client writes the text; the status message is never shown.
	provider.ErrorReasonsFeature,
	// An index the comparison pairs with one declared under another name
	// reaches the table's owner as one rename step; see
	// commonPlanner.indexChanges.
	provider.IndexRenamesFeature,
	// A read names the tables it found and does not describe, and the
	// comparison refuses a declaration at one in the provider's words; see
	// reader.unreadTables.
	provider.UnreadTablesFeature,
	// An offline command plans an engine with the capabilities its provider
	// states for it; see registerDefaultCapabilities.
	provider.DefaultCapabilitiesFeature,
	// Inspection reads the facets of a provider table's indexes, which the
	// comparison compares with the facets a source declares on the index;
	// see reader.tables and observedIndexRules.
	provider.ObservedIndexFacetsFeature,
	// A filtered read reports the objects of the kinds an engine names
	// database kinds, such as resource pools, beside its scope, and the
	// comparison reads them as it reads an unfiltered read's.
	provider.DatabaseKindsFeature,
	// A SQL schema reading carries the facets of its tables, columns and
	// indexes, which the host places where a YAML source places them; see
	// target.sqlFacets.
	provider.SQLFacetsFeature,
	// A provider model's count metrics join the reporting runtime, and the
	// provider counts the model's values; see modelReporter.
	provider.ReportCountsFeature,
	// A comparison counts the objects it compares, and the drift inventory
	// reports them under their change kinds; see boundCounts.
	provider.ComparisonCountsFeature,
	// A described model travels by reference in every message but the
	// description, so a request's coverage of each table grows by a few
	// names rather than by every model's definition, and a message may be
	// 64 MiB rather than 4: a schema of a few thousand tables reaches the
	// provider in one request. The SDK client sends and resolves the
	// references, and no adapter here sees one.
	provider.ModelReferencesFeature,
	provider.LargeMessagesFeature,
	// A planning service that plans no table-attached model receives each
	// table by its subject, its action and the names of its states, not its
	// whole capture; see tableOutline.
	provider.TableOutlinesFeature,
	// A list of coverage kinds that every table of a request repeats travels
	// once per message. The SDK client shares and resolves the lists, and no
	// adapter here sees a shared one.
	provider.SharedCoverageFeature,
}

// Start verifies the selected artifact and negotiates experimental services.
// A failure releases the subprocess and returns no host.
func Start(ctx context.Context, spec Spec) (*Host, error) {
	client, err := providerruntime.Start(ctx, providerruntime.Options{
		Executable: spec.Executable, SHA256: spec.SHA256, ProviderID: spec.ProviderID,
		Environment:      slices.Clone(spec.Environment),
		RequiredFeatures: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature},
		HostFeatures:     slices.Clone(hostFeatures),
	})
	if err != nil {
		return nil, err
	}
	host := &Host{client: client, description: client.Description(), artifact: strings.ToLower(spec.SHA256), targets: make(map[string]*target)}
	for _, engine := range host.description.GetEngines() {
		if err := providercatalog.CheckOwnership(spec.ProviderID, engine.GetId(), engine.GetAliases()); err != nil {
			client.Close()
			return nil, fmt.Errorf("%w: %s", provider.ErrProtocol, err)
		}
		host.targets[engine.GetId()] = &target{host: host, engine: engine}
	}
	if err := host.initializeModels(); err != nil {
		client.Close()
		return nil, err
	}
	if err = providersource.CheckYAMLKeys(host.description.GetEngines()); err != nil {
		client.Close()
		return nil, fmt.Errorf("%w: %w", provider.ErrProtocol, err)
	}
	if err = checkSQLHeaderKinds(host.description); err != nil {
		client.Close()
		return nil, err
	}
	if err = checkHCLSource(host.description); err != nil {
		client.Close()
		return nil, err
	}
	if host.goSource, err = goDirectiveClaims(host.description); err != nil {
		client.Close()
		return nil, err
	}
	if err = registerQueryDialects(host.description.GetEngines()); err != nil {
		client.Close()
		return nil, err
	}
	if err = registerDefaultCapabilities(host.description.GetEngines()); err != nil {
		client.Close()
		return nil, err
	}
	if host.selections, err = newScopeSelections(host.description); err != nil {
		client.Close()
		return nil, err
	}
	host.releases = host.provideScanners()
	return host, nil
}

// Close ends the registrations the host made for its engines and terminates
// the process. Repeated calls are harmless and never imply that an
// interrupted database operation rolled back.
func (h *Host) Close() {
	for _, release := range h.releases {
		release()
	}
	h.client.Close()
}

// Connections returns private scheme bindings for every declared target and
// alias. The application combines these with its selected engine runtime.
func (h *Host) Connections() map[string]dbschema.NativeRegistration {
	result := make(map[string]dbschema.NativeRegistration)
	for name, target := range h.targets {
		binding := dbschema.NativeRegistration{Target: name, Connector: target}
		result[name] = binding
		for _, alias := range target.engine.GetAliases() {
			result[alias] = binding
		}
	}
	return result
}

func identifierRules(rules *protocol.IdentifierRules, defaultSchema string) (identifier.Semantics, error) {
	if err := provider.ValidateIdentifierRules(rules); err != nil {
		return identifier.Semantics{}, err
	}
	comparisons := map[protocol.IdentifierRules_Comparison]identifier.Comparison{
		protocol.IdentifierRules_EXACT:                  identifier.ComparisonExact,
		protocol.IdentifierRules_ASCII_CASE_INSENSITIVE: identifier.ComparisonASCIIInsensitive,
	}
	namespaces := map[protocol.IdentifierRules_IndexNamespace]identifier.IndexNamespace{
		protocol.IdentifierRules_TABLE:  identifier.IndexNamespaceTable,
		protocol.IdentifierRules_SCHEMA: identifier.IndexNamespaceSchema,
	}
	return identifier.Semantics{DefaultSchema: defaultSchema, TableNames: comparisons[rules.GetTableNames()],
		ColumnNames: comparisons[rules.GetColumnNames()], IndexNames: comparisons[rules.GetIndexNames()], IndexNamespace: namespaces[rules.GetIndexNamespace()],
		SchemaSeparator: rules.GetSchemaSeparator()}, nil
}

func (t *target) identity(session *protocol.Session) (catalog.ProviderIdentity, error) {
	rules, err := identifierRules(t.engine.GetIdentifiers(), t.engine.GetDefaultSchema())
	if err != nil {
		return catalog.ProviderIdentity{}, err
	}
	// Version 8 binds session capability capture and owner transaction enforcement
	// to the host-owned JSON projection. Repeated protocol fields
	// are sets here; their wire order is not semantic identity. Session IDs and
	// database addresses are deliberately absent. An engine that reads its SQL
	// documents whole adds how it reads them, and one that declares scope
	// selections adds them; one that declares neither keeps the digest it has
	// without the fields.
	projection := struct {
		Version            int                          `json:"version"`
		Engine             string                       `json:"engine"`
		ProviderFeatures   []string                     `json:"provider_features"`
		EngineFeatures     []string                     `json:"engine_features"`
		SessionFeatures    []string                     `json:"session_features"`
		OfflineIdentifiers identifier.Semantics         `json:"offline_identifiers"`
		Models             []modelIdentity              `json:"models"`
		YAMLKeys           []yamlKeyIdentity            `json:"yaml_keys"`
		GoDirectives       []goDirectiveIdentity        `json:"go_directives,omitempty"`
		Comparisons        []comparisonIdentityContract `json:"comparisons"`
		Planning           []planningIdentityContract   `json:"planning"`
		SQLSchema          *sqlSchemaIdentity           `json:"sql_schema,omitempty"`
		ScopeSelections    []scopeSelectionIdentity     `json:"scope_selections,omitempty"`
	}{8,
		t.engine.GetId(),
		featureSet(t.host.description.GetFeatures()),
		featureSet(t.engine.GetFeatures()),
		featureSet(session.GetFeatures()),
		rules,
		modelIdentities(t.engine.GetValueModels()),
		yamlKeyIdentities(t.engine.GetYamlSourceKeys()),
		goDirectiveIdentities(t.engine.GetGoSourceDirectives()),
		comparisonContracts(t.engine.GetComparisons()), planningContracts(t.engine.GetPlanning()),
		sqlSchemaIdentityOf(t.engine.GetSqlSchema()), scopeSelectionIdentities(t.engine.GetScopeSelections())}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return catalog.ProviderIdentity{}, fmt.Errorf("encode negotiated provider contract: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return catalog.ProviderIdentity{
		ID: t.host.description.GetProviderId(), Version: t.host.description.GetVersion(),
		SemanticIdentity: t.host.description.GetSemanticIdentity(), ArtifactSHA256: t.host.artifact,
		ProtocolRevision: t.host.description.GetRevision(), ContractSHA256: hex.EncodeToString(sum[:]),
	}, nil
}

func featureSet(features []string) []string {
	result := make([]string, len(features))
	copy(result, features)
	slices.Sort(result)
	return slices.Compact(result)
}
