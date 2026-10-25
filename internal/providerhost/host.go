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
	// The migrator hands a migration's statement timeout to the provider with
	// each unit; see session.BoundsStatements.
	provider.StatementTimeoutFeature,
	// A dev URL of an engine whose provider creates dev realms enters a
	// realm; see DevDatabaseOwner.EnterRealm.
	provider.DevRealmsFeature,
	// `ptah sql lint` reads a provider's statement facts; see
	// sqllint.Options.Owner.
	provider.LintStatementFactsFeature,
	// The query builder renders an engine from the spelling its provider
	// declares; see registerQueryDialects.
	provider.QueryDialectFeature,
	// `ptah inference` keeps a generation's run state and vectors in a
	// provider's embedding store; see EmbeddingSession.
	provider.EmbeddingStoreFeature,
	// A provider's planner receives the operator's planning policy, and its
	// comparisons resolve the requests the operator spells, such as
	// --rotate-secret; see target.planningPolicy and writtenRequests.
	provider.PlanningPolicyFeature,
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
	if host.goSource, err = goDirectiveClaims(host.description); err != nil {
		client.Close()
		return nil, err
	}
	if err = registerQueryDialects(host.description.GetEngines()); err != nil {
		client.Close()
		return nil, err
	}
	return host, nil
}

// Close terminates the process. Repeated calls are harmless and never imply
// that an interrupted database operation rolled back.
func (h *Host) Close() { h.client.Close() }

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
		ColumnNames: comparisons[rules.GetColumnNames()], IndexNames: comparisons[rules.GetIndexNames()], IndexNamespace: namespaces[rules.GetIndexNamespace()]}, nil
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
	// documents whole adds how it reads them; one that declares nothing keeps
	// the digest it has without the field.
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
		sqlSchemaIdentityOf(t.engine.GetSqlSchema())}
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
