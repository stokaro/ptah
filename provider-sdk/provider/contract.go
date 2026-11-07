package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ContractMinor is the minor version of the contract this SDK speaks. It
// counts the compatible additions within the major Revision. A host built
// with this SDK announces Revision.ContractMinor, and a provider built with it
// may require at most that minor from its host.
const ContractMinor uint32 = 35

// The features below each enable an addition to an existing message. An
// addition is used only when both sides list its feature: the provider in its
// description, the host in DescribeRequest.host_features. A peer that lists
// none of them gets the messages of contract 25.0.
const (
	// StatementTimeoutFeature enables ExecuteRequest.statement_timeout_nanoseconds
	// and Execution.statement_timed_out. A provider lists it in its features.
	StatementTimeoutFeature = "units.statement-timeout"
	// LintStatementFactsFeature enables the statement kind, missing primary key
	// and required capability lint facts. An engine lists it beside
	// LintFeature.
	LintStatementFactsFeature = "lint.statement-facts"
	// QueryDialectFeature enables Engine.query_dialect in a description. A
	// provider declares the field; the host lists the feature.
	QueryDialectFeature = "query.dialect"
)

// TwoWayFeatures returns the features this SDK knows that enable an addition to
// an existing message, in a new slice. A host lists the ones it acts on in
// Options.HostFeatures; a provider sees what the host listed in HostFeatures.
// A new RPC needs no host feature, since a host that does not know it never
// calls it, unless it comes with an addition to an existing message: the
// embedding store declares Engine.embeddings, the common-object wire declares
// Engine.common_objects, and a migration split comes with the abort flags of
// Execution and TransactionResult, so their calls are two-way too.
func TwoWayFeatures() []string {
	return []string{StatementTimeoutFeature, LintStatementFactsFeature, DevRealmsFeature, QueryDialectFeature, EmbeddingStoreFeature,
		CommonObjectsFeature, TextUnitsFeature, PlanningPolicyFeature, PlanningStatementStyleFeature, EntryKeysFeature, HCLSourceFeature, ScopeSelectionsFeature,
		EncodeGroupsFeature, CommonAccessFeature, SchemaDeclarationsFeature, SourceLimitsFeature, PlanningLifecycleFeature, SensitiveInputsFeature, ColumnsFeature,
		DatabaseContextFeature, ValueContextFeature, CommonViewFacetsFeature, ObservedColumnsFeature, ObservedPrimaryKeysFeature, ObservedIndexesFeature, PlanningObjectContextFeature,
		PlanningContextInventoryFeature, MigrationSplitFeature, ComparisonCountsFeature, IndexMethodsFeature, TypedArgumentsFeature, ErrorReasonsFeature,
		IndexRenamesFeature, ReverseChangesFeature, UnreadTablesFeature, DatabaseKindsFeature, SQLFacetsFeature, ReportCountsFeature,
		ObservedIndexFacetsFeature, SchemaSubtreeCoverageFeature, PasswordInputsFeature, HCLPlatformFeature, DefaultCapabilitiesFeature, LintTransactionOwnerFeature,
		IndexKindsFeature, ModelPredecessorsFeature, RoleScopeReasonsFeature, GoPlacementsFeature, PrimaryKeyChangesFeature, ModelReferencesFeature,
		LargeMessagesFeature, TableOutlinesFeature, SharedCoverageFeature, OwnPredecessorsFeature, ObservedConstraintsFeature, ConstraintChangesFeature,
		IndexNullsOrderFeature, CommonNamespacesFeature, SQLTableClausesFeature, ReservedSeedsFeature, IndexPredicatesFeature, LinkedReleaseLinesFeature,
		EnumColumnsFeature, ObservedUniqueConstraintsFeature, ColumnCollationsFeature, ViewCheckOptionsFeature, DeclaredRelationsFeature,
		TableOwnershipFeature, DeclaredObjectsFeature, RenderOmissionsFeature, TableGrantsFeature, SQLRolePasswordsFeature, SQLStatementFactsFeature,
		QueryLexicalFeature}
}

// ContractError reports a host and a provider that cannot work together:
// another major, or a host whose minor is below the provider's minimum. It
// wraps ErrProtocol, and its text names both versions.
type ContractError struct {
	Host    *protocol.ContractVersion
	Minimum *protocol.ContractVersion
}

// Error names the host's version and the provider's minimum.
func (e *ContractError) Error() string {
	return fmt.Sprintf("provider requires contract %s or a later minor of it; this host speaks %s",
		ContractString(e.Minimum), ContractString(e.Host))
}

// Unwrap identifies an incompatible protocol exchange.
func (e *ContractError) Unwrap() error { return ErrProtocol }

// ContractString writes a version as major.minor.
func ContractString(version *protocol.ContractVersion) string {
	return fmt.Sprintf("%d.%d", version.GetMajor(), version.GetMinor())
}

// Contract returns the version this SDK speaks.
func Contract() *protocol.ContractVersion {
	return &protocol.ContractVersion{Major: Revision, Minor: ContractMinor}
}

// MinimumContract returns the oldest host contract a provider works with. A
// description that declares none, as one from an SDK of minor 0 does, needs
// minor 0 of its revision.
func MinimumContract(description *protocol.Description) *protocol.ContractVersion {
	if minimum := description.GetMinContract(); minimum != nil {
		return proto.CloneOf(minimum)
	}
	return &protocol.ContractVersion{Major: description.GetRevision()}
}

// HostContract returns the version a host announced. A request that announces
// none, as one from an SDK of minor 0 does, speaks minor 0 of its revision.
func HostContract(request *protocol.DescribeRequest) *protocol.ContractVersion {
	if host := request.GetHostContract(); host != nil {
		return proto.CloneOf(host)
	}
	return &protocol.ContractVersion{Major: request.GetRevision()}
}

// CheckContract accepts a host that shares the provider's major and whose
// minor is at least the provider's minimum. Anything else is a
// *ContractError.
func CheckContract(host, minimum *protocol.ContractVersion) error {
	if host.GetMajor() != minimum.GetMajor() || host.GetMinor() < minimum.GetMinor() {
		return &ContractError{Host: proto.CloneOf(host), Minimum: proto.CloneOf(minimum)}
	}
	return nil
}

// validateMinimumContract refuses a minimum of another major, and one above
// the minor this SDK speaks: a provider cannot require what its own SDK does
// not know.
func validateMinimumContract(description *protocol.Description) error {
	minimum := description.GetMinContract()
	if minimum == nil {
		return nil
	}
	if minimum.GetMajor() != description.GetRevision() || minimum.GetMinor() > ContractMinor {
		return fmt.Errorf("%w: minimum contract %s is outside contract %s", ErrProtocol, ContractString(minimum), ContractString(Contract()))
	}
	return nil
}

// ValidateHostFeatures refuses an empty or repeated feature name and one with
// a space or a control character in it. A name this SDK does not know is
// accepted: a newer host lists features an older provider ignores.
func ValidateHostFeatures(features []string) error {
	for i, feature := range features {
		if feature == "" || strings.ContainsFunc(feature, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) ||
			slices.Contains(features[:i], feature) {
			return fmt.Errorf("%w: invalid or repeated host feature", ErrProtocol)
		}
	}
	return nil
}

// NegotiatedDescription returns a copy of description with every two-way
// feature the host did not list removed, from the provider's features and
// from each engine's. The query dialects are removed unless the host lists
// QueryDialectFeature and their lexical rules unless it lists
// QueryLexicalFeature, the embedding store declarations unless it lists
// EmbeddingStoreFeature, the common-object declarations unless it lists
// CommonObjectsFeature, the HCL claims unless it lists HCLSourceFeature, the
// claims of a table's platform group unless it lists HCLPlatformFeature, the
// scope selections unless it lists ScopeSelectionsFeature, the reversal
// services unless it lists ReverseChangesFeature, the database kinds unless
// it lists DatabaseKindsFeature, the model reports unless it lists
// ReportCountsFeature, the schema separator of each engine's identifier rules
// unless it lists SchemaSubtreeCoverageFeature, the offline capability facts
// unless it lists DefaultCapabilitiesFeature, the index-kind claims unless
// it lists IndexKindsFeature, the model predecessors unless it lists
// ModelPredecessorsFeature, those that name an earlier identity of their
// own model unless it lists OwnPredecessorsFeature, the enum columns
// unless it lists EnumColumnsFeature, the table ownership of its planning
// services unless it lists TableOwnershipFeature, and the declared kinds of
// its comparisons unless it lists DeclaredObjectsFeature. An engine that
// keeps no GRANT common objects loses TableGrantsFeature. A model keeps its
// source formats without HCLPlatformFeature: a platform claim's models need
// not list HCL. Without
// EntryKeysFeature, the YAML claims of a COLUMN or INDEX scope and the
// YAML and Go claims that inherit common attributes are removed, since their
// owner needs what the host would not send, and a model left with no YAML
// claim loses YAML from its source formats. Both sides validate with this
// copy, so neither sends an addition the other did not ask for. Services and
// everything the contract had at minor 0 are kept.
func NegotiatedDescription(description *protocol.Description, hostFeatures []string) *protocol.Description {
	negotiated := proto.CloneOf(description)
	if negotiated == nil {
		return nil
	}
	unlisted := func(feature string) bool {
		return slices.Contains(TwoWayFeatures(), feature) && !slices.Contains(hostFeatures, feature)
	}
	negotiated.Features = slices.DeleteFunc(negotiated.Features, unlisted)
	for _, engine := range negotiated.GetEngines() {
		engine.Features = slices.DeleteFunc(engine.Features, unlisted)
		if !slices.Contains(hostFeatures, QueryDialectFeature) {
			engine.QueryDialect = nil
		}
		if !slices.Contains(hostFeatures, EmbeddingStoreFeature) {
			engine.Embeddings = nil
		}
		negotiateCommonObjects(engine, hostFeatures)
		negotiateCommonNamespaces(engine, hostFeatures)
		negotiatePlanningObjects(engine, hostFeatures)
		negotiateHCLSource(engine, hostFeatures)
		negotiateIndexKinds(engine, hostFeatures)
		negotiateModelPredecessors(negotiated.GetProviderId(), engine, hostFeatures)
		if !slices.Contains(hostFeatures, ScopeSelectionsFeature) {
			engine.ScopeSelections = nil
		}
		if !slices.Contains(hostFeatures, SchemaDeclarationsFeature) {
			engine.Declarations = nil
			if len(engine.GetPlanning()) == 0 {
				engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == PlanningLifecycleFeature })
			}
		}
		if !slices.Contains(hostFeatures, SourceLimitsFeature) {
			engine.GoHeaderKinds = nil
		}
		negotiateLaterAdditions(engine, hostFeatures)
		if len(engine.GetGoHeaderKinds())+len(engine.GetHclSource().GetHeaderKinds()) == 0 {
			engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == SourceLimitsFeature })
		}
		if !slices.Contains(hostFeatures, EntryKeysFeature) {
			withoutEntryKeys(engine)
		}
	}
	return negotiated
}

// negotiateLaterAdditions removes the engine fields of contracts 25.19 and
// later whose features the host does not list: the reversal services, the
// database kinds, the model reports, the schema separator, the offline
// capability facts of the engine and of its release lines, the table
// outlines of its planning services, the table clauses and the statement
// facts of its SQL grammar, the linked engines of its release lines, its enum
// columns, the table ownership of its planning services, the declared
// kinds of its comparisons and the lexical rules of its query dialect. It
// also removes TableGrantsFeature from an engine
// the negotiation left without GRANT common objects.
func negotiateLaterAdditions(engine *protocol.Engine, hostFeatures []string) {
	negotiateTableOutlines(engine, hostFeatures)
	negotiateTableOwnership(engine, hostFeatures)
	negotiateDeclaredObjects(engine, hostFeatures)
	negotiateSQLTableClauses(engine, hostFeatures)
	negotiateSQLStatementFacts(engine, hostFeatures)
	negotiateLinkedReleaseLines(engine, hostFeatures)
	negotiateEnumColumns(engine, hostFeatures)
	negotiateQueryLexical(engine, hostFeatures)
	negotiateTableGrants(engine)
	if !slices.Contains(hostFeatures, ReverseChangesFeature) {
		engine.Reversals = nil
	}
	if !slices.Contains(hostFeatures, DatabaseKindsFeature) {
		engine.DatabaseKinds = nil
	}
	if !slices.Contains(hostFeatures, ReportCountsFeature) {
		engine.Reports = nil
	}
	if !slices.Contains(hostFeatures, SchemaSubtreeCoverageFeature) && engine.GetIdentifiers() != nil {
		engine.Identifiers.SchemaSeparator = ""
	}
	if !slices.Contains(hostFeatures, DefaultCapabilitiesFeature) {
		engine.DefaultCapabilities = nil
		for _, line := range engine.GetReleaseLines() {
			line.Capabilities = nil
		}
	}
}

// negotiateHCLSource removes the HCL claims unless the host lists
// HCLSourceFeature, and the claims of a table's platform group unless it lists
// HCLPlatformFeature.
func negotiateHCLSource(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, HCLSourceFeature) {
		engine.HclSource = nil
	}
	if !slices.Contains(hostFeatures, HCLPlatformFeature) && engine.HclSource != nil {
		engine.HclSource.Blocks = slices.DeleteFunc(engine.HclSource.Blocks, func(block *protocol.HCLSourceBlock) bool {
			return block.GetSite() == protocol.HCLSourceBlock_TABLE_PLATFORM
		})
	}
}

// withoutEntryKeys removes the source claims that need EntryKeysFeature, and
// YAML from the source formats of a model that no YAML claim names after it.
func withoutEntryKeys(engine *protocol.Engine) {
	engine.YamlSourceKeys = slices.DeleteFunc(engine.YamlSourceKeys, func(claim *protocol.YAMLSourceKey) bool {
		// Only an entry scope inherits, so the scope decides alone.
		return claim.GetScope() != protocol.YAMLSourceKey_DOCUMENT && claim.GetScope() != protocol.YAMLSourceKey_TABLE
	})
	engine.GoSourceDirectives = slices.DeleteFunc(engine.GoSourceDirectives, func(claim *protocol.GoSourceDirective) bool {
		return len(claim.GetInherited()) != 0
	})
	claimed := make(map[string]bool)
	for _, claim := range engine.GetYamlSourceKeys() {
		for _, kind := range claim.GetModels() {
			claimed[kind] = true
		}
	}
	for _, model := range engine.GetValueModels() {
		if !claimed[model.GetKind()] {
			model.SourceFormats = slices.DeleteFunc(model.SourceFormats, func(format protocol.SourceDocument_Format) bool {
				return format == protocol.SourceDocument_YAML
			})
		}
	}
}

type hostFeaturesKey struct{}

// ContextWithHostFeatures returns a copy of ctx that carries the features the
// host listed. The SDK server sets it on every call it passes to a service.
func ContextWithHostFeatures(ctx context.Context, features []string) context.Context {
	return context.WithValue(ctx, hostFeaturesKey{}, slices.Clone(features))
}

// HostFeatures returns the features the host listed, in a new slice. A service
// checks it before it uses a two-way feature: a host of contract 25.0 lists
// none, and a reply that uses an addition it did not list is refused at the
// SDK server.
func HostFeatures(ctx context.Context) []string {
	features, _ := ctx.Value(hostFeaturesKey{}).([]string)
	return slices.Clone(features)
}

func negotiateCommonObjects(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, CommonObjectsFeature) {
		engine.CommonObjects = nil
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool {
			return feature == CommonAccessFeature || feature == CommonViewFacetsFeature || feature == RoleScopeReasonsFeature || feature == CommonNamespacesFeature
		})
	}
	if !slices.Contains(hostFeatures, CommonAccessFeature) && engine.CommonObjects != nil {
		engine.CommonObjects.Kinds = slices.DeleteFunc(engine.CommonObjects.Kinds, commonAccessKind)
		if len(engine.CommonObjects.Kinds) == 0 {
			engine.CommonObjects = nil
			engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool {
				return feature == CommonObjectsFeature || feature == RoleScopeReasonsFeature
			})
		}
	}
}
