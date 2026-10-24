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
const ContractMinor uint32 = 2

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
// embedding store declares Engine.embeddings, so its calls are two-way too.
func TwoWayFeatures() []string {
	return []string{StatementTimeoutFeature, LintStatementFactsFeature, DevRealmsFeature, QueryDialectFeature, EmbeddingStoreFeature}
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
// QueryDialectFeature, and the embedding store declarations unless it lists
// EmbeddingStoreFeature. Both sides validate with this copy, so neither sends
// an addition the other did not ask for. Services and everything the
// contract had at minor 0 are kept.
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
	}
	return negotiated
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
