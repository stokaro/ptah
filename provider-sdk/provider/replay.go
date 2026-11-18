package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ReplayGuardFeature advertises the offline replay guard: the owner reads the
// statements of a migration replay and refuses each one whose effects it
// cannot confine to the dev database the run may change. A cleanup removes
// what a replay created inside that database, and nothing outside it, so a
// host that cannot read the dialect asks the owner before it replays. Each
// supporting engine also declares it.
const ReplayGuardFeature = "database.dev-replay-guard"

// ReplayTrustFeature enables ReplayScope.trusted_sql and
// OpenRequest.trusted_sql: the operator's statement that they trust the SQL a
// run executes on its dev database, with the privileges of the dev
// connection. Under it the owner's replay guard accepts a statement it refuses
// only because it cannot confine the statement's effects to the scope, and a
// dev session opened with it lifts the same refusals where the owner confines
// execution too. Trust is not ownership: it never stands for
// ReplayScope.server, never selects another guard, and never widens what a
// cleanup removes. The owner keeps every refusal that protects the run: the
// replay session's own state, an effect the cleanup cannot remove, and what
// its execution relies on.
//
// It is a two-way feature: the provider and each engine that honors trust
// list it beside ReplayGuardFeature, and the host lists it to send either
// field. A host whose operator asked for trust from a provider or an engine
// that does not list it refuses before the replay rather than drop the
// answer. Contract 25.43.
const ReplayTrustFeature = "database.dev-replay-trust"

// ReplayGuard judges replay statements without a session. It refuses a
// statement it cannot read rather than accept it.
type ReplayGuard interface {
	GuardReplay(context.Context, *protocol.GuardReplayRequest) (*protocol.GuardedReplay, error)
}

func replayGuardEngine(description *protocol.Description, name string) error {
	if !slices.Contains(description.GetFeatures(), ReplayGuardFeature) {
		return fmt.Errorf("%w: the replay guard is unavailable", ErrProtocol)
	}
	engine, _, err := valueTarget(description, name, nil)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ReplayGuardFeature) {
		return fmt.Errorf("%w: engine replay guard is unavailable", ErrProtocol)
	}
	return nil
}

// validateReplayTrustFeature refuses an engine that lists ReplayTrustFeature
// without the provider's, and a provider or an engine that lists it without
// the replay guard whose refusals trust lifts.
func validateReplayTrustFeature(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(description.GetFeatures(), ReplayTrustFeature) && !slices.Contains(description.GetFeatures(), ReplayGuardFeature) {
		return fmt.Errorf("%w: %s requires the provider's replay guard", ErrProtocol, ReplayTrustFeature)
	}
	if !slices.Contains(engine.GetFeatures(), ReplayTrustFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), ReplayTrustFeature) {
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, ReplayTrustFeature)
	}
	if !slices.Contains(engine.GetFeatures(), ReplayGuardFeature) {
		return fmt.Errorf("%w: engine %s requires the engine's replay guard", ErrProtocol, ReplayTrustFeature)
	}
	return nil
}

// ValidateGuardReplayRequest checks the negotiated service, a scope the host
// can grant, and the statements. A provisioned server is a server the run
// owns, and a run that owns the server's databases does not own the server.
// A trusted scope needs ReplayTrustFeature negotiated for the engine.
func ValidateGuardReplayRequest(description *protocol.Description, request *protocol.GuardReplayRequest) error {
	if request == nil {
		return fmt.Errorf("%w: replay guard request is missing", ErrProtocol)
	}
	if err := replayGuardEngine(description, request.GetEngine()); err != nil {
		return err
	}
	scope := request.GetScope()
	if scope.GetProvisioned() && !scope.GetServer() || scope.GetDatabases() && scope.GetServer() {
		return fmt.Errorf("%w: replay scope combines realms that exclude each other", ErrProtocol)
	}
	if scope.GetTrustedSql() && !EngineOffers(description, request.GetEngine(), ReplayTrustFeature) {
		return fmt.Errorf("%w: trusted replay SQL requires %s for engine %s", ErrProtocol, ReplayTrustFeature, request.GetEngine())
	}
	if strings.ContainsRune(scope.GetDatabase(), 0) || !utf8.ValidString(scope.GetDatabase()) {
		return fmt.Errorf("%w: invalid replay database", ErrProtocol)
	}
	for _, statement := range request.GetStatements() {
		if strings.TrimSpace(statement) == "" || !utf8.ValidString(statement) {
			return fmt.Errorf("%w: replay statement is empty or not text", ErrProtocol)
		}
	}
	return nil
}

// ValidateGuardedReplay refuses a reply that does not answer every statement
// in order, and a verdict whose operation does not match it: a refusal names
// what cannot be confined in one line, and an accepted statement names
// nothing.
func ValidateGuardedReplay(description *protocol.Description, request *protocol.GuardReplayRequest, result *protocol.GuardedReplay) error {
	if err := ValidateGuardReplayRequest(description, request); err != nil {
		return err
	}
	return ValidateGuardedReplayReply(description, request, result)
}

// ValidateGuardedReplayReply is [ValidateGuardedReplay] for a request that
// [ValidateGuardReplayRequest] accepted with description and that has not
// changed since: it checks the reply and not the request again. The SDK's
// client and server validate each request once and call it for the reply.
func ValidateGuardedReplayReply(description *protocol.Description, request *protocol.GuardReplayRequest, result *protocol.GuardedReplay) error {
	if result == nil || len(result.GetVerdicts()) != len(request.GetStatements()) {
		return fmt.Errorf("%w: replay guard does not answer every statement", ErrProtocol)
	}
	for _, verdict := range result.GetVerdicts() {
		if verdict.GetRefused() != diagnosticLine(verdict.GetOperation()) || !verdict.GetRefused() && verdict.GetOperation() != "" {
			return fmt.Errorf("%w: replay verdict and operation disagree", ErrProtocol)
		}
	}
	return nil
}

// ValidateOpenRequest checks what the contract adds to an OpenRequest: a
// session opened for trusted dev SQL needs ReplayTrustFeature negotiated for
// the engine. The engine and the address are checked where the session opens.
func ValidateOpenRequest(description *protocol.Description, request *protocol.OpenRequest) error {
	if request == nil {
		return fmt.Errorf("%w: open request is missing", ErrProtocol)
	}
	if request.GetTrustedSql() && !EngineOffers(description, request.GetEngine(), ReplayTrustFeature) {
		return fmt.Errorf("%w: a session for trusted dev SQL requires %s for engine %s", ErrProtocol, ReplayTrustFeature, request.GetEngine())
	}
	return nil
}
