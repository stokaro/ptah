package provider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ReplayGuardFeature advertises the offline replay guard: the owner reads the
// statements of a migration replay and refuses each one whose effects it
// cannot confine to the dev database the run may change. A cleanup removes
// what a replay created inside that database, and nothing outside it, so a
// host that cannot read the dialect asks the owner before it replays. Each
// supporting engine also declares it.
const ReplayGuardFeature = "database.dev-replay-guard"

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

// ValidateGuardReplayRequest checks the negotiated service, a scope the host
// can grant, and the statements. A provisioned server is a server the run
// owns, and a run that owns the server's databases does not own the server.
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
