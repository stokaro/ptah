package provider

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ValidateSession checks a newly opened session's identity, identifier rules,
// and capability names. Absent capabilities remain unknown; false is an
// established answer. The SDK does not own the host's capability vocabulary.
// Invalid replies satisfy errors.Is(err, ErrProtocol).
func ValidateSession(engine string, session *protocol.Session) error {
	if session.GetId() == "" || session.GetEngine() != engine || engine == "" {
		return fmt.Errorf("%w: invalid session identity", ErrProtocol)
	}
	if err := ValidateIdentifierRules(session.GetIdentifiers()); err != nil {
		return err
	}
	for name := range session.GetCapabilities() {
		if !comparisonReason(name) {
			return fmt.Errorf("%w: invalid session capability name", ErrProtocol)
		}
	}
	return nil
}
