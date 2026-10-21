package provider

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateNormalizationFacts(representation protocol.ValueInput_Representation, value *protocol.NormalizedValue) error {
	if err := validateComparisonIdentities(value.GetRequiredObjects()); err != nil {
		return err
	}
	semantics := value.GetChangeSemantics()
	if representation != protocol.ValueInput_CHANGE {
		if semantics != nil {
			return fmt.Errorf("%w: change semantics require a change representation", ErrProtocol)
		}
		return nil
	}
	if semantics == nil || semantics.ReplacesOwner == nil || semantics.RemovesState == nil || !comparisonReason(semantics.GetReason()) || !comparisonReason(semantics.GetAccessReason()) {
		return fmt.Errorf("%w: change semantics require explicit lifecycle and effect assessments", ErrProtocol)
	}
	switch semantics.GetImpact() {
	case protocol.ChangeSemantics_UNKNOWN, protocol.ChangeSemantics_ADDITIVE, protocol.ChangeSemantics_BEHAVIORAL, protocol.ChangeSemantics_DESTRUCTIVE:
	default:
		return fmt.Errorf("%w: unrecognized change impact", ErrProtocol)
	}
	switch semantics.GetAccess() {
	case protocol.ChangeSemantics_ACCESS_UNKNOWN, protocol.ChangeSemantics_UNCHANGED, protocol.ChangeSemantics_WIDENS, protocol.ChangeSemantics_NARROWS:
	default:
		return fmt.Errorf("%w: unrecognized change access assessment", ErrProtocol)
	}
	return nil
}

func comparisonReason(reason string) bool {
	return reason != "" && utf8.ValidString(reason) && strings.TrimSpace(reason) == reason && !strings.ContainsFunc(reason, func(r rune) bool { return unicode.IsControl(r) || r == '\u2028' || r == '\u2029' })
}
