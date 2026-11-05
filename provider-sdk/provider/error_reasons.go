package provider

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ErrorReasonsFeature lets a provider say why a call failed. A failed call's
// gRPC status carries one ProviderDiagnostic in its details, and a lock
// receipt whose outcome is not the one its operation asked for carries one in
// LockResult.diagnostic. Contract 25.16. An execution result that is not
// APPLIED carries one in Execution.diagnostic, and a transaction result whose
// outcome is not the one its action asked for carries one in
// TransactionResult.diagnostic. Contract 25.18. It is a two-way feature of
// the provider alone: a provider lists it, the host lists it in its host
// features, and a host shows a reason only when the negotiated description
// lists it.
//
// Listing the feature is the provider's promise about the text. The provider
// writes each reason itself: it names what failed and why, and it never
// quotes statement text, a value, an address or a credential, because the
// host prints it. A reason is never an error's text passed on, since a
// driver's error can quote any of those. The SDK checks the shape of a
// reason, with ValidateDiagnostic; only the provider can keep its content
// clean. The status message and the message of an execution or transaction
// result are never shown, whatever the feature.
const ErrorReasonsFeature = "errors.reasons"

// ValidateDiagnostic refuses a diagnostic that is missing or whose reason is
// not one line a host may show: empty, longer than 512 bytes, not valid
// UTF-8, with white space around it, or with a control character or a line
// or paragraph separator in it. It cannot tell whether the text quotes a
// value; that is the provider's to keep out.
func ValidateDiagnostic(diagnostic *protocol.ProviderDiagnostic) error {
	if !diagnosticLine(diagnostic.GetReason()) {
		return fmt.Errorf("%w: a provider's reason is one line of at most %d bytes of text", ErrProtocol, maxDiagnosticBytes)
	}
	return nil
}
