package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// LintTransactionOwnerFeature lets a lint check say who opens the transaction
// each file of the run runs in, in CheckLintRequest.transaction_owner. It is a
// two-way feature: a provider lists it beside LintFeature, and so does each
// engine whose rules read the field. A request names an owner other than the
// migrator only when the engine and the host list it. Contract 25.23.
const LintTransactionOwnerFeature = "lint.transaction-owner"

// validateLintTransactionOwner refuses an owner the contract does not name,
// and an owner other than the migrator for an engine that did not negotiate
// the feature: such an engine judges every file as one its migrator wraps, so
// a rule would report a transaction the file's tool never opens.
func validateLintTransactionOwner(engine *protocol.Engine, owner protocol.LintTransactionOwner) error {
	if _, known := protocol.LintTransactionOwner_name[int32(owner)]; !known {
		return fmt.Errorf("%w: lint check names an unknown transaction owner %d", ErrProtocol, owner)
	}
	if owner != protocol.LintTransactionOwner_LINT_TRANSACTION_OWNER_MIGRATOR && !slices.Contains(engine.GetFeatures(), LintTransactionOwnerFeature) {
		return fmt.Errorf("%w: lint check names a transaction owner, which needs %s", ErrProtocol, LintTransactionOwnerFeature)
	}
	return nil
}
