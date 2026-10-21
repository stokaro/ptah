package providerhost

import (
	"errors"
	"fmt"

	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ptaherr"
)

// nativeOutcomeError translates the provider contract at the host boundary so
// migration policy and embedders need no SDK dependency to recognize uncertainty.
// Keep the cause: a refusal and an uncertain result can both appear in a joined
// cleanup failure, and the uncertainty must not be lost.
func nativeOutcomeError(err error) error {
	if errors.Is(err, provider.ErrOutcomeUnknown) && !errors.Is(err, ptaherr.ErrOutcomeUnknown) {
		return fmt.Errorf("%w: %w", ptaherr.ErrOutcomeUnknown, err)
	}
	return err
}
