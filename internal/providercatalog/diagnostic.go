package providercatalog

import (
	"fmt"

	"ptah.run/internal/providerdiagnostic"
)

// Missing reports an absent configured installation for an official engine.
// Callers must establish absence, rather than use this for startup failures.
func Missing(entry Entry) *providerdiagnostic.Diagnostic {
	message := fmt.Sprintf("%s is supported through the official Ptah %s plugin, but no configured installation was found. Install and configure %s: %s. Plugin repository: %s.",
		entry.DisplayName, entry.DisplayName, entry.ProviderID, entry.SetupURL, entry.RepositoryURL)
	if entry.BundledByDefault {
		message += " The full distribution requires this bundled provider; a standalone/source installation or incomplete bundle needs it installed or restored."
	} else {
		message += " This provider is an optional installation."
	}
	return &providerdiagnostic.Diagnostic{Code: "provider_missing", EngineID: entry.EngineID, ProviderID: entry.ProviderID,
		RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL, Message: message}
}
