package providercatalog

import (
	"fmt"

	"ptah.run/internal/providerdiagnostic"
)

// Missing reports that no installation of an official engine was found and
// that this Ptah release pins no provider for it. Callers must establish
// absence, rather than use this for startup failures.
func Missing(entry Entry) *providerdiagnostic.Diagnostic {
	return missing(entry, fmt.Sprintf("%s is supported through the official Ptah %s plugin, but this Ptah release pins no provider release and no configured installation was found. Install and configure %s: %s. Plugin repository: %s.",
		entry.DisplayName, entry.DisplayName, entry.ProviderID, entry.SetupURL, entry.RepositoryURL))
}

// MissingInstallable reports that no bundled or installed provider of an
// official engine was found, for an engine whose release pins a provider. The
// message carries the install instruction.
func MissingInstallable(entry Entry) *providerdiagnostic.Diagnostic {
	return missing(entry, fmt.Sprintf("%s is supported through the official Ptah %s plugin, but no bundled or installed provider was found. "+
		"Run `ptah providers install %s` to install the release pinned by this Ptah, "+
		"or select a provider you verified yourself through the provider manifest: %s. Plugin repository: %s.",
		entry.DisplayName, entry.DisplayName, entry.EngineID, entry.SetupURL, entry.RepositoryURL))
}

func missing(entry Entry, message string) *providerdiagnostic.Diagnostic {
	if entry.BundledByDefault {
		message += " The full distribution requires this bundled provider; a standalone/source installation or incomplete bundle needs it installed or restored."
	} else {
		message += " This provider is an optional installation."
	}
	return &providerdiagnostic.Diagnostic{Code: "provider_missing", EngineID: entry.EngineID, ProviderID: entry.ProviderID,
		RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL, Message: message}
}
