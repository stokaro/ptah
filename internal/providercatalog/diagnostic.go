package providercatalog

import (
	"fmt"

	"ptah.run/internal/providerdiagnostic"
)

// Missing reports that no installation of an official engine was found. The
// message carries the install instruction, because every official provider is
// installed separately from Ptah. Callers must establish absence, rather than
// use this for startup failures.
func Missing(entry Entry) *providerdiagnostic.Diagnostic {
	return &providerdiagnostic.Diagnostic{Code: "provider_missing", EngineID: entry.EngineID, ProviderID: entry.ProviderID,
		RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL,
		Message: fmt.Sprintf("%s is supported through the official Ptah %s plugin, which is installed separately, and no installation was found. "+
			"Run `ptah providers install %s` to install the newest release this Ptah can run, "+
			"or select a provider you verified yourself through the provider manifest: %s. Plugin repository: %s.",
			entry.DisplayName, entry.DisplayName, entry.EngineID, entry.SetupURL, entry.RepositoryURL)}
}
