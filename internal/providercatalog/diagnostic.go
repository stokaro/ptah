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

// Identified reports that a connection made through a URL of another engine
// reached a server of an official provider-only engine, which the server's
// version banner named. Ptah never reads, plans or writes such a server through
// the engine the URL selected: the operator selects the engine's provider with
// a URL of the engine's own scheme. The message says so whether or not the
// provider is installed, because the URL selects it in either case.
func Identified(entry Entry, scheme string) *providerdiagnostic.Diagnostic {
	own := entry.EngineID
	if len(entry.URLSchemes) > 0 {
		own = entry.URLSchemes[0]
	}
	return &providerdiagnostic.Diagnostic{Code: "provider_engine_identified", EngineID: entry.EngineID, ProviderID: entry.ProviderID,
		RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL,
		Message: fmt.Sprintf("The server behind this %s:// URL reports itself as %s. %s is supported through the official Ptah %s plugin, "+
			"never through the engine a %s:// URL selects. Connect with a %s:// URL, which selects the plugin; "+
			"if it is not installed, run `ptah providers install %s`. Setup: %s. Plugin repository: %s.",
			scheme, entry.DisplayName, entry.DisplayName, entry.DisplayName, scheme, own, entry.EngineID,
			entry.SetupURL, entry.RepositoryURL)}
}
