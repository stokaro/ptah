package providerselect

import (
	"strings"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
)

// Flags that carry a value naming an engine, in the order they are read.
const (
	FlagDatabaseURL = "db-url"
	FlagDialect     = "dialect"
	FlagDevURL      = "dev-url"
)

// Selector reads the engine the targets name: the first non-blank value that
// maps to one. A catalog name or alias maps to its engine, a `docker://<engine>`
// dev URL to that engine, a URL to its lower-cased scheme, and a --dialect value
// to itself. It returns "" when no target names an engine.
func Selector(targets []Target) string {
	for _, target := range targets {
		value := strings.TrimSpace(target.Value)
		if value == "" {
			continue
		}
		if entry, known := providercatalog.Lookup(value); known {
			return entry.EngineID
		}
		if named, found := dockerEngine(target.Flag, value); found {
			return named
		}
		if scheme, _, found := strings.Cut(value, ":"); found {
			return strings.ToLower(scheme)
		}
		if target.Flag == FlagDialect {
			return strings.ToLower(value)
		}
	}
	return ""
}

// dockerEngine reads the engine a `docker://<engine>/...` dev URL names. Only
// the dev URL carries one: the URL asks its owner for a database, so the owner
// is the provider of that engine.
func dockerEngine(flag, value string) (string, bool) {
	rest, found := strings.CutPrefix(value, "docker://")
	if !found || flag != FlagDevURL {
		return "", false
	}
	name, _, _ := strings.Cut(rest, "/")
	name, _, _ = strings.Cut(name, "?")
	return strings.ToLower(name), name != ""
}

// validateTargets checks every explicit target against the chosen provider
// before it starts. A target the provider does not own is refused with the
// provider_target_mismatch diagnostic.
func validateTargets(targets []Target, installation providerconfig.Installation, target string) error {
	for _, named := range targets {
		value := strings.TrimSpace(named.Value)
		if value == "" {
			continue
		}
		selector := value
		if engine, found := dockerEngine(named.Flag, value); found {
			selector = engine
		} else if entry, found := providercatalog.Lookup(value); found {
			selector = entry.EngineID
		} else if scheme, _, found := strings.Cut(value, ":"); found {
			selector = strings.ToLower(scheme)
		}
		if installation.Claims()[selector] != target {
			return installation.Diagnostic(target, "provider_target_mismatch", "The --"+named.Flag+" target is not owned by the selected provider engine.")
		}
	}
	return nil
}
