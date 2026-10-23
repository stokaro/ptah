// Package providerconfig reads explicit provider installations and resolves
// engine ownership without starting executables or consulting PATH.
package providerconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"ptah.run/internal/atlassource"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
)

// FormatVersion identifies manifests with explicit engine ownership.
const FormatVersion = 2

const maxBytes = 1024 * 1024

var engineName = regexp.MustCompile(`^[a-z][a-z0-9_+\-]*$`)

// Engine declares the identity and URL aliases expected from an installation.
type Engine struct {
	ID      string   `json:"id"`
	Aliases []string `json:"aliases,omitempty"`
}

// Installation authorizes exact executable bytes for the declared engines.
// An entry alone never authorizes downloading or changing those bytes.
type Installation struct {
	providerhost.Spec
	Engines  []Engine `json:"engines"`
	Disabled bool     `json:"disabled,omitempty"`

	// origin records where the installation came from. The zero value is a
	// manifest entry. It is never read from or written to a manifest.
	origin origin
}

// Configuration is a versioned explicit installation manifest.
type Configuration struct {
	FormatVersion int            `json:"format_version"`
	Providers     []Installation `json:"providers"`
}

// Load reads one explicitly selected manifest. It never executes an entry.
func Load(path string) (Configuration, error) {
	file, err := os.Open(path)
	if err != nil {
		return Configuration{}, fmt.Errorf("read provider manifest: %w", err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return Configuration{}, fmt.Errorf("read provider manifest: %w", err)
	}
	return Decode(data)
}

// Decode validates the whole manifest, including conflicts in unused entries.
// A malformed installation cannot become an implicit first-match selection.
func Decode(data []byte) (Configuration, error) {
	if len(data) > maxBytes {
		return Configuration{}, fmt.Errorf("provider manifest exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config Configuration
	if err := decoder.Decode(&config); err != nil {
		return Configuration{}, fmt.Errorf("parse provider manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Configuration{}, fmt.Errorf("provider manifest must contain one JSON document")
	}
	if config.FormatVersion != FormatVersion {
		return Configuration{}, fmt.Errorf("provider manifest requires format_version %d", FormatVersion)
	}
	if err := config.validate(); err != nil {
		return Configuration{}, err
	}
	return config, nil
}

func (c Configuration) validate() error {
	owners := make(map[string]string)
	providers := make(map[string]bool)
	for _, installation := range c.Providers {
		if strings.TrimSpace(installation.ProviderID) == "" || providers[installation.ProviderID] {
			return fmt.Errorf("provider manifest requires unique nonempty provider IDs")
		}
		providers[installation.ProviderID] = true
		if !filepath.IsAbs(installation.Executable) {
			return fmt.Errorf("provider %q requires an absolute executable path", installation.ProviderID)
		}
		checksum, err := hex.DecodeString(installation.SHA256)
		if err != nil || len(checksum) != sha256.Size {
			return fmt.Errorf("provider %q requires a SHA-256 checksum", installation.ProviderID)
		}
		if len(installation.Engines) == 0 {
			return fmt.Errorf("provider %q must declare its engines", installation.ProviderID)
		}
		for _, target := range installation.Engines {
			names := append([]string{target.ID}, target.Aliases...)
			if err := providercatalog.CheckOwnership(installation.ProviderID, target.ID, target.Aliases); err != nil {
				return err
			}
			for _, name := range names {
				if !engineName.MatchString(name) || atlassource.IsReservedScheme(name) {
					return fmt.Errorf("provider %q declares an invalid or reserved engine name %q", installation.ProviderID, name)
				}
				if owner, exists := owners[name]; exists {
					return fmt.Errorf("provider engine name %q has multiple declarations (%q and %q)", name, owner, installation.ProviderID)
				}
				owners[name] = installation.ProviderID
			}
		}
	}
	return nil
}

// errUnclaimed says the manifest names no installation for the engine.
var errUnclaimed = errors.New("manifest claims no such engine")

// selected resolves one explicit engine selector against the manifest alone: it
// is selection rule 1. A disabled entry remains distinguishable from an absent
// entry and never selects a different provider. An engine the manifest does not
// claim yields errUnclaimed, so the caller can continue with the next rule.
func (c Configuration) selected(selector string) (Installation, string, error) {
	if err := c.validate(); err != nil {
		return Installation{}, "", err
	}
	name := strings.ToLower(strings.TrimSpace(selector))
	if entry, known := providercatalog.Lookup(name); known {
		name = entry.EngineID
	}
	for _, installation := range c.Providers {
		for alias, target := range installation.Claims() {
			if alias == name {
				if installation.Disabled {
					return Installation{}, "", installation.Diagnostic(target, "provider_disabled", "The configured provider is disabled; enable it explicitly in the trusted manifest.")
				}
				return installation, target, nil
			}
		}
	}
	return Installation{}, "", errUnclaimed
}

// Claims returns every canonical engine and alias with its canonical target.
func (i Installation) Claims() map[string]string {
	result := make(map[string]string)
	for _, target := range i.Engines {
		result[target.ID] = target.ID
		for _, alias := range target.Aliases {
			result[alias] = target.ID
		}
	}
	return result
}

// Diagnostic reports the actual failure of a configured installation, without
// disguising a disabled or untrusted executable as an absent installation.
func (i Installation) Diagnostic(engine, code, message string) *providerdiagnostic.Diagnostic {
	result := &providerdiagnostic.Diagnostic{Code: code, EngineID: engine, ProviderID: i.ProviderID, Message: message}
	if entry, found := providercatalog.Lookup(engine); found {
		result.RepositoryURL, result.SetupURL = entry.RepositoryURL, entry.SetupURL
		result.Message += " Setup: " + entry.SetupURL
	}
	return result
}
