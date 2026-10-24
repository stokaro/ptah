package providerconfig

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
)

// CodeStoreInvalid is the diagnostic code for an install record that exists and
// cannot be used: unreadable, malformed, or naming another engine.
const CodeStoreInvalid = "provider_store_invalid"

// Resolver selects the provider that serves an engine. It holds locations,
// never state: every call reads the file system afresh.
type Resolver struct {
	// StoreRoot is the user store `ptah providers install` writes to, normally
	// [providerinstall.DefaultRoot]. Empty disables the store candidate.
	StoreRoot string
	// Platform selects the install record. The zero value is the running
	// platform.
	Platform providerrelease.Platform
	// Host is the contract version an installation's minimum is checked
	// against. The zero value is [providerrelease.HostContract].
	Host providerrelease.Contract
}

// DefaultResolver returns the resolver of the running Ptah: the default user
// store. A store that cannot be located is left empty and never consulted.
// Nothing is read from PATH, the working directory or a project file.
func DefaultResolver() Resolver {
	var result Resolver
	if root, err := providerinstall.DefaultRoot(); err == nil {
		result.StoreRoot = root
	}
	return result
}

// Select resolves the provider for one engine selector. The first rule that
// matches decides and later rules are not consulted:
//
//  1. the installation the manifest selects for the engine;
//  2. the user-store installation the install record of the engine's provider
//     names for this platform;
//  3. none, which is a provider_missing diagnostic carrying the install
//     instruction.
//
// A candidate exists when its install record exists. A record that exists and
// cannot be used is refused, and so is one whose minimum contract the host
// does not meet: neither is skipped. Select does not hash the executable;
// [Start] and [Installation.Verify] do, and refuse. Other versions in the store
// are not read.
func (r Resolver) Select(config Configuration, selector string) (Installation, string, error) {
	installation, target, err := config.selected(selector)
	if !errors.Is(err, errUnclaimed) {
		return installation, target, err
	}
	entry, official := providercatalog.Lookup(selector)
	if !official {
		return Installation{}, "", errors.New("no configured provider owns the selected engine")
	}
	if r.StoreRoot == "" {
		return Installation{}, "", providercatalog.Missing(entry)
	}
	platform := r.Platform
	if platform == (providerrelease.Platform{}) {
		platform = providerrelease.Running()
	}
	record, executable, err := providerinstall.ReadRecord(r.StoreRoot, entry.ProviderID, platform)
	if errors.Is(err, os.ErrNotExist) {
		return Installation{}, "", providercatalog.Missing(entry)
	}
	if err != nil {
		return Installation{}, "", storeInvalid(entry, err)
	}
	engines, err := recordEngines(entry, record)
	if err != nil {
		return Installation{}, "", storeInvalid(entry, err)
	}
	host := r.Host
	if host == (providerrelease.Contract{}) {
		host = providerrelease.HostContract()
	}
	stored := Installation{
		Spec:    providerhost.Spec{ProviderID: entry.ProviderID, Executable: executable, SHA256: record.SHA256},
		Engines: engines,
		origin:  originStore,
	}
	if !providerrelease.Meets(host, record.MinContract) {
		return Installation{}, "", stored.Diagnostic(entry.EngineID, "provider_incompatible", fmt.Sprintf(
			"The installed %s provider %s needs contract %s; this Ptah speaks contract %s. Run `ptah providers install %s` to install a release this Ptah can run.",
			entry.DisplayName, record.Version, record.MinContract.Requirement(), host, entry.EngineID))
	}
	return stored, entry.EngineID, nil
}

// recordEngines returns the engines an installation declares, with their
// aliases, from the catalog entries of the engines the record names. The
// started process must declare exactly these.
func recordEngines(entry providercatalog.Entry, record providerinstall.Record) ([]Engine, error) {
	if !slices.Contains(record.Engines, entry.EngineID) {
		return nil, fmt.Errorf("the install record names engines %q, not %q", record.Engines, entry.EngineID)
	}
	engines := make([]Engine, 0, len(record.Engines))
	for _, name := range record.Engines {
		owner, official := providercatalog.Lookup(name)
		if !official || owner.EngineID != name || owner.ProviderID != entry.ProviderID {
			return nil, fmt.Errorf("the install record names %q, which is not an official engine of %s", name, entry.ProviderID)
		}
		engines = append(engines, Engine{ID: owner.EngineID, Aliases: owner.Aliases})
	}
	return engines, nil
}

func storeInvalid(entry providercatalog.Entry, cause error) *providerdiagnostic.Diagnostic {
	return &providerdiagnostic.Diagnostic{
		Code: CodeStoreInvalid, EngineID: entry.EngineID, ProviderID: entry.ProviderID,
		RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL, Cause: cause,
		Message: fmt.Sprintf("The install record of the %s provider cannot be used: %v. Run `ptah providers install %s` to replace it. No other provider is tried.",
			entry.DisplayName, cause, entry.EngineID),
	}
}
