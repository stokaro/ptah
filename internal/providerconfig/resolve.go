package providerconfig

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerinstall"
)

// CodeUnsupported is the diagnostic code for an official engine whose pin has
// no artifact for the running platform.
const CodeUnsupported = providerinstall.CodeUnsupported

// BundledDir is the directory below the one that holds the resolved ptah
// executable where a distribution places official providers.
const BundledDir = "providers"

// Resolver selects the provider that serves an engine. It holds locations and
// the pin lookup, never state: every call reads the file system afresh.
type Resolver struct {
	// BundledRoot is the directory a distribution places providers in,
	// normally [BundledDir] beside the resolved ptah executable. Empty disables
	// the bundled candidate.
	BundledRoot string
	// StoreRoot is the user store `ptah providers install` writes to, normally
	// [providerinstall.DefaultRoot]. Empty disables the store candidate.
	StoreRoot string
	// Platform selects the pinned artifact. The zero value is the running
	// platform.
	Platform providercatalog.Platform
	// Pins looks up the pinned release of an engine. Nil selects the pins
	// compiled into [providercatalog].
	Pins func(engineID string) (providercatalog.Pin, bool)
}

// DefaultResolver returns the resolver of the running Ptah: the bundled
// directory beside the resolved executable and the default user store. A
// location that cannot be determined is left empty and its candidate is never
// consulted. Nothing is read from PATH, the working directory or a project file.
func DefaultResolver() Resolver {
	var result Resolver
	if executable, err := os.Executable(); err == nil {
		if resolved, err := filepath.EvalSymlinks(executable); err == nil {
			result.BundledRoot = filepath.Join(filepath.Dir(resolved), BundledDir)
		}
	}
	if root, err := providerinstall.DefaultRoot(); err == nil {
		result.StoreRoot = root
	}
	return result
}

// BundledPath returns where a distribution places the executable of one
// pinned artifact below root: <root>/<provider-id>/<version>/<executable>.
// A distribution is built for one platform, so the path has no platform part.
func BundledPath(root, providerID, version, executable string) string {
	return filepath.Join(root, filepath.FromSlash(providerID), version, executable)
}

// Select resolves the provider for one engine selector. The first rule that
// matches decides and later rules are not consulted:
//
//  1. the installation the manifest selects for the engine;
//  2. the bundled official provider pinned in this release;
//  3. the official provider in the user store with the pinned version and
//     executable digest;
//  4. none, which is a provider_missing diagnostic carrying the install
//     instruction.
//
// A candidate that exists is returned even when its bytes are wrong. Select does
// not hash it: [Start] and [Installation.Verify] do, and refuse. A candidate is
// absent only when its executable does not exist, so a corrupt bundled file is
// never replaced by a good stored copy. Only the pinned version is looked at;
// other versions on disk are not read.
func (r Resolver) Select(config Configuration, selector string) (Installation, string, error) {
	installation, target, err := config.selected(selector)
	if !errors.Is(err, errUnclaimed) {
		return installation, target, err
	}
	entry, official := providercatalog.Lookup(selector)
	if !official {
		return Installation{}, "", errors.New("no configured provider owns the selected engine")
	}
	lookup := r.Pins
	if lookup == nil {
		lookup = providercatalog.LookupPin
	}
	pin, pinned := lookup(entry.EngineID)
	if !pinned {
		return Installation{}, "", providercatalog.Missing(entry)
	}
	platform := r.Platform
	if platform == (providercatalog.Platform{}) {
		platform = providercatalog.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	}
	artifact, covered := pin.Artifact(platform)
	if !covered {
		return Installation{}, "", &providerdiagnostic.Diagnostic{
			Code: CodeUnsupported, EngineID: entry.EngineID, ProviderID: entry.ProviderID,
			RepositoryURL: entry.RepositoryURL, SetupURL: entry.SetupURL,
			Message: fmt.Sprintf("this Ptah release pins no %s provider archive for %s, so no bundled or installed provider can serve it; select an executable you obtained and verified yourself through the provider manifest: %s",
				entry.DisplayName, platform, entry.SetupURL),
		}
	}
	candidates := []struct {
		path   string
		origin origin
	}{
		{bundledCandidate(r.BundledRoot, entry, pin, artifact), originBundled},
		{storeCandidate(r.StoreRoot, entry, pin, platform, artifact), originStore},
	}
	for _, candidate := range candidates {
		if candidate.path == "" || !exists(candidate.path) {
			continue
		}
		return Installation{
			Spec:    providerhost.Spec{ProviderID: entry.ProviderID, Executable: candidate.path, SHA256: artifact.ExecutableSHA256},
			Engines: []Engine{{ID: entry.EngineID, Aliases: entry.Aliases}},
			origin:  candidate.origin,
		}, entry.EngineID, nil
	}
	return Installation{}, "", providercatalog.MissingInstallable(entry)
}

func bundledCandidate(root string, entry providercatalog.Entry, pin providercatalog.Pin, artifact providercatalog.Artifact) string {
	if root == "" {
		return ""
	}
	return BundledPath(root, entry.ProviderID, pin.Version, artifact.Executable)
}

func storeCandidate(root string, entry providercatalog.Entry, pin providercatalog.Pin, platform providercatalog.Platform, artifact providercatalog.Artifact) string {
	if root == "" {
		return ""
	}
	return filepath.Join(providerinstall.Dir(root, entry.ProviderID, pin.Version, platform), artifact.Executable)
}

// exists reports whether something is at path. Any failure other than absence
// counts as presence, so an unreadable or malformed candidate is refused by
// verification instead of being skipped.
func exists(path string) bool {
	_, err := os.Lstat(path)
	return !errors.Is(err, fs.ErrNotExist)
}
