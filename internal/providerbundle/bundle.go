// Package providerbundle lays out the official providers a Ptah distribution
// ships beside the ptah executable. It fetches each pinned archive through
// [providerinstall], so the archive digest and the executable digest are
// checked against the pin in this release before a byte is placed, and then
// copies the verified executable to the bundled location the host reads:
// <out>/<os>_<arch>/providers/<provider-id>/<version>/<executable>.
//
// A release build calls it once before packaging. A normal build, test run or
// `go install` never does, and nothing here resolves a newer provider release.
package providerbundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerinstall"
)

// DirName is the directory below a platform's staging root that a distribution
// copies next to the ptah executable. It is the name the host looks for.
const DirName = "providers"

// Options selects what [Stage] fetches and where it writes.
type Options struct {
	// Out is the staging root. One directory per platform is created below it.
	Out string
	// BaseURL replaces the directory archives are fetched from, for an internal
	// mirror or an offline release build. The pinned digests still decide what
	// is accepted. Empty selects the official release.
	BaseURL string
	// Client performs the downloads. Nil selects the installer default.
	Client *http.Client
	// Pins looks up the pinned release of an engine. Nil selects the pins
	// compiled into [providercatalog].
	Pins func(engineID string) (providercatalog.Pin, bool)
	// Platforms lists the platforms to stage. Empty selects every platform a
	// pin must cover.
	Platforms []providercatalog.Platform
}

// Staged is one executable placed in the staging root.
type Staged struct {
	EngineID   string
	Platform   providercatalog.Platform
	Version    string
	Executable string
	SHA256     string
}

// Stage fetches and places the pinned provider of every engine the
// distribution bundles, for each requested platform. An engine that bundles by
// default but has no pin, or a pin that does not cover a requested platform, is
// an error: a release must not ship a distribution that silently lacks the
// provider. The staging directory of each platform is replaced, never merged
// into.
func Stage(ctx context.Context, options Options) ([]Staged, error) {
	if options.Out == "" {
		return nil, errors.New("providerbundle: the staging root is empty")
	}
	platforms := options.Platforms
	if len(platforms) == 0 {
		platforms = providercatalog.Platforms()
	}
	lookup := options.Pins
	if lookup == nil {
		lookup = providercatalog.LookupPin
	}
	scratch, err := os.MkdirTemp("", "ptah-providerbundle-")
	if err != nil {
		return nil, fmt.Errorf("providerbundle: create the scratch store: %w", err)
	}
	defer func() { _ = os.RemoveAll(scratch) }()

	var staged []Staged
	for _, platform := range platforms {
		platformRoot := filepath.Join(options.Out, platform.OS+"_"+platform.Arch)
		if err := os.RemoveAll(platformRoot); err != nil {
			return nil, fmt.Errorf("providerbundle: clear %s: %w", platformRoot, err)
		}
		if err := os.MkdirAll(platformRoot, 0o750); err != nil {
			return nil, fmt.Errorf("providerbundle: create %s: %w", platformRoot, err)
		}
		for _, entry := range providercatalog.Entries() {
			if !entry.BundledByDefault {
				continue
			}
			installer := providerinstall.Installer{
				Root: scratch, BaseURL: options.BaseURL, Client: options.Client, Pins: lookup, Platform: platform,
			}
			result, err := installer.Install(ctx, entry.EngineID)
			if err != nil {
				return nil, fmt.Errorf("providerbundle: %s for %s: %w", entry.EngineID, platform, err)
			}
			destination := filepath.Join(platformRoot, DirName, filepath.FromSlash(result.ProviderID), result.Version, filepath.Base(result.Executable))
			if err := place(result.Executable, destination, result.SHA256); err != nil {
				return nil, fmt.Errorf("providerbundle: %s for %s: %w", entry.EngineID, platform, err)
			}
			staged = append(staged, Staged{
				EngineID: entry.EngineID, Platform: platform, Version: result.Version,
				Executable: destination, SHA256: result.SHA256,
			})
		}
	}
	return staged, nil
}

// place copies the verified executable and checks the copy against the digest
// again, so what a package contains is what the pin names.
func place(source, destination, wantDigest string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(destination), err)
	}
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open the verified executable: %w", err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		return fmt.Errorf("create %s: %w", destination, err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(out, hash), in)
	closeErr := out.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("write %s: %w", destination, err)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != wantDigest {
		return fmt.Errorf("%s has SHA-256 %s after the copy, want %s", destination, got, wantDigest)
	}
	return nil
}
