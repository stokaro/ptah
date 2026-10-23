package providerbundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"ptah.run/internal/providerarchive"
	"ptah.run/internal/providercatalog"
)

// ArchivePath returns the slash-separated name a release archive gives the
// bundled executable of one pinned artifact: providers/<provider-id>/<version>/<executable>.
func ArchivePath(entry providercatalog.Entry, pin providercatalog.Pin, artifact providercatalog.Artifact) string {
	return path.Join(DirName, entry.ProviderID, pin.Version, artifact.Executable)
}

// VerifyArchive checks that the release archive of one platform contains the
// pinned executable of every engine the distribution bundles, at the bundled
// location and with the pinned digest. It reads the archive and nothing else.
func VerifyArchive(archive string, platform providercatalog.Platform, pins func(engineID string) (providercatalog.Pin, bool)) error {
	if pins == nil {
		pins = providercatalog.LookupPin
	}
	for _, entry := range providercatalog.Entries() {
		if !entry.BundledByDefault {
			continue
		}
		pin, found := pins(entry.EngineID)
		if !found {
			return fmt.Errorf("providerbundle: this release pins no %s provider", entry.DisplayName)
		}
		artifact, found := pin.Artifact(platform)
		if !found {
			return fmt.Errorf("providerbundle: the %s pin has no artifact for %s", entry.DisplayName, platform)
		}
		hash := sha256.New()
		name := ArchivePath(entry, pin, artifact)
		if err := providerarchive.CopyExecutable(archive, name, hash); err != nil {
			return fmt.Errorf("providerbundle: %s: %w", filepath.Base(archive), err)
		}
		if got := hex.EncodeToString(hash.Sum(nil)); got != artifact.ExecutableSHA256 {
			return fmt.Errorf("providerbundle: %s: %s has SHA-256 %s, the pin names %s", filepath.Base(archive), name, got, artifact.ExecutableSHA256)
		}
	}
	return nil
}

// VerifyArchives runs [VerifyArchive] on the host archive of every platform in
// dir, named ptah_<version>_<os>_<arch>.tar.gz or .zip. A platform with no
// archive is an error, so a glob that stopped matching cannot pass.
func VerifyArchives(dir string, platforms []providercatalog.Platform, pins func(engineID string) (providercatalog.Pin, bool)) ([]string, error) {
	if len(platforms) == 0 {
		platforms = providercatalog.Platforms()
	}
	var verified []string
	for _, platform := range platforms {
		var matches []string
		for _, extension := range []string{".tar.gz", ".zip"} {
			found, err := filepath.Glob(filepath.Join(dir, "ptah_*_"+platform.OS+"_"+platform.Arch+extension))
			if err != nil {
				return nil, fmt.Errorf("providerbundle: %w", err)
			}
			matches = append(matches, found...)
		}
		if len(matches) != 1 {
			return nil, fmt.Errorf("providerbundle: want exactly one host archive for %s in %s, found %d (%s)",
				platform, dir, len(matches), strings.Join(matches, ", "))
		}
		if err := VerifyArchive(matches[0], platform, pins); err != nil {
			return nil, err
		}
		verified = append(verified, matches[0])
	}
	return verified, nil
}
