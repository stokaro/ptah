// Package providerinstall installs an official provider release into the user
// store and reads the store back. It is the only code that downloads a
// provider.
//
// Install chooses the newest release of the engine's provider whose minimum
// contract this Ptah meets, or the release the caller names. It checks the
// release's ptah-provider.json and archive against the release's own
// checksums.txt before reading the archive, takes only the one executable the
// manifest names, publishes it with a single directory rename, and writes an
// install record with the executable's digest. Installs into one provider's
// directory take turns on a lock file there, across processes. It never runs
// the executable and never edits the provider manifest.
package providerinstall

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"ptah.run/internal/providerarchive"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerrelease"
)

// maxArchiveSize bounds the bytes read from a server for one archive.
const maxArchiveSize = 2 << 30

// Diagnostic codes of an install that did not complete.
const (
	CodeUnknownEngine       = "provider_unknown_engine"
	CodeVersionInvalid      = "provider_version_invalid"
	CodeVersionRequired     = "provider_version_required"
	CodeNoRelease           = "provider_no_release"
	CodeNoCompatibleRelease = "provider_no_compatible_release"
	CodeReleaseIncompatible = "provider_release_incompatible"
	CodeAssetMissing        = "provider_release_asset_missing"
	CodeReleaseInvalid      = "provider_release_invalid"
	CodeUnsupported         = "provider_platform_unsupported"
	CodeChecksumMissing     = "provider_checksum_missing"
	CodeChecksumInvalid     = "provider_checksum_invalid"
	CodeChecksumMismatch    = "provider_checksum_mismatch"
	CodeDownloadFailed      = "provider_download_failed"
	CodeArchiveInvalid      = "provider_archive_invalid"
	CodeStoreFailed         = "provider_store_failed"
)

// Installer installs provider releases below Root. The zero value is not
// usable; set Root. The other fields default to the running platform, the
// host's contract version, the official release source and a client with a
// header timeout.
type Installer struct {
	// Root is the user store, normally [DefaultRoot].
	Root string
	// BaseURL replaces https://github.com in every download address: a mirror
	// that serves the GitHub release download layout,
	// <base>/<owner>/<repo>/releases/download/<tag>/<asset>. A mirror cannot
	// list releases, so Install requires a version when BaseURL is set. It
	// changes where bytes come from and nothing about what is accepted.
	BaseURL string
	// APIURL is the GitHub REST API root releases are listed through. Empty
	// selects [DefaultAPIURL].
	APIURL string
	// Client performs every request. Nil selects a client with sane timeouts.
	Client *http.Client
	// Platform selects the artifact. The zero value is the running platform.
	Platform providerrelease.Platform
	// Host is the contract version releases are judged against. The zero value
	// is [providerrelease.HostContract].
	Host providerrelease.Contract
}

// Result describes an installation that is present and verified.
type Result struct {
	EngineID    string
	ProviderID  string
	Version     string
	MinContract providerrelease.Contract
	Platform    providerrelease.Platform
	// Executable is the absolute path of the verified executable.
	Executable string
	// SHA256 is the executable's digest, as the install record holds it.
	SHA256 string
	// Installed is false when the install record already named this release
	// and its executable still matched, so no archive was downloaded.
	Installed bool
}

// chosen is a release install decided on, with the manifest bytes it read.
type chosen struct {
	release  release
	manifest providerrelease.Manifest
	bytes    []byte
}

// Install installs a release of the official provider of the engine selector
// names. An empty version selects the newest release whose minimum contract the
// host meets; otherwise version names one release, with or without its
// leading "v". A release the install record already names, with an executable
// that still matches the record, is a success that downloads no archive.
func (i Installer) Install(ctx context.Context, selector, version string) (Result, error) {
	entry, found := providercatalog.Lookup(selector)
	if !found {
		return Result{}, fail(providercatalog.Entry{EngineID: selector}, CodeUnknownEngine, nil,
			"%q is not an official provider engine; engines with a catalog entry can be installed", selector)
	}
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	if version != "" && !providerrelease.ValidVersion(version) {
		return Result{}, fail(entry, CodeVersionInvalid, nil, "--version %q is not a semantic version such as 0.2.0", version)
	}
	if i.Root == "" {
		return Result{}, fail(entry, CodeStoreFailed, nil, "the provider store location is empty")
	}
	if i.BaseURL != "" && version == "" {
		return Result{}, fail(entry, CodeVersionRequired, nil,
			"a download mirror cannot list releases, so --download-base-url needs --version naming the %s provider release to install", entry.DisplayName)
	}
	base := i.BaseURL
	if base == "" {
		base = DefaultDownloadURL
	}
	if !validAddress(base) {
		return Result{}, fail(entry, CodeDownloadFailed, nil, "the download base URL %q is not an absolute http or https URL", i.BaseURL)
	}
	owner, repo, found := entry.GitHubRepository()
	if !found {
		return Result{}, fail(entry, CodeNoRelease, nil, "the catalog names no GitHub release source for %s", entry.ProviderID)
	}
	choice, err := i.choose(ctx, entry, base, owner, repo, version)
	if err != nil {
		return Result{}, err
	}
	return i.installChosen(ctx, entry, choice)
}

// choose decides the release to install: the one version names, or the newest
// listed release whose minimum contract the host meets.
func (i Installer) choose(ctx context.Context, entry providercatalog.Entry, base, owner, repo, version string) (chosen, error) {
	host := i.host()
	if version != "" {
		candidate := layoutRelease(base, owner, repo, version)
		choice, err := i.readManifest(ctx, entry, candidate)
		if err != nil {
			return chosen{}, err
		}
		if !providerrelease.Meets(host, choice.manifest.MinContract) {
			return chosen{}, fail(entry, CodeReleaseIncompatible, nil,
				"the %s provider %s needs contract %s; this Ptah speaks contract %s. Install a release whose minimum this Ptah meets, or omit --version to install the newest one",
				entry.EngineID, version, choice.manifest.MinContract.Requirement(), host)
		}
		return choice, nil
	}
	listed, err := i.list(ctx, entry, owner, repo)
	if err != nil {
		return chosen{}, err
	}
	if len(listed.candidates) == 0 {
		message := fmt.Sprintf("%s has no published release with %s", entry.ProviderID, providerrelease.ManifestName)
		if len(listed.unmarked) > 0 {
			message += fmt.Sprintf("; releases without it cannot be installed by command: %s", strings.Join(listed.unmarked, ", "))
		}
		return chosen{}, fail(entry, CodeNoRelease, nil,
			"%s. Select a provider you obtained and verified yourself through the provider manifest: %s", message, entry.SetupURL)
	}
	var refused []string
	for _, candidate := range listed.candidates {
		choice, err := i.readManifest(ctx, entry, candidate)
		if err != nil {
			return chosen{}, err
		}
		if providerrelease.Meets(host, choice.manifest.MinContract) {
			return choice, nil
		}
		refused = append(refused, fmt.Sprintf("%s needs %s", candidate.version, choice.manifest.MinContract))
	}
	return chosen{}, fail(entry, CodeNoCompatibleRelease, nil,
		"no release of %s has a minimum contract this Ptah meets; this Ptah speaks contract %s, and the newest releases need: %s",
		entry.ProviderID, host, strings.Join(refused[:min(len(refused), 5)], ", "))
}

// readManifest downloads and checks the ptah-provider.json of one release. A
// malformed manifest, or one that contradicts the catalog or its tag, ends the
// search: it is a damaged release, not a release to skip.
func (i Installer) readManifest(ctx context.Context, entry providercatalog.Entry, candidate release) (chosen, error) {
	data, address, err := i.fetchAsset(ctx, entry, candidate, providerrelease.ManifestName, providerrelease.MaxManifestSize)
	if err != nil {
		return chosen{}, err
	}
	manifest, err := providerrelease.DecodeManifest(data)
	if err == nil {
		err = manifest.CheckRelease(entry, candidate.tag)
	}
	if err != nil {
		return chosen{}, fail(entry, CodeReleaseInvalid, err, "release %s of %s publishes an invalid %s at %s: %v",
			candidate.tag, entry.ProviderID, providerrelease.ManifestName, address, err)
	}
	return chosen{release: candidate, manifest: manifest, bytes: data}, nil
}

// installChosen checks the chosen release's checksums, then publishes its
// executable and the install record, unless the record already names it.
//
// The store work runs under the install lock of the provider's directory, from
// reading the record to writing it. Parallel installs of one release then
// download it once: the first publishes it, and each later one finds the record
// naming it. Without the lock each install replaces the record and the
// installation that the others are reading, which Windows refuses.
func (i Installer) installChosen(ctx context.Context, entry providercatalog.Entry, choice chosen) (result Result, err error) {
	platform := i.platform()
	artifact, found := choice.manifest.Artifact(platform)
	if !found {
		return Result{}, fail(entry, CodeUnsupported, nil,
			"the %s provider %s publishes no archive for %s; install it on another platform or select a verified executable through the provider manifest: %s",
			entry.DisplayName, choice.manifest.Version, platform, entry.SetupURL)
	}
	sums, _, err := i.fetchAsset(ctx, entry, choice.release, providerrelease.ChecksumsName, providerrelease.MaxChecksumsSize)
	if err != nil {
		return Result{}, err
	}
	if err := checkDigest(entry, sums, providerrelease.ManifestName, digestOf(choice.bytes)); err != nil {
		return Result{}, err
	}
	archiveDigest, err := checksum(entry, sums, artifact.Archive)
	if err != nil {
		return Result{}, err
	}
	dir := ProviderDir(i.Root, entry.ProviderID)
	lock, err := lockProvider(ctx, dir)
	if err != nil {
		return Result{}, fail(entry, CodeStoreFailed, err, "cannot lock the provider store %s: %v", dir, err)
	}
	defer func() {
		if releaseErr := lock.Close(); releaseErr != nil && err == nil {
			result, err = Result{}, fail(entry, CodeStoreFailed, releaseErr, "cannot release the install lock in %s: %v", dir, releaseErr)
		}
	}()
	target := Dir(i.Root, entry.ProviderID, choice.manifest.Version, platform)
	result = Result{
		EngineID: entry.EngineID, ProviderID: entry.ProviderID, Version: choice.manifest.Version,
		MinContract: choice.manifest.MinContract, Platform: platform, Executable: filepath.Join(target, artifact.Executable),
	}
	if record, executable, err := ReadRecord(i.Root, entry.ProviderID, platform); err == nil &&
		record.Version == result.Version && executable == result.Executable && verified(executable, record.SHA256) {
		result.SHA256 = record.SHA256
		return result, nil
	}
	address, found := choice.release.asset(artifact.Archive)
	if !found {
		return Result{}, fail(entry, CodeAssetMissing, nil, "release %s of %s has no %s asset", choice.release.tag, entry.ProviderID, artifact.Archive)
	}
	digest, err := i.fetchAndPublish(ctx, entry, address, artifact, archiveDigest, target)
	if err != nil {
		return Result{}, err
	}
	record := Record{
		Schema: RecordSchema, ProviderID: entry.ProviderID, Version: result.Version, MinContract: result.MinContract,
		Engines: choice.manifest.Engines, SHA256: digest,
		Executable: path.Join(result.Version, platform.OS+"_"+platform.Arch, artifact.Executable),
	}
	if err := WriteRecord(i.Root, platform, record); err != nil {
		return Result{}, fail(entry, CodeStoreFailed, err, "cannot write the install record %s: %v", RecordPath(i.Root, entry.ProviderID, platform), err)
	}
	result.SHA256, result.Installed = digest, true
	return result, nil
}

// fetchAsset downloads one small asset of a release into memory.
func (i Installer) fetchAsset(ctx context.Context, entry providercatalog.Entry, candidate release, name string, limit int64) ([]byte, string, error) {
	address, found := candidate.asset(name)
	if !found {
		return nil, "", fail(entry, CodeAssetMissing, nil, "release %s of %s has no %s asset", candidate.tag, entry.ProviderID, name)
	}
	if !validAddress(address) {
		return nil, address, fail(entry, CodeDownloadFailed, nil, "the address %q of %s is not an absolute http or https URL", address, name)
	}
	var data []byte
	err := i.get(ctx, address, func(body io.Reader) error {
		read, err := io.ReadAll(io.LimitReader(body, limit+1))
		if err == nil && int64(len(read)) > limit {
			err = fmt.Errorf("larger than %d bytes", limit)
		}
		data = read
		return err
	})
	if errors.Is(err, errNotFound) {
		return nil, address, fail(entry, CodeAssetMissing, nil,
			"release %s of %s has no %s at %s; the release does not exist or does not publish it", candidate.tag, entry.ProviderID, name, address)
	}
	if err != nil {
		return nil, address, fail(entry, CodeDownloadFailed, err, "cannot download %s: %v", address, err)
	}
	return data, address, nil
}

// get requests address and hands the body of a 200 answer to read. A 404 is
// errNotFound.
func (i Installer) get(ctx context.Context, address string, read func(io.Reader) error) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return err
	}
	response, err := i.client().Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	switch response.StatusCode {
	case http.StatusOK:
		return read(response.Body)
	case http.StatusNotFound:
		return errNotFound
	default:
		return errors.New(response.Status)
	}
}

// checksum returns the digest checksums.txt lists for name.
func checksum(entry providercatalog.Entry, sums []byte, name string) (string, error) {
	digest, err := providerrelease.Checksum(sums, name)
	switch {
	case errors.Is(err, providerrelease.ErrChecksumMissing):
		return "", fail(entry, CodeChecksumMissing, err, "the release's %s has no entry for %s; nothing was installed", providerrelease.ChecksumsName, name)
	case err != nil:
		return "", fail(entry, CodeChecksumInvalid, err, "the release's %s cannot verify %s: %v; nothing was installed", providerrelease.ChecksumsName, name, err)
	}
	return digest, nil
}

// checkDigest compares got with the digest checksums.txt lists for name.
func checkDigest(entry providercatalog.Entry, sums []byte, name, got string) error {
	want, err := checksum(entry, sums, name)
	if err != nil {
		return err
	}
	if got != want {
		return fail(entry, CodeChecksumMismatch, nil,
			"%s has SHA-256 %s, but the release's %s lists %s; nothing was installed. A mirror cannot change what is accepted", name, got, providerrelease.ChecksumsName, want)
	}
	return nil
}

// fetchAndPublish downloads the archive, checks it against wantDigest, extracts
// the executable and publishes it at target. It returns the executable's digest.
func (i Installer) fetchAndPublish(ctx context.Context, entry providercatalog.Entry, address string, artifact providerrelease.Artifact, wantDigest, target string) (string, error) {
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fail(entry, CodeStoreFailed, err, "cannot create the provider store %s: %v", parent, err)
	}
	// The staging directory sits beside the target, so publishing is a rename
	// inside one directory and cannot cross a filesystem.
	staging, err := os.MkdirTemp(parent, ".staging-")
	if err != nil {
		return "", fail(entry, CodeStoreFailed, err, "cannot create a staging directory in %s: %v", parent, err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	payload := filepath.Join(staging, "payload")
	if err := os.Mkdir(payload, 0o700); err != nil {
		return "", fail(entry, CodeStoreFailed, err, "cannot create a staging directory in %s: %v", parent, err)
	}
	archive := filepath.Join(staging, "archive"+archiveSuffix(artifact.Archive))
	if err := i.download(ctx, entry, address, archive, artifact.Archive, wantDigest); err != nil {
		return "", err
	}
	digest, err := extract(entry, archive, filepath.Join(payload, artifact.Executable))
	if err != nil {
		return "", err
	}
	executable := filepath.Join(target, artifact.Executable)
	return digest, publish(entry, payload, target, executable, digest)
}

func (i Installer) download(ctx context.Context, entry providercatalog.Entry, address, dst, name, wantDigest string) error {
	file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create the download file: %v", err)
	}
	hash := sha256.New()
	var size int64
	err = i.get(ctx, address, func(body io.Reader) error {
		n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(body, maxArchiveSize+1))
		size = n
		if err != nil {
			return fmt.Errorf("%w: %w", errInterrupted, err)
		}
		return nil
	})
	closeErr := file.Close()
	switch {
	case errors.Is(err, errNotFound):
		return fail(entry, CodeAssetMissing, nil, "the archive %s is not at %s", name, address)
	case errors.Is(err, errInterrupted):
		return fail(entry, CodeDownloadFailed, err, "the download of %s was interrupted: %v", address, err)
	case err != nil:
		return fail(entry, CodeDownloadFailed, err, "cannot download %s: %v", address, err)
	case size > maxArchiveSize:
		return fail(entry, CodeDownloadFailed, nil, "%s is larger than %d bytes", address, int64(maxArchiveSize))
	case closeErr != nil:
		return fail(entry, CodeStoreFailed, closeErr, "cannot write the download file: %v", closeErr)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != wantDigest {
		return fail(entry, CodeChecksumMismatch, nil,
			"the archive %s from %s has SHA-256 %s, but the release's %s lists %s; nothing was extracted. A mirror cannot change what is accepted",
			name, address, got, providerrelease.ChecksumsName, wantDigest)
	}
	return nil
}

// extract writes the executable from the verified archive to dst and returns
// its digest. dst is created with 0700 and removed with its staging directory
// on error.
func extract(entry providercatalog.Entry, archive, dst string) (string, error) {
	file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return "", fail(entry, CodeStoreFailed, err, "cannot create the executable: %v", err)
	}
	hash := sha256.New()
	copyErr := providerarchive.CopyExecutable(archive, filepath.Base(dst), io.MultiWriter(file, hash))
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	switch {
	case copyErr != nil:
		return "", fail(entry, CodeArchiveInvalid, copyErr, "the archive is not a valid provider archive: %v", copyErr)
	case closeErr != nil:
		return "", fail(entry, CodeStoreFailed, closeErr, "cannot write the executable: %v", closeErr)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// publish renames the verified payload directory to target. A target that
// exists and verifies belongs to a concurrent install and is kept. A target that
// exists and fails verification is moved aside and replaced.
func publish(entry providercatalog.Entry, payload, target, executable, wantDigest string) error {
	var renameErr error
	for range 3 {
		if verified(executable, wantDigest) {
			return nil
		}
		if _, err := os.Lstat(target); err == nil {
			aside := fmt.Sprintf("%s.replaced-%d", target, time.Now().UnixNano())
			if err := os.Rename(target, aside); err == nil {
				_ = os.RemoveAll(aside)
			}
		}
		if renameErr = os.Rename(payload, target); renameErr == nil {
			return nil
		}
	}
	if verified(executable, wantDigest) {
		return nil
	}
	return fail(entry, CodeStoreFailed, renameErr, "cannot publish the installation at %s: %v", target, renameErr)
}

// verified reports whether executable is a regular file with the given SHA-256.
func verified(executable, wantDigest string) bool {
	file, err := os.Open(executable)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false
	}
	return strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), wantDigest)
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func archiveSuffix(name string) string {
	if strings.HasSuffix(name, ".zip") {
		return ".zip"
	}
	return ".tar.gz"
}

func validAddress(address string) bool {
	parsed, err := url.Parse(address)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func (i Installer) client() *http.Client {
	if i.Client != nil {
		return i.Client
	}
	return defaultClient()
}

// defaultClient is shared by every Installer that names no client, so the
// requests of one install reuse their connections.
var defaultClient = sync.OnceValue(func() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default transport is an *http.Transport
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: transport}
})

func (i Installer) platform() providerrelease.Platform {
	if i.Platform == (providerrelease.Platform{}) {
		return providerrelease.Running()
	}
	return i.Platform
}

func (i Installer) host() providerrelease.Contract {
	if i.Host == (providerrelease.Contract{}) {
		return providerrelease.HostContract()
	}
	return i.Host
}

func fail(entry providercatalog.Entry, code string, cause error, format string, args ...any) *providerdiagnostic.Diagnostic {
	return &providerdiagnostic.Diagnostic{
		Code: code, EngineID: entry.EngineID, ProviderID: entry.ProviderID, RepositoryURL: entry.RepositoryURL,
		SetupURL: entry.SetupURL, Message: fmt.Sprintf(format, args...), Cause: cause,
	}
}
