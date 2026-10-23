// Package providerinstall installs the pinned official provider of one engine
// into the user store. It is the only code that downloads a provider. It checks
// the archive against the pinned digest before reading it, takes only the one
// expected executable out, checks that against the pinned digest too, and
// publishes the result with a single directory rename. It never runs the
// executable and never edits the provider manifest.
package providerinstall

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"ptah.run/internal/providerarchive"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
)

// maxArchiveSize bounds the bytes read from a server for one archive.
const maxArchiveSize = 2 << 30

// Diagnostic codes of an install that did not complete.
const (
	CodeUnknownEngine    = "provider_unknown_engine"
	CodeNoRelease        = "provider_no_release_pinned"
	CodeUnsupported      = "provider_platform_unsupported"
	CodeDownloadFailed   = "provider_download_failed"
	CodeArchiveUntrusted = "provider_archive_untrusted"
	CodeArchiveInvalid   = "provider_archive_invalid"
	CodeExecutableBad    = "provider_executable_untrusted"
	CodeStoreFailed      = "provider_store_failed"
)

// Installer installs pinned providers below Root. The zero value is not usable;
// set Root. The other fields default to the running platform, the official
// repository and a client with a header timeout.
type Installer struct {
	// Root is the user store, normally [DefaultRoot].
	Root string
	// BaseURL replaces the directory archives are fetched from. It changes where
	// bytes come from and nothing about what is accepted.
	BaseURL string
	// Client performs the download. Nil selects a client with sane timeouts.
	Client *http.Client
	// Pins looks up the pinned release of an engine. Nil selects the pins
	// compiled into [providercatalog]. A test supplies its own to pin a fixture
	// release.
	Pins func(engineID string) (providercatalog.Pin, bool)
	// Platform selects the artifact. The zero value is the running platform.
	Platform providercatalog.Platform
}

// Result describes an installation that is present and verified.
type Result struct {
	EngineID   string
	ProviderID string
	Version    string
	Platform   providercatalog.Platform
	// Executable is the absolute path of the verified executable.
	Executable string
	// SHA256 is the executable's digest.
	SHA256 string
	// Installed is false when a verified installation was already present and
	// nothing was downloaded.
	Installed bool
}

// DefaultRoot returns os.UserCacheDir()/ptah/providers.
func DefaultRoot() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate the user cache directory: %w", err)
	}
	return filepath.Join(cache, "ptah", "providers"), nil
}

// Dir returns the directory that holds one installation:
// <root>/<provider-id>/<version>/<os>_<arch>.
func Dir(root, providerID, version string, platform providercatalog.Platform) string {
	return filepath.Join(root, filepath.FromSlash(providerID), version, platform.OS+"_"+platform.Arch)
}

// Install installs the pinned provider of an official engine. A verified
// installation that is already present is a success that downloads nothing.
func (i Installer) Install(ctx context.Context, selector string) (Result, error) {
	entry, found := providercatalog.Lookup(selector)
	if !found {
		return Result{}, fail(providercatalog.Entry{EngineID: selector}, CodeUnknownEngine, nil,
			"%q is not an official provider engine; engines with a catalog entry can be installed", selector)
	}
	lookup := i.Pins
	if lookup == nil {
		lookup = providercatalog.LookupPin
	}
	pin, found := lookup(entry.EngineID)
	if !found {
		return Result{}, fail(entry, CodeNoRelease, nil,
			"this Ptah release pins no %s provider release, so there is nothing to install; select a provider you obtained and verified yourself through the provider manifest: %s",
			entry.DisplayName, entry.SetupURL)
	}
	platform := i.Platform
	if platform == (providercatalog.Platform{}) {
		platform = providercatalog.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
	}
	artifact, found := pin.Artifact(platform)
	if !found {
		return Result{}, fail(entry, CodeUnsupported, nil,
			"the %s provider %s has no pinned archive for %s; install it from another platform or select a verified executable through the provider manifest: %s",
			entry.DisplayName, pin.Version, platform, entry.SetupURL)
	}
	if i.Root == "" {
		return Result{}, fail(entry, CodeStoreFailed, nil, "the provider store location is empty")
	}
	address, err := pin.ArchiveURL(entry, i.BaseURL, artifact)
	if err != nil || !validAddress(address) {
		return Result{}, fail(entry, CodeDownloadFailed, err, "the download base URL %q is not an absolute http or https URL", i.BaseURL)
	}
	target := Dir(i.Root, entry.ProviderID, pin.Version, platform)
	result := Result{
		EngineID: entry.EngineID, ProviderID: entry.ProviderID, Version: pin.Version, Platform: platform,
		Executable: filepath.Join(target, artifact.Executable), SHA256: artifact.ExecutableSHA256,
	}
	if verified(result.Executable, artifact.ExecutableSHA256) {
		return result, nil
	}
	if err := i.fetchAndPublish(ctx, entry, address, artifact, target); err != nil {
		return Result{}, err
	}
	result.Installed = true
	return result, nil
}

func (i Installer) fetchAndPublish(ctx context.Context, entry providercatalog.Entry, address string, artifact providercatalog.Artifact, target string) error {
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create the provider store %s: %v", parent, err)
	}
	// The staging directory sits beside the target, so publishing is a rename
	// inside one directory and cannot cross a filesystem.
	staging, err := os.MkdirTemp(parent, ".staging-")
	if err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create a staging directory in %s: %v", parent, err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	payload := filepath.Join(staging, "payload")
	if err := os.Mkdir(payload, 0o700); err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create a staging directory in %s: %v", parent, err)
	}
	archive := filepath.Join(staging, "archive"+archiveSuffix(artifact.Archive))
	if err := i.download(ctx, entry, address, archive, artifact.ArchiveSHA256); err != nil {
		return err
	}
	if err := extract(entry, archive, filepath.Join(payload, artifact.Executable), artifact.ExecutableSHA256); err != nil {
		return err
	}
	return publish(entry, payload, target, filepath.Join(target, artifact.Executable), artifact.ExecutableSHA256)
}

func (i Installer) download(ctx context.Context, entry providercatalog.Entry, address, dst, wantDigest string) error {
	client := i.Client
	if client == nil {
		client = defaultClient()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return fail(entry, CodeDownloadFailed, err, "cannot request %s: %v", address, err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fail(entry, CodeDownloadFailed, err, "cannot download %s: %v", address, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fail(entry, CodeDownloadFailed, nil, "cannot download %s: %s", address, response.Status)
	}
	file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create the download file: %v", err)
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, maxArchiveSize+1))
	closeErr := file.Close()
	switch {
	case err != nil:
		return fail(entry, CodeDownloadFailed, err, "the download of %s was interrupted: %v", address, err)
	case n > maxArchiveSize:
		return fail(entry, CodeDownloadFailed, nil, "%s is larger than %d bytes", address, int64(maxArchiveSize))
	case closeErr != nil:
		return fail(entry, CodeStoreFailed, closeErr, "cannot write the download file: %v", closeErr)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != wantDigest {
		return fail(entry, CodeArchiveUntrusted, nil,
			"the archive from %s has SHA-256 %s, but this Ptah release pins %s; nothing was extracted. A mirror cannot change the pinned digest", address, got, wantDigest)
	}
	return nil
}

// extract writes the executable from the verified archive to dst and checks its
// digest. dst is created with 0700 and removed with its staging directory on error.
func extract(entry providercatalog.Entry, archive, dst, wantDigest string) error {
	file, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
	if err != nil {
		return fail(entry, CodeStoreFailed, err, "cannot create the executable: %v", err)
	}
	hash := sha256.New()
	copyErr := providerarchive.CopyExecutable(archive, filepath.Base(dst), io.MultiWriter(file, hash))
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	switch {
	case copyErr != nil:
		return fail(entry, CodeArchiveInvalid, copyErr, "the archive is not a valid provider archive: %v", copyErr)
	case closeErr != nil:
		return fail(entry, CodeStoreFailed, closeErr, "cannot write the executable: %v", closeErr)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != wantDigest {
		return fail(entry, CodeExecutableBad, nil,
			"the executable in the archive has SHA-256 %s, but this Ptah release pins %s; it was not installed", got, wantDigest)
	}
	return nil
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

// verified reports whether path is a regular file with the given SHA-256.
func verified(path, wantDigest string) bool {
	file, err := os.Open(path)
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
	return hex.EncodeToString(hash.Sum(nil)) == wantDigest
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

func defaultClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default transport is an *http.Transport
	transport.ResponseHeaderTimeout = 30 * time.Second
	return &http.Client{Transport: transport}
}

func fail(entry providercatalog.Entry, code string, cause error, format string, args ...any) *providerdiagnostic.Diagnostic {
	return &providerdiagnostic.Diagnostic{
		Code: code, EngineID: entry.EngineID, ProviderID: entry.ProviderID, RepositoryURL: entry.RepositoryURL,
		SetupURL: entry.SetupURL, Message: fmt.Sprintf(format, args...), Cause: cause,
	}
}
