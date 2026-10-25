package providerinstall

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"ptah.run/internal/providerrelease"
)

// RecordSchema is the one install record schema this Ptah writes and reads.
const RecordSchema = 1

const maxRecordSize = 64 << 10

// ErrInvalidRecord marks an install record that cannot be read or does not
// describe an installation of the provider it was read for.
var ErrInvalidRecord = errors.New("invalid provider install record")

// Record is the install record of one provider on one platform: the
// installation `ptah providers install` published last, and the digest of its
// executable. Selection reads it to find the installation and checks the
// executable against SHA256 before every start.
type Record struct {
	Schema      int                      `json:"schema"`
	ProviderID  string                   `json:"provider_id"`
	Version     string                   `json:"version"`
	MinContract providerrelease.Contract `json:"min_contract"`
	Engines     []string                 `json:"engines"`
	// Executable is the executable's path relative to the provider's directory
	// in the store, with forward slashes.
	Executable string `json:"executable"`
	// SHA256 is the executable's digest, computed by install from the bytes it
	// extracted out of a checksum-verified archive.
	SHA256 string `json:"sha256"`
}

// DefaultRoot returns os.UserCacheDir()/ptah/providers.
func DefaultRoot() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate the user cache directory: %w", err)
	}
	return filepath.Join(cache, "ptah", "providers"), nil
}

// ProviderDir returns the directory that holds every installation of one
// provider: <root>/<provider-id>.
func ProviderDir(root, providerID string) string {
	return filepath.Join(root, filepath.FromSlash(providerID))
}

// Dir returns the directory that holds one installation:
// <root>/<provider-id>/<version>/<os>_<arch>.
func Dir(root, providerID, version string, platform providerrelease.Platform) string {
	return filepath.Join(ProviderDir(root, providerID), version, platform.OS+"_"+platform.Arch)
}

// RecordPath returns where the install record of one provider on one platform
// lives: <root>/<provider-id>/installed-<os>_<arch>.json.
func RecordPath(root, providerID string, platform providerrelease.Platform) string {
	return filepath.Join(ProviderDir(root, providerID), "installed-"+platform.OS+"_"+platform.Arch+".json")
}

// ReadRecord reads the install record of providerID on platform and returns it
// with the absolute path of the executable it names. When no record exists the
// error wraps [os.ErrNotExist]; every other failure wraps [ErrInvalidRecord],
// so a caller can tell an absent installation from a damaged one.
func ReadRecord(root, providerID string, platform providerrelease.Platform) (Record, string, error) {
	name := RecordPath(root, providerID, platform)
	file, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, "", err
	}
	if err != nil {
		return Record{}, "", fmt.Errorf("%w: %v", ErrInvalidRecord, err)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRecordSize+1))
	// Closed before decoding: on Windows an install cannot replace the record
	// while it is open.
	_ = file.Close()
	if err != nil || len(data) > maxRecordSize {
		return Record{}, "", fmt.Errorf("%w: cannot read %s", ErrInvalidRecord, name)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record Record
	if err := decoder.Decode(&record); err != nil {
		return Record{}, "", fmt.Errorf("%w: %v", ErrInvalidRecord, err)
	}
	if err := record.validate(providerID); err != nil {
		return Record{}, "", fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	return record, filepath.Join(ProviderDir(root, providerID), filepath.FromSlash(record.Executable)), nil
}

func (r Record) validate(providerID string) error {
	var errs []error
	if r.Schema != RecordSchema {
		errs = append(errs, fmt.Errorf("schema %d is not %d", r.Schema, RecordSchema))
	}
	if r.ProviderID != providerID {
		errs = append(errs, fmt.Errorf("provider_id %q is not %q", r.ProviderID, providerID))
	}
	if !providerrelease.ValidVersion(r.Version) {
		errs = append(errs, fmt.Errorf("version %q is not a semantic version", r.Version))
	}
	if r.MinContract.Major == 0 {
		errs = append(errs, errors.New("min_contract.major must be at least 1"))
	}
	if len(r.Engines) == 0 {
		errs = append(errs, errors.New("engines is empty"))
	}
	if r.Executable == "" || strings.Contains(r.Executable, `\`) || !filepath.IsLocal(filepath.FromSlash(r.Executable)) || path.Clean(r.Executable) != r.Executable {
		errs = append(errs, fmt.Errorf("executable %q is not a clean path below the provider directory", r.Executable))
	}
	if !providerrelease.ValidDigest(r.SHA256) {
		errs = append(errs, fmt.Errorf("sha256 %q is not a SHA-256 digest", r.SHA256))
	}
	return errors.Join(errs...)
}

// WriteRecord publishes record as the install record of its provider on
// platform, replacing any earlier one. It writes a temporary file in the same
// directory and renames it, so a reader sees the old record or the new one and
// never a partial file. A reader that holds the record open for a moment does
// not make the write fail on any platform. WriteRecord takes no lock:
// [Installer.Install] calls it under the install lock of the provider's
// directory, and writers that run at once must be serialized by their caller.
func WriteRecord(root string, platform providerrelease.Platform, record Record) error {
	if err := record.validate(record.ProviderID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	dir := ProviderDir(root, record.ProviderID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(dir, ".installed-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	_, err = temp.Write(append(data, '\n'))
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return replaceRecord(temp.Name(), RecordPath(root, record.ProviderID, platform))
}
