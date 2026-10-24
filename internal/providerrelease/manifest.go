package providerrelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/mod/semver"

	"ptah.run/internal/providercatalog"
)

// ManifestName is the release asset that describes a provider release.
const ManifestName = "ptah-provider.json"

// ManifestSchema is the one ptah-provider.json schema this Ptah reads.
const ManifestSchema = 1

// MaxManifestSize bounds the bytes read for one ptah-provider.json.
const MaxManifestSize = 64 << 10

// ErrInvalidManifest marks a ptah-provider.json that is malformed or that
// contradicts the catalog or its release tag.
var ErrInvalidManifest = errors.New("invalid provider release manifest")

// Manifest is the ptah-provider.json of one release. It carries no digest:
// digests come from the release's checksums.txt only.
type Manifest struct {
	Schema      int        `json:"schema"`
	ProviderID  string     `json:"provider_id"`
	Version     string     `json:"version"`
	MinContract Contract   `json:"min_contract"`
	Engines     []string   `json:"engines"`
	Artifacts   []Artifact `json:"artifacts"`
}

// Artifact is the archive a release publishes for one platform and the
// executable inside it.
type Artifact struct {
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	Archive    string `json:"archive"`
	Executable string `json:"executable"`
}

// Platform returns the platform the artifact is built for.
func (a Artifact) Platform() Platform { return Platform{OS: a.OS, Arch: a.Arch} }

var (
	platformName = regexp.MustCompile(`^[a-z0-9]+$`)
	engineName   = regexp.MustCompile(`^[a-z][a-z0-9_+\-]*$`)
)

// DecodeManifest parses one ptah-provider.json and checks what it says about
// itself: the schema, the version spelling, the minimum contract, and the
// engine, platform, archive and executable names. A field it does not know is
// ignored; a schema it does not know is refused. Every error wraps
// [ErrInvalidManifest].
func DecodeManifest(data []byte) (Manifest, error) {
	if len(data) > MaxManifestSize {
		return Manifest{}, fmt.Errorf("%w: larger than %d bytes", ErrInvalidManifest, MaxManifestSize)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("%w: more than one JSON document", ErrInvalidManifest)
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	return manifest, nil
}

func (m Manifest) validate() error {
	var errs []error
	if m.Schema != ManifestSchema {
		errs = append(errs, fmt.Errorf("schema %d is not one this Ptah reads (it reads %d)", m.Schema, ManifestSchema))
	}
	if strings.TrimSpace(m.ProviderID) == "" {
		errs = append(errs, errors.New("provider_id is empty"))
	}
	if !ValidVersion(m.Version) {
		errs = append(errs, fmt.Errorf("version %q is not a semantic version without a leading v", m.Version))
	}
	if m.MinContract.Major == 0 {
		errs = append(errs, errors.New("min_contract.major must be at least 1"))
	}
	if len(m.Engines) == 0 {
		errs = append(errs, errors.New("engines is empty"))
	}
	for i, name := range m.Engines {
		if !engineName.MatchString(name) {
			errs = append(errs, fmt.Errorf("engine %q is not an engine name", name))
		}
		if slices.Contains(m.Engines[:i], name) {
			errs = append(errs, fmt.Errorf("engine %q is listed twice", name))
		}
	}
	if len(m.Artifacts) == 0 {
		errs = append(errs, errors.New("artifacts is empty"))
	}
	seen := make(map[Platform]bool)
	for _, artifact := range m.Artifacts {
		if seen[artifact.Platform()] {
			errs = append(errs, fmt.Errorf("artifact platform %s is listed twice", artifact.Platform()))
		}
		seen[artifact.Platform()] = true
		errs = append(errs, artifact.validate())
	}
	return errors.Join(errs...)
}

func (a Artifact) validate() error {
	var errs []error
	if !platformName.MatchString(a.OS) || !platformName.MatchString(a.Arch) {
		errs = append(errs, fmt.Errorf("artifact platform %q is not a GOOS/GOARCH pair", a.Platform()))
	}
	if !PlainName(a.Archive) || (!strings.HasSuffix(a.Archive, ".tar.gz") && !strings.HasSuffix(a.Archive, ".zip")) {
		errs = append(errs, fmt.Errorf("%s: archive %q is not a plain .tar.gz or .zip file name", a.Platform(), a.Archive))
	}
	if !PlainName(a.Executable) {
		errs = append(errs, fmt.Errorf("%s: executable %q is not a plain file name", a.Platform(), a.Executable))
	}
	return errors.Join(errs...)
}

// CheckRelease checks the manifest against the catalog entry of the engine
// being installed and the tag of the release it came from: the provider ID is
// the entry's, the tag is "v" followed by the version, and every engine is an
// official engine the catalog assigns to that provider, the installed one
// among them. Every error wraps [ErrInvalidManifest].
func (m Manifest) CheckRelease(entry providercatalog.Entry, tag string) error {
	var errs []error
	if m.ProviderID != entry.ProviderID {
		errs = append(errs, fmt.Errorf("provider_id %q is not %q", m.ProviderID, entry.ProviderID))
	}
	if tag != "v"+m.Version {
		errs = append(errs, fmt.Errorf("version %q does not match the release tag %q", m.Version, tag))
	}
	if !slices.Contains(m.Engines, entry.EngineID) {
		errs = append(errs, fmt.Errorf("engines %q do not include %q", m.Engines, entry.EngineID))
	}
	for _, name := range m.Engines {
		owner, official := providercatalog.Lookup(name)
		if !official || owner.EngineID != name || owner.ProviderID != entry.ProviderID {
			errs = append(errs, fmt.Errorf("engine %q is not an official engine of %s", name, entry.ProviderID))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	return nil
}

// Artifact returns the artifact for one platform, or false when the release
// publishes none for it.
func (m Manifest) Artifact(platform Platform) (Artifact, bool) {
	for _, artifact := range m.Artifacts {
		if artifact.Platform() == platform {
			return artifact, true
		}
	}
	return Artifact{}, false
}

// PlainName reports whether name is a file name with no directory part: no
// slash, no backslash, no NUL, and neither "." nor "..".
func PlainName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

// ValidVersion reports whether version is a semantic version without a leading
// "v" and without build metadata, such as "0.2.0" or "1.0.0-rc.1".
func ValidVersion(version string) bool {
	tagged := "v" + version
	return !strings.HasPrefix(version, "v") && semver.IsValid(tagged) && semver.Canonical(tagged) == tagged
}

// Tag returns the release tag of version: "v" followed by the version.
func Tag(version string) string { return "v" + version }

// VersionOfTag returns the version a release tag names, or false when the tag
// is not "v" followed by a version [ValidVersion] accepts.
func VersionOfTag(tag string) (string, bool) {
	version, found := strings.CutPrefix(tag, "v")
	return version, found && ValidVersion(version)
}

// CompareVersions orders two versions [ValidVersion] accepts by semantic
// version precedence: negative when a precedes b, zero when they are equal,
// positive otherwise.
func CompareVersions(a, b string) int { return semver.Compare(Tag(a), Tag(b)) }
