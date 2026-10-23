package providercatalog

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// Platform names one operating system and architecture a provider archive is
// built for, spelled the way Go spells GOOS and GOARCH.
type Platform struct {
	OS   string
	Arch string
}

// String returns the platform as "<os>/<arch>".
func (p Platform) String() string { return p.OS + "/" + p.Arch }

// Platforms returns the platforms every pinned release must cover.
func Platforms() []Platform {
	return []Platform{
		{"darwin", "amd64"}, {"darwin", "arm64"},
		{"linux", "amd64"}, {"linux", "arm64"},
		{"windows", "amd64"}, {"windows", "arm64"},
	}
}

// ArchiveName returns the name a provider release gives the archive of one
// platform: a ".zip" on Windows and a ".tar.gz" elsewhere.
func ArchiveName(engineID, version string, platform Platform) string {
	name := fmt.Sprintf("ptah-plugin-%s_%s_%s_%s", engineID, version, platform.OS, platform.Arch)
	if platform.OS == "windows" {
		return name + ".zip"
	}
	return name + ".tar.gz"
}

// ExecutableName returns the name of the provider executable inside the archive
// of one platform.
func ExecutableName(engineID string, platform Platform) string {
	name := "ptah-plugin-" + engineID
	if platform.OS == "windows" {
		return name + ".exe"
	}
	return name
}

// Artifact is one platform's entry in a pin.
type Artifact struct {
	Platform         Platform
	Archive          string
	ArchiveSHA256    string
	Executable       string
	ExecutableSHA256 string
}

// Pin names the one provider release a Ptah release was tested with. It holds
// data only: nothing here downloads, resolves a newer release or runs a file.
type Pin struct {
	EngineID  string
	Version   string
	Tag       string
	Artifacts []Artifact
}

var (
	digestPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`)
)

// ValidDigest reports whether s is a lowercase hexadecimal SHA-256 digest.
func ValidDigest(s string) bool { return digestPattern.MatchString(s) }

// Pins returns private copies of every pinned provider release. Engines with
// no release carry no pin.
func Pins() []Pin {
	result := make([]Pin, len(pins))
	for i, pin := range pins {
		pin.Artifacts = slices.Clone(pin.Artifacts)
		result[i] = pin
	}
	return result
}

// LookupPin returns the pin of an official engine, or false when the engine has
// no pinned release.
func LookupPin(engineID string) (Pin, bool) {
	for _, pin := range Pins() {
		if pin.EngineID == engineID {
			return pin, true
		}
	}
	return Pin{}, false
}

// Artifact returns the entry for one platform, or false when the pin does not
// cover it.
func (p Pin) Artifact(platform Platform) (Artifact, bool) {
	for _, artifact := range p.Artifacts {
		if artifact.Platform == platform {
			return artifact, true
		}
	}
	return Artifact{}, false
}

// ReleaseBaseURL returns the address of the directory that serves the archives
// of the pinned release, derived from the repository and the release tag.
func (p Pin) ReleaseBaseURL(entry Entry) string {
	return strings.TrimSuffix(entry.RepositoryURL, "/") + "/releases/download/" + p.Tag
}

// ArchiveURL returns the address of one archive below base. An empty base
// selects the pinned release on the official repository.
func (p Pin) ArchiveURL(entry Entry, base string, artifact Artifact) (string, error) {
	if base == "" {
		base = p.ReleaseBaseURL(entry)
	}
	return url.JoinPath(base, artifact.Archive)
}

// Validate checks what the compiler cannot: the version and tag spelling, the
// digest format, the archive and executable names, and that no platform is
// listed twice or unknown. It does not require every platform to be present;
// the pin generator does.
func (p Pin) Validate() error {
	var errs []error
	if _, known := Lookup(p.EngineID); !known {
		errs = append(errs, fmt.Errorf("pin names unknown engine %q", p.EngineID))
	}
	if !versionPattern.MatchString(p.Version) {
		errs = append(errs, fmt.Errorf("pin version %q is not a semantic version", p.Version))
	}
	if p.Tag != "v"+p.Version {
		errs = append(errs, fmt.Errorf("pin tag %q must be %q", p.Tag, "v"+p.Version))
	}
	if len(p.Artifacts) == 0 {
		errs = append(errs, errors.New("pin has no artifacts"))
	}
	seen := make(map[Platform]bool)
	for _, a := range p.Artifacts {
		if !slices.Contains(Platforms(), a.Platform) {
			errs = append(errs, fmt.Errorf("pin lists unsupported platform %s", a.Platform))
		}
		if seen[a.Platform] {
			errs = append(errs, fmt.Errorf("pin lists platform %s twice", a.Platform))
		}
		seen[a.Platform] = true
		if want := ArchiveName(p.EngineID, p.Version, a.Platform); a.Archive != want {
			errs = append(errs, fmt.Errorf("%s: archive %q must be %q", a.Platform, a.Archive, want))
		}
		if want := ExecutableName(p.EngineID, a.Platform); a.Executable != want {
			errs = append(errs, fmt.Errorf("%s: executable %q must be %q", a.Platform, a.Executable, want))
		}
		if !ValidDigest(a.ArchiveSHA256) {
			errs = append(errs, fmt.Errorf("%s: archive digest %q is not a lowercase SHA-256", a.Platform, a.ArchiveSHA256))
		}
		if !ValidDigest(a.ExecutableSHA256) {
			errs = append(errs, fmt.Errorf("%s: executable digest %q is not a lowercase SHA-256", a.Platform, a.ExecutableSHA256))
		}
	}
	return errors.Join(errs...)
}
