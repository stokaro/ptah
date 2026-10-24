// Package releasetest serves fake provider releases for tests: the GitHub
// releases API of one repository and the GitHub release download layout, from
// one HTTP server. A test builds well-formed releases with [ProviderRelease],
// damages the assets it wants to see refused, and points an installer at
// [Server.APIURL] or, as a mirror, at [Server.MirrorURL].
package releasetest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"

	"ptah.run/internal/providerarchive/archivetest"
	"ptah.run/internal/providerrelease"
)

// Release is one release of the fake repository: its tag, its listing flags
// and its assets by name.
type Release struct {
	Tag        string
	Draft      bool
	Prerelease bool
	Assets     map[string][]byte
}

// Spec describes a well-formed provider release.
type Spec struct {
	ProviderID  string
	Engine      string
	Version     string
	MinContract providerrelease.Contract
	Platforms   []providerrelease.Platform
	// Executable is the content of the executable in every archive.
	Executable []byte
}

// ArchiveName returns the archive name a release of spec uses for platform:
// ptah-plugin-<engine>_<version>_<os>_<arch>, ".zip" on Windows and ".tar.gz"
// elsewhere.
func ArchiveName(spec Spec, platform providerrelease.Platform) string {
	name := fmt.Sprintf("ptah-plugin-%s_%s_%s_%s", spec.Engine, spec.Version, platform.OS, platform.Arch)
	if platform.OS == "windows" {
		return name + ".zip"
	}
	return name + ".tar.gz"
}

// ExecutableName returns the executable name inside the archive for platform.
func ExecutableName(spec Spec, platform providerrelease.Platform) string {
	if platform.OS == "windows" {
		return "ptah-plugin-" + spec.Engine + ".exe"
	}
	return "ptah-plugin-" + spec.Engine
}

// Manifest returns the ptah-provider.json spec describes.
func Manifest(spec Spec) providerrelease.Manifest {
	manifest := providerrelease.Manifest{Schema: providerrelease.ManifestSchema, ProviderID: spec.ProviderID,
		Version: spec.Version, MinContract: spec.MinContract, Engines: []string{spec.Engine}}
	for _, platform := range spec.Platforms {
		manifest.Artifacts = append(manifest.Artifacts, providerrelease.Artifact{OS: platform.OS, Arch: platform.Arch,
			Archive: ArchiveName(spec, platform), Executable: ExecutableName(spec, platform)})
	}
	return manifest
}

// ProviderRelease builds a well-formed release of spec: one archive per
// platform holding the executable and a README, ptah-provider.json, and a
// checksums.txt that lists every other asset.
func ProviderRelease(spec Spec) Release {
	release := Release{Tag: providerrelease.Tag(spec.Version), Assets: make(map[string][]byte)}
	for _, platform := range spec.Platforms {
		pack := archivetest.TarGz
		if platform.OS == "windows" {
			pack = archivetest.Zip
		}
		release.Assets[ArchiveName(spec, platform)] = pack(
			archivetest.Entry{Name: ExecutableName(spec, platform), Body: string(spec.Executable)},
			archivetest.Entry{Name: "README.md", Body: "readme"},
		)
	}
	release.SetManifest(Manifest(spec))
	return release
}

// SetManifest replaces the release's ptah-provider.json and lists the new bytes
// in checksums.txt.
func (r *Release) SetManifest(manifest providerrelease.Manifest) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		panic(err) // a Manifest always marshals
	}
	r.Assets[providerrelease.ManifestName] = data
	r.Rechecksum()
}

// Rechecksum rewrites checksums.txt so that it lists every other asset with its
// digest, sorted by name.
func (r *Release) Rechecksum() {
	names := make([]string, 0, len(r.Assets))
	for name := range r.Assets {
		if name != providerrelease.ChecksumsName {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	var sums strings.Builder
	for _, name := range names {
		fmt.Fprintf(&sums, "%s  %s\n", archivetest.SHA256(r.Assets[name]), name)
	}
	r.Assets[providerrelease.ChecksumsName] = []byte(sums.String())
}

// Server is a fake GitHub release source for one repository. Its methods are
// safe for concurrent use.
type Server struct {
	server *httptest.Server
	owner  string
	repo   string

	mu        sync.Mutex
	releases  []Release
	requests  []string
	handlers  map[string]http.HandlerFunc
	refuseAPI int
}

// New starts a server for owner/repo with no releases. The caller closes it.
func New(owner, repo string) *Server {
	s := &Server{owner: owner, repo: repo, handlers: make(map[string]http.HandlerFunc)}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Close stops the server.
func (s *Server) Close() { s.server.Close() }

// APIURL is the root of the fake releases API, for an installer's API setting.
func (s *Server) APIURL() string { return s.server.URL + "/api" }

// MirrorURL is the base of the fake download layout, for an installer's
// download base URL: <base>/<owner>/<repo>/releases/download/<tag>/<asset>.
func (s *Server) MirrorURL() string { return s.server.URL + "/download" }

// Add publishes releases, listed in the order given.
func (s *Server) Add(releases ...Release) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases = append(s.releases, releases...)
}

// Handle answers every download of the asset called name with handler instead
// of its bytes, so a test can cut a body short or fail a request.
func (s *Server) Handle(name string, handler http.HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = handler
}

// RefuseListing makes the releases API answer status. A 403 carries
// "X-RateLimit-Remaining: 0", the way GitHub answers an exhausted
// unauthenticated limit.
func (s *Server) RefuseListing(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuseAPI = status
}

// Requests returns the path of every request the server answered, in order.
func (s *Server) Requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r.URL.Path)
	listing := "/api/repos/" + s.owner + "/" + s.repo + "/releases"
	downloads := "/download/" + s.owner + "/" + s.repo + "/releases/download/"
	switch {
	case r.Method != http.MethodGet:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	case r.URL.Path == listing && s.refuseAPI != 0:
		w.Header().Set("X-RateLimit-Remaining", "0")
		http.Error(w, http.StatusText(s.refuseAPI), s.refuseAPI)
	case r.URL.Path == listing:
		s.list(w, r)
	case strings.HasPrefix(r.URL.Path, downloads):
		tag, name, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, downloads), "/")
		if handler, found := s.handlers[name]; found {
			handler(w, r)
			return
		}
		s.download(w, tag, name)
	default:
		http.NotFound(w, r)
	}
}

type listedAsset struct {
	Name string `json:"name"`
	URL  string `json:"browser_download_url"`
}

type listedRelease struct {
	TagName    string        `json:"tag_name"`
	Draft      bool          `json:"draft"`
	Prerelease bool          `json:"prerelease"`
	Assets     []listedAsset `json:"assets"`
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	perPage, err := strconv.Atoi(r.URL.Query().Get("per_page"))
	if err != nil || perPage <= 0 {
		perPage = 30
	}
	page, err := strconv.Atoi(r.URL.Query().Get("page"))
	if err != nil || page <= 0 {
		page = 1
	}
	listed := make([]listedRelease, 0)
	for _, release := range s.releases[min((page-1)*perPage, len(s.releases)):min(page*perPage, len(s.releases))] {
		entry := listedRelease{TagName: release.Tag, Draft: release.Draft, Prerelease: release.Prerelease, Assets: make([]listedAsset, 0)}
		names := make([]string, 0, len(release.Assets))
		for name := range release.Assets {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			entry.Assets = append(entry.Assets, listedAsset{Name: name,
				URL: s.MirrorURL() + "/" + s.owner + "/" + s.repo + "/releases/download/" + release.Tag + "/" + name})
		}
		listed = append(listed, entry)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(listed)
}

func (s *Server) download(w http.ResponseWriter, tag, name string) {
	for _, release := range s.releases {
		if data, found := release.Assets[name]; found && release.Tag == tag {
			_, _ = w.Write(data)
			return
		}
	}
	http.Error(w, "not found", http.StatusNotFound)
}
