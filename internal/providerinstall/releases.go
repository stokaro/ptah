package providerinstall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerrelease"
)

// DefaultAPIURL is the GitHub REST API root newest-compatible discovery lists
// releases through.
const DefaultAPIURL = "https://api.github.com"

// DefaultDownloadURL is the host whose release download layout install reads
// when no mirror is set.
const DefaultDownloadURL = "https://github.com"

const (
	releasesPerPage = 100
	maxReleasePages = 10
	maxListingSize  = 16 << 20
)

// release is one candidate release: its tag and the address of each asset.
type release struct {
	version string
	tag     string
	// assets maps an asset name to its address. Nil means the layout decides
	// the address and presence is learned by downloading.
	assets map[string]string
	layout string
}

// asset returns the address of one asset of the release, or false when the
// listing says the release has no such asset.
func (r release) asset(name string) (string, bool) {
	if r.assets == nil {
		return r.layout + "/" + url.PathEscape(name), true
	}
	address, found := r.assets[name]
	return address, found
}

// layoutRelease is the release of version under the GitHub download layout
// below base: <base>/<owner>/<repo>/releases/download/<tag>.
func layoutRelease(base, owner, repo, version string) release {
	tag := providerrelease.Tag(version)
	return release{version: version, tag: tag, layout: strings.TrimSuffix(base, "/") + "/" +
		url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/releases/download/" + url.PathEscape(tag)}
}

type listedRelease struct {
	TagName    string `json:"tag_name"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Assets     []struct {
		Name string `json:"name"`
		URL  string `json:"browser_download_url"`
	} `json:"assets"`
}

// listing is what the releases API says about a repository: the candidates
// that publish a manifest, newest first, and the tags of published releases
// that do not.
type listing struct {
	candidates []release
	unmarked   []string
}

// list asks the releases API for every published release of owner/repo and
// keeps the ones newest-compatible discovery considers: not a draft, not a
// prerelease, tagged "v<version>", with a ptah-provider.json asset.
func (i Installer) list(ctx context.Context, entry providercatalog.Entry, owner, repo string) (listing, error) {
	api := i.APIURL
	if api == "" {
		api = DefaultAPIURL
	}
	var all []listedRelease
	for page := 1; page <= maxReleasePages; page++ {
		address := fmt.Sprintf("%s/repos/%s/%s/releases?per_page=%d&page=%d",
			strings.TrimSuffix(api, "/"), url.PathEscape(owner), url.PathEscape(repo), releasesPerPage, page)
		batch, err := i.listPage(ctx, entry, address)
		if err != nil {
			return listing{}, err
		}
		all = append(all, batch...)
		if len(batch) < releasesPerPage {
			break
		}
	}
	var result listing
	for _, listed := range all {
		version, tagged := providerrelease.VersionOfTag(listed.TagName)
		if listed.Draft || listed.Prerelease || !tagged {
			continue
		}
		assets := make(map[string]string, len(listed.Assets))
		for _, asset := range listed.Assets {
			assets[asset.Name] = asset.URL
		}
		if _, found := assets[providerrelease.ManifestName]; !found {
			result.unmarked = append(result.unmarked, listed.TagName)
			continue
		}
		result.candidates = append(result.candidates, release{version: version, tag: listed.TagName, assets: assets})
	}
	slices.SortStableFunc(result.candidates, func(a, b release) int { return providerrelease.CompareVersions(b.version, a.version) })
	return result, nil
}

func (i Installer) listPage(ctx context.Context, entry providercatalog.Entry, address string) ([]listedRelease, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, http.NoBody)
	if err != nil {
		return nil, fail(entry, CodeDownloadFailed, err, "cannot request %s: %v", address, err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := i.client().Do(request)
	if err != nil {
		return nil, fail(entry, CodeDownloadFailed, err, "cannot list the releases of %s: %v", entry.ProviderID, err)
	}
	defer func() { _ = response.Body.Close() }()
	switch {
	case response.StatusCode == http.StatusNotFound:
		return nil, fail(entry, CodeNoRelease, nil, "the release source of %s, %s, was not found", entry.ProviderID, entry.RepositoryURL)
	case rateLimited(response):
		return nil, fail(entry, CodeDownloadFailed, nil,
			"the GitHub API refused to list the releases of %s (%s): its hourly limit for unauthenticated requests is used up; pass --version to install a release without listing them",
			entry.ProviderID, response.Status)
	case response.StatusCode != http.StatusOK:
		return nil, fail(entry, CodeDownloadFailed, nil, "cannot list the releases of %s: %s", entry.ProviderID, response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxListingSize+1))
	if err != nil {
		return nil, fail(entry, CodeDownloadFailed, err, "the release listing of %s was interrupted: %v", entry.ProviderID, err)
	}
	if len(data) > maxListingSize {
		return nil, fail(entry, CodeDownloadFailed, nil, "the release listing of %s is larger than %d bytes", entry.ProviderID, maxListingSize)
	}
	var batch []listedRelease
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fail(entry, CodeDownloadFailed, err, "the release listing of %s is not the GitHub releases format: %v", entry.ProviderID, err)
	}
	return batch, nil
}

func rateLimited(response *http.Response) bool {
	if response.StatusCode == http.StatusTooManyRequests {
		return true
	}
	remaining, err := strconv.Atoi(response.Header.Get("X-RateLimit-Remaining"))
	return response.StatusCode == http.StatusForbidden && err == nil && remaining == 0
}

var (
	// errNotFound reports an asset the server does not have.
	errNotFound = errors.New("not found")
	// errInterrupted reports a body that ended before it was complete.
	errInterrupted = errors.New("interrupted")
)
