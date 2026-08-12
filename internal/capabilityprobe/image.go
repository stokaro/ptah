package capabilityprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const dockerHubTagsEndpoint = "https://hub.docker.com/v2/repositories"

var dockerHubClient = &http.Client{Timeout: 30 * time.Second}

// ResolveDockerRun returns the concrete docker run arguments for a CI cell.
// Most registries publish a floating release-line tag and need no resolution.
// For a cell whose registry does not, it replaces the declared line selector
// with Docker Hub's newest numeric patch tag for that line.
func ResolveDockerRun(ctx context.Context, cell CICell) ([]string, error) {
	arguments := slices.Clone(cell.DockerRun)
	if !cell.ResolveNewestPatch {
		return arguments, nil
	}

	image, err := resolveDockerHubImage(ctx, dockerHubClient, dockerHubTagsEndpoint, cell.Image, cell.Line)
	if err != nil {
		return nil, err
	}
	return replaceDockerImage(arguments, cell.Image, image)
}

func replaceDockerImage(arguments []string, selector, resolved string) ([]string, error) {
	arguments = slices.Clone(arguments)
	replaced := false
	for i, argument := range arguments {
		if argument != selector {
			continue
		}
		arguments[i] = resolved
		replaced = true
	}
	if !replaced {
		return nil, fmt.Errorf("docker arguments do not contain the declared image selector %q", selector)
	}
	return arguments, nil
}

type dockerHubTagsPage struct {
	Next    string `json:"next"`
	Results []struct {
		Name string `json:"name"`
	} `json:"results"`
}

func resolveDockerHubImage(
	ctx context.Context,
	client *http.Client,
	endpoint, selector, line string,
) (string, error) {
	repository, tag := splitImageTag(selector)
	if repository == "" || tag != line {
		return "", fmt.Errorf("image selector %q must tag repository with its release line %q", selector, line)
	}
	if strings.Count(repository, "/") != 1 {
		return "", fmt.Errorf("Docker Hub repository %q must have the owner/image form", repository)
	}

	var newest string
	var newestVersion []int
	for page := 1; page <= 100; page++ {
		pageURL, err := dockerHubTagsPageURL(endpoint, repository, line, page)
		if err != nil {
			return "", err
		}
		response, err := requestDockerHubTags(ctx, client, pageURL)
		if err != nil {
			return "", err
		}
		for _, result := range response.Results {
			version, ok := numericPatchTag(line, result.Name)
			if !ok || (newest != "" && compareNumericVersions(version, newestVersion) <= 0) {
				continue
			}
			newest = result.Name
			newestVersion = version
		}
		if response.Next == "" {
			if newest == "" {
				return "", fmt.Errorf(
					"Docker Hub repository %s has no numeric patch tag for release line %s", repository, line)
			}
			return repository + ":" + newest, nil
		}
	}
	return "", fmt.Errorf("Docker Hub returned more than 100 tag pages for repository %s and release line %s", repository, line)
}

func dockerHubTagsPageURL(endpoint, repository, line string, page int) (string, error) {
	parsed, err := url.Parse(strings.TrimSuffix(endpoint, "/") + "/" + repository + "/tags")
	if err != nil {
		return "", fmt.Errorf("parse Docker Hub endpoint: %w", err)
	}
	query := parsed.Query()
	query.Set("name", line+".")
	query.Set("page", strconv.Itoa(page))
	query.Set("page_size", "100")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func requestDockerHubTags(ctx context.Context, client *http.Client, pageURL string) (dockerHubTagsPage, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return dockerHubTagsPage{}, fmt.Errorf("build Docker Hub request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "ptah-capability-matrix")

	response, err := client.Do(request)
	if err != nil {
		return dockerHubTagsPage{}, fmt.Errorf("query Docker Hub tags: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return dockerHubTagsPage{}, fmt.Errorf("query Docker Hub tags: %s", response.Status)
	}

	var page dockerHubTagsPage
	decoder := json.NewDecoder(io.LimitReader(response.Body, 4<<20))
	if err := decoder.Decode(&page); err != nil {
		return dockerHubTagsPage{}, fmt.Errorf("decode Docker Hub tags: %w", err)
	}
	return page, nil
}

// numericPatchTag returns a sortable numeric version for a YugabyteDB-style
// release tag such as 2025.2.5.1-b1. The release line itself is required as an
// exact component prefix; preview, floating, and unrelated tags are ignored.
func numericPatchTag(line, tag string) ([]int, bool) {
	version, build, found := strings.Cut(tag, "-b")
	if !found || build == "" || !strings.HasPrefix(version, line+".") {
		return nil, false
	}
	components := strings.Split(version, ".")
	if len(components) <= len(strings.Split(line, ".")) {
		return nil, false
	}
	components = append(components, build)

	numbers := make([]int, 0, len(components))
	for _, component := range components {
		number, err := strconv.Atoi(component)
		if err != nil || number < 0 {
			return nil, false
		}
		numbers = append(numbers, number)
	}
	return numbers, true
}

func compareNumericVersions(a, b []int) int {
	for i := range max(len(a), len(b)) {
		var left, right int
		if i < len(a) {
			left = a[i]
		}
		if i < len(b) {
			right = b[i]
		}
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
	}
	return 0
}
