package lint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/registry/remote/retry"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/lintartifact"
	"go.5x5.cz/ptah/internal/migrationartifact"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

const testOCIReference = "oci://registry.example/acme/migrations:latest"

func TestRunLint_AttachesCleanOCIReportBeforeSuccess(t *testing.T) {
	c := qt.New(t)
	registry := installTestRegistry(t, c.TB, fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE users;\n")},
	})

	stdout, stderr, err := execute(
		"--dir",
		testOCIReference,
		"--plain-http",
		"--attach",
		"--format",
		"json",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `"findings": []`)
	c.Assert(registry.attachmentPayload(c), qt.DeepEquals, []byte(stdout))
	c.Assert(registry.legacyReferrerIndexWriteCount(), qt.Equals, 0)
}

func TestRunLint_AttachesFailedOCIReportBeforeLintExit(t *testing.T) {
	c := qt.New(t)
	registry := installTestRegistry(t, c.TB, fstest.MapFS{
		"0000000001_drop.up.sql":   {Data: []byte("DROP TABLE users;\n")},
		"0000000001_drop.down.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	})

	stdout, stderr, err := execute(
		"--dir",
		testOCIReference,
		"--plain-http",
		"--attach",
		"--format",
		"json",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `"failed": true`)
	c.Assert(registry.attachmentPayload(c), qt.DeepEquals, []byte(stderr))
}

func TestRunLint_AttachmentFailureIsCommandErrorWithCleanJSONStdout(t *testing.T) {
	c := qt.New(t)
	registry := installTestRegistry(t, c.TB, fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE users;\n")},
	})
	registry.rejectAttachments = true

	stdout, stderr, err := execute(
		"--dir",
		testOCIReference,
		"--plain-http",
		"--attach",
		"--format",
		"json",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `"error": "attach migration lint report:`)
}

func TestRunLint_AttachesProjectConfiguredOCIReport(t *testing.T) {
	c := qt.New(t)
	registry := installTestRegistry(t, c.TB, fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE users;\n")},
	})
	projectDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(projectDir, "ptah.yaml"),
		[]byte("migration:\n  dir: "+testOCIReference+"\n"),
		0o600,
	), qt.IsNil)
	originalDir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(projectDir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalDir), qt.IsNil)
	}()

	stdout, stderr, err := execute("--plain-http", "--attach", "--format", "json")

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(registry.attachmentPayload(c), qt.DeepEquals, []byte(stdout))
}

func TestRunLint_WaitsForDelayedOCIReferrerVisibility(t *testing.T) {
	c := qt.New(t)
	registry := installTestRegistry(t, c.TB, fstest.MapFS{
		"0000000001_init.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"0000000001_init.down.sql": {Data: []byte("DROP TABLE users;\n")},
	})
	registry.hideAttachmentsFor = 5

	stdout, stderr, err := execute(
		"--dir",
		testOCIReference,
		"--plain-http",
		"--attach",
		"--format",
		"json",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `"findings": []`)
	c.Assert(registry.referrerCheckCount(), qt.Equals, 6)
}

type registryContent struct {
	mediaType string
	body      []byte
}

type testRegistryTransport struct {
	mu                        sync.Mutex
	content                   map[string]registryContent
	tags                      map[string]string
	attachments               []string
	legacyReferrerIndexWrites int
	referrerChecks            int
	hideAttachmentsFor        int
	rejectAttachments         bool
}

func installTestRegistry(t *testing.T, tb testing.TB, migrations fs.FS) *testRegistryTransport {
	c := qt.New(tb)
	t.Helper()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	registry := newTestRegistry(c.TB, migrations)
	previousClient := retry.DefaultClient
	retry.DefaultClient = &http.Client{Transport: registry}
	t.Cleanup(func() {
		retry.DefaultClient = previousClient
	})
	return registry
}

func newTestRegistry(tb testing.TB, migrations fs.FS) *testRegistryTransport {
	c := qt.New(tb)
	c.Helper()
	ctx := context.Background()
	store := memory.New()
	pushed, err := migrationartifact.PushTo(ctx, store, migrations, migrationartifact.PushOptions{
		Tags:      []string{ociartifact.DefaultTag},
		DirFormat: migrator.MigrationDirFormatPtah,
	})
	c.Assert(err, qt.IsNil)
	manifestBytes, err := content.FetchAll(ctx, store, pushed.Descriptor)
	c.Assert(err, qt.IsNil)
	var manifest ocispec.Manifest
	c.Assert(json.Unmarshal(manifestBytes, &manifest), qt.IsNil)

	registry := &testRegistryTransport{
		content: map[string]registryContent{
			pushed.Descriptor.Digest.String(): {
				mediaType: pushed.Descriptor.MediaType,
				body:      manifestBytes,
			},
		},
		tags: map[string]string{
			ociartifact.DefaultTag: pushed.Descriptor.Digest.String(),
		},
	}
	for _, descriptor := range append([]ocispec.Descriptor{manifest.Config}, manifest.Layers...) {
		contents, err := content.FetchAll(ctx, store, descriptor)
		c.Assert(err, qt.IsNil)
		registry.content[descriptor.Digest.String()] = registryContent{
			mediaType: descriptor.MediaType,
			body:      contents,
		}
	}
	return registry
}

func (r *testRegistryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	const repositoryPrefix = "/v2/acme/migrations/"
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/v2/":
		return registryResponse(request, http.StatusOK, "", nil, nil), nil
	case request.Method == http.MethodGet &&
		strings.HasPrefix(request.URL.Path, repositoryPrefix+"referrers/"):
		return r.referrersResponse(request)
	case request.Method == http.MethodGet && request.URL.Path == repositoryPrefix+"tags/list":
		return r.tagsResponse(request)
	case strings.HasPrefix(request.URL.Path, repositoryPrefix+"manifests/"):
		return r.manifestResponse(request, strings.TrimPrefix(request.URL.Path, repositoryPrefix+"manifests/"))
	case request.Method == http.MethodPost && request.URL.Path == repositoryPrefix+"blobs/uploads/":
		headers := http.Header{"Location": {repositoryPrefix + "blobs/uploads/test-upload"}}
		return registryResponse(request, http.StatusAccepted, "", nil, headers), nil
	case strings.HasPrefix(request.URL.Path, repositoryPrefix+"blobs/uploads/"):
		return r.completeBlobUpload(request)
	case strings.HasPrefix(request.URL.Path, repositoryPrefix+"blobs/"):
		return r.blobResponse(request, strings.TrimPrefix(request.URL.Path, repositoryPrefix+"blobs/"))
	default:
		return registryErrorResponse(request, http.StatusNotFound, "NAME_UNKNOWN"), nil
	}
}

func (r *testRegistryTransport) referrersResponse(request *http.Request) (*http.Response, error) {
	subjectDigest := strings.TrimPrefix(request.URL.Path, "/v2/acme/migrations/referrers/")
	artifactType := request.URL.Query().Get("artifactType")

	r.mu.Lock()
	r.referrerChecks++
	attachments := append([]string(nil), r.attachments...)
	if r.referrerChecks <= r.hideAttachmentsFor {
		attachments = nil
	}
	contents := make(map[string]registryContent, len(r.content))
	maps.Copy(contents, r.content)
	r.mu.Unlock()

	manifests := make([]ocispec.Descriptor, 0, len(attachments))
	seen := make(map[string]struct{}, len(attachments))
	for _, manifestDigest := range attachments {
		stored := contents[manifestDigest]
		var manifest ocispec.Manifest
		if err := json.Unmarshal(stored.body, &manifest); err != nil {
			return nil, err
		}
		if manifest.Subject == nil ||
			manifest.Subject.Digest.String() != subjectDigest ||
			(artifactType != "" && manifest.ArtifactType != artifactType) {
			continue
		}
		if _, exists := seen[manifestDigest]; exists {
			continue
		}
		seen[manifestDigest] = struct{}{}
		manifests = append(manifests, ocispec.Descriptor{
			MediaType:    stored.mediaType,
			Digest:       digest.Digest(manifestDigest),
			Size:         int64(len(stored.body)),
			ArtifactType: manifest.ArtifactType,
			Annotations:  manifest.Annotations,
		})
	}
	body, err := json.Marshal(ocispec.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageIndex,
		Manifests: manifests,
	})
	if err != nil {
		return nil, err
	}
	return registryResponse(request, http.StatusOK, ocispec.MediaTypeImageIndex, body, nil), nil
}

func (r *testRegistryTransport) tagsResponse(request *http.Request) (*http.Response, error) {
	r.mu.Lock()
	tags := make([]string, 0, len(r.tags))
	for tag := range r.tags {
		if r.referrerChecks <= r.hideAttachmentsFor &&
			strings.HasPrefix(tag, "ptah-r-") {
			continue
		}
		tags = append(tags, tag)
	}
	r.mu.Unlock()

	body, err := json.Marshal(struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}{
		Name: "acme/migrations",
		Tags: tags,
	})
	if err != nil {
		return nil, err
	}
	return registryResponse(request, http.StatusOK, "application/json", body, nil), nil
}

func (r *testRegistryTransport) manifestResponse(request *http.Request, reference string) (*http.Response, error) {
	if request.Method == http.MethodPut {
		return r.storeManifest(request, reference)
	}
	return r.fetchContent(request, r.resolveReference(reference), "MANIFEST_UNKNOWN")
}

func (r *testRegistryTransport) blobResponse(request *http.Request, reference string) (*http.Response, error) {
	return r.fetchContent(request, reference, "BLOB_UNKNOWN")
}

func (r *testRegistryTransport) fetchContent(
	request *http.Request,
	reference string,
	errorCode string,
) (*http.Response, error) {
	r.mu.Lock()
	stored, ok := r.content[reference]
	r.mu.Unlock()
	if !ok || (request.Method != http.MethodGet && request.Method != http.MethodHead) {
		return registryErrorResponse(request, http.StatusNotFound, errorCode), nil
	}
	headers := http.Header{
		"Content-Type":          {stored.mediaType},
		"Docker-Content-Digest": {reference},
	}
	body := stored.body
	if request.Method == http.MethodHead {
		body = nil
	}
	response := registryResponse(request, http.StatusOK, stored.mediaType, body, headers)
	response.ContentLength = int64(len(stored.body))
	return response, nil
}

func (r *testRegistryTransport) resolveReference(reference string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if resolved := r.tags[reference]; resolved != "" {
		return resolved
	}
	return reference
}

func (r *testRegistryTransport) completeBlobUpload(request *http.Request) (*http.Response, error) {
	contents, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	value := request.URL.Query().Get("digest")
	r.mu.Lock()
	r.content[value] = registryContent{
		mediaType: "application/octet-stream",
		body:      contents,
	}
	r.mu.Unlock()
	headers := http.Header{"Docker-Content-Digest": {value}}
	return registryResponse(request, http.StatusCreated, "", nil, headers), nil
}

func (r *testRegistryTransport) storeManifest(request *http.Request, reference string) (*http.Response, error) {
	contents, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	value := digest.FromBytes(contents).String()
	mediaType := request.Header.Get("Content-Type")
	var manifest ocispec.Manifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		return nil, err
	}
	if manifest.Subject != nil && r.rejectAttachments {
		return registryErrorResponse(request, http.StatusBadRequest, "MANIFEST_INVALID"), nil
	}

	r.mu.Lock()
	r.content[value] = registryContent{mediaType: mediaType, body: contents}
	if reference != value {
		r.tags[reference] = value
	}
	if strings.HasPrefix(reference, "sha256-") {
		r.legacyReferrerIndexWrites++
	}
	headers := http.Header{"Docker-Content-Digest": {value}}
	if manifest.Subject != nil {
		r.attachments = append(r.attachments, value)
		headers.Set("OCI-Subject", manifest.Subject.Digest.String())
	}
	r.mu.Unlock()
	return registryResponse(request, http.StatusCreated, "", nil, headers), nil
}

func (r *testRegistryTransport) legacyReferrerIndexWriteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.legacyReferrerIndexWrites
}

func (r *testRegistryTransport) referrerCheckCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.referrerChecks
}

func (r *testRegistryTransport) attachmentPayload(c *qt.C) []byte {
	c.Helper()
	r.mu.Lock()
	manifestDigest := r.attachments[len(r.attachments)-1]
	manifestBytes := bytes.Clone(r.content[manifestDigest].body)
	r.mu.Unlock()

	var manifest ocispec.Manifest
	c.Assert(json.Unmarshal(manifestBytes, &manifest), qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.LintArtifactType)
	c.Assert(manifest.Subject, qt.IsNotNil)
	c.Assert(manifest.Layers, qt.HasLen, 1)
	c.Assert(manifest.Layers[0].MediaType, qt.Equals, lintartifact.LayerMediaType)
	c.Assert(manifest.Layers[0].Annotations[ocispec.AnnotationTitle], qt.Equals, lintartifact.FileName)

	r.mu.Lock()
	payload := bytes.Clone(r.content[manifest.Layers[0].Digest.String()].body)
	r.mu.Unlock()
	return payload
}

func registryResponse(
	request *http.Request,
	statusCode int,
	contentType string,
	body []byte,
	headers http.Header,
) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	if contentType != "" {
		headers.Set("Content-Type", contentType)
	}
	return &http.Response{
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		StatusCode:    statusCode,
		Header:        headers,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       request,
	}
}

func registryErrorResponse(request *http.Request, statusCode int, code string) *http.Response {
	body := fmt.Appendf(nil, `{"errors":[{"code":%q,"message":"not found"}]}`, code)
	return registryResponse(request, statusCode, "application/json", body, nil)
}
