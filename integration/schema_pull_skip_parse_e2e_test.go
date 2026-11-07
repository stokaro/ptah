//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
)

const (
	// skipParseFutureSchema stands for what a newer Ptah publishes: its digest
	// verifies, and this build's parser refuses it.
	skipParseFutureSchema = `table "t" {
  schema = schema.public
  column "id" { type = bigint "a second value" }
}
schema "public" {}
`
	skipParseRegionsSchema = `schema "main" {}
table "regions" {
  schema = schema.main
  column "code" {
    null = false
    type = text
  }
  column "name" {
    null = true
    type = text
  }
  primary_key {
    columns = [column.code]
  }
}
`
	// skipParseDataBlock names a file. Inside a working copy that is how rows
	// arrive; inside a published artifact it is a path the publisher chose on a
	// machine they do not own.
	skipParseDataBlock = `data {
  table = table.regions
  keys  = ["code"]
  file  = "rows.yaml"
}
`
	skipParseRowLayer = `{"sets":[{"schema":"main","table":"regions","keys":["code"],"columns":["code","name"],"rows":[{"code":{"tag":"str","text":"CZ"},"name":{"tag":"str","text":"Czechia"}}]}]}` + "\n"
	skipParseChecks   = `-- +ptah check name="anything" assert="SELECT 1"` + "\n"
)

// skipParseFormat is the manifest annotation of a schema artifact. It is written
// out because an artifact's wire format is what these tests are about, and
// package schemaartifact keeps its constant to itself.
var skipParseFormat = map[string]string{"io.stokaro.ptah.schema-format": "hcl"}

// publishSkipParseLayers publishes exactly the layers it is given under a schema
// artifact's manifest, with no parse in front of them, and returns the manifest
// digest. A real publish refuses a schema it cannot read, so this is the only
// way one reaches a registry, which is what a newer Ptah does in effect.
func publishSkipParseLayers(c *qt.C, reference string, files fstest.MapFS) digest.Digest {
	c.Helper()
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	pushed, err := client.Push(c.Context(), reference, files, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		LayerMediaTypes: map[string]string{
			schemaartifact.ManagedDataFileName: schemaartifact.ManagedDataLayerMediaType,
			schemaartifact.ChecksFileName:      schemaartifact.ChecksLayerMediaType,
		},
		Annotations: skipParseFormat,
	})
	c.Assert(err, qt.IsNil)
	return pushed.Descriptor.Digest
}

// skipParseReference names a repository nothing else in this run writes to.
func skipParseReference(registry, name string) string {
	return fmt.Sprintf("oci://%s/ptah/skip-parse-%s-%d:v1", registry, name, time.Now().UnixNano())
}

func readSkipParseFile(c *qt.C, path string) string {
	c.Helper()
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return string(data)
}

// TestSchemaPullSkipParseMovesAnUnreadableSchemaToTheNextCommandE2E is the
// measurement the flag exists for, through the shipped binary against a real
// registry: the step holding registry credentials writes the file, and the step
// that reads it is the one that says why it cannot.
func TestSchemaPullSkipParseMovesAnUnreadableSchemaToTheNextCommandE2E(t *testing.T) {
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	reference := skipParseReference(registry, "future")
	published := publishSkipParseLayers(c, reference, fstest.MapFS{
		schemaartifact.FileName: {Data: []byte(skipParseFutureSchema)},
	})
	dir := c.TempDir()

	// The default still parses, and writes nothing.
	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "pull", reference, "--out", "default.hcl", "--plain-http")
	c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, "parse schema artifact: parse HCL schema: schema.hcl:3,31-32: Invalid single-argument block definition")
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)

	// The flag writes the published bytes and reports the published digest.
	pulled := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "pull", reference, "--out", "schema.hcl", "--plain-http", "--skip-parse", "--json")
	c.Assert(pulled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", pulled.Stdout, pulled.Stderr))
	var report struct {
		Digest   string   `json:"digest"`
		Written  []string `json:"written"`
		Unparsed bool     `json:"unparsed"`
	}
	c.Assert(json.Unmarshal([]byte(pulled.Stdout), &report), qt.IsNil, qt.Commentf("%s", pulled.Stdout))
	c.Assert(report.Digest, qt.Equals, published.String())
	c.Assert(report.Unparsed, qt.IsTrue)
	c.Assert(report.Written, qt.HasLen, 2)
	c.Assert(filepath.Base(report.Written[0]), qt.Equals, "schema.hcl")
	c.Assert(filepath.Base(report.Written[1]), qt.Equals, schemaartifact.ManagedDataFileName)
	c.Assert(readSkipParseFile(c, filepath.Join(dir, "schema.hcl")), qt.Equals, skipParseFutureSchema)

	// The next command is the one that says why it cannot read the file.
	plan := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "plan", "--schema-file", "schema.hcl", "--dry-run",
		"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")))
	c.Assert(plan.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	c.Assert(plan.Stderr, qt.Contains, "parse HCL schema: ")
	c.Assert(plan.Stderr, qt.Contains, "schema.hcl:3,31-32: Invalid single-argument block definition")
}

// TestSchemaPullSkipParseDoesNotOpenAFileADataBlockNamesE2E measures the hole a
// parser-free write would open. A default pull refuses a `data` block with no row
// layer, so a block published in an artifact is never followed. The reader
// follows one in a working copy, and a file with no layer beside it is what a
// working copy looks like, so the flag writes a layer stating there are no rows.
//
// The control removes that layer and shows the plan carrying the file's content.
// Without it, the refusal above could be anything, including a table the database
// does not have.
func TestSchemaPullSkipParseDoesNotOpenAFileADataBlockNamesE2E(t *testing.T) {
	const secret = "not-for-the-publisher"
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	reference := skipParseReference(registry, "data-block")
	publishSkipParseLayers(c, reference, fstest.MapFS{
		schemaartifact.FileName: {Data: []byte(skipParseRegionsSchema + skipParseDataBlock)},
	})
	dir := c.TempDir()
	// Inside the project, so the plan's own boundary admits it: the file a
	// publisher would aim at is one the consumer can already read.
	c.Assert(os.WriteFile(filepath.Join(dir, "rows.yaml"), []byte("- code: TOKEN\n  name: "+secret+"\n"), 0o600), qt.IsNil)
	target := atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db"))

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "pull", reference, "--out", "default.hcl", "--plain-http")
	c.Assert(refused.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, "declares managed data for table main.regions and carries no rows for it")

	pulled := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "pull", reference, "--out", "schema.hcl", "--plain-http", "--skip-parse")
	c.Assert(pulled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", pulled.Stdout, pulled.Stderr))

	for _, verb := range []struct {
		name string
		args []string
	}{
		{name: "plan", args: []string{"schema", "plan", "--schema-file", "schema.hcl", "--dry-run", "--db-url", target}},
		{name: "drift", args: []string{"schema", "drift", "--schema-file", "schema.hcl", "--db-url", target}},
	} {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, verb.args...)
			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, "declares managed data for table main.regions and carries no rows for it")
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), secret)
		})
	}

	// The control: the layer is what stands between the block and the file.
	c.Assert(os.Remove(filepath.Join(dir, schemaartifact.ManagedDataFileName)), qt.IsNil)
	followed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"schema", "plan", "--schema-file", "schema.hcl", "--dry-run", "--db-url", target)
	c.Assert(followed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", followed.Stdout, followed.Stderr))
	c.Assert(followed.Stdout, qt.Contains, secret)
}

// TestSchemaPullSkipParseWritesAnArtifactWithSeveralLayersE2E publishes every
// layer the format admits. The files on disk are the ones a default pull writes,
// byte for byte, and the rows reach a plan through the layer rather than through
// the `file` the block names, which does not exist here.
func TestSchemaPullSkipParseWritesAnArtifactWithSeveralLayersE2E(t *testing.T) {
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	reference := skipParseReference(registry, "layers")
	schema := skipParseRegionsSchema + skipParseDataBlock
	publishSkipParseLayers(c, reference, fstest.MapFS{
		schemaartifact.FileName:            {Data: []byte(schema)},
		schemaartifact.ManagedDataFileName: {Data: []byte(skipParseRowLayer)},
		schemaartifact.ChecksFileName:      {Data: []byte(skipParseChecks)},
	})
	unparsedDir := c.TempDir()
	defaultDir := c.TempDir()

	unparsed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: unparsedDir},
		"schema", "pull", reference, "--out", "schema.hcl", "--plain-http", "--skip-parse")
	c.Assert(unparsed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", unparsed.Stdout, unparsed.Stderr))
	parsed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: defaultDir},
		"schema", "pull", reference, "--out", "schema.hcl", "--plain-http")
	c.Assert(parsed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", parsed.Stdout, parsed.Stderr))

	c.Assert(fileNames(c, unparsedDir), qt.DeepEquals, fileNames(c, defaultDir))
	c.Assert(fileNames(c, unparsedDir), qt.DeepEquals, []string{schemaartifact.ManagedDataFileName, schemaartifact.FileName})
	c.Assert(readSkipParseFile(c, filepath.Join(unparsedDir, schemaartifact.FileName)), qt.Equals, schema)
	c.Assert(readSkipParseFile(c, filepath.Join(unparsedDir, schemaartifact.ManagedDataFileName)), qt.Equals, skipParseRowLayer)

	plan := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: unparsedDir},
		"schema", "plan", "--schema-file", "schema.hcl", "--dry-run",
		"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(unparsedDir, "target.db")))
	c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", plan.Stdout, plan.Stderr))
	c.Assert(plan.Stdout, qt.Contains, "Czechia")
}

func fileNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// publishHandBuiltManifest tags a manifest in the registry that no client of this
// build would publish. A registry checks digests and nothing else, so what such
// a manifest claims is decided by the pull.
func publishHandBuiltManifest(c *qt.C, reference, artifactType string, annotations map[string]string, title, mediaType string) {
	c.Helper()
	ref, err := ociartifact.ParseRef(reference)
	c.Assert(err, qt.IsNil)
	repository, err := ociartifact.NewRepository(ref, ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	layer, err := oras.PushBytes(c.Context(), repository, mediaType, []byte(skipParseRegionsSchema))
	c.Assert(err, qt.IsNil)
	layer.Annotations = map[string]string{ocispec.AnnotationTitle: title}
	manifest, err := oras.PackManifest(c.Context(), repository, oras.PackManifestVersion1_1, artifactType, oras.PackManifestOptions{
		Layers:              []ocispec.Descriptor{layer},
		ManifestAnnotations: annotations,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(repository.Tag(c.Context(), manifest, "v1"), qt.IsNil)
}

// TestSchemaPullSkipParseRefusesAWrongArtifactE2E holds the flag to everything a
// default pull refuses without reading a layer's content. Each row is an artifact
// a real registry stores, and none of them leaves a file behind.
func TestSchemaPullSkipParseRefusesAWrongArtifactE2E(t *testing.T) {
	registry := requiredOCIRegistry(t)
	tests := []struct {
		name         string
		artifactType string
		annotations  map[string]string
		title        string
		mediaType    string
		wantStderr   string
	}{
		{
			name:         "another artifact type",
			artifactType: ociartifact.MigrationArtifactType,
			annotations:  skipParseFormat,
			title:        schemaartifact.FileName,
			mediaType:    schemaartifact.LayerMediaType,
			wantStderr:   "unexpected OCI artifact type",
		},
		{
			name:         "a layer media type from a newer Ptah",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  skipParseFormat,
			title:        schemaartifact.FileName,
			mediaType:    "application/vnd.stokaro.ptah.schema.hcl.v2",
			wantStderr:   `file "schema.hcl" has media type "application/vnd.stokaro.ptah.schema.hcl.v2"`,
		},
		{
			name:         "no format annotation",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  make(map[string]string),
			title:        schemaartifact.FileName,
			mediaType:    schemaartifact.LayerMediaType,
			wantStderr:   `unsupported schema artifact format ""`,
		},
		{
			name:         "a file this build does not write",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  skipParseFormat,
			title:        "authorized_keys",
			mediaType:    schemaartifact.LayerMediaType,
			wantStderr:   "schema artifact carries unexpected file authorized_keys",
		},
		{
			name:         "a path that climbs out of the output directory",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  skipParseFormat,
			title:        "../schema.hcl",
			mediaType:    schemaartifact.LayerMediaType,
			wantStderr:   `unsafe OCI artifact path: "../schema.hcl"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			reference := skipParseReference(registry, "refused")
			publishHandBuiltManifest(c, reference, tt.artifactType, tt.annotations, tt.title, tt.mediaType)
			parent := c.TempDir()
			dir := filepath.Join(parent, "work")
			c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)

			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "pull", reference, "--out", "schema.hcl", "--plain-http", "--skip-parse")

			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, tt.wantStderr)
			c.Assert(fileNames(c, dir), qt.HasLen, 0)
			c.Assert(fileNames(c, parent), qt.DeepEquals, []string{"work"})
		})
	}
}

// tamperingRegistry serves registry through a proxy that alters the body of one
// content-addressed response, leaving its length and its headers alone. A real
// registry refuses to store bytes its digest does not cover, so this is the
// remaining place they can come from: a path to the registry, or a registry that
// is not what it says.
func tamperingRegistry(c *qt.C, registry string, tampered digest.Digest) string {
	c.Helper()
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: registry})
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method != http.MethodGet || !strings.HasSuffix(response.Request.URL.Path, tampered.String()) {
			return nil
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if err := response.Body.Close(); err != nil {
			return err
		}
		body[len(body)-1] ^= 0xff
		response.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}
	server := httptest.NewServer(proxy)
	c.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

// TestSchemaPullSkipParseRefusesTamperedBytesE2E alters the layer and the manifest
// on their way from a real registry. The digest is what ties what was approved to
// what lands on disk, and verifying it reads no content, so the flag has no reason
// to skip it and this measures that it does not.
//
// The control pulls the same artifact through the same proxy with the tampering
// aimed at bytes nobody requests, so the refusals are the digest and not the proxy.
func TestSchemaPullSkipParseRefusesTamperedBytesE2E(t *testing.T) {
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	repository := fmt.Sprintf("ptah/skip-parse-tampered-%d", time.Now().UnixNano())
	manifest := publishSkipParseLayers(c, fmt.Sprintf("oci://%s/%s:v1", registry, repository), fstest.MapFS{
		schemaartifact.FileName: {Data: []byte(skipParseRegionsSchema)},
	})
	pullThrough := func(c *qt.C, tampered digest.Digest, reference string) (clirun.Result, []string) {
		c.Helper()
		dir := c.TempDir()
		run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
			"schema", "pull", fmt.Sprintf("oci://%s/%s%s", tamperingRegistry(c, registry, tampered), repository, reference),
			"--out", "schema.hcl", "--plain-http", "--skip-parse")
		return run, fileNames(c, dir)
	}

	tests := []struct {
		name       string
		tampered   digest.Digest
		wantStderr string
	}{
		{
			name:       "the schema layer",
			tampered:   digest.FromString(skipParseRegionsSchema),
			wantStderr: `fetch artifact file "schema.hcl"`,
		},
		{
			name:       "the manifest",
			tampered:   manifest,
			wantStderr: "fetch OCI manifest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			run, files := pullThrough(c, tt.tampered, "@"+manifest.String())
			c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stderr, qt.Contains, tt.wantStderr)
			c.Assert(files, qt.HasLen, 0)
		})
	}

	run, files := pullThrough(c, digest.FromString("nobody requests this"), "@"+manifest.String())
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(files, qt.DeepEquals, []string{schemaartifact.ManagedDataFileName, schemaartifact.FileName})
}

// TestSchemaPullSkipParseReadsItsVariableE2E measures the binding `--help` prints.
// A value that is not a boolean refuses rather than reads as the default.
func TestSchemaPullSkipParseReadsItsVariableE2E(t *testing.T) {
	c := qt.New(t)
	registry := requiredOCIRegistry(t)
	reference := skipParseReference(registry, "variable")
	publishSkipParseLayers(c, reference, fstest.MapFS{
		schemaartifact.FileName: {Data: []byte(skipParseFutureSchema)},
	})
	dir := c.TempDir()

	malformed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_SKIP_PARSE=maybe")},
		"schema", "pull", reference, "--out", "malformed.hcl", "--plain-http")
	c.Assert(malformed.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", malformed.Stdout, malformed.Stderr))
	c.Assert(malformed.Stderr, qt.Contains, `invalid boolean value "maybe" for PTAH_SKIP_PARSE`)

	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: append(os.Environ(), "PTAH_SKIP_PARSE=1")},
		"schema", "pull", reference, "--out", "schema.hcl", "--plain-http")
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(readSkipParseFile(c, filepath.Join(dir, "schema.hcl")), qt.Equals, skipParseFutureSchema)
}
