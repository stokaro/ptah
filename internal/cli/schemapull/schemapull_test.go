package schemapull_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/schemapull"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/ociregistrytest"
	"ptah.run/internal/schemaartifact"
)

func TestNewSchemaPullCommand_Help(t *testing.T) {
	c := qt.New(t)
	cmd := schemapull.NewSchemaPullCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "pull <oci-reference | git-reference | https-url>")
	c.Assert(output.String(), qt.Contains, "--out")
	c.Assert(output.String(), qt.Contains, "--plain-http")
	c.Assert(output.String(), qt.Contains, "--skip-parse")
}

func TestNewSchemaPullCommand_RequiresOutput(t *testing.T) {
	c := qt.New(t)
	cmd := schemapull.NewSchemaPullCommand()
	cmd.SetArgs([]string{"oci://registry.example/acme/schema"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "schema artifact output file is required")
}

// futureSchema stands for a schema a newer Ptah wrote: this build refuses to
// parse it, and its digest verifies.
const futureSchema = "table \"t\" {\n  column \"id\" { type = bigint \"a second value\" }\n}\n"

// publishSchema serves one schema artifact from an in-process registry and
// returns its reference and manifest digest.
func publishSchema(c *qt.C, schema string) (reference, digest string) {
	c.Helper()
	c.Setenv("DOCKER_CONFIG", c.TempDir())
	c.Setenv("PTAH_OCI_CONFIG", filepath.Join(c.TempDir(), "registries.json"))
	c.Setenv("PTAH_OCI_CREDENTIAL_STORE", "file")
	registry := ociregistrytest.Start(c, "acme/schema")
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	pushed, err := client.Push(c.Context(), registry.Reference("v1"), fstest.MapFS{
		schemaartifact.FileName: {Data: []byte(schema)},
	}, ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		Annotations:    map[string]string{"io.stokaro.ptah.schema-format": "hcl"},
	})
	c.Assert(err, qt.IsNil)
	return registry.Reference("v1"), pushed.Descriptor.Digest.String()
}

func runPull(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := schemapull.NewSchemaPullCommand()
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetArgs(args)
	cmd.SetContext(c.Context())
	err := cmd.Execute()
	return stdout.String(), err
}

func TestSkipParse_HappyPath_WritesWhatTheDefaultRefuses(t *testing.T) {
	c := qt.New(t)
	reference, digest := publishSchema(c, futureSchema)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	stdout, err := runPull(c, reference, "--out", out, "--plain-http", "--skip-parse")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Digest: "+digest+"\n")
	c.Assert(stdout, qt.Contains, schemaartifact.ManagedDataFileName)
	c.Assert(stdout, qt.Contains, "Not parsed: ")
	written, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, futureSchema)
}

// The default is the control: the same artifact, the same command, and the
// parse is what stands between them.
func TestSkipParse_FailurePath_TheDefaultStillParses(t *testing.T) {
	c := qt.New(t)
	reference, _ := publishSchema(c, futureSchema)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	stdout, err := runPull(c, reference, "--out", out, "--plain-http")

	c.Assert(err, qt.ErrorMatches, `parse schema artifact: parse HCL schema: schema\.hcl:2,.*`)
	c.Assert(stdout, qt.Equals, "")
	_, err = os.Stat(out)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}

func TestSkipParse_HappyPath_JSONSaysItDidNotParse(t *testing.T) {
	c := qt.New(t)
	reference, digest := publishSchema(c, futureSchema)
	directory := c.TempDir()

	stdout, err := runPull(c, reference, "--out", filepath.Join(directory, "schema.hcl"), "--plain-http", "--skip-parse", "--json")

	c.Assert(err, qt.IsNil)
	var report struct {
		Source    string   `json:"source"`
		Reference string   `json:"reference"`
		Digest    string   `json:"digest"`
		Written   []string `json:"written"`
		Unparsed  bool     `json:"unparsed"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	c.Assert(report.Source, qt.Equals, "oci")
	c.Assert(report.Reference, qt.Equals, reference)
	c.Assert(report.Digest, qt.Equals, digest)
	c.Assert(report.Written, qt.HasLen, 2)
	c.Assert(filepath.Base(report.Written[0]), qt.Equals, "schema.hcl")
	c.Assert(filepath.Base(report.Written[1]), qt.Equals, schemaartifact.ManagedDataFileName)
	c.Assert(report.Unparsed, qt.IsTrue)
}

// The document of a default pull is unchanged by the field an unparsed one adds.
func TestDefaultPull_JSONDoesNotMentionParsing(t *testing.T) {
	c := qt.New(t)
	reference, _ := publishSchema(c, "schema \"public\" {}\n")

	stdout, err := runPull(c, reference, "--out", filepath.Join(c.TempDir(), "schema.hcl"), "--plain-http", "--json")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Not(qt.Contains), "unparsed")
	var report struct {
		Written []string `json:"written"`
	}
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("%s", stdout))
	c.Assert(report.Written, qt.HasLen, 1)
}

// A Git or an HTTPS pull writes the file's bytes and parses nothing, so the flag
// would state something the command does not do. Each is refused before any
// network access: the address does not resolve.
func TestOCIOnlyFlags_RefusedOnAGitOrHTTPSReference(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		flag      string
		wantErr   string
	}{
		{name: "git skip-parse", reference: "git+https://git.invalid/acme/app.git", flag: "--skip-parse", wantErr: `--skip-parse is for an oci:// reference`},
		{name: "git plain-http", reference: "git+https://git.invalid/acme/app.git", flag: "--plain-http", wantErr: `--plain-http is for an oci:// reference`},
		{name: "https skip-parse", reference: "https://artifacts.invalid/app/schema.hcl", flag: "--skip-parse", wantErr: `--skip-parse is for an oci:// reference`},
		{name: "https plain-http", reference: "https://artifacts.invalid/app/schema.hcl", flag: "--plain-http", wantErr: `--plain-http is for an oci:// reference`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, err := runPull(c, tt.reference, "--path", "schema.hcl",
				"--out", filepath.Join(c.TempDir(), "schema.hcl"), tt.flag)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(stdout, qt.Equals, "")
		})
	}
}
