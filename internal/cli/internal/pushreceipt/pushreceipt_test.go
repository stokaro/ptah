package pushreceipt_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/internal/cli/internal/pushreceipt"
	"ptah.run/internal/ociartifact"
)

const (
	registryDestination = "oci://registry.example/acme/app:v1"
	manifestDigest      = "sha256:20ec84e49b907163744a887dc2126491053bba1f2c82ddf6c73c006babc81512"
)

func TestParseFormat_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		destination string
		want        pushreceipt.Format
	}{
		{name: "text", value: "text", destination: registryDestination, want: pushreceipt.FormatText},
		{name: "json", value: "json", destination: registryDestination, want: pushreceipt.FormatJSON},
		{name: "reference", value: "reference", destination: registryDestination, want: pushreceipt.FormatReference},
		{name: "case and space are ignored", value: " JSON ", destination: registryDestination, want: pushreceipt.FormatJSON},
		// The receipt names the layout it wrote to, so the text format has
		// something true to say about a layout push.
		{name: "text to a layout", value: "text", destination: "oci-layout:///tmp/release:v1", want: pushreceipt.FormatText},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pushreceipt.ParseFormat(tc.value, tc.destination)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestParseFormat_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		destination string
		wantErr     string
	}{
		{
			name:        "unknown format",
			value:       "yaml",
			destination: registryDestination,
			wantErr:     `unsupported output format "yaml": expected text, json, or reference`,
		},
		{
			name:        "empty format",
			value:       "",
			destination: registryDestination,
			wantErr:     `unsupported output format "": expected text, json, or reference`,
		},
		{
			name:        "json to a layout",
			value:       "json",
			destination: "oci-layout:///tmp/release:v1",
			wantErr: `--format json prints a pinned oci:// reference, and an oci-layout:// destination has none; ` +
				`push to a registry, or use --format text`,
		},
		{
			name:        "reference to a layout",
			value:       "reference",
			destination: "oci-layout:///tmp/release:v1",
			wantErr: `--format reference prints a pinned oci:// reference, and an oci-layout:// destination has none; ` +
				`push to a registry, or use --format text`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := pushreceipt.ParseFormat(tc.value, tc.destination)

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(got, qt.Equals, pushreceipt.Format(""))
		})
	}
}

func registryResult(c *qt.C, tags []string) ociartifact.PushResult {
	c.Helper()
	reference, err := ociartifact.ParseRef(registryDestination)
	c.Assert(err, qt.IsNil)
	return ociartifact.PushResult{
		Reference: reference,
		Descriptor: ocispec.Descriptor{
			MediaType:    ocispec.MediaTypeImageManifest,
			Digest:       digest.Digest(manifestDigest),
			Size:         512,
			ArtifactType: ociartifact.SchemaArtifactType,
		},
		Tags: tags,
	}
}

// The text format prints the receipt the command worded, and nothing of its own.
func TestWrite_TextPrintsTheReceipt(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := pushreceipt.Write(&out, pushreceipt.FormatText, registryResult(c, []string{"v1"}), "", "Pushed schema\n")

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Pushed schema\n")
}

// The reference format prints the pinned reference and a newline, which is what
// `ptah oci resolve` prints, so `$(...)` captures the same value from both.
func TestWrite_ReferencePrintsThePinnedReferenceAlone(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := pushreceipt.Write(&out, pushreceipt.FormatReference, registryResult(c, []string{"v1"}), "", "Pushed schema\n")

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "oci://registry.example/acme/app@"+manifestDigest+"\n")
}

// The JSON document is the contract a script parses, so its bytes are pinned:
// a renamed or reordered field fails here before it reaches a caller.
func TestWrite_JSONDocument(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer
	result := registryResult(c, []string{"v20261101120000-ABCD", "v1"})

	err := pushreceipt.Write(&out, pushreceipt.FormatJSON, result, "v20261101120000-ABCD", "Pushed schema\n")

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `{
  "reference": "oci://registry.example/acme/app:v1",
  "pinned_reference": "oci://registry.example/acme/app@`+manifestDigest+`",
  "digest": "`+manifestDigest+`",
  "media_type": "application/vnd.oci.image.manifest.v1+json",
  "size": 512,
  "artifact_type": "application/vnd.stokaro.ptah.schema.v1",
  "tags": [
    "v20261101120000-ABCD",
    "v1"
  ],
  "version": "v20261101120000-ABCD"
}
`)
}

// Every field is present on every push: a push that wrote no version tag
// reports an empty version, and a result that carries no tags reports an empty
// list rather than null.
func TestWrite_JSONKeepsEveryField(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := pushreceipt.Write(&out, pushreceipt.FormatJSON, registryResult(c, nil), "", "")

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `"tags": [],`)
	c.Assert(out.String(), qt.Contains, `"version": ""`)
}

// layoutResult pushes a real artifact into a layout on disk: a layout
// reference is built only by the push that writes one.
func layoutResult(c *qt.C) ociartifact.PushResult {
	c.Helper()
	c.Setenv("DOCKER_CONFIG", c.TempDir())
	c.Setenv("PTAH_OCI_CONFIG", filepath.Join(c.TempDir(), "registries.json"))
	c.Setenv("PTAH_OCI_CREDENTIAL_STORE", "file")
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{})
	c.Assert(err, qt.IsNil)
	result, err := client.Push(c.Context(), "oci-layout://"+filepath.Join(c.TempDir(), "layout")+":v1", fstest.MapFS{
		"migration.sql": {Data: []byte("SELECT 1;\n")},
	}, ociartifact.PushOptions{ArtifactType: ociartifact.MigrationArtifactType})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Reference.IsLayout(), qt.IsTrue)
	return result
}

// A machine format never prints a reference it cannot build. ParseFormat
// refuses a layout destination before a push; this is the same refusal for a
// caller that wrote a layout result anyway.
func TestWrite_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  pushreceipt.Format
		wantErr string
	}{
		{name: "reference", format: pushreceipt.FormatReference, wantErr: `an oci-layout:// destination has no pinned reference to print`},
		{name: "json", format: pushreceipt.FormatJSON, wantErr: `an oci-layout:// destination has no pinned reference to print`},
		{name: "unknown", format: pushreceipt.Format("yaml"), wantErr: `unsupported output format "yaml": expected text, json, or reference`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			result := layoutResult(c)
			var out bytes.Buffer

			err := pushreceipt.Write(&out, tc.format, result, "", "Pushed schema\n")

			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}
