package schemaartifact_test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/core/coverage"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/ociregistrytest"
	"ptah.run/internal/schemaartifact"
)

const (
	// futureSchema stands for a schema a newer Ptah wrote: this build's parser
	// refuses it, and its digest verifies.
	futureSchema = "table \"t\" {\n  column \"id\" { type = bigint \"a second value\" }\n}\n"
	plainSchema  = "schema \"public\" {}\n"
	rowLayer     = `{"sets":[{"table":"users","keys":["id"],"columns":["id"],"rows":[]}]}` + "\n"
	checksLayer  = "-- +ptah check name=\"anything\" assert=\"SELECT 1\"\n"

	tag = "v1"
)

var schemaFormat = map[string]string{"io.stokaro.ptah.schema-format": "hcl"}

// layer is one layer of a manifest a test builds by hand. A declared size above
// zero replaces the real one, for a manifest that claims more than it holds.
type layer struct {
	title     string
	mediaType string
	content   string
	size      int64
}

// publishManifest tags a manifest in store that nothing in this build would
// publish. The registry stores what it is given, so a pull is all that stands
// between one of these and the disk.
func publishManifest(c *qt.C, store *memory.Store, artifactType string, annotations map[string]string, layers ...layer) ocispec.Descriptor {
	c.Helper()
	descriptors := make([]ocispec.Descriptor, 0, len(layers))
	for _, spec := range layers {
		descriptor, err := oras.PushBytes(c.Context(), store, spec.mediaType, []byte(spec.content))
		c.Assert(err, qt.IsNil)
		descriptor.Annotations = map[string]string{ocispec.AnnotationTitle: spec.title}
		descriptors = append(descriptors, withDeclaredSize(descriptor, spec.size))
	}
	manifest, err := oras.PackManifest(c.Context(), store, oras.PackManifestVersion1_1, artifactType, oras.PackManifestOptions{
		Layers:              descriptors,
		ManifestAnnotations: annotations,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(store.Tag(c.Context(), manifest, tag), qt.IsNil)
	return manifest
}

func withDeclaredSize(descriptor ocispec.Descriptor, size int64) ocispec.Descriptor {
	if size > 0 {
		descriptor.Size = size
	}
	return descriptor
}

func schemaLayer(schema string) layer {
	return layer{title: schemaartifact.FileName, mediaType: schemaartifact.LayerMediaType, content: schema}
}

// An artifact this build cannot parse is the reason the flag exists: the pull
// that reads it refuses, and the pull that does not hands over its bytes.
func TestPullUnparsedFrom_HappyPath(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	manifest := publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schemaLayer(futureSchema))

	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, tag)

	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Descriptor.Digest, qt.Equals, manifest.Digest)
	schema, err := fs.ReadFile(pulled.FileSystem, schemaartifact.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(schema), qt.Equals, futureSchema)

	_, err = schemaartifact.PullFrom(c.Context(), store, tag, coverage.Vocabulary{}, builtintest.Runtime().HCL())
	c.Assert(err, qt.ErrorMatches, `parse schema artifact: parse HCL schema: schema\.hcl:2,.*Invalid single-argument block definition.*`)
}

// Everything a default pull refuses without reading a layer's content, an
// unparsed pull refuses too. Each row is a manifest a registry will store and
// this build would never publish.
func TestPullUnparsedFrom_FailurePath(t *testing.T) {
	const declaredAboveTheLimit = 65 << 20
	tests := []struct {
		name         string
		artifactType string
		annotations  map[string]string
		layers       []layer
		wantErr      string
	}{
		{
			name:         "another artifact type",
			artifactType: ociartifact.MigrationArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{schemaLayer(plainSchema)},
			wantErr:      `unexpected OCI artifact type: "application/vnd\.stokaro\.ptah\.migrations\.v1"`,
		},
		{
			name:         "a layer media type from a newer Ptah",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers: []layer{
				schemaLayer(plainSchema),
				{title: schemaartifact.ManagedDataFileName, mediaType: "application/vnd.stokaro.ptah.managed-data.v2+json", content: rowLayer},
			},
			wantErr: `unexpected OCI artifact type: file "managed-data\.json" has media type "application/vnd\.stokaro\.ptah\.managed-data\.v2\+json"`,
		},
		{
			name:         "a schema layer under a media type nothing writes",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: schemaartifact.FileName, mediaType: "application/octet-stream", content: plainSchema}},
			wantErr:      `unexpected OCI artifact type: file "schema\.hcl" has media type "application/octet-stream"`,
		},
		{
			name:         "no format annotation",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  make(map[string]string),
			layers:       []layer{schemaLayer(plainSchema)},
			wantErr:      `unsupported schema artifact format ""`,
		},
		{
			name:         "another format",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  map[string]string{"io.stokaro.ptah.schema-format": "sql"},
			layers:       []layer{schemaLayer(plainSchema)},
			wantErr:      `unsupported schema artifact format "sql"`,
		},
		{
			name:         "a file this package does not write",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers: []layer{
				schemaLayer(plainSchema),
				{title: "authorized_keys", mediaType: schemaartifact.LayerMediaType, content: "ssh-ed25519 AAAA\n"},
			},
			wantErr: `schema artifact carries unexpected file authorized_keys`,
		},
		{
			name:         "a file in a directory",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: "nested/schema.hcl", mediaType: schemaartifact.LayerMediaType, content: plainSchema}},
			wantErr:      `schema artifact must contain only schema\.hcl, managed-data\.json and checks\.sql`,
		},
		{
			name:         "no schema layer",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: schemaartifact.ManagedDataFileName, mediaType: schemaartifact.ManagedDataLayerMediaType, content: rowLayer}},
			wantErr:      `schema artifact must contain schema\.hcl`,
		},
		{
			name:         "a path that climbs out of the output directory",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: "../schema.hcl", mediaType: schemaartifact.LayerMediaType, content: plainSchema}},
			wantErr:      `unsafe OCI artifact path: "\.\./schema\.hcl"`,
		},
		{
			name:         "an absolute path",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: "/etc/schema.hcl", mediaType: schemaartifact.LayerMediaType, content: plainSchema}},
			wantErr:      `unsafe OCI artifact path: "/etc/schema\.hcl"`,
		},
		{
			name:         "the same file twice",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{schemaLayer(plainSchema), schemaLayer(futureSchema)},
			wantErr:      `unsafe OCI artifact path: duplicate file "schema\.hcl"`,
		},
		{
			name:         "a layer above the size limit",
			artifactType: ociartifact.SchemaArtifactType,
			annotations:  schemaFormat,
			layers:       []layer{{title: schemaartifact.FileName, mediaType: schemaartifact.LayerMediaType, content: plainSchema, size: declaredAboveTheLimit}},
			wantErr:      `OCI artifact limit exceeded: file "schema\.hcl" is 68157440 bytes, maximum is 67108864`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			store := memory.New()
			publishManifest(c, store, tt.artifactType, tt.annotations, tt.layers...)

			pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, tag)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(pulled.FileSystem, qt.IsNil)
		})
	}
}

// tamperedTarget answers a read of one digest with other bytes, which is what a
// registry or a path to it that altered a stored blob does.
type tamperedTarget struct {
	oras.ReadOnlyTarget
	digest  digest.Digest
	content []byte
}

func (t tamperedTarget) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	if target.Digest == t.digest {
		return io.NopCloser(bytes.NewReader(t.content)), nil
	}
	return t.ReadOnlyTarget.Fetch(ctx, target)
}

// The digest is the one thing tying what was approved to what lands on disk, and
// verifying it reads no content, so skipping the parse cannot skip it. Other
// bytes of the same length reach the digest comparison rather than the size
// comparison in front of it.
func TestPullUnparsedFrom_RefusesBytesTheDigestDoesNotCover(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	schema := layer{title: schemaartifact.FileName, mediaType: schemaartifact.LayerMediaType, content: "schema \"public\" {}\n"}
	publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schema)
	tampered := tamperedTarget{
		ReadOnlyTarget: store,
		digest:         digest.FromString(schema.content),
		content:        []byte("schema \"private\"{}\n"),
	}

	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), tampered, tag)

	c.Assert(err, qt.ErrorIs, content.ErrMismatchedDigest)
	c.Assert(err, qt.ErrorMatches, `fetch artifact file "schema\.hcl": .*`)
	c.Assert(pulled.FileSystem, qt.IsNil)
}

// The control on the test above: the same artifact through the same target,
// tampering with a digest nothing reads, is pulled.
func TestPullUnparsedFrom_ReadsThroughATargetThatTampersWithNothingItServes(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schemaLayer(plainSchema))
	untouched := tamperedTarget{ReadOnlyTarget: store, digest: digest.FromString("no layer has this content")}

	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), untouched, tag)

	c.Assert(err, qt.IsNil)
	c.Assert(pulled.FileSystem, qt.IsNotNil)
}

// A manifest whose body is not what its digest names is refused before any layer
// is looked at.
func TestPullUnparsedFrom_RefusesAManifestItsDigestDoesNotCover(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	manifest := publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schemaLayer(plainSchema))
	tampered := tamperedTarget{ReadOnlyTarget: store, digest: manifest.Digest, content: make([]byte, manifest.Size)}

	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), tampered, tag)

	c.Assert(err, qt.ErrorIs, content.ErrMismatchedDigest)
	c.Assert(err, qt.ErrorMatches, `fetch OCI manifest: .*`)
	c.Assert(pulled.FileSystem, qt.IsNil)
}

// Every file is the bytes the digest covered, under the names a default pull
// writes. The checks layer travels in the artifact and is written by neither.
func TestMaterializeUnparsed_HappyPath(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat,
		schemaLayer(futureSchema),
		layer{title: schemaartifact.ManagedDataFileName, mediaType: schemaartifact.ManagedDataLayerMediaType, content: rowLayer},
		layer{title: schemaartifact.ChecksFileName, mediaType: schemaartifact.ChecksLayerMediaType, content: checksLayer},
	)
	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, tag)
	c.Assert(err, qt.IsNil)
	directory := c.TempDir()

	written, err := schemaartifact.MaterializeUnparsed(pulled, filepath.Join(directory, "out.hcl"))

	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.DeepEquals, []string{
		filepath.Join(directory, "out.hcl"),
		filepath.Join(directory, schemaartifact.ManagedDataFileName),
	})
	c.Assert(readFile(c, written[0]), qt.Equals, futureSchema)
	c.Assert(readFile(c, written[1]), qt.Equals, rowLayer)
	_, err = os.Stat(filepath.Join(directory, schemaartifact.ChecksFileName))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

// An artifact with no row layer gets a layer that says so. What it protects is
// measured where a `data` block would otherwise be followed, by
// TestSchemaPullSkipParseDoesNotOpenAFileADataBlockNamesE2E.
func TestMaterializeUnparsed_StatesThatAnArtifactCarriesNoRows(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schemaLayer(plainSchema))
	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, tag)
	c.Assert(err, qt.IsNil)

	written, err := schemaartifact.MaterializeUnparsed(pulled, filepath.Join(c.TempDir(), "out.hcl"))

	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 2)
	c.Assert(readFile(c, written[1]), qt.Equals, `{"sets":[]}`+"\n")
}

// The layer has to land for the schema to stay: a schema without it reads as a
// working copy, and the reader would follow its `data` blocks.
func TestMaterializeUnparsed_LeavesNoSchemaWithoutItsLayer(t *testing.T) {
	c := qt.New(t)
	store := memory.New()
	publishManifest(c, store, ociartifact.SchemaArtifactType, schemaFormat, schemaLayer(plainSchema))
	pulled, err := schemaartifact.PullUnparsedFrom(c.Context(), store, tag)
	c.Assert(err, qt.IsNil)
	directory := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(directory, schemaartifact.ManagedDataFileName), []byte("taken"), 0o600), qt.IsNil)

	written, err := schemaartifact.MaterializeUnparsed(pulled, filepath.Join(directory, "out.hcl"))

	c.Assert(err, qt.ErrorMatches, `create schema artifact output: .*`)
	c.Assert(written, qt.IsNil)
	_, err = os.Stat(filepath.Join(directory, "out.hcl"))
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
	c.Assert(readFile(c, filepath.Join(directory, schemaartifact.ManagedDataFileName)), qt.Equals, "taken")
}

// isolateCredentials keeps a client away from the developer's credential stores.
// The in-process registry never challenges, so no credential is read; the client
// still opens the stores when it is built.
func isolateCredentials(c *qt.C) {
	c.Helper()
	c.Setenv("DOCKER_CONFIG", c.TempDir())
	c.Setenv("PTAH_OCI_CONFIG", filepath.Join(c.TempDir(), "registries.json"))
	c.Setenv("PTAH_OCI_CREDENTIAL_STORE", "file")
}

func TestPullUnparsedToFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, "acme/schema")
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	pushed, err := client.Push(c.Context(), registry.Reference(tag), fsWithSchema(futureSchema), ociartifact.PushOptions{
		ArtifactType:   ociartifact.SchemaArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		Annotations:    schemaFormat,
	})
	c.Assert(err, qt.IsNil)

	pulled, written, err := schemaartifact.PullUnparsedToFile(c.Context(), registry.Reference(tag), filepath.Join(c.TempDir(), "out.hcl"), true)

	c.Assert(err, qt.IsNil)
	c.Assert(pulled.Descriptor.Digest, qt.Equals, pushed.Descriptor.Digest)
	c.Assert(written, qt.HasLen, 2)
	c.Assert(filepath.Base(written[0]), qt.Equals, "out.hcl")
	c.Assert(filepath.Base(written[1]), qt.Equals, schemaartifact.ManagedDataFileName)
	c.Assert(readFile(c, written[0]), qt.Equals, futureSchema)
}

// A refusal of the artifact leaves the directory as it found it. It is the same
// client call as the path above, so this is what a registry serving a wrong
// artifact costs a process that wrote nothing.
func TestPullUnparsedToFile_WritesNothingForARefusedArtifact(t *testing.T) {
	c := qt.New(t)
	isolateCredentials(c)
	registry := ociregistrytest.Start(c, "acme/schema")
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: true})
	c.Assert(err, qt.IsNil)
	_, err = client.Push(c.Context(), registry.Reference(tag), fsWithSchema(plainSchema), ociartifact.PushOptions{
		ArtifactType:   ociartifact.MigrationArtifactType,
		LayerMediaType: schemaartifact.LayerMediaType,
		Annotations:    schemaFormat,
	})
	c.Assert(err, qt.IsNil)
	directory := c.TempDir()

	_, written, err := schemaartifact.PullUnparsedToFile(c.Context(), registry.Reference(tag), filepath.Join(directory, "out.hcl"), true)

	c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
	c.Assert(written, qt.IsNil)
	entries, err := os.ReadDir(directory)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}

// These answers need no registry. The reference names a host that does not
// resolve, so a pull that went on would report a dial failure instead.
func TestPullUnparsedToFile_RefusesAnOutputBeforeTheNetwork(t *testing.T) {
	c := qt.New(t)
	existing := filepath.Join(c.TempDir(), "taken.hcl")
	c.Assert(os.WriteFile(existing, []byte("existing"), 0o600), qt.IsNil)
	tests := []struct {
		name    string
		output  string
		wantErr string
	}{
		{name: "no output", output: " ", wantErr: `schema artifact output file is required`},
		{name: "an output that exists", output: existing, wantErr: `schema artifact output already exists: .*`},
		{name: "another parser's extension", output: filepath.Join(c.TempDir(), "schema.sql"), wantErr: `an unparsed schema artifact is written to a \.hcl file, .*schema\.sql`},
		{name: "no extension", output: filepath.Join(c.TempDir(), "schema"), wantErr: `an unparsed schema artifact is written to a \.hcl file, .*schema`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			pulled, written, err := schemaartifact.PullUnparsedToFile(c.Context(), "oci://registry.invalid/acme/schema:latest", tt.output, false)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(pulled.FileSystem, qt.IsNil)
			c.Assert(written, qt.IsNil)
		})
	}
}

func fsWithSchema(schema string) fs.FS {
	return fstest.MapFS{schemaartifact.FileName: {Data: []byte(schema)}}
}

func readFile(c *qt.C, path string) string {
	c.Helper()
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return string(data)
}
