package ociartifact_test

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content/memory"

	"ptah.run/internal/ociartifact"
)

// referrerKind is one report Ptah attaches: the artifact type its manifest
// declares, the layer media type its payload carries, and the file it writes.
type referrerKind struct {
	name         string
	artifactType string
	layerType    string
	fileName     string
}

// reportReferrerKinds is every referrer `ptah oci fetch --type` accepts.
//
// The media types are spelled here rather than imported from the three report
// packages on purpose: importing them would make this test agree with the
// writers by construction, and agreement between the writer and the reader is
// the property under test.
var reportReferrerKinds = []referrerKind{
	{
		name:         "deployment",
		artifactType: ociartifact.DeploymentArtifactType,
		layerType:    "application/vnd.stokaro.ptah.deployment.report.v1+json",
		fileName:     "deployment.json",
	},
	{
		name:         "lint",
		artifactType: ociartifact.LintArtifactType,
		layerType:    "application/vnd.stokaro.ptah.migration.lint.report.v1+json",
		fileName:     "lint.json",
	},
	{
		name:         "plan",
		artifactType: ociartifact.PlanArtifactType,
		layerType:    "application/vnd.stokaro.ptah.migration.plan.v1+json",
		fileName:     "plan.json",
	},
}

// TestPullFrom_ReadsAReferrerWrittenWithItsOwnLayerMediaType_HappyPath is the
// round trip `ptah oci fetch` performs.
//
// Every report Ptah attaches names its own layer media type, and a pull that
// named none defaulted to FileMediaType, which no report layer uses. The layer
// check then refused the artifact the caller had just asked for by digest, so
// each of the three referrer kinds the verb accepts was the one it could not
// read (stokaro/ptah#3120).
//
// The attachment is written with the media type the real writers pass, not with
// the default: an artifact hand-built with FileMediaType round-trips even with
// the defect present, which is why this test attaches the way production does.
func TestPullFrom_ReadsAReferrerWrittenWithItsOwnLayerMediaType_HappyPath(t *testing.T) {
	for _, kind := range reportReferrerKinds {
		t.Run(kind.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			store := memory.New()
			subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
				"migration.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
			}, ociartifact.PushOptions{
				ArtifactType: ociartifact.MigrationArtifactType,
				Tags:         []string{"latest"},
				Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
			})
			c.Assert(err, qt.IsNil)

			attachment, err := ociartifact.AttachTo(ctx, store, subject.Descriptor, fstest.MapFS{
				kind.fileName: {Data: []byte(`{"schema_version":1}`)},
			}, ociartifact.AttachmentOptions{
				ArtifactType:   kind.artifactType,
				LayerMediaType: kind.layerType,
				Annotations:    map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(store.Tag(ctx, attachment.Descriptor, "referrer"), qt.IsNil)

			pulled, err := ociartifact.PullFrom(ctx, store, "referrer", ociartifact.PullOptions{})

			c.Assert(err, qt.IsNil)
			c.Assert(pulled.ArtifactType, qt.Equals, kind.artifactType)
			contents, err := fs.ReadFile(pulled.FileSystem, kind.fileName)
			c.Assert(err, qt.IsNil)
			c.Assert(string(contents), qt.Equals, `{"schema_version":1}`)
		})
	}
}

// TestPullFrom_KeepsTheFileMediaTypeForAPlainArtifact_HappyPath is the control
// for the resolution above.
//
// A migration or schema artifact carries plain file layers, and resolving the
// layer media type from the manifest must not change what those pulls expect.
// Without this, "expect whatever the layers happen to carry" would satisfy the
// round trips above while dropping the check that makes a mislabeled layer an
// error.
func TestPullFrom_KeepsTheFileMediaTypeForAPlainArtifact_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	pushed, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(pushed.Descriptor.Digest, qt.Not(qt.Equals), "")

	pulled, err := ociartifact.PullFrom(ctx, store, "latest", ociartifact.PullOptions{})

	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(pulled.FileSystem, "migration.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "CREATE TABLE users (id INTEGER);\n")
}

// TestPullFrom_RefusesAMislabeledReferrerLayer_FailurePath keeps the layer
// check from becoming a formality.
//
// Resolving the expected media type from the manifest must still refuse a layer
// that does not carry it, or the repair would have replaced a false refusal
// with no refusal at all.
func TestPullFrom_RefusesAMislabeledReferrerLayer_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"migration.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest"},
		Annotations:  map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	// A deployment manifest whose layer carries the lint report media type.
	attachment, err := ociartifact.AttachTo(ctx, store, subject.Descriptor, fstest.MapFS{
		"deployment.json": {Data: []byte(`{"schema_version":1}`)},
	}, ociartifact.AttachmentOptions{
		ArtifactType:   ociartifact.DeploymentArtifactType,
		LayerMediaType: "application/vnd.stokaro.ptah.migration.lint.report.v1+json",
		Annotations:    map[string]string{ocispec.AnnotationCreated: fixedCreatedAt},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(store.Tag(ctx, attachment.Descriptor, "mislabeled"), qt.IsNil)

	_, err = ociartifact.PullFrom(ctx, store, "mislabeled", ociartifact.PullOptions{})

	c.Assert(err, qt.ErrorIs, ociartifact.ErrUnexpectedArtifactType)
}

// TestReferrerLayerMediaTypeCoversEveryReportArtifactType is the guard the
// mapping's declaration names.
//
// A referrer kind added without an entry keeps the FileMediaType default, which
// is exactly the shape that made all three existing kinds unreadable. The
// failure mode is silent: the writer works, the fetch verb accepts the type,
// and only a pull discovers it.
func TestReferrerLayerMediaTypeCoversEveryReportArtifactType(t *testing.T) {
	for _, kind := range reportReferrerKinds {
		t.Run(kind.name, func(t *testing.T) {
			c := qt.New(t)

			mediaType, ok := ociartifact.ReferrerLayerMediaType(kind.artifactType)

			c.Assert(ok, qt.IsTrue)
			c.Assert(mediaType, qt.Equals, kind.layerType)
		})
	}
}

// TestReferrerLayerMediaType_FailurePath pins that an artifact type carrying
// plain files reports no report media type, which is what keeps the migration
// and schema pulls on FileMediaType.
func TestReferrerLayerMediaType_FailurePath(t *testing.T) {
	rows := []struct {
		name         string
		artifactType string
	}{
		{name: "migration", artifactType: ociartifact.MigrationArtifactType},
		{name: "schema", artifactType: ociartifact.SchemaArtifactType},
		{name: "unknown", artifactType: "application/vnd.example.nothing.v1"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			mediaType, ok := ociartifact.ReferrerLayerMediaType(row.artifactType)

			c.Assert(ok, qt.IsFalse)
			c.Assert(mediaType, qt.Equals, "")
		})
	}
}
