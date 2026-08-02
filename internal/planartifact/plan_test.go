package planartifact_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/core/platform/capability"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/internal/planartifact"
	"go.5x5.cz/ptah/migration/safety"
)

func TestNewFS_UsesStateBoundCanonicalJSON(t *testing.T) {
	c := qt.New(t)
	subject := ocispec.Descriptor{
		Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		MediaType: ocispec.MediaTypeImageManifest,
		Size:      123,
	}
	report, err := planartifact.NewReport(
		subject,
		&dbtypes.DBSchema{},
		"postgres",
		capability.Capabilities{capability.CreateIndexConcurrently: true},
		[]string{"tenant", "public", "tenant"},
		sampleAssessments(),
	)
	c.Assert(err, qt.IsNil)

	planFS, err := planartifact.NewFS(report)
	c.Assert(err, qt.IsNil)
	got, err := fs.ReadFile(planFS, planartifact.FileName)
	c.Assert(err, qt.IsNil)
	var decoded planartifact.Report
	c.Assert(json.Unmarshal(got, &decoded), qt.IsNil)
	c.Assert(decoded.SchemaVersion, qt.Equals, planartifact.SchemaVersion)
	c.Assert(decoded.DesiredArtifactDigest, qt.Equals, subject.Digest.String())
	c.Assert(decoded.CurrentSchemaDigest, qt.Matches, `sha256:[a-f0-9]{64}`)
	c.Assert(decoded.Dialect, qt.Equals, "postgres")
	c.Assert(decoded.Schemas, qt.DeepEquals, []string{"public", "tenant"})
	c.Assert(decoded.Capabilities, qt.DeepEquals, capability.Capabilities{capability.CreateIndexConcurrently: true})
	c.Assert(decoded.Assessments, qt.DeepEquals, sampleAssessments())
	c.Assert(fstest.TestFS(planFS, planartifact.FileName), qt.IsNil)
}

func TestPublishTo_AttachesPlanToExactSubject(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"schema.hcl": {Data: []byte(`table "users" {}`)},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.SchemaArtifactType,
		Tags:         []string{"latest"},
	})
	c.Assert(err, qt.IsNil)

	report, err := planartifact.NewReport(
		subject.Descriptor,
		&dbtypes.DBSchema{},
		"postgres",
		capability.Capabilities{},
		nil,
		sampleAssessments(),
	)
	c.Assert(err, qt.IsNil)
	attachment, err := planartifact.PublishTo(ctx, store, subject.Descriptor, report)
	c.Assert(err, qt.IsNil)

	manifestBytes, err := content.FetchAll(ctx, store, attachment.Descriptor)
	c.Assert(err, qt.IsNil)
	var manifest ocispec.Manifest
	c.Assert(json.Unmarshal(manifestBytes, &manifest), qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.PlanArtifactType)
	c.Assert(manifest.Subject, qt.IsNotNil)
	c.Assert(manifest.Subject.Digest, qt.Equals, subject.Descriptor.Digest)
	c.Assert(manifest.Layers, qt.HasLen, 1)
	c.Assert(manifest.Layers[0].MediaType, qt.Equals, planartifact.LayerMediaType)
	c.Assert(manifest.Layers[0].Annotations[ocispec.AnnotationTitle], qt.Equals, planartifact.FileName)

	err = store.Tag(ctx, attachment.Descriptor, "plan")
	c.Assert(err, qt.IsNil)
	pulled, err := ociartifact.PullFrom(ctx, store, "plan", ociartifact.PullOptions{
		ExpectedArtifactTypes: []string{ociartifact.PlanArtifactType},
		LayerMediaType:        planartifact.LayerMediaType,
	})
	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(pulled.FileSystem, planartifact.FileName)
	c.Assert(err, qt.IsNil)
	var pulledReport planartifact.Report
	c.Assert(json.Unmarshal(contents, &pulledReport), qt.IsNil)
	c.Assert(pulledReport.DesiredArtifactDigest, qt.Equals, subject.Descriptor.Digest.String())
	c.Assert(pulledReport.Assessments, qt.DeepEquals, sampleAssessments())
}

func TestPublishTo_RejectsDesiredDigestMismatch(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"schema.hcl": {Data: []byte(`table "users" {}`)},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.SchemaArtifactType,
	})
	c.Assert(err, qt.IsNil)
	report, err := planartifact.NewReport(
		ocispec.Descriptor{
			Digest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      123,
		},
		&dbtypes.DBSchema{},
		"postgres",
		capability.Capabilities{},
		nil,
		sampleAssessments(),
	)
	c.Assert(err, qt.IsNil)

	_, err = planartifact.PublishTo(ctx, store, subject.Descriptor, report)

	c.Assert(err, qt.ErrorMatches, "migration plan desired artifact digest does not match attachment subject")
}

func TestPublish_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := planartifact.Publish(
		context.Background(),
		nil,
		"oci://registry.example/acme/schema:latest",
		ocispec.Descriptor{Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		planartifact.Report{},
	)

	c.Assert(err, qt.ErrorMatches, "OCI client is required")
}

func sampleAssessments() []safety.StatementAssessment {
	return []safety.StatementAssessment{{
		Index:     1,
		NodeType:  "drop_table",
		Subject:   "legacy_users",
		Statement: "DROP TABLE legacy_users;",
		Severity:  safety.Destructive,
		Reason:    "DROP TABLE removes the table and all rows",
	}}
}
