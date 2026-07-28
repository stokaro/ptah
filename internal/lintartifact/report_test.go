package lintartifact_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"

	"github.com/stokaro/ptah/internal/lintartifact"
	"github.com/stokaro/ptah/internal/migrationlintreport"
	"github.com/stokaro/ptah/internal/ociartifact"
	migrationlint "github.com/stokaro/ptah/migration/lint"
)

func TestNewFS_CleanReportUsesCanonicalJSON(t *testing.T) {
	c := qt.New(t)
	report := migrationlintreport.Report{
		FailureThreshold: migrationlintreport.FailOnError,
		Dir:              "oci://registry.example/acme/migrations:latest",
	}

	reportFS, err := lintartifact.NewFS(report)

	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(reportFS, lintartifact.FileName)
	c.Assert(err, qt.IsNil)
	report.Findings = []migrationlint.Finding{}
	c.Assert(contents, qt.DeepEquals, canonicalJSON(c, report))
	c.Assert(string(contents), qt.Contains, `"findings": []`)
}

func TestNewFS_FailedReportUsesCanonicalJSON(t *testing.T) {
	c := qt.New(t)
	report := failedReport()

	reportFS, err := lintartifact.NewFS(report)

	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(reportFS, lintartifact.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.DeepEquals, canonicalJSON(c, report))
	c.Assert(string(contents), qt.Contains, `"failed": true`)
}

func TestPublishTo_AttachesCanonicalReportToExactSubject(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"0000000001_init.up.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
		Tags:         []string{"latest"},
	})
	c.Assert(err, qt.IsNil)
	report := failedReport()

	attachment, err := lintartifact.PublishTo(ctx, store, subject.Descriptor, report)

	c.Assert(err, qt.IsNil)
	manifestBytes, err := content.FetchAll(ctx, store, attachment.Descriptor)
	c.Assert(err, qt.IsNil)
	var manifest ocispec.Manifest
	c.Assert(json.Unmarshal(manifestBytes, &manifest), qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.LintArtifactType)
	c.Assert(manifest.Subject, qt.IsNotNil)
	c.Assert(*manifest.Subject, qt.DeepEquals, subject.Descriptor)
	c.Assert(manifest.Layers, qt.HasLen, 1)
	c.Assert(manifest.Layers[0].MediaType, qt.Equals, lintartifact.LayerMediaType)
	c.Assert(manifest.Layers[0].Annotations[ocispec.AnnotationTitle], qt.Equals, lintartifact.FileName)
	contents, err := content.FetchAll(ctx, store, manifest.Layers[0])
	c.Assert(err, qt.IsNil)
	c.Assert(contents, qt.DeepEquals, canonicalJSON(c, report))
}

func TestPublish_RejectsNilClient(t *testing.T) {
	c := qt.New(t)

	_, err := lintartifact.Publish(
		context.Background(),
		nil,
		"oci://registry.example/acme/migrations:latest",
		ocispec.Descriptor{},
		migrationlintreport.Report{},
	)

	c.Assert(err, qt.ErrorMatches, "OCI client is required")
}

func canonicalJSON(c *qt.C, report migrationlintreport.Report) []byte {
	c.Helper()
	var output bytes.Buffer
	err := migrationlintreport.Write(&output, migrationlintreport.FormatJSON, report)
	c.Assert(err, qt.IsNil)
	return output.Bytes()
}

func failedReport() migrationlintreport.Report {
	return migrationlintreport.Report{
		Failed:           true,
		FailureThreshold: migrationlintreport.FailOnError,
		Dialect:          "postgres",
		Dir:              "oci://registry.example/acme/migrations:latest",
		Findings: []migrationlint.Finding{{
			Rule:     "DS101",
			Title:    "Table dropped",
			Severity: migrationlint.SeverityError,
			File:     "oci://registry.example/acme/migrations:latest/0000000001_drop.up.sql",
			Line:     1,
			Message:  "DROP TABLE permanently removes the table and its data",
		}},
	}
}
