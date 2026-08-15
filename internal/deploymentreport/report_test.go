package deploymentreport_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"

	"go.5x5.cz/ptah/internal/deploymentreport"
	"go.5x5.cz/ptah/internal/ociartifact"
	"go.5x5.cz/ptah/migration/migrator"
)

const artifactDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestAppliedVersionDelta(t *testing.T) {
	tests := []struct {
		name   string
		before []int64
		after  []int64
		want   []int64
	}{
		{
			name:   "linear deployment",
			before: []int64{1},
			after:  []int64{1, 2, 3},
			want:   []int64{2, 3},
		},
		{
			name:   "out-of-order version applied",
			before: []int64{5},
			after:  []int64{5, 3},
			want:   []int64{3},
		},
		{
			name:   "duplicates do not leak into report",
			before: nil,
			after:  []int64{2, 1, 2},
			want:   []int64{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := deploymentreport.AppliedVersionDelta(tt.before, tt.after)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestHasAppliedChanges(t *testing.T) {
	c := qt.New(t)

	c.Assert(
		deploymentreport.HasAppliedChanges(
			&migrator.MigrationStatus{AppliedMigrations: []int64{1}},
			&migrator.MigrationStatus{AppliedMigrations: []int64{1, 2}},
		),
		qt.IsTrue,
	)
	c.Assert(
		deploymentreport.HasAppliedChanges(
			&migrator.MigrationStatus{AppliedMigrations: []int64{1}},
			&migrator.MigrationStatus{AppliedMigrations: []int64{1}},
		),
		qt.IsFalse,
	)
	c.Assert(deploymentreport.HasAppliedChanges(nil, &migrator.MigrationStatus{}), qt.IsFalse)
	c.Assert(deploymentreport.HasAppliedChanges(&migrator.MigrationStatus{}, nil), qt.IsFalse)
}

func TestNewSuccessfulReport_HappyPath(t *testing.T) {
	c := qt.New(t)
	startedAt := time.Date(2026, time.July, 28, 6, 9, 10, 0, time.UTC)
	finishedAt := time.Date(2026, time.July, 28, 6, 10, 11, 0, time.UTC)

	report, err := deploymentreport.NewSuccessfulReport(deploymentreport.SuccessfulOptions{
		Subject: ocispec.Descriptor{
			Digest: artifactDigest,
		},
		Dialect: "postgres",
		Before: &migrator.MigrationStatus{
			CurrentVersion:    1,
			AppliedMigrations: []int64{1},
		},
		After: &migrator.MigrationStatus{
			CurrentVersion:    3,
			AppliedMigrations: []int64{1, 3, 2},
		},
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.DeploymentID, qt.Matches, `deployment-[A-Z2-7]+`)
	c.Assert(report.MigrationArtifactDigest, qt.Equals, artifactDigest)
	c.Assert(report.Dialect, qt.Equals, "postgres")
	c.Assert(report.FromVersion, qt.Equals, int64(1))
	c.Assert(report.ToVersion, qt.Equals, int64(3))
	c.Assert(report.AppliedVersions, qt.DeepEquals, []int64{2, 3})
	c.Assert(report.StartedAt, qt.Equals, startedAt)
	c.Assert(report.FinishedAt, qt.Equals, finishedAt)
	c.Assert(report.Outcome, qt.Equals, deploymentreport.OutcomeSucceeded)
}

func TestNewSuccessfulReport_FailurePath(t *testing.T) {
	t.Run("missing before status", func(t *testing.T) {
		c := qt.New(t)
		opts := validSuccessfulOptions()
		opts.Before = nil

		report, err := deploymentreport.NewSuccessfulReport(opts)

		c.Assert(err, qt.ErrorMatches, "invalid deployment report: before migration status is required")
		c.Assert(report, qt.DeepEquals, deploymentreport.Report{})
	})

	t.Run("missing after status", func(t *testing.T) {
		c := qt.New(t)
		opts := validSuccessfulOptions()
		opts.After = nil

		report, err := deploymentreport.NewSuccessfulReport(opts)

		c.Assert(err, qt.ErrorMatches, "invalid deployment report: after migration status is required")
		c.Assert(report, qt.DeepEquals, deploymentreport.Report{})
	})
}

func TestNewFS_HappyPath(t *testing.T) {
	c := qt.New(t)
	startedAt := time.Date(2026, time.July, 28, 8, 9, 10, 123456789, time.FixedZone("CEST", 2*60*60))
	finishedAt := time.Date(2026, time.July, 28, 8, 10, 11, 987654321, time.FixedZone("CEST", 2*60*60))

	reportFS, err := deploymentreport.NewFS(deploymentreport.Report{
		DeploymentID:            "deployment-019fa5",
		MigrationArtifactDigest: artifactDigest,
		Dialect:                 "postgres",
		FromVersion:             202607280001,
		ToVersion:               202607280003,
		AppliedVersions:         []int64{202607280002, 202607280003},
		StartedAt:               startedAt,
		FinishedAt:              finishedAt,
		Outcome:                 deploymentreport.OutcomeSucceeded,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fstest.TestFS(reportFS, deploymentreport.FileName), qt.IsNil)

	contents, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals,
		"{\"schema_version\":1,\"deployment_id\":\"deployment-019fa5\","+
			"\"migration_artifact_digest\":\""+artifactDigest+"\","+
			"\"dialect\":\"postgres\",\"from_version\":202607280001,"+
			"\"to_version\":202607280003,\"applied_versions\":[202607280002,202607280003],"+
			"\"started_at\":\"2026-07-28T06:09:10.123456789Z\","+
			"\"finished_at\":\"2026-07-28T06:10:11.987654321Z\",\"outcome\":\"succeeded\"}\n")
}

func TestNewFS_EmptyAppliedVersionsAreCanonical(t *testing.T) {
	c := qt.New(t)
	report := validReport()
	report.AppliedVersions = nil
	report.ToVersion = report.FromVersion

	reportFS, err := deploymentreport.NewFS(report)
	c.Assert(err, qt.IsNil)

	contents, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Contains, `"applied_versions":[]`)
}

func TestNewFS_ReturnsImmutableFilesystem(t *testing.T) {
	c := qt.New(t)
	report := validReport()

	reportFS, err := deploymentreport.NewFS(report)
	c.Assert(err, qt.IsNil)

	firstRead, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	firstRead[0] = '['
	report.AppliedVersions[0] = 999

	secondRead, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(secondRead[0], qt.Equals, byte('{'))
	c.Assert(string(secondRead), qt.Not(qt.Contains), "999")
}

func TestNewFS_DoesNotLeakSecrets(t *testing.T) {
	c := qt.New(t)
	t.Setenv("DATABASE_URL", "postgres://admin:database-secret@prod.internal/app")
	t.Setenv("PTAH_REGISTRY_TOKEN", "registry-secret")

	reportFS, err := deploymentreport.NewFS(validReport())
	c.Assert(err, qt.IsNil)

	contents, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	for _, prohibited := range []string{
		"database-secret",
		"registry-secret",
		"database_url",
		"hostname",
		"environment",
		"local_path",
		"sql",
		"credentials",
	} {
		c.Assert(string(contents), qt.Not(qt.Contains), prohibited)
	}
}

func TestNewFS_FailurePath(t *testing.T) {
	t.Run("missing deployment ID", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.DeploymentID = ""
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: deployment ID is required")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("invalid deployment ID", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.DeploymentID = "deployment/../../secret"
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: deployment ID has an invalid format")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("missing artifact digest", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.MigrationArtifactDigest = ""
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: migration artifact digest is required")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("invalid artifact digest", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.MigrationArtifactDigest = "https://user:credential@registry.example/artifact"
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: migration artifact digest has an invalid format")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("unsupported artifact digest algorithm", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.MigrationArtifactDigest = "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: migration artifact digest has an invalid format")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("missing dialect", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.Dialect = ""
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: dialect is required")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("invalid dialect", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.Dialect = "postgres; DROP TABLE users"
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: dialect has an invalid format")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("negative from version", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.FromVersion = -1
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: from version must not be negative")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("to version before from version", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.ToVersion = report.FromVersion - 1
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: to version must not be less than from version")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("non-positive applied version", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.AppliedVersions = []int64{0}
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: applied versions must be positive")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("unordered applied versions", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.AppliedVersions = []int64{3, 2}
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: applied versions must be strictly increasing")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("missing started timestamp", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.StartedAt = time.Time{}
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: started timestamp is invalid")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("missing finished timestamp", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.FinishedAt = time.Time{}
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: finished timestamp is invalid")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("finished before started", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.FinishedAt = report.StartedAt.Add(-time.Nanosecond)
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches,
			"invalid deployment report: finished timestamp must not be before started timestamp")
		c.Assert(reportFS, qt.IsNil)
	})

	t.Run("invalid outcome", func(t *testing.T) {
		c := qt.New(t)
		report := validReport()
		report.Outcome = deploymentreport.Outcome("failed")
		reportFS, err := deploymentreport.NewFS(report)
		c.Assert(err, qt.ErrorMatches, "invalid deployment report: outcome is invalid")
		c.Assert(reportFS, qt.IsNil)
	})
}

func TestNewFS_ValidationErrorsDoNotLeakInput(t *testing.T) {
	c := qt.New(t)
	report := validReport()
	report.DeploymentID = "postgres://admin:top-secret@prod.internal/app"

	reportFS, err := deploymentreport.NewFS(report)
	c.Assert(err, qt.ErrorMatches, "invalid deployment report: deployment ID has an invalid format")
	c.Assert(err.Error(), qt.Not(qt.Contains), "top-secret")
	c.Assert(reportFS, qt.IsNil)
}

func TestPublishTo_AttachesReportToExactSubject(t *testing.T) {
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

	report := validReport()
	report.MigrationArtifactDigest = subject.Descriptor.Digest.String()
	attachment, err := deploymentreport.PublishTo(ctx, store, subject.Descriptor, report)
	c.Assert(err, qt.IsNil)

	manifestBytes, err := content.FetchAll(ctx, store, attachment.Descriptor)
	c.Assert(err, qt.IsNil)
	var manifest ocispec.Manifest
	err = json.Unmarshal(manifestBytes, &manifest)
	c.Assert(err, qt.IsNil)
	c.Assert(manifest.ArtifactType, qt.Equals, ociartifact.DeploymentArtifactType)
	c.Assert(manifest.Subject, qt.IsNotNil)
	c.Assert(manifest.Subject.Digest, qt.Equals, subject.Descriptor.Digest)
	c.Assert(manifest.Layers, qt.HasLen, 1)
	c.Assert(manifest.Layers[0].MediaType, qt.Equals, deploymentreport.LayerMediaType)
}

func TestPublishTo_RejectsSubjectDigestMismatch(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	store := memory.New()
	subject, err := ociartifact.PushTo(ctx, store, fstest.MapFS{
		"0000000001_init.up.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
	}, ociartifact.PushOptions{
		ArtifactType: ociartifact.MigrationArtifactType,
	})
	c.Assert(err, qt.IsNil)

	_, err = deploymentreport.PublishTo(ctx, store, subject.Descriptor, validReport())

	c.Assert(err, qt.ErrorMatches, "invalid deployment report: migration artifact digest does not match attachment subject")
}

func TestPublish_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := deploymentreport.Publish(
		context.Background(),
		nil,
		"oci://registry.example/acme/migrations:latest",
		ocispec.Descriptor{Digest: artifactDigest},
		validReport(),
	)

	c.Assert(err, qt.ErrorMatches, "OCI client is required")
}

func validReport() deploymentreport.Report {
	return deploymentreport.Report{
		DeploymentID:            "deployment-019fa5",
		MigrationArtifactDigest: artifactDigest,
		Dialect:                 "postgres",
		FromVersion:             1,
		ToVersion:               3,
		AppliedVersions:         []int64{2, 3},
		StartedAt:               time.Date(2026, time.July, 28, 6, 9, 10, 0, time.UTC),
		FinishedAt:              time.Date(2026, time.July, 28, 6, 10, 11, 0, time.UTC),
		Outcome:                 deploymentreport.OutcomeSucceeded,
	}
}

func validSuccessfulOptions() deploymentreport.SuccessfulOptions {
	return deploymentreport.SuccessfulOptions{
		Subject: ocispec.Descriptor{
			Digest: artifactDigest,
		},
		Dialect: "postgres",
		Before: &migrator.MigrationStatus{
			CurrentVersion:    1,
			AppliedMigrations: []int64{1},
		},
		After: &migrator.MigrationStatus{
			CurrentVersion:    3,
			AppliedMigrations: []int64{1, 2, 3},
		},
		StartedAt:  time.Date(2026, time.July, 28, 6, 9, 10, 0, time.UTC),
		FinishedAt: time.Date(2026, time.July, 28, 6, 10, 11, 0, time.UTC),
	}
}
