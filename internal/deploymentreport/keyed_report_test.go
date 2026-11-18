package deploymentreport_test

import (
	"io/fs"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"ptah.run/internal/deploymentreport"
	"ptah.run/migration/migrator"
)

// keyedSuccessfulOptions is a deployment over 1_x, 2_x, 10_x and v1_x, which
// run as 10_x, 1_x, 2_x and v1_x: 10 and 1 were applied, and the run applied
// 2 and v1. The numbers are positions.
func keyedSuccessfulOptions() deploymentreport.SuccessfulOptions {
	return deploymentreport.SuccessfulOptions{
		Subject: ocispec.Descriptor{Digest: artifactDigest},
		Dialect: "sqlite",
		Before: &migrator.MigrationStatus{
			CurrentVersion: 2, CurrentVersionKey: "1", CurrentVersionKeySet: true,
			AppliedMigrations: []int64{1, 2}, AppliedMigrationKeys: []string{"10", "1"},
		},
		After: &migrator.MigrationStatus{
			CurrentVersion: 4, CurrentVersionKey: "v1", CurrentVersionKeySet: true,
			AppliedMigrations: []int64{1, 2, 3, 4}, AppliedMigrationKeys: []string{"10", "1", "2", "v1"},
		},
		StartedAt:       time.Date(2026, time.July, 28, 6, 9, 10, 0, time.UTC),
		FinishedAt:      time.Date(2026, time.July, 28, 6, 10, 11, 0, time.UTC),
		KeyedByPosition: true,
	}
}

// TestNewSuccessfulReportKeyedByPosition_HappyPath names the migrations of a
// positional directory by their keys and writes no number: position 4 is
// v1_x.sql in this run and another file after a file is added before it.
func TestNewSuccessfulReportKeyedByPosition_HappyPath(t *testing.T) {
	c := qt.New(t)

	report, err := deploymentreport.NewSuccessfulReport(keyedSuccessfulOptions())
	c.Assert(err, qt.IsNil)
	c.Assert(report.FromVersionKey, qt.Equals, "1")
	c.Assert(report.ToVersionKey, qt.Equals, "v1")
	c.Assert(report.AppliedVersionKeys, qt.DeepEquals, []string{"2", "v1"})

	report.DeploymentID = "deployment-019fa5"
	reportFS, err := deploymentreport.NewFS(report)
	c.Assert(err, qt.IsNil)
	contents, err := fs.ReadFile(reportFS, deploymentreport.FileName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals,
		"{\"schema_version\":1,\"deployment_id\":\"deployment-019fa5\","+
			"\"migration_artifact_digest\":\""+artifactDigest+"\","+
			"\"dialect\":\"sqlite\",\"from_version_key\":\"1\","+
			"\"to_version_key\":\"v1\",\"applied_version_keys\":[\"2\",\"v1\"],"+
			"\"started_at\":\"2026-07-28T06:09:10Z\","+
			"\"finished_at\":\"2026-07-28T06:10:11Z\",\"outcome\":\"succeeded\"}\n")
}

// TestNewSuccessfulReportKeyedByPositionFromNothing_HappyPath writes an
// empty from key, which names no migration.
func TestNewSuccessfulReportKeyedByPositionFromNothing_HappyPath(t *testing.T) {
	c := qt.New(t)
	opts := keyedSuccessfulOptions()
	opts.Before = &migrator.MigrationStatus{}

	report, err := deploymentreport.NewSuccessfulReport(opts)

	c.Assert(err, qt.IsNil)
	c.Assert(report.FromVersionKey, qt.Equals, "")
	c.Assert(report.AppliedVersionKeys, qt.DeepEquals, []string{"10", "1", "2", "v1"})
}

func TestNewFSKeyedByPosition_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		applied []string
		wantErr string
	}{
		{name: "no to key", applied: []string{"v1"}, wantErr: "invalid deployment report: to version key is required"},
		{name: "an empty applied key", to: "v1", applied: []string{"2", ""}, wantErr: "invalid deployment report: applied version keys must not be empty"},
		{name: "a repeated applied key", to: "v1", applied: []string{"v1", "v1"}, wantErr: "invalid deployment report: applied version keys must be distinct"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			report := validReport()
			report.KeyedByPosition = true
			report.ToVersionKey = tt.to
			report.AppliedVersionKeys = tt.applied

			reportFS, err := deploymentreport.NewFS(report)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(reportFS, qt.IsNil)
		})
	}
}
