package migrator

// White-box testing required: the skip warning is written while the run
// selects its migrations, and the public paths that reach the selection need a
// database. migrationsToApply is the one place the verdict and the warning
// meet, so the warning's labels are asserted there over registered migrations.

import (
	"bytes"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrationsToApplyLinearSkipWarningNamesSourceVersions(t *testing.T) {
	tests := []struct {
		name           string
		versions       []int64
		applied        []int64
		revisions      []MigrationRevision
		sourceVersions map[int64]string
		want           []string
	}{
		{
			// A converted directory: the int64 keys appear in no file name, so
			// the warning names the source tool's tokens.
			name:           "converted directory",
			versions:       []int64{1, 3, 5},
			applied:        []int64{1, 5},
			sourceVersions: map[int64]string{1: "1", 3: "1.5", 5: "2"},
			want: []string{
				`level=WARN msg="Skipping out-of-order migration" version=1.5 currentVersion=2`,
			},
		},
		{
			// The current version is applied but the directory no longer
			// holds it, as after a squash. Its key maps to no token, so the
			// mark is named by the highest token the history records.
			name:     "current version the directory no longer holds",
			versions: []int64{1, 3},
			applied:  []int64{1, 7},
			revisions: []MigrationRevision{
				{Version: 1, AtlasVersion: "1", State: migrationStateApplied},
				{Version: 7, AtlasVersion: "2", State: migrationStateApplied},
			},
			sourceVersions: map[int64]string{1: "1", 3: "1.5"},
			want: []string{
				`level=WARN msg="Skipping out-of-order migration" version=1.5 currentVersion=2`,
			},
		},
		{
			name:     "native directory",
			versions: []int64{1, 3, 5},
			applied:  []int64{1, 5},
			want: []string{
				`level=WARN msg="Skipping out-of-order migration" version=3 currentVersion=5`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			migrations := testMigrations(tt.versions...)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			skip := NewMigrator(nil, NewRegisteredMigrationProvider(migrations...)).
				WithExecOrder(ExecOrderLinearSkip).
				WithSourceVersions(tt.sourceVersions).
				WithLogger(logger)

			got, err := skip.migrationsToApply(migrations, tt.applied, newMigrationIdentitySet(tt.applied, tt.revisions), 0)

			c.Assert(err, qt.IsNil)
			c.Assert(got.skipped, qt.DeepEquals, []int64{3})
			lines := logTimeRe.ReplaceAllString(strings.TrimSuffix(logs.String(), "\n"), "")
			c.Assert(strings.Split(lines, "\n"), qt.DeepEquals, tt.want)
		})
	}
}

// logTimeRe matches the time attribute that opens each line slog's text
// handler writes.
var logTimeRe = regexp.MustCompile(`(?m)^time=\S+ `)
