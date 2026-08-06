package atlasreport_test

import (
	"bytes"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/migration/migrator"
)

// statusTextFS is the two-migration directory every row below reports on.
func statusTextFS() fstest.MapFS {
	return fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id integer);")},
		"2_add_email.sql":    {Data: []byte("ALTER TABLE users ADD COLUMN email text;")},
	}
}

// TestWriteMigrateStatusText_MirrorsTheAtlasReport pins the report the compat
// surface prints without --format, at the layer that renders it.
//
// Reverted, cmd/atlas prints its own block and this file does not compile,
// because WriteMigrateStatusText is the function the revert removes. The
// end-to-end reproduction lives in cmd/atlas/migrate_status_report_shape_test.go;
// this one exists so a padding column or a sentinel string can be changed and
// caught without a database.
func TestWriteMigrateStatusText_MirrorsTheAtlasReport(t *testing.T) {
	tests := []struct {
		name       string
		status     *migrator.MigrationStatus
		revisions  []migrator.MigrationRevision
		wantStdout string
	}{
		{
			name: "nothing applied",
			status: &migrator.MigrationStatus{
				PendingMigrations: []int64{1, 2},
				TotalMigrations:   2,
				HasPendingChanges: true,
			},
			wantStdout: "Migration Status: PENDING\n" +
				"  -- Current Version: No migration applied yet\n" +
				"  -- Next Version:    1\n" +
				"  -- Executed Files:  0\n" +
				"  -- Pending Files:   2\n",
		},
		{
			name: "all applied",
			status: &migrator.MigrationStatus{
				CurrentVersion:    2,
				AppliedMigrations: []int64{1, 2},
				TotalMigrations:   2,
			},
			revisions: []migrator.MigrationRevision{
				{Version: 1, Applied: 1, Total: 1},
				{Version: 2, Applied: 1, Total: 1},
			},
			wantStdout: "Migration Status: OK\n" +
				"  -- Current Version: 2\n" +
				"  -- Next Version:    Already at latest version\n" +
				"  -- Executed Files:  2\n" +
				"  -- Pending Files:   0\n",
		},
		{
			name: "half-applied with a multi-line error",
			status: &migrator.MigrationStatus{
				CurrentVersion:    2,
				AppliedMigrations: []int64{1},
				PendingMigrations: []int64{2},
				TotalMigrations:   2,
				HasPendingChanges: true,
			},
			revisions: []migrator.MigrationRevision{
				{Version: 1, Applied: 1, Total: 1},
				{
					Version:        2,
					Applied:        1,
					Total:          2,
					Error:          "boom\ndetail",
					ErrorStatement: "ALTER TABLE users\n  ADD COLUMN email text;",
				},
			},
			wantStdout: "Migration Status: PENDING\n" +
				"  -- Current Version: 2 (1 statements applied)\n" +
				"  -- Next Version:    2 (1 statements left)\n" +
				"  -- Executed Files:  2 (last one partially)\n" +
				"  -- Pending Files:   1\n" +
				"\n" +
				"Last migration attempt had errors:\n" +
				"  -- SQL:   ALTER TABLE users   ADD COLUMN email text;\n" +
				"  -- ERROR: boom detail\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateStatusText(&out, atlasreport.MigrateStatusOptions{
				Driver:           "sqlite",
				URL:              "sqlite://app.db",
				Dir:              "file://migrations?format=atlas",
				FS:               statusTextFS(),
				Status:           tt.status,
				AppliedRevisions: tt.revisions,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, tt.wantStdout)
		})
	}
}

// TestWriteMigrateStatusFormat_PopulatesPartialFields covers the same four
// fields from the template side, because they were declared on the report and
// never written before #1102 — `{{ .Total }}` read 0 on a wedged database and
// the failing statement was unreachable from a template.
//
// Reverted, this renders "0|0||" instead.
func TestWriteMigrateStatusFormat_PopulatesPartialFields(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out,
		`{{ .Count }}|{{ .Total }}|{{ .Error }}|{{ .SQL }}`,
		atlasreport.MigrateStatusOptions{
			FS: statusTextFS(),
			Status: &migrator.MigrationStatus{
				CurrentVersion:    2,
				AppliedMigrations: []int64{1},
				PendingMigrations: []int64{2},
				TotalMigrations:   2,
				HasPendingChanges: true,
			},
			AppliedRevisions: []migrator.MigrationRevision{
				{Version: 1, Applied: 1, Total: 1},
				{
					Version:        2,
					Applied:        1,
					Total:          2,
					Error:          "boom\ndetail",
					ErrorStatement: "ALTER TABLE users\n  ADD COLUMN email text;",
				},
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|2|boom detail|ALTER TABLE users   ADD COLUMN email text;")
}

// TestWriteMigrateStatusFormat_KeepsRevisionErrorNewlines is the other half of
// the fold rule, and the reason it is applied to the top-level fields only.
//
// Measured on the pinned community binary v1.3.0: `{{ printf "%q" .SQL }}`
// folds the stored newline to a space while
// `{{ range .Applied }}{{ printf "%q" .ErrorStmt }}` keeps it. Folding both
// would be one line of code less and would diverge on the second.
func TestWriteMigrateStatusFormat_KeepsRevisionErrorNewlines(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out,
		`{{ range .Applied }}{{ printf "%q" .ErrorStmt }}{{ end }}`,
		atlasreport.MigrateStatusOptions{
			FS: statusTextFS(),
			Status: &migrator.MigrationStatus{
				CurrentVersion:    2,
				PendingMigrations: []int64{2},
				TotalMigrations:   2,
				HasPendingChanges: true,
			},
			AppliedRevisions: []migrator.MigrationRevision{
				{
					Version:        2,
					Applied:        1,
					Total:          2,
					Error:          "boom",
					ErrorStatement: "ALTER TABLE users\n  ADD COLUMN email text;",
				},
			},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, `"ALTER TABLE users\n  ADD COLUMN email text;"`)
}
