package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrator"
)

// outOfOrderStatusOptions is the status of a directory where 1_a.sql and
// 2_b.sql are applied and 10_c.sql and 3_d.sql were added, 10_c.sql out of
// order because it sorts before 1_a.sql.
func outOfOrderStatusOptions(c *qt.C) atlasreport.MigrateStatusOptions {
	c.Helper()
	var revisions []migrator.MigrationRevision
	c.Assert(json.Unmarshal([]byte(`[
		{"version":1,"atlas_version":"1","description":"a","applied":1,"total":1},
		{"version":2,"atlas_version":"2","description":"b","applied":1,"total":1}
	]`), &revisions), qt.IsNil)
	return atlasreport.MigrateStatusOptions{
		AppliedRevisions: revisions,
		FS: fstest.MapFS{
			"1_a.sql":  {Data: []byte("CREATE TABLE a (id int);")},
			"2_b.sql":  {Data: []byte("CREATE TABLE b (id int);")},
			"10_c.sql": {Data: []byte("CREATE TABLE c (id int);")},
			"3_d.sql":  {Data: []byte("CREATE TABLE d (id int);")},
		},
		Status: &migrator.MigrationStatus{
			CurrentVersion:          2,
			AppliedMigrations:       []int64{1, 2},
			PendingMigrations:       []int64{3, 10},
			OutOfOrderMigrations:    []int64{10},
			OutOfOrderMigrationKeys: []string{"10"},
			HasPendingChanges:       true,
		},
	}
}

// TestWriteMigrateStatusFormat_OutOfOrderLeavesPending holds the report to
// what the pinned community binary prints for that directory: the late
// file leaves Pending, Next is absent, and Error is the apply refusal.
func TestWriteMigrateStatusFormat_OutOfOrderLeavesPending(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusFormat(&out,
		`{{ range .Pending }}{{ .Name }} {{ end }}|{{ range .OutOfOrder }}{{ .Name }} {{ end }}|{{ .Next }}|{{ .Error }}`,
		outOfOrderStatusOptions(c))

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "3_d.sql |10_c.sql ||"+
		"migration file 10_c.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error")
}

// TestWriteMigrateStatusText_CountsLateFilesAsPending holds the text report to
// the binary's: both files count as pending, one of them out of order, and
// the out-of-order Error is the trailing ERROR block alone.
func TestWriteMigrateStatusText_CountsLateFilesAsPending(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := atlasreport.WriteMigrateStatusText(&out, outOfOrderStatusOptions(c))

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Migration Status: PENDING\n"+
		"  -- Current Version: 2\n"+
		"  -- Next Version:    UNKNOWN\n"+
		"  -- Executed Files:  2\n"+
		"  -- Pending Files:   2 (1 out of order)\n"+
		"\n"+
		"  ERROR: migration file 10_c.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error\n")
}
