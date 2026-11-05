//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbtarget"
)

// TestCompatMigrateDiffE2E_AWidthChangeIsPlanned changes the width of an nvarchar
// column. Unreported, the change wrote no statement and a repeat diff read as synced,
// so the column kept its old width. The file runs on the server, and the width reads
// back from the catalog, forward and after the rollback. A varchar on the same server
// is the control: its change was always reported.
func TestCompatMigrateDiffE2E_AWidthChangeIsPlanned(t *testing.T) {
	tests := []struct {
		name      string
		before    string
		after     string
		wantFirst string
		wantThen  string
	}{
		{
			name:   "nvarchar",
			before: "CREATE TABLE t (id bigint PRIMARY KEY, c nvarchar(50));\n", after: "CREATE TABLE t (id bigint PRIMARY KEY, c nvarchar(70));\n",
			wantFirst: "100", wantThen: "140",
		},
		{
			name:   "varchar",
			before: "CREATE TABLE t (id bigint PRIMARY KEY, c varchar(50));\n", after: "CREATE TABLE t (id bigint PRIMARY KEY, c varchar(70));\n",
			wantFirst: "100", wantThen: "140",
		},
	}
	const width = "SELECT CAST(max_length AS varchar(10)) FROM sys.columns WHERE object_id = OBJECT_ID('t') AND name = 'c'"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs := readMigrationPairs(c, diffEngineSteps(c, "", widthScratch(c), []string{test.before, test.after}))
			target := widthScratch(c)

			c.Assert(pairs, qt.HasLen, 2)
			applyEngineFile(c, target, platform.SQLServer, pairs[0].Up)
			c.Assert(heldColumns(c, target, width), qt.Equals, test.wantFirst)
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Up)
			c.Assert(heldColumns(c, target, width), qt.Equals, test.wantThen)
			applyEngineFile(c, target, platform.SQLServer, pairs[1].Down)
			c.Assert(heldColumns(c, target, width), qt.Equals, test.wantFirst)
		})
	}
}

// widthScratch is a scratch database on SQL Server.
func widthScratch(c *qt.C) string {
	c.Helper()
	return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
}
