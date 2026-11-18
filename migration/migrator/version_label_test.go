package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// numberlessMigrator reads 1_a.sql, 2_b.sql and v1_x.sql, so v1_x.sql runs
// under the order key 3, which no file carries.
func numberlessMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles("1_a.sql", "2_b.sql", "v1_x.sql"),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func TestMigratorVersionLabel_NamesTheVersionAFileSpells(t *testing.T) {
	c := qt.New(t)
	mig := numberlessMigrator(c)

	c.Assert(mig.VersionLabels([]int64{0, 1, 2, 3, 4}), qt.DeepEquals, []string{"0", "1", "2", "v1", "4"})
	c.Assert((*migrator.Migrator)(nil).VersionLabel(3), qt.Equals, "3")
}

func TestMigratorResolveVersion_HappyPath(t *testing.T) {
	tests := []struct {
		label string
		want  int64
	}{
		{label: "0", want: 0},
		{label: "2", want: 2},
		{label: "v1", want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			c := qt.New(t)
			got, err := numberlessMigrator(c).ResolveVersion(tt.label)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestMigratorResolveVersion_FailurePath(t *testing.T) {
	tests := []struct {
		label   string
		wantErr string
	}{
		{label: "3", wantErr: `no migration file carries version 3: Ptah numbers v1_x\.sql after the highest numbered file, so name that file by its version, v1`},
		{label: "v2", wantErr: `no migration file carries version "v2"`},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			c := qt.New(t)
			got, err := numberlessMigrator(c).ResolveVersion(tt.label)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.Equals, int64(0))
		})
	}
}
