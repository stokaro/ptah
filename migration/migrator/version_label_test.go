package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// numberlessMigrator reads 1_a.sql, 2_b.sql and v1_x.sql, so the three run
// under their positions, 1, 2 and 3.
func numberlessMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	return atlasMigrator(c, "1_a.sql", "2_b.sql", "v1_x.sql")
}

// atlasMigrator reads names over an Atlas revision table on a fresh SQLite
// database; the connection closes when the test ends.
func atlasMigrator(c *qt.C, names ...string) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles(names...),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func TestMigratorVersionLabel_HappyPath(t *testing.T) {
	c := qt.New(t)
	mig := numberlessMigrator(c)

	c.Assert(mig.VersionLabels([]int64{0, 1, 2, 3, 4}), qt.DeepEquals, []string{"0", "1", "2", "v1", "4"})
	c.Assert((*migrator.Migrator)(nil).VersionLabel(3), qt.Equals, "3")
}

// spelledNumberMigrator reads 1_a.sql, 2_b.sql, 00_z.sql, -5_m.sql and
// v1_v.sql. 00 and -5 read as numbers, 0 and -5, yet neither is a decimal
// above zero, so Atlas reads each as a version string, and so does Ptah. The
// five run under their positions in file-name order: -5_m.sql, 00_z.sql,
// 1_a.sql, 2_b.sql, v1_v.sql.
func spelledNumberMigrator(c *qt.C) *migrator.Migrator {
	c.Helper()
	return atlasMigrator(c, "1_a.sql", "2_b.sql", "00_z.sql", "-5_m.sql", "v1_v.sql")
}

// TestMigratorResolveVersion_ReadsAFileVersionBeforeANumber names 00_z.sql by
// 00 and -5_m.sql by -5. Read as numbers first, 00 is version 0, which a set
// reads as removing every revision, and -5 is refused as negative. 1 names
// 1_a.sql by its version string, so it resolves to that file's position.
func TestMigratorResolveVersion_ReadsAFileVersionBeforeANumber(t *testing.T) {
	tests := []struct {
		label string
		want  int64
		// named is how VersionLabel names the version read back.
		named string
	}{
		{label: "00", want: 2, named: "00"},
		{label: "-5", want: 1, named: "-5"},
		{label: "v1", want: 5, named: "v1"},
		{label: "0", want: 0, named: "0"},
		{label: "1", want: 3, named: "1"},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			c := qt.New(t)
			mig := spelledNumberMigrator(c)

			got, err := mig.ResolveVersion(tt.label)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
			c.Assert(mig.VersionLabel(got), qt.Equals, tt.named)
		})
	}
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
		{label: "3", wantErr: `no migration file carries version "3"`},
		{label: "01", wantErr: `no migration file carries version "01"`},
		{label: "v2", wantErr: `no migration file carries version "v2"`},
		{label: "-6", wantErr: `no migration file carries version "-6"`},
		{label: "99999999999999999999", wantErr: `no migration file carries version "99999999999999999999"`},
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

// TestMigrateDownTo_NamesAZeroPaddedFileByItsNumber pins the wording for a
// numbered file whose name pads its number: the refusal names 3, as the
// message for every numbered file does, and the label is for a file whose
// version is not a number.
func TestMigrateDownTo_NamesAZeroPaddedFileByItsNumber(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "app.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, atlasLinearityFiles("000003_c.sql"),
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	mig = mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)

	err = mig.MigrateDownTo(c.Context(), 0)

	c.Assert(err, qt.ErrorMatches, `(?s).*migration 3 has no Atlas down migration.*`)
}

// TestMigratorResolveVersion_NumberedDirectory_HappyPath reads a number as
// that number where every file spells one, so 1 names 01_a.sql, and finds a
// numbered repeatable by its version string.
func TestMigratorResolveVersion_NumberedDirectory_HappyPath(t *testing.T) {
	tests := []struct {
		label string
		want  int64
	}{
		{label: "1", want: 1},
		{label: "01", want: 1},
		{label: "0", want: 0},
		{label: "3R", want: 3},
		{label: "5", want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasMigrator(c, "01_a.sql", "2_b.sql", "3R__rep.sql").ResolveVersion(tt.label)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestMigratorResolveVersion_NumberedDirectory_FailurePath refuses a negative
// number, and a number that reaches the key of R__view.sql, which runs above
// every number.
func TestMigratorResolveVersion_NumberedDirectory_FailurePath(t *testing.T) {
	tests := []struct {
		label   string
		wantErr string
	}{
		{label: "-5", wantErr: `no migration file carries version -5: a migration version is not negative`},
		{label: "3", wantErr: `no migration file carries version 3: Ptah numbers R__view\.sql after the highest ` +
			`numbered file, so name that file by its version, R`},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			c := qt.New(t)
			got, err := atlasMigrator(c, "1_a.sql", "2_b.sql", "R__view.sql").ResolveVersion(tt.label)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.Equals, int64(0))
		})
	}
}
