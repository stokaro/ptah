package atlasreport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

func TestRevisionIdentityLabel_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		identity string
		want     string
	}{
		{name: "dotted version", identity: "1.5", want: "1.5"},
		{name: "empty identity of a repeatable", identity: "", want: `""`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasreport.RevisionIdentityLabel(tt.identity), qt.Equals, tt.want)
		})
	}
}

func TestNameRevisionIdentities_HappyPath(t *testing.T) {
	identities := map[int64]string{
		4611686018427471935: "1.5",
		4611686018427836747: "",
		20:                  "2",
	}
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "key after migration",
			message: "pre-migration check (unnamed) for migration 4611686018427471935 was not satisfied (assert: SELECT 0)",
			want:    "pre-migration check (unnamed) for migration 1.5 was not satisfied (assert: SELECT 0)",
		},
		{
			name:    "key after version, revision and above",
			message: "version 20, revision 20, above 20",
			want:    "version 2, revision 2, above 2",
		},
		{
			name:    "empty identity is quoted",
			message: "migration 4611686018427836747 failed",
			want:    `migration "" failed`,
		},
		{
			name:    "key without a label is data",
			message: "INSERT INTO t VALUES (20)",
			want:    "INSERT INTO t VALUES (20)",
		},
		{
			name:    "digits that only start with a key stay",
			message: "migration 201 failed",
			want:    "migration 201 failed",
		},
		{
			name:    "number no key matches stays",
			message: "migration 7 failed",
			want:    "migration 7 failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasreport.NameRevisionIdentities(tt.message, identities), qt.Equals, tt.want)
		})
	}
}

func TestNameRevisionIdentities_NoIdentitiesKeepsMessage(t *testing.T) {
	c := qt.New(t)
	c.Assert(atlasreport.NameRevisionIdentities("migration 20 failed", nil), qt.Equals, "migration 20 failed")
}

func TestMigrateApplyRevisionIdentities_HappyPath(t *testing.T) {
	c := qt.New(t)
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"10_dotted.sql": {Data: []byte("SELECT 1;")},
			"20_repeat.sql": {Data: []byte("SELECT 2;")},
		},
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{10: "1.5", 20: ""}),
	)
	c.Assert(err, qt.IsNil)

	got := atlasreport.MigrateApplyRevisionIdentities(
		[]int64{10, 30, 40},
		[]string{"selected", "3"},
		provider.Migrations(),
	)

	// A migration the directory holds names itself; a selected key the
	// directory lacks keeps the identity it was selected under, and a key
	// past the end of keys has none.
	c.Assert(got, qt.DeepEquals, map[int64]string{10: "1.5", 20: "", 30: "3"})
}
