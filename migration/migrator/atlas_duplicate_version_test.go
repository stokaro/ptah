package migrator_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// TestAtlasDuplicateVersion_FailurePath pins the refusal of an Atlas
// directory where two files carry the same direction of one version. The
// refusal names both files, because the version alone leaves the reader to
// search the directory for the pair, and keeping either file would leave the
// other unapplied.
func TestAtlasDuplicateVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		fsys    fstest.MapFS
		wantErr string
	}{
		{
			name: "two up files",
			fsys: fstest.MapFS{
				"20260101000000_a.sql": {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"20260101000000_b.sql": {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
			},
			wantErr: `duplicate Atlas up migration for version 20260101000000: 20260101000000_a\.sql and 20260101000000_b\.sql`,
		},
		{
			name: "two down files",
			fsys: fstest.MapFS{
				"1_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"1_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"1_b.down.sql": {Data: []byte("DROP TABLE a;\n")},
			},
			wantErr: `duplicate Atlas down migration for version 1: 1_a\.down\.sql and 1_b\.down\.sql`,
		},
		{
			name: "a down file beside a txtar down section",
			fsys: fstest.MapFS{
				"1_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"1_a.sql": {Data: []byte("-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE a (id INTEGER);\n\n" +
					"-- down.sql --\nDROP TABLE a;\n")},
			},
			wantErr: `duplicate Atlas down migration for version 1: 1_a\.down\.sql and 1_a\.sql`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			provider, err := migrator.NewFSMigrationProvider(
				test.fsys,
				migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(provider, qt.IsNil)
		})
	}
}
