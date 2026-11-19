package migrator_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// TestPtahDuplicateVersion_FailurePath pins that the provider refuses a
// Ptah-format directory where two files carry one direction of one version,
// with the refusal an Atlas directory gets. Without it the provider builds one
// migration whose up is whichever file loaded last, and the other file never
// runs.
func TestPtahDuplicateVersion_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  migrationfile.DirFormat
		wantErr string
	}{
		{name: "read in the Ptah format", format: migrationfile.DirFormatPtah,
			wantErr: `duplicate up migration for version 2: 0000000002_b\.up\.sql and 0000000002_c\.up\.sql`},
		{name: "read with format detection", format: migrationfile.DirFormatAuto,
			wantErr: `duplicate up migration for version 2: 0000000002_b\.up\.sql and 0000000002_c\.up\.sql`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
				"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
				"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
				"0000000002_b.up.sql":   {Data: []byte("CREATE TABLE b (id INTEGER);\n")},
				"0000000002_b.down.sql": {Data: []byte("DROP TABLE b;\n")},
				"0000000002_c.up.sql":   {Data: []byte("CREATE TABLE c (id INTEGER);\n")},
			}, migrator.WithMigrationDirFormat(test.format))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var duplicate *migrationfile.DuplicateFileError
			c.Assert(err, qt.ErrorAs, &duplicate)
			c.Assert(provider, qt.IsNil)
		})
	}
}
