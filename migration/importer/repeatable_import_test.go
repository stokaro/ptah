package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratesum"
	"ptah.run/migration/importer"
	"ptah.run/migration/migrationfile"
)

// flywayRepeatableFS is a Flyway location with one versioned migration and two
// repeatable ones, one of whose names carries a character a Ptah repeatable
// identity does not take.
func flywayRepeatableFS() fstest.MapFS {
	return fstest.MapFS{
		"V1__init.sql":     {Data: []byte("CREATE TABLE t (id int);\n")},
		"R__summary.sql":   {Data: []byte("CREATE OR REPLACE VIEW summary AS SELECT 1;\n")},
		"R__report.v2.sql": {Data: []byte("CREATE OR REPLACE VIEW report AS SELECT 2;\n")},
	}
}

func flywayParser(c *qt.C) importer.Parser {
	c.Helper()
	parser, err := importer.ParserByName("flyway")
	c.Assert(err, qt.IsNil)
	return parser
}

func TestImportRepeatablesOnChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(c.Context(), flywayRepeatableFS(), flywayParser(c), out,
		importer.Options{Repeatables: importer.RepeatablesOnChange})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.DeepEquals, []string{
		"0000000001_init.up.sql",
		"0000000001_init.down.sql",
		"R__report_v2.sql",
		"R__summary.sql",
	})

	body, err := os.ReadFile(filepath.Join(out, "R__summary.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Equals, "CREATE OR REPLACE VIEW summary AS SELECT 1;\n")
	_, err = os.Stat(filepath.Join(out, "R__summary.down.sql"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)

	// The written directory reads back as one versioned migration and two
	// repeatable ones, and ptah.sum covers all of them.
	repeatables, err := migrationfile.DiscoverRepeatables(os.DirFS(out), migrationfile.DirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(repeatables, qt.HasLen, 2)
	verified, err := migratesum.VerifyWithFormat(os.DirFS(out), migrationfile.DirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(verified.OK(), qt.IsTrue)
}

func TestImportRepeatablesOnce_HappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := importer.Import(c.Context(), flywayRepeatableFS(), flywayParser(c), t.TempDir(),
		importer.Options{Repeatables: importer.RepeatablesOnce, DryRun: true})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.Contains, "0000000002_repeatable_reportv2.up.sql")
	c.Assert(result.Files, qt.Not(qt.Contains), "R__summary.sql")
}

func TestImportRepeatablesOnChange_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		migrations []importer.SourceMigration
		opts       importer.Options
		wantErr    string
	}{
		{
			name: "two names that land on one identity",
			migrations: []importer.SourceMigration{
				{Repeatable: true, Name: "report.v2", UpSQL: "SELECT 1;"},
				{Repeatable: true, Name: "Report_v2", UpSQL: "SELECT 2;"},
			},
			opts:    importer.Options{Repeatables: importer.RepeatablesOnChange},
			wantErr: `cannot import repeatable migrations "report.v2" and "Report_v2": both become R__Report_v2.sql`,
		},
		{
			name:       "a repeatable migration with a rollback",
			migrations: []importer.SourceMigration{{Repeatable: true, Name: "view", UpSQL: "SELECT 1;", DownSQL: "SELECT 2;"}},
			opts:       importer.Options{Repeatables: importer.RepeatablesOnChange},
			wantErr:    `cannot import repeatable migration "view" as a Ptah repeatable migration: it carries a rollback.*`,
		},
		{
			name:       "an unknown mode",
			migrations: []importer.SourceMigration{{Repeatable: true, Name: "view", UpSQL: "SELECT 1;"}},
			opts:       importer.Options{Repeatables: "always"},
			wantErr:    `unknown repeatable import mode "always": use "once" or "on-change"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			out := filepath.Join(t.TempDir(), "out")
			normalized, err := importer.Normalize(tc.migrations)
			c.Assert(err, qt.IsNil)

			result, err := importer.Emit(out, normalized, nil, tc.opts)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(result, qt.IsNil)
			_, err = os.Stat(out)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}
