package generator_test

import (
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestGenerateEmptyMigrationKeepsTheAtlasNameVerbatim pins the file name
// `migrate new` writes into an Atlas-layout directory against the pinned Atlas
// community binary v1.3.0, which composes `<version>_<name>.sql` from the name
// it was given without rewriting it.
//
// Measured on that binary on 2026-08-08, each row run in its own directory:
//
//	migrate new "add users table"    -> <version>_add users table.sql
//	migrate new "add_users.sql"      -> <version>_add_users.sql.sql
//	migrate new "caps-AND.dots+plus" -> <version>_caps-AND.dots+plus.sql
//	migrate new "x.up"               -> <version>_x.up.sql
//
// Ptah wrote `<version>_add-users-table.sql` and `<version>_add_userssql.sql`
// for the first two, because it mapped spaces to hyphens and dropped every
// character outside [-_0-9A-Za-z] (stokaro/ptah#1235 findings 8.6 and 8.7). The
// file name is covered by atlas.sum, so the two tools produced different
// checksums for the same command.
//
// The `x.up` row is not decoration: the guard added alongside this change
// refuses a name whose file name reads back as the down half of a pair, and this
// row is what keeps that guard from widening into every dotted suffix.
func TestGenerateEmptyMigrationKeepsTheAtlasNameVerbatim(t *testing.T) {
	tests := []struct {
		name          string
		migrationName string
		wantSuffix    string
	}{
		{
			name:          "interior spaces survive",
			migrationName: "add users table",
			wantSuffix:    "_add users table.sql",
		},
		{
			name:          "a dot survives, extension and all",
			migrationName: "add_users.sql",
			wantSuffix:    "_add_users.sql.sql",
		},
		{
			name:          "case and punctuation survive",
			migrationName: "caps-AND.dots+plus",
			wantSuffix:    "_caps-AND.dots+plus.sql",
		},
		{
			name:          "an up suffix is a name, not a direction",
			migrationName: "x.up",
			wantSuffix:    "_x.up.sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
				MigrationName: tt.migrationName,
				OutputDir:     t.TempDir(),
				DirFormat:     migrator.MigrationDirFormatAtlas,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(files.Files, qt.HasLen, 1)

			want := fmt.Sprintf("%d%s", files.Files[0].Version, tt.wantSuffix)
			c.Assert(filepath.Base(files.Files[0].UpFile), qt.Equals, want)
		})
	}
}

// TestGenerateEmptyMigrationRefusesAnAtlasNameItCannotReadBack is the guard that
// keeps writing the name verbatim from producing a directory Ptah's own reader
// rejects.
//
// `migrator.ParseAtlasMigrationFileName` classifies `<version>_x.down.sql` as
// the down half of a pair, because Atlas importers emit that spelling for
// golang-migrate directories. Without this refusal `migrate new "x.down"` wrote
// a file `migrate status` then answered `Atlas migration version <version> has
// down migration but no up migration` on, exit 1 -- one verb producing a
// directory another verb cannot read.
//
// This is stricter than the pinned binary, which writes that file at exit 0 and
// reads it back as pending at exit 0. Refusing to write it does not close the
// reader gap, which is reported separately; it only keeps Ptah from opening the
// gap on itself.
func TestGenerateEmptyMigrationRefusesAnAtlasNameItCannotReadBack(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	_, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "x.down",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.ErrorMatches,
		`migration name "x.down" composes the file name <version>_x\.down\.sql, `+
			`which this tool does not read back as a new migration`)

	// The refusal runs before the directory is bound, so nothing is left behind.
	entries, readErr := filepath.Glob(filepath.Join(dir, "*"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}
