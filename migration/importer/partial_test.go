package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

// nestedGolangMigrateFS is a golang-migrate source whose second migration sits
// one directory down, which is the layout that used to import silently short.
func nestedGolangMigrateFS() fstest.MapFS {
	return fstest.MapFS{
		"000001_create.up.sql":     {Data: []byte("CREATE TABLE t (id INTEGER);")},
		"000001_create.down.sql":   {Data: []byte("DROP TABLE t;")},
		"tenant/000002_add.up.sql": {Data: []byte("ALTER TABLE t ADD c TEXT;")},
	}
}

// ptah.sum is not written over an import that dropped SQL -- stokaro/ptah#2231.
//
// This is the laundering stokaro/ptah#1095 closed for tampered source
// directories, reached through a different door: the checksum covered the subset
// that survived, so `migrations validate` reported the truncated directory as
// matching ptah.sum and exited 0.
func TestImport_RefusesToChecksumAnImportThatDroppedSQL(t *testing.T) {
	c := qt.New(t)
	outDir := filepath.Join(t.TempDir(), "out")

	_, err := importer.Import(nestedGolangMigrateFS(), nil, outDir, importer.Options{})

	var partial *importer.PartialImportError
	c.Assert(err, qt.ErrorAs, &partial)
	c.Assert(partial.Declined, qt.HasLen, 1)
	c.Assert(partial.Declined[0].Path, qt.Equals, "tenant/000002_add.up.sql")
	c.Assert(err, qt.ErrorMatches, `.*--allow-partial.*`)

	// Nothing is left behind for a later run to validate as clean.
	_, statErr := os.Stat(outDir)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// The opt-in imports the rest and still reports what it left behind.
//
// The flag is a decision the user makes, not a way to stop hearing about it.
func TestImport_AllowPartialImportsTheRestAndStillReportsIt(t *testing.T) {
	c := qt.New(t)
	outDir := filepath.Join(t.TempDir(), "out")

	result, err := importer.Import(nestedGolangMigrateFS(), nil, outDir, importer.Options{AllowPartial: true})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.SumFile, qt.Not(qt.Equals), "")
	c.Assert(result.Declined, qt.HasLen, 1)
	c.Assert(result.Declined[0].Path, qt.Equals, "tenant/000002_add.up.sql")
}

// A complete import is unaffected, which is the control the refusal above needs.
func TestImport_ACompleteSourceStillImportsWithoutAnOptIn(t *testing.T) {
	c := qt.New(t)
	outDir := filepath.Join(t.TempDir(), "out")

	result, err := importer.Import(fstest.MapFS{
		"000001_create.up.sql":   {Data: []byte("CREATE TABLE t (id INTEGER);")},
		"000001_create.down.sql": {Data: []byte("DROP TABLE t;")},
	}, nil, outDir, importer.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Declined, qt.HasLen, 0)
}

// Flyway sources are read recursively, which is Flyway's own contract for a
// location, and what `ptah-compat migrate import` already did for the same
// directory -- the two verbs used to convert it differently with neither saying
// so (stokaro/ptah#2231).
func TestFlywayParse_ReadsMigrationsBelowTheTopLevel(t *testing.T) {
	c := qt.New(t)
	parser, err := importer.ParserByName("flyway")
	c.Assert(err, qt.IsNil)

	migrations, err := parseMigrations(c, parser, fstest.MapFS{
		"V1__create.sql":        {Data: []byte("CREATE TABLE t (id INTEGER);")},
		"common/V2__add.sql":    {Data: []byte("ALTER TABLE t ADD c TEXT;")},
		"release/3/V3__idx.sql": {Data: []byte("CREATE INDEX i ON t (id);")},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 3)
	names := []string{migrations[0].Name, migrations[1].Name, migrations[2].Name}
	c.Assert(names, qt.DeepEquals, []string{"create", "add", "idx"})
}

// A Flyway directory laid out per module is detected as Flyway, rather than
// reported as undetectable, because Detect walks the same tree Parse does.
func TestFlywayDetect_RecognizesALayoutWithNothingAtTheTopLevel(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.DetectParser(fstest.MapFS{
		"common/V1__create.sql": {Data: []byte("CREATE TABLE t (id INTEGER);")},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "flyway")
}

// A directory whose migrations all sit one level down is named for what it is.
//
// It used to report "could not detect the source migration tool; pass --from",
// and --from then failed with "no golang-migrate migration files found" --
// two messages, neither naming the depth that was the actual cause.
func TestDetectParser_NamesTheDepthRatherThanAskingForFrom(t *testing.T) {
	c := qt.New(t)

	_, err := importer.DetectParser(fstest.MapFS{
		"v1/000001_create.up.sql": {Data: []byte("CREATE TABLE t (id INTEGER);")},
	})

	c.Assert(err, qt.ErrorMatches, `no migration files at the top level.*golang-migrate.*below it.*v1/000001_create\.up\.sql.*`)
}

// A directory that really holds no migrations keeps the general message.
//
// Without this control, a detector that answered "they are one level down" for
// everything would pass the test above.
func TestDetectParser_ADirectoryWithNoMigrationsKeepsTheGeneralMessage(t *testing.T) {
	c := qt.New(t)

	_, err := importer.DetectParser(fstest.MapFS{
		"README.md":     {Data: []byte("nothing here\n")},
		"docs/notes.md": {Data: []byte("nor here\n")},
	})

	c.Assert(err, qt.ErrorMatches, `could not detect the source migration tool.*`)
}
