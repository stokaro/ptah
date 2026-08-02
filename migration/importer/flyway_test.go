package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

// flywayFS has dotted versions (forcing sequential reassignment), an undo file
// (a down paired by version), and a repeatable migration.
func flywayFS() fstest.MapFS {
	return fstest.MapFS{
		"V1__create_users.sql":   {Data: []byte("CREATE TABLE users (id int);\n")},
		"U1__create_users.sql":   {Data: []byte("DROP TABLE users;\n")},
		"V1.1__add_email.sql":    {Data: []byte("ALTER TABLE users ADD email text;\n")},
		"V2__create_orders.sql":  {Data: []byte("CREATE TABLE orders (id int);\n")},
		"R__refresh_summary.sql": {Data: []byte("REFRESH MATERIALIZED VIEW summary;\n")},
		"README.md":              {Data: []byte("# migrations")},
	}
}

func TestFlywayDetectAndResolve(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.DetectParser(flywayFS())
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "flyway")

	byName, err := importer.ParserByName("flyway")
	c.Assert(err, qt.IsNil)
	c.Assert(byName.Name(), qt.Equals, "flyway")
}

// TestFlywayParseDottedRemapsAndPairsUndo checks that a dotted version forces
// sequential reassignment (with the original version folded into the name), that
// an undo file becomes the matching migration's down, and that the repeatable
// sorts after every versioned migration.
func TestFlywayParseDottedRemapsAndPairsUndo(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("flyway")

	migrations, err := parser.Parse(flywayFS())
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(normalized, qt.HasLen, 4)

	// Versioned migrations, ordered 1 < 1.1 < 2, reassigned to sequential
	// versions with the original version carried into the name.
	c.Assert(normalized[0].Version, qt.Equals, int64(1))
	c.Assert(normalized[0].Name, qt.Equals, "v1_create_users")
	c.Assert(normalized[0].UpSQL, qt.Contains, "CREATE TABLE users")
	c.Assert(normalized[0].DownSQL, qt.Equals, "DROP TABLE users;") // paired undo

	c.Assert(normalized[1].Version, qt.Equals, int64(2))
	c.Assert(normalized[1].Name, qt.Equals, "v1_1_add_email")
	c.Assert(normalized[1].DownSQL, qt.Equals, "") // no undo -> placeholder later

	c.Assert(normalized[2].Version, qt.Equals, int64(3))
	c.Assert(normalized[2].Name, qt.Equals, "v2_create_orders")

	// The repeatable sorts last and is flagged.
	c.Assert(normalized[3].Repeatable, qt.IsTrue)
	c.Assert(normalized[3].Name, qt.Equals, "refresh_summary")
	c.Assert(normalized[3].UpSQL, qt.Contains, "REFRESH MATERIALIZED VIEW")
}

// TestFlywayParsePreservesIntegerVersions checks that when every version is a
// single integer that fits Ptah's format, the original versions are preserved
// and names are left untouched.
func TestFlywayParsePreservesIntegerVersions(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("flyway")

	src := fstest.MapFS{
		"V1__init.sql": {Data: []byte("CREATE TABLE t (id int);\n")},
		"V5__seed.sql": {Data: []byte("INSERT INTO t VALUES (1);\n")},
	}
	migrations, err := parser.Parse(src)
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(normalized, qt.HasLen, 2)

	c.Assert(normalized[0].Version, qt.Equals, int64(1))
	c.Assert(normalized[0].Name, qt.Equals, "init")
	c.Assert(normalized[1].Version, qt.Equals, int64(5))
	c.Assert(normalized[1].Name, qt.Equals, "seed")
}

// TestFlywayParseUnderscoreVersionSeparator guards against silently dropping
// migrations that use Flyway's underscore version-part separator (V1_1 == V1.1).
// In a directory mixed with integer/dotted siblings, an underscore-versioned
// file (and its underscore undo) must be imported, not ignored.
func TestFlywayParseUnderscoreVersionSeparator(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("flyway")

	src := fstest.MapFS{
		"V1__init.sql":        {Data: []byte("CREATE TABLE users (id int);\n")},
		"V1_1__add_email.sql": {Data: []byte("ALTER TABLE users ADD email text;\n")}, // version 1.1
		"U1_1__add_email.sql": {Data: []byte("ALTER TABLE users DROP email;\n")},     // undo for 1.1
		"V2__orders.sql":      {Data: []byte("CREATE TABLE orders (id int);\n")},
	}
	migrations, err := parser.Parse(src)
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	// All three versioned migrations are present — none silently dropped.
	c.Assert(normalized, qt.HasLen, 3)

	// A dotted/underscore version forces sequential reassignment; ordering is
	// 1 < 1.1 < 2 and the underscore version folds to the same name as a dot.
	c.Assert(normalized[1].Name, qt.Equals, "v1_1_add_email")
	c.Assert(normalized[1].UpSQL, qt.Contains, "ADD email")
	c.Assert(normalized[1].DownSQL, qt.Equals, "ALTER TABLE users DROP email;") // underscore undo paired
}

// TestFlywayUnderscoreAndDotVersionsCollide checks that V1.1 and V1_1 are the
// same version and therefore a duplicate, not two migrations.
func TestFlywayUnderscoreAndDotVersionsCollide(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("flyway")

	_, err := parser.Parse(fstest.MapFS{
		"V1.1__a.sql": {Data: []byte("SELECT 1;")},
		"V1_1__b.sql": {Data: []byte("SELECT 2;")},
	})
	c.Assert(err, qt.ErrorMatches, `.*duplicate flyway version 1.1 .*`)
}

func TestFlywayImportEndToEnd(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(flywayFS(), nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	// 3 versioned + 1 repeatable -> 4 up + 4 down Ptah files.
	c.Assert(result.Files, qt.HasLen, 8)
	c.Assert(result.SumFile, qt.Equals, "ptah.sum")

	// Sequential versions, original Flyway version in the name.
	c.Assert(result.Files, qt.Contains, "0000000001_v1_create_users.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000002_v1_1_add_email.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000003_v2_create_orders.up.sql")
	// The repeatable is imported as a one-time migration ordered last.
	c.Assert(result.Files, qt.Contains, "0000000004_repeatable_refresh_summary.up.sql")

	// The undo file became the first migration's down.
	down, err := os.ReadFile(filepath.Join(out, "0000000001_v1_create_users.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "DROP TABLE users;")

	// A versioned migration without an undo gets a placeholder down.
	placeholder, err := os.ReadFile(filepath.Join(out, "0000000002_v1_1_add_email.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(placeholder), qt.Contains, "No rollback")
}

func TestFlywayParseErrors(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		re   string
	}{
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"V1__a.sql":  {Data: []byte("SELECT 1;")},
				"V01__b.sql": {Data: []byte("SELECT 2;")},
			},
			re: `.*duplicate flyway version 1 .*`,
		},
		{
			name: "duplicate undo version",
			fsys: fstest.MapFS{
				"V1__a.sql": {Data: []byte("SELECT 1;")},
				"U1__a.sql": {Data: []byte("SELECT 2;")},
				"U1__b.sql": {Data: []byte("SELECT 3;")},
			},
			re: `.*duplicate flyway undo version 1 .*`,
		},
		{
			name: "undo without versioned migration",
			fsys: fstest.MapFS{
				"V1__a.sql": {Data: []byte("SELECT 1;")},
				"U2__b.sql": {Data: []byte("SELECT 2;")},
			},
			re: `.*flyway undo migration for version 2 .*no matching versioned migration.*`,
		},
		{
			name: "undo only, no versioned migration",
			fsys: fstest.MapFS{"U1__a.sql": {Data: []byte("SELECT 1;")}},
			re:   `.*flyway undo migration for version 1 .*no matching versioned migration.*`,
		},
		{
			name: "empty migration file",
			fsys: fstest.MapFS{"V1__a.sql": {Data: []byte("   \n")}},
			re:   `.*flyway migration .* is empty.*`,
		},
		{
			name: "no flyway files",
			fsys: fstest.MapFS{"schema.sql": {Data: []byte("CREATE TABLE t (id int);")}},
			re:   `.*no flyway migration files.*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			parser, _ := importer.ParserByName("flyway")
			_, err := parser.Parse(tt.fsys)
			c.Assert(err, qt.ErrorMatches, tt.re)
		})
	}
}

// TestFlywayDistinguishedFromOthers proves auto-detect picks Flyway for its
// layout and does not confuse it with golang-migrate or Goose.
func TestFlywayDistinguishedFromOthers(t *testing.T) {
	c := qt.New(t)

	flyway, err := importer.DetectParser(flywayFS())
	c.Assert(err, qt.IsNil)
	c.Assert(flyway.Name(), qt.Equals, "flyway")

	glm, err := importer.DetectParser(golangMigrateFS())
	c.Assert(err, qt.IsNil)
	c.Assert(glm.Name(), qt.Equals, "golang-migrate")
}
