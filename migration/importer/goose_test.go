package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

const gooseInitSQL = `-- +goose Up
-- +goose StatementBegin
CREATE TABLE users (id int);
-- +goose StatementEnd
CREATE INDEX idx_users ON users (id);

-- +goose Down
DROP INDEX idx_users;
DROP TABLE users;
`

const gooseUpOnlySQL = `-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY idx2 ON users (id);
`

func gooseFS() fstest.MapFS {
	return fstest.MapFS{
		"20230101_init.sql":    {Data: []byte(gooseInitSQL)},
		"20230102_up_only.sql": {Data: []byte(gooseUpOnlySQL)},
		"README.md":            {Data: []byte("# migrations")},
	}
}

func TestGooseDetectAndResolve(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.DetectParser(gooseFS())
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "goose")

	byName, err := importer.ParserByName("goose")
	c.Assert(err, qt.IsNil)
	c.Assert(byName.Name(), qt.Equals, "goose")
}

func TestGooseParse(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("goose")

	migrations, err := parser.Parse(gooseFS())
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(normalized, qt.HasLen, 2)

	// Up section: StatementBegin/End markers stripped, both statements kept.
	c.Assert(normalized[0].Version, qt.Equals, int64(20230101))
	c.Assert(normalized[0].Name, qt.Equals, "init")
	c.Assert(normalized[0].UpSQL, qt.Contains, "CREATE TABLE users (id int);")
	c.Assert(normalized[0].UpSQL, qt.Contains, "CREATE INDEX idx_users ON users (id);")
	c.Assert(normalized[0].UpSQL, qt.Not(qt.Contains), "+goose")
	c.Assert(normalized[0].DownSQL, qt.Contains, "DROP TABLE users;")
	c.Assert(normalized[0].NoTransaction, qt.IsFalse)

	// Second migration: NO TRANSACTION becomes typed metadata, with no down section.
	c.Assert(normalized[1].Version, qt.Equals, int64(20230102))
	c.Assert(normalized[1].UpSQL, qt.Contains, "CREATE INDEX CONCURRENTLY idx2 ON users (id);")
	c.Assert(normalized[1].UpSQL, qt.Not(qt.Contains), "+goose")
	c.Assert(normalized[1].DownSQL, qt.Equals, "")
	c.Assert(normalized[1].NoTransaction, qt.IsTrue)
}

// TestGooseParseStatementBlockKeepsBodyVerbatim guards against a `-- +goose`
// line inside a StatementBegin/End block flipping the section mid-statement: the
// function body (including a `-- +goose Down` comment) must stay in the up
// section, and the real down section must start only at the top-level marker.
func TestGooseParseStatementBlockKeepsBodyVerbatim(t *testing.T) {
	c := qt.New(t)
	const sql = `-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION f() RETURNS int AS $$
BEGIN
  -- +goose Down
  RETURN 1;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION f();
`
	parser, _ := importer.ParserByName("goose")
	migrations, err := parser.Parse(fstest.MapFS{"1_fn.sql": {Data: []byte(sql)}})
	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 1)

	// The whole function body (with the internal -- +goose Down comment) stays up.
	c.Assert(migrations[0].UpSQL, qt.Contains, "CREATE FUNCTION f() RETURNS int AS $$")
	c.Assert(migrations[0].UpSQL, qt.Contains, "END;")
	c.Assert(migrations[0].UpSQL, qt.Contains, "$$ LANGUAGE plpgsql;")
	c.Assert(migrations[0].UpSQL, qt.Contains, "-- +goose Down") // the body comment is preserved verbatim
	// The real down is only the top-level statement.
	c.Assert(migrations[0].DownSQL, qt.Equals, "DROP FUNCTION f();")
	// The StatementBegin/End annotations themselves are stripped.
	c.Assert(migrations[0].UpSQL, qt.Not(qt.Contains), "StatementBegin")
	c.Assert(migrations[0].UpSQL, qt.Not(qt.Contains), "StatementEnd")
}

func TestGooseParseStatementBlockConsumesNoTransaction(t *testing.T) {
	c := qt.New(t)
	const sql = `-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION f() RETURNS int AS $$
BEGIN
-- +goose NO TRANSACTION
  -- +goose NO TRANSACTION extra
  -- +goose Down
  RETURN 1;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION f();
`
	const wantUp = `CREATE FUNCTION f() RETURNS int AS $$
BEGIN
  -- +goose NO TRANSACTION extra
  -- +goose Down
  RETURN 1;
END;
$$ LANGUAGE plpgsql;`

	parser, _ := importer.ParserByName("goose")
	migrations, err := parser.Parse(fstest.MapFS{"1_fn.sql": {Data: []byte(sql)}})
	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].NoTransaction, qt.IsTrue)
	c.Assert(migrations[0].UpSQL, qt.Equals, wantUp)
	c.Assert(migrations[0].DownSQL, qt.Equals, "DROP FUNCTION f();")

	out := t.TempDir()
	result, err := importer.Import(fstest.MapFS{"1_fn.sql": {Data: []byte(sql)}}, nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.HasLen, 2)
	up, err := os.ReadFile(filepath.Join(out, "0000000001_fn.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "-- +ptah no_transaction\n"+wantUp)
	down, err := os.ReadFile(filepath.Join(out, "0000000001_fn.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Equals, "-- +ptah no_transaction\nDROP FUNCTION f();")
}

func TestGooseParseNoTransactionRequiresExactMarker(t *testing.T) {
	tests := []struct {
		name          string
		marker        string
		noTransaction bool
		wantSQL       string
	}{
		{
			name:          "exact",
			marker:        "-- +goose NO TRANSACTION",
			noTransaction: true,
			wantSQL:       "SELECT 0;\nSELECT 1;",
		},
		{
			name:          "exact CRLF line",
			marker:        "-- +goose NO TRANSACTION\r",
			noTransaction: true,
			wantSQL:       "SELECT 0;\nSELECT 1;",
		},
		{
			name:    "lowercase",
			marker:  "-- +goose no transaction",
			wantSQL: "SELECT 0;\n-- +goose no transaction\nSELECT 1;",
		},
		{
			name:    "space after prefix",
			marker:  "-- +goose  NO TRANSACTION",
			wantSQL: "SELECT 0;\n-- +goose  NO TRANSACTION\nSELECT 1;",
		},
		{
			name:    "space between words",
			marker:  "-- +goose NO  TRANSACTION",
			wantSQL: "SELECT 0;\n-- +goose NO  TRANSACTION\nSELECT 1;",
		},
		{
			name:    "leading space",
			marker:  " -- +goose NO TRANSACTION",
			wantSQL: "SELECT 0;\n -- +goose NO TRANSACTION\nSELECT 1;",
		},
		{
			name:    "trailing space",
			marker:  "-- +goose NO TRANSACTION ",
			wantSQL: "SELECT 0;\n-- +goose NO TRANSACTION \nSELECT 1;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName("goose")
			c.Assert(err, qt.IsNil)

			sql := "-- +goose Up\nSELECT 0;\n" + tt.marker + "\nSELECT 1;\n"
			migrations, err := parser.Parse(fstest.MapFS{"1_marker.sql": {Data: []byte(sql)}})
			c.Assert(err, qt.IsNil)
			c.Assert(migrations, qt.HasLen, 1)
			c.Assert(migrations[0].NoTransaction, qt.Equals, tt.noTransaction)
			c.Assert(migrations[0].UpSQL, qt.Equals, tt.wantSQL)
		})
	}
}

func TestGooseParseErrors(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		re   string
	}{
		{
			name: "go-based migration rejected",
			fsys: fstest.MapFS{
				"1_init.sql": {Data: []byte("-- +goose Up\nSELECT 1;")},
				"2_seed.go":  {Data: []byte("package migrations")},
			},
			re: `.*Go-based Goose migration.*not supported.*`,
		},
		{
			name: "no goose files",
			fsys: fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE t (id int);")}},
			re:   `.*no goose migration files.*`,
		},
		{
			name: "empty up section",
			fsys: fstest.MapFS{"1_init.sql": {Data: []byte("-- +goose Up\n-- +goose Down\nDROP TABLE t;")}},
			re:   `.*empty up section.*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			parser, _ := importer.ParserByName("goose")
			_, err := parser.Parse(tt.fsys)
			c.Assert(err, qt.ErrorMatches, tt.re)
		})
	}
}

// TestGooseAndGolangMigrateAreDistinguished proves auto-detect picks the right
// parser for each layout and does not treat one as the other.
func TestGooseAndGolangMigrateAreDistinguished(t *testing.T) {
	c := qt.New(t)

	goose, err := importer.DetectParser(gooseFS())
	c.Assert(err, qt.IsNil)
	c.Assert(goose.Name(), qt.Equals, "goose")

	glm, err := importer.DetectParser(golangMigrateFS())
	c.Assert(err, qt.IsNil)
	c.Assert(glm.Name(), qt.Equals, "golang-migrate")
}

func TestGooseImportEndToEnd(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(gooseFS(), nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	// Two goose migrations -> two up + two down Ptah files.
	c.Assert(result.Files, qt.HasLen, 4)
	// 8-digit versions fit Ptah's 10-digit format, so they are preserved (padded).
	c.Assert(result.Files, qt.Contains, "0020230101_init.up.sql")
	c.Assert(result.Files, qt.Contains, "0020230101_init.down.sql")
	c.Assert(result.Remapped, qt.IsFalse)
	c.Assert(result.SumFile, qt.Equals, "ptah.sum")
	up, readErr := os.ReadFile(filepath.Join(out, "0020230102_up_only.up.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(up), qt.Equals,
		"-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx2 ON users (id);")
	down, readErr := os.ReadFile(filepath.Join(out, "0020230102_up_only.down.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(down), qt.Equals,
		"-- +ptah no_transaction\n-- No rollback was provided by the source migration.\n")
}
