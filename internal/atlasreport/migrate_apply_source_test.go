package atlasreport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// A converted file is reported under the name and description it has in the
// operator's directory. Conversion names `V1.5__b c.sql` by its numeric
// execution-order key and rewrites the description, and the community binary
// reports the file the repository holds. A file the map does not name keeps
// the converted name.
func TestWriteMigrateApplyFormat_ReportsConvertedFilesBySourceName(t *testing.T) {
	c := qt.New(t)
	conn := connectApplyReportSQLite(c, "source-names.db")
	fsys := fstest.MapFS{
		"4611686018427469511_a.sql":   {Data: []byte("CREATE TABLE a (id int);\n")},
		"4611686018427471935_b_c.sql": {Data: []byte("CREATE TABLE b (id int);\n")},
		"4611686018427510315_n.sql":   {Data: []byte("CREATE TABLE n (id int);\n")},
	}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		migrator.WithAtlasRevisionVersions(map[int64]string{
			4611686018427469511: "1",
			4611686018427471935: "1.5",
			4611686018427510315: "2",
		}),
	)
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(&out, `{{ json . }}`, atlasreport.MigrateApplyResultOptions{
		Conn: conn,
		FS:   fsys,
		SourceFiles: map[int64]atlasreport.MigrateApplySourceFile{
			4611686018427469511: {Name: "V1__a.sql", Description: "a"},
			4611686018427471935: {Name: "V1.5__b c.sql", Description: "b c"},
		},
		Status:           &migrator.MigrationStatus{},
		Migrations:       provider.Migrations(),
		SelectedVersions: []int64{4611686018427469511, 4611686018427471935, 4611686018427510315},
		SelectedKeys:     []string{"1", "1.5", "2"},
		Applied:          true,
	})

	c.Assert(err, qt.IsNil)
	var got migrateApplyJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	want := []migrateApplyJSONFile{
		{Name: "V1__a.sql", Version: "1", Description: "a"},
		{Name: "V1.5__b c.sql", Version: "1.5", Description: "b c"},
		{Name: "4611686018427510315_n.sql", Version: "2", Description: "n"},
	}
	c.Assert(got.Pending, qt.DeepEquals, want)
	c.Assert(appliedJSONFiles(got.Applied), qt.DeepEquals, want)
	c.Assert(got.Target, qt.Equals, "2")
}

// The statements a file executed are listed as the file spells them, the way
// the community binary lists them. The expected lists are its measured output
// for the same bodies on SQLite.
func TestWriteMigrateApplyFormat_ListsStatementsAsTheFileSpellsThem(t *testing.T) {
	c := qt.New(t)
	conn := connectApplyReportSQLite(c, "written-statements.db")
	fsys := fstest.MapFS{
		"1_b.sql": {Data: []byte(
			"-- comment above\nCREATE TABLE b (id int);\nINSERT INTO b (id)\n  -- comment inside\n  VALUES (1);\n" +
				"INSERT INTO b VALUES (2)\n",
		)},
		"2_c.sql": {Data: []byte("-- atlas:txmode none\n\nCREATE TABLE c (id int);\nINSERT INTO c VALUES (1);\n")},
		"3_edge.sql": {Data: []byte(
			"CREATE TABLE x (id int); -- after x\nCREATE TABLE y (id int)\n;\n" +
				"/* block above */ CREATE TABLE z (id int /* in z */);\n\n\n" +
				"INSERT INTO x (id)\n  -- in\n  VALUES (2) -- trailing\n\n",
		)},
		"4_delimited.sql": {Data: []byte("-- atlas:delimiter $$\nCREATE TABLE d (id int)$$\nCREATE TABLE e (id int);$$\n")},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(&out, `{{ json . }}`, atlasreport.MigrateApplyResultOptions{
		Conn:             conn,
		FS:               fsys,
		Status:           &migrator.MigrationStatus{},
		Migrations:       provider.Migrations(),
		SelectedVersions: []int64{1, 2, 3, 4},
		Applied:          true,
	})

	c.Assert(err, qt.IsNil)
	var got migrateApplyJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(appliedJSONStatements(got.Applied), qt.DeepEquals, [][]string{
		{
			"CREATE TABLE b (id int);",
			"INSERT INTO b (id)\n  -- comment inside\n  VALUES (1);",
			"INSERT INTO b VALUES (2)",
		},
		{"CREATE TABLE c (id int);", "INSERT INTO c VALUES (1);"},
		{
			"CREATE TABLE x (id int);",
			"CREATE TABLE y (id int)\n;",
			"CREATE TABLE z (id int /* in z */);",
			"INSERT INTO x (id)\n  -- in\n  VALUES (2) -- trailing",
		},
		{"CREATE TABLE d (id int)", "CREATE TABLE e (id int);"},
	})
}

// A statement Ptah refused before sending it never reached the database, so
// the failed file lists only the statements before it. A statement the
// database answered is listed last instead. Only the migrator marks a
// statement as sent, so the tests of internal/atlasmigratereport drive that
// case through a real run.
func TestWriteMigrateApplyFormat_LeavesOutAStatementNeverSent(t *testing.T) {
	c := qt.New(t)
	conn := connectApplyReportSQLite(c, "never-sent.db")
	fsys := fstest.MapFS{
		"1_b.sql": {Data: []byte("CREATE TABLE b (id int);\nATTACH DATABASE 'x' AS x;\nCREATE TABLE b2 (id int);\n")},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas))
	c.Assert(err, qt.IsNil)
	applyErr := &migrator.FailedMigrationError{
		Version:    1,
		VersionKey: "1",
		Err: &migrator.MigrationExecutionError{
			Err:            errors.New("sqlite migration rejects ATTACH"),
			Statement:      "ATTACH DATABASE 'x' AS x",
			StatementIndex: 2,
			Total:          3,
		},
	}
	var out bytes.Buffer

	err = atlasreport.WriteMigrateApplyFormat(&out, `{{ json . }}`, atlasreport.MigrateApplyResultOptions{
		Conn:             conn,
		FS:               fsys,
		Status:           &migrator.MigrationStatus{},
		Migrations:       provider.Migrations(),
		SelectedVersions: []int64{1},
		ErrorText:        applyErr.Error(),
		ApplyError:       applyErr,
		Applied:          true,
	})

	c.Assert(err, qt.IsNil)
	var got migrateApplyJSONReport
	c.Assert(json.Unmarshal(out.Bytes(), &got), qt.IsNil)
	c.Assert(appliedJSONStatements(got.Applied), qt.DeepEquals, [][]string{{"CREATE TABLE b (id int);"}})
	c.Assert(got.Applied[0].Error, qt.DeepEquals, &migrateApplyJSONError{
		Stmt: "ATTACH DATABASE 'x' AS x",
		Text: "sqlite migration rejects ATTACH",
	})
}

func connectApplyReportSQLite(c *qt.C, name string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func appliedJSONFiles(applied []migrateApplyJSONAppliedFile) []migrateApplyJSONFile {
	files := make([]migrateApplyJSONFile, 0, len(applied))
	for _, file := range applied {
		files = append(files, migrateApplyJSONFile{Name: file.Name, Version: file.Version, Description: file.Description})
	}
	return files
}

func appliedJSONStatements(applied []migrateApplyJSONAppliedFile) [][]string {
	statements := make([][]string, 0, len(applied))
	for _, file := range applied {
		statements = append(statements, file.Applied)
	}
	return statements
}
