package atlasmigrateimport_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/internal/atlasmigrateimport"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestImportFlywayBaselineAndUndo(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "V1__initial.sql", "CREATE TABLE skipped (id int);\n")
	writeFile(c, source, "V2__second.sql", "CREATE TABLE skipped_2 (id int);\n")
	writeFile(c, source, "B2__baseline.sql", "CREATE TABLE baseline (id int);\n")
	writeFile(c, source, "V3__third_migration.sql", "ALTER TABLE baseline ADD name text;\n")
	writeFile(c, source, "U1__initial.sql", "DROP TABLE skipped;\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"2_baseline.sql",
		"3_third_migration.sql",
	})
	c.Assert(readFile(c, target, "2_baseline.sql"), qt.Equals, "CREATE TABLE baseline (id int);\n")
	c.Assert(readFile(c, target, "3_third_migration.sql"), qt.Equals, "ALTER TABLE baseline ADD name text;\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportFlywayRejectsRepeatableMigrations(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "V1__initial.sql", "CREATE TABLE users (id int);\n")
	writeFile(c, source, "R__views.sql", "CREATE VIEW users_view AS SELECT * FROM users;\n")

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `Flyway repeatable migration R__views\.sql cannot be imported yet because Ptah does not execute Atlas R-suffixed migrations`)
	_, statErr := os.Stat(filepath.Join(target, "1_initial.sql"))
	c.Assert(os.IsNotExist(statErr), qt.Equals, true)
}

func TestImportFlywayNormalizesDottedAndUnderscoreVersions(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "V1.1__add_users.sql", "CREATE TABLE users (id int);\n")
	writeFile(c, source, "V1_2__add_posts.sql", "CREATE TABLE posts (id int);\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"11_add_users.sql",
		"12_add_posts.sql",
	})
	c.Assert(readFile(c, target, "11_add_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c, target, "12_add_posts.sql"), qt.Equals, "CREATE TABLE posts (id int);\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportGolangMigrateSkipsDownFiles(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "1_initial.up.sql", "CREATE TABLE users (id int);\n")
	writeFile(c, source, "1_initial.down.sql", "DROP TABLE users;\n")
	writeFile(c, source, "2_second.up.sql", "ALTER TABLE users ADD name text;\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=golang-migrate",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_initial.sql", "2_second.sql"})
	c.Assert(readFile(c, target, "1_initial.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c, target, "2_second.sql"), qt.Equals, "ALTER TABLE users ADD name text;\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportGooseAndDBMateUseOnlyUpSections(t *testing.T) {
	tests := []struct {
		name   string
		format string
		sql    string
		want   string
	}{
		{
			name:   "goose",
			format: "goose",
			sql: `-- +goose Up
CREATE TABLE posts (id int);

-- +goose StatementBegin
CREATE FUNCTION f() RETURNS void AS $$
BEGIN

END
$$;
-- +goose StatementEnd
-- +goose Down
DROP TABLE posts;
`,
			want: `CREATE TABLE posts (id int);
CREATE FUNCTION f() RETURNS void AS $$
BEGIN

END
$$;
`,
		},
		{
			name:   "dbmate",
			format: "dbmate",
			sql: `-- migrate:up
CREATE TABLE posts (id int);

INSERT INTO posts (id) VALUES ('one

two');

-- migrate:down
DROP TABLE posts;
`,
			want: `CREATE TABLE posts (id int);
INSERT INTO posts (id) VALUES ('one

two');
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source := t.TempDir()
			target := t.TempDir()
			writeFile(c, source, "1_initial.sql", tt.sql)

			result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
				FromURL: "file://" + source + "?format=" + tt.format,
				ToURL:   "file://" + target,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_initial.sql"})
			c.Assert(readFile(c, target, "1_initial.sql"), qt.Equals, tt.want)
			assertAtlasSumOK(c, target, result.SumFile)
		})
	}
}

func TestImportLiquibaseDropsRollbackDirectives(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "1_initial.sql", `--liquibase formatted sql

--changeset atlas:1-1
CREATE TABLE posts (id int);
--rollback DROP TABLE posts;
`)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_initial.sql"})
	c.Assert(readFile(c, target, "1_initial.sql"), qt.Equals, "--changeset atlas:1-1\nCREATE TABLE posts (id int);\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportRejectsRemoteSourceURL(t *testing.T) {
	c := qt.New(t)

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{FromURL: "atlas://repo/migrations?format=flyway"})

	c.Assert(err, qt.ErrorMatches, `import --from: only local file:// migration directories are supported`)
}

func TestImportRejectsSameSourceAndTargetDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeFile(c, dir, "1_initial.sql", `-- +goose Up
CREATE TABLE users (id int);
-- +goose Down
DROP TABLE users;
`)

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + dir + "?format=goose",
		ToURL:   "file://" + dir,
	})

	c.Assert(err, qt.ErrorMatches, `import --to must be different from --from for format "goose"`)
	c.Assert(readFile(c, dir, "1_initial.sql"), qt.Contains, "-- +goose Down")
}

func TestImportRejectsExistingTargetFiles(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "1_initial.up.sql", "CREATE TABLE users (id int);\n")
	writeFile(c, target, "1_initial.sql", "SELECT 1;\n")

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=golang-migrate",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `target migration directory already contains SQL file: .*1_initial\.sql`)
	c.Assert(readFile(c, target, "1_initial.sql"), qt.Equals, "SELECT 1;\n")
}

func TestImportRejectsDuplicateGeneratedNames(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "V1__same-name.sql", "CREATE TABLE first (id int);\n")
	writeFile(c, source, "V01__same-name.sql", "CREATE TABLE second (id int);\n")

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `import produced duplicate migration file name 1_same-name.sql`)
}

func writeFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func readFile(c *qt.C, dir, name string) string {
	c.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	c.Assert(err, qt.IsNil)
	return string(data)
}

func baseNames(paths []string) []string {
	names := make([]string, 0, len(paths))
	for _, path := range paths {
		names = append(names, filepath.Base(path))
	}
	slices.Sort(names)
	return names
}

func assertAtlasSumOK(c *qt.C, dir, sumFile string) {
	c.Helper()
	c.Assert(filepath.Base(sumFile), qt.Equals, atlascompat.AtlasSumFileName)
	result, err := atlascompat.VerifySumDir(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.Equals, true)
}
