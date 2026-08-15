package atlasmigrateimport_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestLoadFS_RejectsUnsupportedSourceFiles(t *testing.T) {

	t.Run("Go-based Goose migration", func(t *testing.T) {
		c := qt.New(t)
		source := fstest.MapFS{
			"1_init.sql": &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE users (id int);\n")},
			"2_seed.go":  &fstest.MapFile{Data: []byte("package migrations\n")},
		}

		_, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatGoose)

		c.Assert(err, qt.ErrorMatches, `Go-based Goose migration "2_seed\.go" is not supported \(SQL migrations only\)`)
	})

	t.Run("Liquibase XML changelog", func(t *testing.T) {
		c := qt.New(t)
		source := fstest.MapFS{
			"1_init.sql":    &fstest.MapFile{Data: []byte("--liquibase formatted sql\n--changeset ptah:1\nCREATE TABLE users (id int);\n")},
			"changelog.xml": &fstest.MapFile{Data: []byte("<databaseChangeLog></databaseChangeLog>\n")},
		}

		_, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

		c.Assert(err, qt.ErrorMatches, `liquibase XML/YAML/JSON changelogs are not yet supported .* found changelog\.xml`)
	})
}

func TestImportFlywayBaselineAndUndo(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "V1__initial.sql", "CREATE TABLE skipped (id int);\n")
	writeFile(c.TB, source, "V2__second.sql", "CREATE TABLE skipped_2 (id int);\n")
	writeFile(c.TB, source, "B2__baseline.sql", "CREATE TABLE baseline (id int);\n")
	writeFile(c.TB, source, "V3__third_migration.sql", "ALTER TABLE baseline ADD name text;\n")
	writeFile(c.TB, source, "U1__initial.sql", "DROP TABLE skipped;\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	// The surviving baseline lands in the low band so it executes first, and
	// V3 in the versioned band. V1, V2 are squashed by B2 and U1 is an undo
	// file, so neither is covered by atlas.sum nor executed.
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"122412_baseline.sql",
		"4611686018427551119_third_migration.sql",
	})
	c.Assert(readFile(c.TB, target, "122412_baseline.sql"), qt.Equals, "CREATE TABLE baseline (id int);\n")
	c.Assert(readFile(c.TB, target, "4611686018427551119_third_migration.sql"), qt.Equals, "ALTER TABLE baseline ADD name text;\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportFlywayConvertsRepeatableMigrations(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "V1__initial.sql", "CREATE TABLE users (id int);\n")
	writeFile(c.TB, source, "R__views.sql", "CREATE VIEW users_view AS SELECT * FROM users;\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	// Atlas CE hashes AND executes a repeatable, once, as version "". Refusing
	// it was an over-refusal on a directory the oracle applies (#982).
	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"4611686018427469511_initial.sql",
		"9223372036854775807_views.sql",
	})
	c.Assert(readFile(c.TB, target, "9223372036854775807_views.sql"), qt.Equals, "CREATE VIEW users_view AS SELECT * FROM users;\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportFlywayOrdersDottedAndUnderscoreVersions(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// V1.5 must order before V2: component-wise 1.5 < 2. The old digit-stripping
	// parser produced 15 and 2 and inverted the order.
	writeFile(c.TB, source, "V2__add_posts.sql", "CREATE TABLE posts (id int);\n")
	writeFile(c.TB, source, "V1.5__add_users.sql", "CREATE TABLE users (id int);\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"4611686018427471935_add_users.sql",
		"4611686018427510315_add_posts.sql",
	})
	c.Assert(readFile(c.TB, target, "4611686018427471935_add_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c.TB, target, "4611686018427510315_add_posts.sql"), qt.Equals, "CREATE TABLE posts (id int);\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportGolangMigrateSkipsDownFiles(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "1_initial.up.sql", "CREATE TABLE users (id int);\n")
	writeFile(c.TB, source, "1_initial.down.sql", "DROP TABLE users;\n")
	writeFile(c.TB, source, "2_second.up.sql", "ALTER TABLE users ADD name text;\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=golang-migrate",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_initial.sql", "2_second.sql"})
	c.Assert(readFile(c.TB, target, "1_initial.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c.TB, target, "2_second.sql"), qt.Equals, "ALTER TABLE users ADD name text;\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
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
			writeFile(c.TB, source, "1_initial.sql", tt.sql)

			result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
				FromURL: "file://" + source + "?format=" + tt.format,
				ToURL:   "file://" + target,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(baseNames(result.Files), qt.DeepEquals, []string{"1_initial.sql"})
			c.Assert(readFile(c.TB, target, "1_initial.sql"), qt.Equals, tt.want)
			assertAtlasSumOK(c.TB, target, result.SumFile)
		})
	}
}

func TestImportLiquibaseDropsRollbackDirectives(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "1_initial.sql", `--liquibase formatted sql

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
	c.Assert(readFile(c.TB, target, "1_initial.sql"), qt.Equals, "--changeset atlas:1-1\nCREATE TABLE posts (id int);\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportLiquibaseConventionalChangelogSplitsChangesets(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "changelog.sql", `--liquibase formatted sql

--changeset alice:create-users
CREATE TABLE users (id int);
--rollback DROP TABLE users;

--changeset bob:add-email
ALTER TABLE users ADD COLUMN email text;
--rollback ALTER TABLE users DROP COLUMN email;
`)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"1_alice_create_users.sql",
		"2_bob_add_email.sql",
	})
	c.Assert(readFile(c.TB, target, "1_alice_create_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c.TB, target, "2_bob_add_email.sql"), qt.Equals, "ALTER TABLE users ADD COLUMN email text;\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportLiquibaseConventionalFilesUseGlobalChangesetOrder(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "b.sql", `--liquibase formatted sql
--changeset bob:third
CREATE TABLE third_table (id int);
`)
	writeFile(c.TB, source, "a.sql", `--liquibase formatted sql
--changeset alice:first
CREATE TABLE first_table (id int);
--changeset alice:second
CREATE TABLE second_table (id int);
`)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"1_alice_first.sql",
		"2_alice_second.sql",
		"3_bob_third.sql",
	})
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportLiquibaseConventionalNameConvertsEntireCoveredSet(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "1_numbered.sql", `--liquibase formatted sql
--changeset numbered:first
CREATE TABLE numbered_table (id int);
`)
	writeFile(c.TB, source, "changelog.sql", `--liquibase formatted sql
--changeset conventional:second
CREATE TABLE conventional_table (id int);
`)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"1_numbered_first.sql",
		"2_conventional_second.sql",
	})
	c.Assert(readFile(c.TB, target, "1_numbered_first.sql"), qt.Equals, "CREATE TABLE numbered_table (id int);\n")
	c.Assert(readFile(c.TB, target, "2_conventional_second.sql"), qt.Equals, "CREATE TABLE conventional_table (id int);\n")
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func TestImportLiquibaseConventionalInputRefusesBeforeCreatingTarget(t *testing.T) {
	tests := []struct {
		name      string
		files     map[string]string
		wantError string
	}{
		{
			name: "mixed formatted and headerless SQL",
			files: map[string]string{
				"1_legacy.sql": "CREATE TABLE skipped_table (id int);\n",
				"changelog.sql": `--liquibase formatted sql
--changeset alice:valid
CREATE TABLE valid_table (id int);
`,
			},
			wantError: `parse liquibase source file 1_legacy\.sql: no liquibase formatted-SQL changelogs .* found`,
		},
		{
			name: "malformed changeset",
			files: map[string]string{
				"changelog.sql": `--liquibase formatted sql
--changeset missing
CREATE TABLE invalid_table (id int);
`,
			},
			wantError: `parse liquibase source file changelog\.sql: liquibase changeset marker "missing" in "changelog\.sql" is missing author:id`,
		},
		{
			name: "empty sanitized identity",
			files: map[string]string{
				"changelog.sql": `--liquibase formatted sql
--changeset !!!:???
CREATE TABLE invalid_table (id int);
`,
			},
			wantError: `liquibase changeset identity "_" in changelog\.sql cannot be represented in an Atlas migration file name`,
		},
		{
			name: "unsupported structured changelog",
			files: map[string]string{
				"changelog.sql": `--liquibase formatted sql
--changeset alice:valid
CREATE TABLE valid_table (id int);
`,
				"master.xml": "<databaseChangeLog></databaseChangeLog>\n",
			},
			wantError: `liquibase XML/YAML/JSON changelogs are not yet supported .* found master\.xml`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			source := filepath.Join(root, "source")
			target := filepath.Join(root, "target")
			c.Assert(os.Mkdir(source, 0o700), qt.IsNil)
			for name, content := range tt.files {
				writeFile(c.TB, source, name, content)
			}

			_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
				FromURL: "file://" + source + "?format=liquibase",
				ToURL:   "file://" + target,
			})

			c.Assert(err, qt.ErrorMatches, tt.wantError)
			_, statErr := os.Stat(target)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestImportRejectsRemoteSourceURL(t *testing.T) {
	c := qt.New(t)

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{FromURL: "atlas://repo/migrations?format=flyway"})

	c.Assert(err, qt.ErrorMatches, `import --from: only local file:// migration directories are supported`)
}

func TestImportRejectsSameSourceAndTargetDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeFile(c.TB, dir, "1_initial.sql", `-- +goose Up
CREATE TABLE users (id int);
-- +goose Down
DROP TABLE users;
`)

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + dir + "?format=goose",
		ToURL:   "file://" + dir,
	})

	c.Assert(err, qt.ErrorMatches, `import --to must be different from --from for format "goose"`)
	c.Assert(readFile(c.TB, dir, "1_initial.sql"), qt.Contains, "-- +goose Down")
}

func TestImportRejectsExistingTargetFiles(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c.TB, source, "1_initial.up.sql", "CREATE TABLE users (id int);\n")
	writeFile(c.TB, target, "1_initial.sql", "SELECT 1;\n")

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=golang-migrate",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `target migration directory already contains SQL file: .*1_initial\.sql`)
	c.Assert(readFile(c.TB, target, "1_initial.sql"), qt.Equals, "SELECT 1;\n")
}

func TestImportRejectsDuplicateFlywayVersions(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// "1" and "1" really are one version to Atlas CE, which panics rather than
	// executing such a directory, so refusing it is the oracle's own answer.
	writeFile(c.TB, source, "V1__first.sql", "CREATE TABLE first (id int);\n")
	writeFile(c.TB, source, "V1__second.sql", "CREATE TABLE second (id int);\n")

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `Flyway migrations V1__first\.sql and V1__second\.sql both carry the Atlas version "1"`)
}

func TestImportConvertsFlywayTokensThatOnlyOrderAlike(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// "1" and "01" are DIFFERENT versions to Atlas CE, which runs both in walk
	// order. Scoring them into components merges them, and the previous
	// implementation refused the pair on that merge.
	writeFile(c.TB, source, "V1__first.sql", "CREATE TABLE first (id int);\n")
	writeFile(c.TB, source, "V01__second.sql", "CREATE TABLE second (id int);\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"4611686018427469511_second.sql",
		"4611686018427469512_first.sql",
	})
	assertAtlasSumOK(c.TB, target, result.SumFile)
}

func writeFile(tb testing.TB, dir, name, content string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func readFile(tb testing.TB, dir, name string) string {
	c := qt.New(tb)
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

func assertAtlasSumOK(tb testing.TB, dir, sumFile string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(filepath.Base(sumFile), qt.Equals, atlascompat.AtlasSumFileName)
	result, err := atlascompat.VerifySumDir(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.Equals, true)
}
