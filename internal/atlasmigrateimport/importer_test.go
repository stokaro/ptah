package atlasmigrateimport_test

import (
	"fmt"
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

	// `migrate import` converts a serialized changelog now (stokaro/ptah#1629).
	// This entry point is the DIRECT-APPLY reading, which keeps one entry per
	// file and takes each file's name as its version -- a changelog has no such
	// name, so it is still refused here, by a message naming the verb that does
	// convert it.
	t.Run("Liquibase changelog on the direct-apply path", func(t *testing.T) {
		c := qt.New(t)
		source := fstest.MapFS{
			"1_init.sql":    &fstest.MapFile{Data: []byte("--liquibase formatted sql\n--changeset ptah:1\nCREATE TABLE users (id int);\n")},
			"changelog.xml": &fstest.MapFile{Data: []byte("<databaseChangeLog></databaseChangeLog>\n")},
		}

		_, err := atlasmigrateimport.LoadFS(source, "migrations", atlasmigrateimport.FormatLiquibase)

		c.Assert(err, qt.ErrorMatches,
			".*found serialized changelog\\(s\\) changelog\\.xml, which are imported by `migrate import`")
	})
}

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
	// The surviving baseline lands in the low band so it executes first, and
	// V3 in the versioned band. V1, V2 are squashed by B2 and U1 is an undo
	// file, so neither is covered by atlas.sum nor executed.
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"122412_baseline.sql",
		"4611686018427551119_third_migration.sql",
	})
	c.Assert(readFile(c, target, "122412_baseline.sql"), qt.Equals, "CREATE TABLE baseline (id int);\n")
	c.Assert(readFile(c, target, "4611686018427551119_third_migration.sql"), qt.Equals, "ALTER TABLE baseline ADD name text;\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportFlywayConvertsRepeatableMigrations(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "V1__initial.sql", "CREATE TABLE users (id int);\n")
	writeFile(c, source, "R__views.sql", "CREATE VIEW users_view AS SELECT * FROM users;\n")

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
	c.Assert(readFile(c, target, "9223372036854775807_views.sql"), qt.Equals, "CREATE VIEW users_view AS SELECT * FROM users;\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportFlywayOrdersDottedAndUnderscoreVersions(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// V1.5 must order before V2: component-wise 1.5 < 2. The old digit-stripping
	// parser produced 15 and 2 and inverted the order.
	writeFile(c, source, "V2__add_posts.sql", "CREATE TABLE posts (id int);\n")
	writeFile(c, source, "V1.5__add_users.sql", "CREATE TABLE users (id int);\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"4611686018427471935_add_users.sql",
		"4611686018427510315_add_posts.sql",
	})
	c.Assert(readFile(c, target, "4611686018427471935_add_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c, target, "4611686018427510315_add_posts.sql"), qt.Equals, "CREATE TABLE posts (id int);\n")
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

func TestImportLiquibaseConventionalChangelogSplitsChangesets(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "changelog.sql", `--liquibase formatted sql

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
	c.Assert(readFile(c, target, "1_alice_create_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readFile(c, target, "2_bob_add_email.sql"), qt.Equals, "ALTER TABLE users ADD COLUMN email text;\n")
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportLiquibaseConventionalFilesUseGlobalChangesetOrder(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "b.sql", `--liquibase formatted sql
--changeset bob:third
CREATE TABLE third_table (id int);
`)
	writeFile(c, source, "a.sql", `--liquibase formatted sql
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
	assertAtlasSumOK(c, target, result.SumFile)
}

func TestImportLiquibaseConventionalNameConvertsEntireCoveredSet(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeFile(c, source, "1_numbered.sql", `--liquibase formatted sql
--changeset numbered:first
CREATE TABLE numbered_table (id int);
`)
	writeFile(c, source, "changelog.sql", `--liquibase formatted sql
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
	c.Assert(readFile(c, target, "1_numbered_first.sql"), qt.Equals, "CREATE TABLE numbered_table (id int);\n")
	c.Assert(readFile(c, target, "2_conventional_second.sql"), qt.Equals, "CREATE TABLE conventional_table (id int);\n")
	assertAtlasSumOK(c, target, result.SumFile)
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
			name: "structured changelog mixed with formatted SQL",
			files: map[string]string{
				"changelog.sql": `--liquibase formatted sql
--changeset alice:valid
CREATE TABLE valid_table (id int);
`,
				"master.xml": "<databaseChangeLog></databaseChangeLog>\n",
			},
			wantError: `liquibase source holds both changelog files \(master\.xml\) and formatted-SQL changelogs \(changelog\.sql\).*`,
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
				writeFile(c, source, name, content)
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

func TestImportRejectsDuplicateFlywayVersions(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// "1" and "1" really are one version to Atlas CE, which panics rather than
	// executing such a directory, so refusing it is the oracle's own answer.
	writeFile(c, source, "V1__first.sql", "CREATE TABLE first (id int);\n")
	writeFile(c, source, "V1__second.sql", "CREATE TABLE second (id int);\n")

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
	writeFile(c, source, "V1__first.sql", "CREATE TABLE first (id int);\n")
	writeFile(c, source, "V01__second.sql", "CREATE TABLE second (id int);\n")

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=flyway",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"4611686018427469511_second.sql",
		"4611686018427469512_first.sql",
	})
	assertAtlasSumOK(c, target, result.SumFile)
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

// TestImportLiquibaseSerializedChangelogs is the destination half of
// stokaro/ptah#1629: the three serializations reach an Atlas migration
// directory, not just the shared parser.
//
// Each row is the same changelog written the way its tool writes it, so a row
// failing means that serialization stopped reaching the destination rather than
// that the changelog changed.
func TestImportLiquibaseSerializedChangelogs(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name: "xml",
			file: "db.changelog.xml",
			content: `<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog>
  <changeSet id="create-users" author="alice">
    <sql>CREATE TABLE users (id int);</sql>
    <rollback><sql>DROP TABLE users;</sql></rollback>
  </changeSet>
  <changeSet id="add-email" author="bob">
    <sql>ALTER TABLE users ADD COLUMN email text;</sql>
  </changeSet>
</databaseChangeLog>
`,
		},
		{
			name: "yaml",
			file: "db.changelog.yaml",
			content: `databaseChangeLog:
  - changeSet:
      id: create-users
      author: alice
      changes:
        - sql:
            sql: "CREATE TABLE users (id int);"
      rollback:
        - sql:
            sql: "DROP TABLE users;"
  - changeSet:
      id: add-email
      author: bob
      changes:
        - sql:
            sql: "ALTER TABLE users ADD COLUMN email text;"
`,
		},
		{
			name: "json",
			file: "db.changelog.json",
			content: `{"databaseChangeLog": [
  {"changeSet": {"id": "create-users", "author": "alice",
    "changes": [{"sql": {"sql": "CREATE TABLE users (id int);"}}],
    "rollback": [{"sql": {"sql": "DROP TABLE users;"}}]}},
  {"changeSet": {"id": "add-email", "author": "bob",
    "changes": [{"sql": {"sql": "ALTER TABLE users ADD COLUMN email text;"}}]}}
]}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			source := t.TempDir()
			target := t.TempDir()
			writeFile(c, source, tt.file, tt.content)

			result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
				FromURL: "file://" + source + "?format=liquibase",
				ToURL:   "file://" + target,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
				"1_alice_create_users.sql",
				"2_bob_add_email.sql",
			})
			c.Assert(readFile(c, target, "1_alice_create_users.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
			c.Assert(readFile(c, target, "2_bob_add_email.sql"),
				qt.Equals, "ALTER TABLE users ADD COLUMN email text;\n")
			// An Atlas migration directory has no down file, so the rollback is
			// not carried here -- the same as Flyway undo and Goose down on this
			// path. Ptah's own layout keeps it; see the migrations import tests.
			assertAtlasSumOK(c, target, result.SumFile)
		})
	}
}

// TestImportLiquibaseChangelogsOrderedAcrossFiles pins the ordering rule at the
// destination, where it is observable as file names.
//
// The versions are global across the directory, not per file, so a second
// changelog cannot restart at 1 and collide with the first.
func TestImportLiquibaseChangelogsOrderedAcrossFiles(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	// Written in the order that would be wrong if insertion order won.
	writeFile(c, source, "02-second.xml",
		`<databaseChangeLog><changeSet id="second" author="bob">`+
			`<sql>ALTER TABLE users ADD COLUMN email text;</sql></changeSet></databaseChangeLog>`)
	writeFile(c, source, "01-first.xml",
		`<databaseChangeLog><changeSet id="first" author="alice">`+
			`<sql>CREATE TABLE users (id int);</sql></changeSet></databaseChangeLog>`)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(baseNames(result.Files), qt.DeepEquals, []string{
		"1_alice_first.sql",
		"2_bob_second.sql",
	})
	assertAtlasSumOK(c, target, result.SumFile)
}

// TestImportLiquibaseChangelogRefusalWritesNothing is the "refused rather than
// dropped" rule seen at the destination.
//
// A refusal that had already written half a directory would leave a target that
// applies cleanly and is missing the rest, which is the state this whole path
// exists to avoid.
func TestImportLiquibaseChangelogRefusalWritesNothing(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "target")
	c.Assert(os.Mkdir(source, 0o700), qt.IsNil)
	// The first changeset converts; the second does not.
	writeFile(c, source, "changelog.xml",
		`<databaseChangeLog>`+
			`<changeSet id="ok" author="alice"><sql>CREATE TABLE users (id int);</sql></changeSet>`+
			`<changeSet id="typed" author="bob"><createTable tableName="posts"/></changeSet>`+
			`</databaseChangeLog>`)

	_, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.ErrorMatches, `.*uses <createTable>, which is not SQL text.*`)
	_, statErr := os.Stat(target)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("target directory must not be created"))
}

// TestImportLiquibaseChangelogPadsVersionsToWidth pins the padding on the
// changelog path.
//
// atlas.sum orders entries lexically and Ptah executes a hashed Atlas directory
// in that order, so an unpadded 10 sorts before 2 and the directory applies in
// an order neither tool would have run. Ten changesets is the smallest fixture
// that separates padded from unpadded -- with nine, both spellings agree.
func TestImportLiquibaseChangelogPadsVersionsToWidth(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	changelog := "<databaseChangeLog>"
	for i := 1; i <= 10; i++ {
		changelog += fmt.Sprintf(
			`<changeSet id="c%d" author="alice"><sql>CREATE TABLE t%d (id int);</sql></changeSet>`, i, i)
	}
	changelog += "</databaseChangeLog>"
	writeFile(c, source, "db.changelog.xml", changelog)

	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL: "file://" + source + "?format=liquibase",
		ToURL:   "file://" + target,
	})

	c.Assert(err, qt.IsNil)
	names := baseNames(result.Files)
	c.Assert(names, qt.HasLen, 10)
	// Padded: 01 first and 10 last. Unpadded, "10" sorts between "1" and "2".
	c.Assert(names[0], qt.Equals, "01_alice_c1.sql")
	c.Assert(names[1], qt.Equals, "02_alice_c2.sql")
	c.Assert(names[9], qt.Equals, "10_alice_c10.sql")
	assertAtlasSumOK(c, target, result.SumFile)
}
