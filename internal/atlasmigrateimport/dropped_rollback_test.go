package atlasmigrateimport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasmigrateimport"
)

// importedRollbackReport runs one import over a source directory the row
// describes and returns what the conversion said it left behind.
func importedRollbackReport(c *qt.C, format string, files map[string]string) []atlasmigrateimport.DroppedRollback {
	c.Helper()
	from := filepath.Join(c.TempDir(), "src")
	c.Assert(os.MkdirAll(from, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(from, name), []byte(body), 0o600), qt.IsNil)
	}
	result, err := atlasmigrateimport.Import(atlasmigrateimport.Options{
		FromURL:   "file://" + from,
		ToURL:     "file://" + filepath.Join(c.TempDir(), "dst"),
		DirFormat: format,
	})
	c.Assert(err, qt.IsNil)
	return result.DroppedRollbacks
}

// TestImport_NamesTheRollbacksItCannotCarry is the reproduction from
// stokaro/ptah#3116.
//
// An Atlas single-file migration is up-only, so the undo file or down section a
// source layout carries has nowhere to go. Dropping it is the conversion's
// shape; dropping it in silence was the defect -- every import of every layout
// exited 0 with nothing on either stream, and an operator had no way to learn
// their undo scripts stayed in the source directory.
//
// The rows are the five layouts, each written the way its own tool writes a
// rollback, and the assertion is the same for all of them: the source file
// holding the rollback is named. A Liquibase rollback belongs to a changeset,
// so the changeset is named with its file, on every path the import takes: a
// numbered file copied whole, a changelog split into changesets, and an XML,
// YAML or JSON changelog (stokaro/ptah#3753).
func TestImport_NamesTheRollbacksItCannotCarry(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
		want   []atlasmigrateimport.DroppedRollback
	}{
		{
			name:   "golang-migrate keeps the rollback in its own file",
			format: "golang-migrate",
			files: map[string]string{
				"1_create_users.up.sql":   "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"1_create_users.down.sql": "DROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "1_create_users.down.sql"}},
		},
		{
			name:   "flyway keeps it in an undo file",
			format: "flyway",
			files: map[string]string{
				"V1__create_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"U1__create_users.sql": "DROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "U1__create_users.sql"}},
		},
		{
			name:   "goose keeps it in a Down section",
			format: "goose",
			files: map[string]string{
				"20260101000000_create_users.sql": "-- +goose Up\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"-- +goose Down\nDROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "20260101000000_create_users.sql"}},
		},
		{
			name:   "dbmate keeps it in a migrate:down section",
			format: "dbmate",
			files: map[string]string{
				"20260101000000_create_users.sql": "-- migrate:up\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"-- migrate:down\nDROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "20260101000000_create_users.sql"}},
		},
		{
			name:   "liquibase keeps it on a rollback line",
			format: "liquibase",
			files: map[string]string{
				"20260101000000_create_users.sql": "--liquibase formatted sql\n" +
					"--changeset a:1\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"--rollback DROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "20260101000000_create_users.sql", Changeset: "a:1"}},
		},
		{
			name:   "liquibase keeps it in a rollback block",
			format: "liquibase",
			files: map[string]string{
				"20260101000000_create_users.sql": "--liquibase formatted sql\n" +
					"--changeset a:1\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"/* liquibase rollback\nDROP TABLE users;\n*/\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "20260101000000_create_users.sql", Changeset: "a:1"}},
		},
		{
			name:   "liquibase names each changeset of a numbered file that had one",
			format: "liquibase",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n" +
					"--changeset a:1\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE users;\n" +
					"--changeset a:2\nCREATE TABLE audit (id INTEGER PRIMARY KEY);\n" +
					"--changeset a:3\nCREATE TABLE posts (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE posts;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "1_init.sql", Changeset: "a:1"}, {Path: "1_init.sql", Changeset: "a:3"}},
		},
		{
			name:   "liquibase splits a conventional changelog into changesets",
			format: "liquibase",
			files: map[string]string{
				"changelog.sql": "--liquibase formatted sql\n" +
					"--changeset a:1\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE users;\n" +
					"--changeset a:2\nCREATE TABLE audit (id INTEGER PRIMARY KEY);\n" +
					"--changeset a:3\nCREATE TABLE posts (id INTEGER PRIMARY KEY);\n" +
					"/* liquibase rollback\nDROP TABLE posts;\n*/\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "changelog.sql", Changeset: "a:1"}, {Path: "changelog.sql", Changeset: "a:3"}},
		},
		{
			// A numbered file beside a conventional one is split as well, so
			// each rollback is named once, by its changeset.
			name:   "liquibase numbered and conventional files together",
			format: "liquibase",
			files: map[string]string{
				"1_numbered.sql": "--liquibase formatted sql\n--changeset n:1\n" +
					"CREATE TABLE numbered (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE numbered;\n",
				"changelog.sql": "--liquibase formatted sql\n--changeset c:1\n" +
					"CREATE TABLE conventional (id INTEGER PRIMARY KEY);\n--rollback DROP TABLE conventional;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "1_numbered.sql", Changeset: "n:1"}, {Path: "changelog.sql", Changeset: "c:1"}},
		},
		{
			name:   "liquibase xml",
			format: "liquibase",
			files: map[string]string{
				"changelog.xml": `<databaseChangeLog><changeSet id="1" author="a">` +
					`<sql>CREATE TABLE users (id INTEGER PRIMARY KEY);</sql><rollback>DROP TABLE users;</rollback>` +
					`</changeSet></databaseChangeLog>`,
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "changelog.xml", Changeset: "a:1"}},
		},
		{
			name:   "liquibase yaml",
			format: "liquibase",
			files: map[string]string{
				"changelog.yaml": "databaseChangeLog:\n  - changeSet:\n      id: \"1\"\n      author: a\n" +
					"      changes:\n        - sql: CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"      rollback: DROP TABLE users;\n",
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "changelog.yaml", Changeset: "a:1"}},
		},
		{
			name:   "liquibase json",
			format: "liquibase",
			files: map[string]string{
				"changelog.json": `{"databaseChangeLog": [{"changeSet": {"id": "1", "author": "a", ` +
					`"changes": [{"sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);"}], ` +
					`"rollback": "DROP TABLE users;"}}]}`,
			},
			want: []atlasmigrateimport.DroppedRollback{{Path: "changelog.json", Changeset: "a:1"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dropped := importedRollbackReport(c, test.format, test.files)

			c.Assert(dropped, qt.DeepEquals, test.want)
		})
	}
}

// TestImport_ReportsNothingWhenNoRollbackWasWritten is the control the issue
// asked for. A report that fires on every import teaches an operator to ignore
// it, so it has to fire only where something was actually left behind.
func TestImport_ReportsNothingWhenNoRollbackWasWritten(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
	}{
		{
			name:   "goose with no Down section",
			format: "goose",
			files: map[string]string{
				"20260101000000_only_up.sql": "-- +goose Up\nCREATE TABLE only_up (id INTEGER PRIMARY KEY);\n",
			},
		},
		{
			name:   "goose with an empty Down section",
			format: "goose",
			files: map[string]string{
				"20260101000000_blank_down.sql": "-- +goose Up\n" +
					"CREATE TABLE blank_down (id INTEGER PRIMARY KEY);\n" +
					"-- +goose Down\n\n",
			},
		},
		{
			name:   "golang-migrate with no down file",
			format: "golang-migrate",
			files: map[string]string{
				"1_create_users.up.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
			},
		},
		{
			name:   "a liquibase changelog with an empty rollback and one with none",
			format: "liquibase",
			files: map[string]string{
				"changelog.xml": `<databaseChangeLog><changeSet id="1" author="a">` +
					`<sql>CREATE TABLE users (id INTEGER PRIMARY KEY);</sql><rollback/></changeSet>` +
					`<changeSet id="2" author="a"><sql>CREATE TABLE posts (id INTEGER PRIMARY KEY);</sql></changeSet>` +
					`</databaseChangeLog>`,
			},
		},
		{
			name:   "a conventional liquibase changelog whose rollback is not required",
			format: "liquibase",
			files: map[string]string{
				"changelog.sql": "--liquibase formatted sql\n--changeset a:1\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n--rollback not required\n",
			},
		},
		{
			// Liquibase rolls such a changeset back by running nothing.
			name:   "liquibase with a rollback that is not required",
			format: "liquibase",
			files: map[string]string{
				"20260101000000_create_users.sql": "--liquibase formatted sql\n--changeset a:1\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n--rollback not required\n",
			},
		},
		{
			// A comment to Liquibase, which reads no rollback without the
			// blank after the keyword.
			name:   "liquibase with rollback and no blank after it",
			format: "liquibase",
			files: map[string]string{
				"20260101000000_create_users.sql": "--liquibase formatted sql\n--changeset a:1\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n--rollback;\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dropped := importedRollbackReport(c, test.format, test.files)

			c.Assert(dropped, qt.HasLen, 0)
		})
	}
}
