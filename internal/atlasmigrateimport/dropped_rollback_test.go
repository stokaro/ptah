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
func importedRollbackReport(c *qt.C, format string, files map[string]string) []string {
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
// holding the rollback is named.
func TestImport_NamesTheRollbacksItCannotCarry(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
		want   []string
	}{
		{
			name:   "golang-migrate keeps the rollback in its own file",
			format: "golang-migrate",
			files: map[string]string{
				"1_create_users.up.sql":   "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"1_create_users.down.sql": "DROP TABLE users;\n",
			},
			want: []string{"1_create_users.down.sql"},
		},
		{
			name:   "flyway keeps it in an undo file",
			format: "flyway",
			files: map[string]string{
				"V1__create_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"U1__create_users.sql": "DROP TABLE users;\n",
			},
			want: []string{"U1__create_users.sql"},
		},
		{
			name:   "goose keeps it in a Down section",
			format: "goose",
			files: map[string]string{
				"20260101000000_create_users.sql": "-- +goose Up\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"-- +goose Down\nDROP TABLE users;\n",
			},
			want: []string{"20260101000000_create_users.sql"},
		},
		{
			name:   "dbmate keeps it in a migrate:down section",
			format: "dbmate",
			files: map[string]string{
				"20260101000000_create_users.sql": "-- migrate:up\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n" +
					"-- migrate:down\nDROP TABLE users;\n",
			},
			want: []string{"20260101000000_create_users.sql"},
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
			want: []string{"20260101000000_create_users.sql"},
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dropped := importedRollbackReport(c, test.format, test.files)

			c.Assert(dropped, qt.HasLen, 0)
		})
	}
}
