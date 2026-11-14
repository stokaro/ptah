package migratetests_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// The expected values in this file are the pinned community binary's output,
// measured on SQLite with the same directories, each hashed with its
// `migrate hash --dir file://...?format=<format>`.

// applyNamesTemplate prints the name of every pending and every applied file.
const applyNamesTemplate = "{{ range .Pending }}{{ .Name }} {{ end }}|{{ range .Applied }}{{ .Name }} {{ end }}"

// writeApplyFiles writes each file under dir.
func writeApplyFiles(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	for name, body := range files {
		atlastest.WriteAtlasApplyProjectMigration(c, dir, name, body)
	}
}

// TestMigrateApplyNamesConvertedFilesAsTheDirectoryHoldsThem applies one file
// of a directory in each layout the import converts and reads the names the
// --format report gives it. A golang-migrate file executes under its converted
// name and is reported as the directory holds it, with its direction.
func TestMigrateApplyNamesConvertedFilesAsTheDirectoryHoldsThem(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
		want   string
	}{
		{
			name:   "golang-migrate",
			format: "golang-migrate",
			files: map[string]string{
				"1_a.up.sql":          "CREATE TABLE a (id int);\n",
				"1_a.down.sql":        "DROP TABLE a;\n",
				"000002_b_c.up.sql":   "CREATE TABLE b (id int);\n",
				"000002_b_c.down.sql": "DROP TABLE b;\n",
			},
			// The directory orders files by name, so 000002_b_c sorts before
			// 1_a and is the one file the amount applies, on the pinned
			// binary too.
			want: "000002_b_c.up.sql |000002_b_c.up.sql ",
		},
		{
			name:   "goose",
			format: "goose",
			files: map[string]string{
				"1_a.sql":   "-- +goose Up\nCREATE TABLE a (id int);\n-- +goose Down\nDROP TABLE a;\n",
				"2_b_c.sql": "-- +goose Up\nCREATE TABLE b (id int);\n-- +goose Down\nDROP TABLE b;\n",
			},
			want: "1_a.sql |1_a.sql ",
		},
		{
			name:   "dbmate",
			format: "dbmate",
			files: map[string]string{
				"20200101000000_a.sql":   "-- migrate:up\nCREATE TABLE a (id int);\n-- migrate:down\nDROP TABLE a;\n",
				"20200102000000_b_c.sql": "-- migrate:up\nCREATE TABLE b (id int);\n-- migrate:down\nDROP TABLE b;\n",
			},
			want: "20200101000000_a.sql |20200101000000_a.sql ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(t.TempDir(), "migrations")
			writeApplyFiles(c, dir, tt.files)
			hashConvertedApplyDir(c, dir, tt.format)

			stdout, stderr, err := atlastest.RunCompat("migrate", "apply", "1",
				"--url", "sqlite://"+filepath.Join(t.TempDir(), "apply.db"),
				"--dir", "file://"+dir+"?format="+tt.format,
				"--format", applyNamesTemplate)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, tt.want)
		})
	}
}

// TestMigrateApplyNamesAnOutOfOrderGolangMigrateFile refuses a golang-migrate
// file added below the applied history and names it as the directory holds
// it, with and without --format.
func TestMigrateApplyNamesAnOutOfOrderGolangMigrateFile(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "text"},
		{name: "format", args: []string{"--format", "{{ json . }}"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(t.TempDir(), "migrations")
			dbPath := filepath.Join(t.TempDir(), "apply.db")
			writeApplyFiles(c, dir, map[string]string{
				"1_a.up.sql":   "CREATE TABLE a (id int);\n",
				"1_a.down.sql": "DROP TABLE a;\n",
				"3_c.up.sql":   "CREATE TABLE c (id int);\n",
				"3_c.down.sql": "DROP TABLE c;\n",
			})
			hashConvertedApplyDir(c, dir, "golang-migrate")
			apply := []string{"migrate", "apply", "--url", "sqlite://" + dbPath, "--dir", "file://" + dir + "?format=golang-migrate"}
			_, stderr, err := atlastest.RunCompat(apply...)
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			writeApplyFiles(c, dir, map[string]string{
				"2_b.up.sql":   "CREATE TABLE b (id int);\n",
				"2_b.down.sql": "DROP TABLE b;\n",
			})
			hashConvertedApplyDir(c, dir, "golang-migrate")

			stdout, _, err := atlastest.RunCompat(append(apply, tt.args...)...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				"migration file 2_b.up.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error"))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"a", "c"})
		})
	}
}

// TestMigrateApplyRefusesTheAmountAsTheCommunityBinaryDoes refuses each amount
// the pinned binary refuses, with its line, before the database is opened,
// with and without --format.
func TestMigrateApplyRefusesTheAmountAsTheCommunityBinaryDoes(t *testing.T) {
	tests := []struct {
		name    string
		amount  []string
		wantErr string
	}{
		{
			name:    "not a number",
			amount:  []string{"x"},
			wantErr: `invalid amount argument "x" (invalid syntax). Omit the argument or pass a valid integer instead`,
		},
		{
			name:    "past 64 bits",
			amount:  []string{"9223372036854775808"},
			wantErr: `invalid amount argument "9223372036854775808" (value out of range). Omit the argument or pass a valid integer instead`,
		},
		{name: "zero", amount: []string{"0"}, wantErr: "cannot apply '0' migration files"},
		{name: "negative zero", amount: []string{"-0"}, wantErr: "cannot apply '0' migration files"},
		{name: "negative", amount: []string{"-1"}, wantErr: "cannot apply '-1' migration files"},
		{name: "two amounts", amount: []string{"1", "2"}, wantErr: "accepts at most 1 arg(s), received 2"},
	}
	outputs := []struct {
		name string
		args []string
	}{
		{name: "text"},
		{name: "format", args: []string{"--format", "{{ json . }}"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, output := range outputs {
				t.Run(output.name, func(t *testing.T) {
					c := qt.New(t)
					dir := filepath.Join(t.TempDir(), "migrations")
					atlastest.WriteAtlasApplyProjectMigration(c, dir, "1_a.sql", "CREATE TABLE a (id int);\n")
					atlastest.WriteAtlasApplyProjectSum(c, dir)
					dbPath := filepath.Join(t.TempDir(), "apply.db")
					args := append([]string{"migrate", "apply", "--url", "sqlite://" + dbPath, "--dir", "file://" + dir}, output.args...)
					// The separator lets "-1" reach the command as an amount.
					args = append(append(args, "--"), tt.amount...)

					stdout, _, err := atlastest.RunCompat(args...)

					c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(tt.wantErr))
					c.Assert(stdout, qt.Equals, "")
					_, statErr := os.Stat(dbPath)
					c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
				})
			}
		})
	}
}

// TestMigrateApplyAcceptsTheAmountAsTheCommunityBinaryDoes applies one file
// for each spelling of one the pinned binary accepts.
func TestMigrateApplyAcceptsTheAmountAsTheCommunityBinaryDoes(t *testing.T) {
	for _, amount := range []string{"1", "+1", "01"} {
		t.Run(amount, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(t.TempDir(), "migrations")
			atlastest.WriteAtlasApplyProjectMigration(c, dir, "1_a.sql", "CREATE TABLE a (id int);\n")
			atlastest.WriteAtlasApplyProjectMigration(c, dir, "2_b.sql", "CREATE TABLE b (id int);\n")
			atlastest.WriteAtlasApplyProjectSum(c, dir)
			dbPath := filepath.Join(t.TempDir(), "apply.db")

			_, stderr, err := atlastest.RunCompat("migrate", "apply", amount, "--url", "sqlite://"+dbPath, "--dir", "file://"+dir)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(userTables(c, dbPath), qt.DeepEquals, []string{"a"})
		})
	}
}

// TestMigrateApplyNamesTheBaselineByItsSpelling resolves --baseline against
// the spelling of each file's version, as the pinned binary does: a
// golang-migrate `000001_a.up.sql` is named by `000001`, and `1` names
// nothing.
func TestMigrateApplyNamesTheBaselineByItsSpelling(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(t.TempDir(), "migrations")
	writeApplyFiles(c, dir, map[string]string{
		"000001_a.up.sql":   "CREATE TABLE a (id int);\n",
		"000001_a.down.sql": "DROP TABLE a;\n",
		"000002_b.up.sql":   "CREATE TABLE b (id int);\n",
		"000002_b.down.sql": "DROP TABLE b;\n",
	})
	hashConvertedApplyDir(c, dir, "golang-migrate")
	apply := func(dbPath, baseline string) error {
		_, _, err := atlastest.RunCompat("migrate", "apply", "--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir+"?format=golang-migrate", "--baseline", baseline)
		return err
	}
	refused := filepath.Join(t.TempDir(), "refused.db")
	baselined := filepath.Join(t.TempDir(), "baselined.db")

	c.Assert(apply(refused, "1"), qt.ErrorMatches, `baseline version "1" not found`)
	c.Assert(apply(baselined, "000001"), qt.IsNil)
	c.Assert(userTables(c, baselined), qt.DeepEquals, []string{"b"})
}

// TestMigrateApplyRefusesABaselineAnEmptyFlywayDirectoryDoesNotHold refuses
// --baseline on a Flyway directory that holds no migration the import
// executes with the pinned binary's line, whether or not the operand is a
// number.
func TestMigrateApplyRefusesABaselineAnEmptyFlywayDirectoryDoesNotHold(t *testing.T) {
	directories := []struct {
		name  string
		files map[string]string
	}{
		{name: "empty"},
		{name: "undo only", files: map[string]string{"U1__a.sql": "DROP TABLE a;\n"}},
	}
	for _, directory := range directories {
		for _, baseline := range []string{"abc", "1"} {
			t.Run(directory.name+"/"+baseline, func(t *testing.T) {
				c := qt.New(t)
				dir := filepath.Join(t.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
				writeApplyFiles(c, dir, directory.files)
				hashConvertedApplyDir(c, dir, "flyway")

				stdout, _, err := atlastest.RunCompat("migrate", "apply",
					"--url", "sqlite://"+filepath.Join(t.TempDir(), "apply.db"),
					"--dir", "file://"+dir+"?format=flyway",
					"--baseline", baseline)

				c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`baseline version "`+baseline+`" not found`))
				c.Assert(stdout, qt.Equals, "")
			})
		}
	}
}
