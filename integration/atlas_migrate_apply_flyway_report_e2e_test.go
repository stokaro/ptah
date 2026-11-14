//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// flywayReportE2EFile is one entry of `.Pending` in a `migrate apply` report.
type flywayReportE2EFile struct {
	Name        string
	Version     string
	Description string
}

// flywayReportE2EApplied is one entry of `.Applied` in a `migrate apply`
// report.
type flywayReportE2EApplied struct {
	Name        string
	Version     string
	Description string
	Applied     []string
	Error       *struct{ Stmt string }
}

// flywayReportE2E is the part of a `migrate apply --format '{{ json . }}'`
// report these tests read. Current and Target are pointers because the report
// leaves out a version it has none of.
type flywayReportE2E struct {
	Current *string
	Target  *string
	Pending []flywayReportE2EFile
	Applied []flywayReportE2EApplied
}

// writeFlywayReportDirectory writes files, keyed by their path in the
// directory, as a Flyway directory and hashes it with the binary under test.
func writeFlywayReportDirectory(c *qt.C, ctx context.Context, binaryPath string, files map[string]string) string {
	c.Helper()
	migrations := c.TempDir()
	for name, body := range files {
		path := filepath.Join(migrations, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	}
	_, stderr, err := runLintE2EBinary(ctx, binaryPath, "migrate", "hash", "--dir", "file://"+migrations+"?format=flyway")
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	return migrations
}

// readFlywayReportE2E decodes a JSON apply report.
func readFlywayReportE2E(c *qt.C, stdout string) flywayReportE2E {
	c.Helper()
	var report flywayReportE2E
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("stdout: %s", stdout))
	return report
}

// flywayReportFiles projects the applied entries of a report onto the fields
// a pending entry carries, so both lists compare against one expectation.
func flywayReportFiles(applied []flywayReportE2EApplied) []flywayReportE2EFile {
	files := make([]flywayReportE2EFile, 0, len(applied))
	for _, file := range applied {
		files = append(files, flywayReportE2EFile{Name: file.Name, Version: file.Version, Description: file.Description})
	}
	return files
}

// flywayReportStatements lists the statements each applied entry of a report
// executed.
func flywayReportStatements(applied []flywayReportE2EApplied) [][]string {
	statements := make([][]string, 0, len(applied))
	for _, file := range applied {
		statements = append(statements, file.Applied)
	}
	return statements
}

// TestAtlasMigrateApplyFlywayReportE2E drives `ptah-compat migrate apply
// --format '{{ json . }}'` over a Flyway directory on SQLite and reads the
// report back. Every expectation is what the community binary v1.3.0 printed
// for the same directory: each file under the name and description the
// directory gives it rather than the numeric file the run converts it to, the
// exact Flyway token as its version, and each statement a file executed as the
// file spells it, the statement the database refused listed last.
func TestAtlasMigrateApplyFlywayReportE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)

	files := map[string]string{
		"V1__a.sql": "CREATE TABLE a (id int);\n",
		"V1.5__b c.sql": "-- comment above\nCREATE TABLE b (id int);\nINSERT INTO b (id)\n  -- comment inside\n  VALUES (1);\n" +
			"INSERT INTO b VALUES (2)\n",
		"sub/V2__nested.sql": "-- atlas:txmode none\n\nCREATE TABLE n (id int);\nINSERT INTO n VALUES (1);\n",
	}
	everyFile := []flywayReportE2EFile{
		{Name: "V1__a.sql", Version: "1", Description: "a"},
		{Name: "V1.5__b c.sql", Version: "1.5", Description: "b c"},
		{Name: "sub/V2__nested.sql", Version: "2", Description: "nested"},
	}
	everyStatement := [][]string{
		{"CREATE TABLE a (id int);"},
		{"CREATE TABLE b (id int);", "INSERT INTO b (id)\n  -- comment inside\n  VALUES (1);", "INSERT INTO b VALUES (2)"},
		{"CREATE TABLE n (id int);", "INSERT INTO n VALUES (1);"},
	}

	t.Run("a run that applies every file", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeFlywayReportDirectory(c, ctx, binaryPath, files)

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "apply",
			"--dir", "file://"+migrations+"?format=flyway",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
			"--format", "{{ json . }}",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
		report := readFlywayReportE2E(c, stdout)
		c.Assert(report.Pending, qt.DeepEquals, everyFile)
		c.Assert(flywayReportFiles(report.Applied), qt.DeepEquals, everyFile)
		c.Assert(flywayReportStatements(report.Applied), qt.DeepEquals, everyStatement)
		c.Assert(report.Current, qt.IsNil)
		c.Assert(report.Target, qt.DeepEquals, new("2"))
	})

	t.Run("a run that continues from an applied file", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeFlywayReportDirectory(c, ctx, binaryPath, files)
		url := "sqlite://" + filepath.Join(c.TempDir(), "app.db")
		_, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "apply", "--dir", "file://"+migrations+"?format=flyway", "--url", url, "1",
		)
		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "apply",
			"--dir", "file://"+migrations+"?format=flyway",
			"--url", url,
			"--format", "{{ json . }}",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
		report := readFlywayReportE2E(c, stdout)
		c.Assert(report.Pending, qt.DeepEquals, everyFile[1:])
		c.Assert(flywayReportFiles(report.Applied), qt.DeepEquals, everyFile[1:])
		c.Assert(flywayReportStatements(report.Applied), qt.DeepEquals, everyStatement[1:])
		c.Assert(report.Current, qt.DeepEquals, new("1"))
		c.Assert(report.Target, qt.DeepEquals, new("2"))
	})

	for _, test := range []struct {
		name   string
		failed string
		want   []string
	}{
		{
			name:   "a statement the database refuses",
			failed: "CREATE TABLE b (id int);\nINSERT INTO nosuch VALUES (1);\nCREATE TABLE b2 (id int);\n",
			want:   []string{"CREATE TABLE b (id int);", "INSERT INTO nosuch VALUES (1);"},
		},
		{
			name:   "the first statement",
			failed: "INSERT INTO nosuch VALUES (1);\nCREATE TABLE b2 (id int);\n",
			want:   []string{"INSERT INTO nosuch VALUES (1);"},
		},
		{
			name:   "a last statement with no terminator",
			failed: "CREATE TABLE b (id int);\nINSERT INTO nosuch VALUES (1)\n",
			want:   []string{"CREATE TABLE b (id int);", "INSERT INTO nosuch VALUES (1)"},
		},
		{
			name:   "comments above and inside the statement",
			failed: "-- above one\nCREATE TABLE b (id int);\n-- above two\nINSERT INTO nosuch (id)\n  -- inside\n  VALUES (1);\nCREATE TABLE b2 (id int);\n",
			want:   []string{"CREATE TABLE b (id int);", "INSERT INTO nosuch (id)\n  -- inside\n  VALUES (1);"},
		},
		{
			name:   "a statement a declared delimiter ends",
			failed: "-- atlas:delimiter $$\nCREATE TABLE b (id int)$$\nINSERT INTO nosuch VALUES (1)$$\nCREATE TABLE b2 (id int)$$\n",
			want:   []string{"CREATE TABLE b (id int)", "INSERT INTO nosuch VALUES (1)"},
		},
		{
			name:   "a file outside a transaction",
			failed: "-- atlas:txmode none\n\nCREATE TABLE b (id int);\nINSERT INTO nosuch VALUES (1);\nCREATE TABLE b2 (id int);\n",
			want:   []string{"CREATE TABLE b (id int);", "INSERT INTO nosuch VALUES (1);"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrations := writeFlywayReportDirectory(c, ctx, binaryPath, map[string]string{
				"V1__a.sql":   "CREATE TABLE a (id int);\n",
				"V1.5__b.sql": test.failed,
			})

			stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
				"migrate", "apply",
				"--dir", "file://"+migrations+"?format=flyway",
				"--url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
				"--format", "{{ json . }}",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stderr, qt.Equals, "")
			report := readFlywayReportE2E(c, stdout)
			c.Assert(flywayReportFiles(report.Applied), qt.DeepEquals, []flywayReportE2EFile{
				{Name: "V1__a.sql", Version: "1", Description: "a"},
				{Name: "V1.5__b.sql", Version: "1.5", Description: "b"},
			})
			c.Assert(flywayReportStatements(report.Applied), qt.DeepEquals, [][]string{{"CREATE TABLE a (id int);"}, test.want})
			c.Assert(report.Applied[1].Error, qt.IsNotNil)
			c.Assert(report.Applied[1].Error.Stmt, qt.Equals, test.want[len(test.want)-1])
			c.Assert(report.Target, qt.DeepEquals, new("1.5"))
		})
	}
}
