//go:build integration

package integration_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// liquibaseSelectorRefusal is the explanation every selector refusal carries
// after the changeset and the attribute it names.
const liquibaseSelectorRefusal = "; a migration directory has no equivalent, so importing it would turn " +
	"a conditional history into an unconditional one -- split the changelog or import it by hand\n"

// TestLiquibaseImportRunConditionsE2E_FailurePath drives the shipped binary
// over changelogs that Liquibase runs on one database only, or runs again after
// their first run (stokaro/ptah#3631). Imported without the refusal, each would
// become an unconditional migration that runs on every database what Liquibase
// ran on one. Every row asserts the process refuses, names the attribute, and
// writes nothing.
func TestLiquibaseImportRunConditionsE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	tests := []struct {
		name    string
		file    string
		content string
		args    []string
		stderr  string
	}{
		{
			name:    "changeset dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="mysql"><sql>CREATE TABLE only_on_mysql (id int);</sql></changeSet></databaseChangeLog>`,
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" is conditional on dbms` +
				liquibaseSelectorRefusal,
		},
		{
			name:    "sql change dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sql dbms="mysql">CREATE TABLE only_on_mysql (id int);</sql></changeSet></databaseChangeLog>`,
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" is conditional on <sql> dbms` +
				liquibaseSelectorRefusal,
		},
		{
			name:    "sqlFile change dbms",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s"><sqlFile dbms="mysql" path="t.sql"/></changeSet></databaseChangeLog>`,
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" is conditional on <sqlFile> dbms` +
				liquibaseSelectorRefusal,
		},
		{
			name:    "formatted sql dbms",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE only_on_mysql (id int);\n",
			stderr: `error: parse liquibase source: liquibase changeset s:1 in "changelog.sql" is conditional on dbms` +
				liquibaseSelectorRefusal,
		},
		{
			// The dialect renders typed changes and selects nothing: the
			// changeset the dbms names is refused on that very dialect.
			name:    "changeset dbms with the dialect it names",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" dbms="mysql"><sql>CREATE TABLE only_on_mysql (id int);</sql></changeSet></databaseChangeLog>`,
			args:    []string{"--dialect", "mysql"},
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" is conditional on dbms` +
				liquibaseSelectorRefusal,
		},
		{
			name:    "runAlways",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runAlways="true"><sql>INSERT INTO audit VALUES (1);</sql></changeSet></databaseChangeLog>`,
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.xml" sets runAlways, so ` +
				"Liquibase can run it again on a later update; a Ptah migration runs once, so importing it would " +
				"turn a repeated changeset into a one-time one -- import it by hand\n",
		},
		{
			name: "runOnChange",
			file: "changelog.yaml",
			content: "databaseChangeLog:\n" +
				"  - changeSet: {id: \"1\", author: s, runOnChange: true, changes: [{sql: {sql: \"CREATE VIEW v AS SELECT 1;\"}}]}\n",
			stderr: `error: parse liquibase source: liquibase changeset s_1 in "changelog.yaml" sets runOnChange, so ` +
				"Liquibase can run it again on a later update; a Ptah migration runs once, so importing it would " +
				"turn a repeated changeset into a one-time one -- import it by hand\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeLiquibaseSource(c, work, test.file, test.content)
			args := append([]string{
				"migrations", "import", "--from", "liquibase",
				"--source-dir", "legacy", "--migrations-dir", "migrations",
			}, test.args...)

			stdout, stderr, err := runCLIProcess(ctx, work, binary, args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.stderr)
			_, statErr := os.Stat(filepath.Join(work, "migrations"))
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

// TestLiquibaseImportRunConditionsE2E_HappyPath is the control: the same
// attributes set to false, which is Liquibase's default, import as an ordinary
// migration. Without it, the refusals above could come from refusing the
// attribute names outright.
func TestLiquibaseImportRunConditionsE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), binary)

	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name:    "xml",
			file:    "changelog.xml",
			content: `<databaseChangeLog><changeSet id="1" author="s" runAlways="false" runOnChange="false" dbms=""><sql>CREATE TABLE t (id int);</sql></changeSet></databaseChangeLog>`,
		},
		{
			name:    "formatted sql",
			file:    "changelog.sql",
			content: "--liquibase formatted sql\n--changeset s:1 runAlways:false runOnChange:false\nCREATE TABLE t (id int);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeLiquibaseSource(c, work, test.file, test.content)

			stdout, stderr, err := runCLIProcess(ctx, work, binary,
				"migrations", "import", "--from", "liquibase",
				"--source-dir", "legacy", "--migrations-dir", "migrations",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 0)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(stdout, qt.Contains, "Wrote 2 migration file(s) to migrations\n")
			up, readErr := os.ReadFile(filepath.Join(work, "migrations", "0000000001_s_1.up.sql"))
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(up), qt.Contains, "CREATE TABLE t (id int);")
		})
	}
}

// TestLiquibaseImportRunConditionsCompatE2E_FailurePath covers the
// compatibility import, which reads Liquibase through the same parser. The
// pinned community binary v1.3.0 copies this changelog as it is and applies it
// on SQLite, creating the table the changeset reserves for MySQL; refusing it is
// deliberately stricter.
func TestLiquibaseImportRunConditionsCompatE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binary)
	work := c.TempDir()
	writeLiquibaseSource(c, work, "changelog.sql",
		"--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE only_on_mysql (id int);\n")

	stdout, stderr, err := runCLIProcess(ctx, work, binary,
		"migrate", "import", "--from", "file://legacy", "--to", "file://migrations", "--dir-format", "liquibase",
	)

	c.Assert(exitStatusOf(c, err), qt.Equals, 1)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, `Error: parse liquibase source file changelog.sql: liquibase changeset s:1 in "changelog.sql" `+
		"is conditional on dbms"+liquibaseSelectorRefusal)
	_, statErr := os.Stat(filepath.Join(work, "migrations"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestLiquibaseNumberedRunConditionsCompatE2E_FailurePath covers the
// compatibility surface's one-file conversion, which reads a directory of
// numbered formatted-SQL files by copying each file whole (stokaro/ptah#3713).
// A copy of a changeset Liquibase runs on MySQL alone runs everywhere, and the
// pinned community binary v1.3.0 imports and applies it; the conversion refuses
// it on import and on direct apply, which read the directory the same way.
func TestLiquibaseNumberedRunConditionsCompatE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binary := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binary)
	const refusal = `liquibase changeset s:1 in "1_only_mysql.sql" is conditional on dbms` + liquibaseSelectorRefusal

	tests := []struct {
		name   string
		args   []string
		stderr string
	}{
		{
			name:   "import",
			args:   []string{"migrate", "import", "--from", "file://legacy?format=liquibase", "--to", "file://migrations"},
			stderr: "Error: " + refusal,
		},
		{
			name:   "apply",
			args:   []string{"migrate", "apply", "--dir", "file://legacy?format=liquibase", "--url", "sqlite://apply.db"},
			stderr: "Error: atlas migrate apply --dir: " + refusal,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeLiquibaseSource(c, work, "1_only_mysql.sql",
				"--liquibase formatted sql\n--changeset s:1 dbms:mysql\nCREATE TABLE only_on_mysql (id int);\n")
			_, hashErr, err := runCLIProcess(ctx, work, binary, "migrate", "hash", "--dir", "file://legacy?format=liquibase")
			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate hash: %s", hashErr))

			stdout, stderr, err := runCLIProcess(ctx, work, binary, test.args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.stderr)
			_, statErr := os.Stat(filepath.Join(work, "migrations"))
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
			_, dbErr := os.Stat(filepath.Join(work, "apply.db"))
			c.Assert(dbErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

// writeLiquibaseSource writes one changelog into work/legacy.
func writeLiquibaseSource(c *qt.C, work, name, content string) {
	c.Helper()
	source := filepath.Join(work, "legacy")
	c.Assert(os.MkdirAll(source, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(source, name), []byte(content), 0o600), qt.IsNil)
}
