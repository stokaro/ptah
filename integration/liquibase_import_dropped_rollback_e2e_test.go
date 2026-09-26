//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestLiquibaseImportDroppedRollbackE2E_HappyPath drives the shipped
// ptah-compat over Liquibase changelogs the import splits into one Atlas
// migration per changeset. An Atlas migration holds no rollback, so the import
// names every changeset whose rollback it left out, with its file, and says
// nothing of a changeset whose rollback runs nothing (stokaro/ptah#3753).
// Liquibase 5.0.4 applied each changelog on SQLite, and rollback-count ran
// the rollbacks named here and nothing for the others.
func TestLiquibaseImportDroppedRollbackE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	compat := filepath.Join(c.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), compat)

	tests := []struct {
		name   string
		file   string
		source string
		out    []string
		stderr string
	}{
		{
			name: "formatted sql",
			file: "changelog.sql",
			source: "--liquibase formatted sql\n" +
				"--changeset s:1\nCREATE TABLE accounts (id int);\n--rollback DROP TABLE accounts;\n" +
				"--changeset s:2\nCREATE TABLE audit_log (id int);\n--rollback not required\n" +
				"--changeset s:3\nCREATE TABLE ledgers (id int);\n/* liquibase rollback\nDROP TABLE ledgers;\n*/\n",
			out: []string{"1_s_1.sql", "2_s_2.sql", "3_s_3.sql", "atlas.sum"},
			stderr: "warning: an Atlas migration holds no rollback, so the rollbacks of these 2 changesets were " +
				"not imported:\n  changelog.sql s:1\n  changelog.sql s:3\n",
		},
		{
			name: "xml",
			file: "changelog.xml",
			source: `<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog" ` +
				`xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="` +
				`http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">` +
				`<changeSet id="1" author="s"><sql>CREATE TABLE accounts (id int);</sql>` +
				`<rollback>DROP TABLE accounts;</rollback></changeSet>` +
				`<changeSet id="2" author="s"><sql>CREATE TABLE audit_log (id int);</sql><rollback/></changeSet>` +
				`</databaseChangeLog>`,
			out: []string{"1_s_1.sql", "2_s_2.sql", "atlas.sum"},
			stderr: "warning: an Atlas migration holds no rollback, so the rollback of this changeset was not " +
				"imported:\n  changelog.xml s:1\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			work := c.TempDir()
			writeLiquibaseSource(c, work, test.file, test.source)

			stdout, stderr, err := runCLIProcess(ctx, work, compat,
				"migrate", "import", "--from", "file://legacy?format=liquibase", "--to", "file://out")

			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("migrate import: %s", stderr))
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.stderr)
			c.Assert(readDirectoryNames(c, filepath.Join(work, "out")), qt.DeepEquals, test.out)
		})
	}
}
