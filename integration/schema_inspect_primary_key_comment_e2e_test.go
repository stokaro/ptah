//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// SQL inspection must describe a key that can be applied without replacing
// the stored key or clearing its comment. A replay into an empty database
// must also preserve the key and its block-size hint.
func TestSchemaInspectPreservesPrimaryKeyCommentE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		for _, surface := range []struct {
			name   string
			target clirun.Target
			flag   string
			format string
		}{
			{name: "native", target: clirun.Ptah, flag: "--db-url", format: "sql"},
			{name: "compat", target: clirun.Compat, flag: "--url", format: "{{ sql . }}"},
		} {
			for _, key := range []struct {
				name    string
				columns string
				options string
			}{
				{name: "comment alone", columns: "id"},
				{name: "comment and block size", columns: "id", options: " KEY_BLOCK_SIZE=8"},
				{name: "composite key", columns: "id, tenant_id", options: " KEY_BLOCK_SIZE=8"},
			} {
				t.Run(engine.name+"/"+surface.name+"/"+key.name, func(t *testing.T) {
					c := qt.New(t)
					scratch := newMySQLFamilyScratch(c, engine.engine)
					schema := fmt.Sprintf("CREATE TABLE bt (id bigint NOT NULL, tenant_id bigint NOT NULL, email varchar(255) NOT NULL, PRIMARY KEY (%s) COMMENT 'account identity'%s, KEY accounts_email (email) COMMENT 'lookup by email' KEY_BLOCK_SIZE=8) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;", key.columns, key.options)
					name, url := scratch.builtFrom(c, schema)
					before := primaryKeyIndexComment(c, scratch, name)
					c.Assert(before, qt.Equals, "account identity")
					dir := c.TempDir()
					opts := clirun.Options{Dir: dir}

					inspected := clirun.Run(c, surface.target, opts, "schema", "inspect", surface.flag, url, "--format", surface.format)

					c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("%s", inspected.Stderr))
					c.Assert(inspected.Stdout, qt.Contains, "COMMENT 'account identity'")
					c.Assert(inspected.Stdout, qt.Contains, "COMMENT 'lookup by email'")
					file := writeKeyFile(c, inspected.Stdout)
					compared := clirun.Run(c, clirun.Ptah, opts, "schema", "compare", "--db-url", url, "--schema-file", file, "--exit-code")
					c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", compared.Stdout, compared.Stderr))
					planned := clirun.Run(c, clirun.Ptah, opts, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")
					c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("%s", planned.Stderr))
					c.Assert(planned.Stdout, qt.Contains, "Schema is synced")
					applied := clirun.Run(c, clirun.Ptah, opts, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")
					c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", applied.Stderr))
					c.Assert(primaryKeyIndexComment(c, scratch, name), qt.Equals, before)

					copyName, copyURL := scratch.database(c, "inspect_key_copy")
					replayed := clirun.Run(c, clirun.Ptah, opts, "schema", "apply", "--db-url", copyURL, "--schema-file", file, "--auto-approve")
					c.Assert(replayed.ExitCode, qt.Equals, 0, qt.Commentf("%s", replayed.Stderr))
					c.Assert(primaryKeyIndexComment(c, scratch, copyName), qt.Equals, before)
					c.Assert(blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, copyName)), qt.Equals,
						blockSizeDDL(c, mySQLDSNForDatabase(c, scratch.adminDSN, name)),
					)
				})
			}
		}
	}
}

func primaryKeyIndexComment(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var comment string
	c.Assert(scratch.admin.QueryRowContext(c.Context(),
		"SELECT INDEX_COMMENT FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? "+
			"AND TABLE_NAME = 'bt' AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX LIMIT 1", database,
	).Scan(&comment), qt.IsNil)
	return comment
}
