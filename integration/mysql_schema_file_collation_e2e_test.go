//go:build integration

package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// schemaFileCollationHCL is a one-table HCL document whose table names collate
// as its default collation.
func schemaFileCollationHCL(collate string) string {
	return `schema "app" {}
table "t" {
  schema  = schema.app
  charset = "utf8mb4"
  collate = "` + collate + `"
  column "id" {
    type = int
  }
  column "n" {
    type = varchar(20)
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`
}

// schemaFileCollationRows are two schema files that differ only in a
// collation a server decides: the table's default collation, which the
// column n takes, written in SQL and in HCL, the same with three columns
// whose names sort in another order than the table holds them, and the
// character set of a column that names no collation. community is the plan
// the pinned community binary prints, by engine, and compat the plan
// ptah-compat prints; both name the same change, and the binary spells the
// column's collation where ptah-compat spells its character set.
var schemaFileCollationRows = []struct {
	name      string
	file      string
	from, to  string
	community map[string][]string
	compat    []string
}{
	{
		name: "a table collation in SQL",
		file: "schema.sql",
		from: "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n",
		to:   "CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n",
		community: map[string][]string{
			"MySQL":   {"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
			"MariaDB": {"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
		},
		compat: []string{"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
	},
	{
		name: "a table collation in HCL",
		file: "schema.hcl",
		from: schemaFileCollationHCL("utf8mb4_bin"),
		to:   schemaFileCollationHCL("utf8mb4_general_ci"),
		community: map[string][]string{
			"MySQL":   {"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
			"MariaDB": {"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
		},
		compat: []string{"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
	},
	{
		name: "columns in the order the table holds them",
		file: "schema.sql",
		from: "CREATE TABLE t (id int PRIMARY KEY, zeta varchar(20) NULL, alpha varchar(20) NULL, mid varchar(10) NULL) " +
			"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n",
		to: "CREATE TABLE t (id int PRIMARY KEY, zeta varchar(20) NULL, alpha varchar(20) NULL, mid varchar(10) NULL) " +
			"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n",
		community: map[string][]string{
			"MySQL":   {heldOrderPlan},
			"MariaDB": {heldOrderPlan},
		},
		compat: []string{heldOrderPlan},
	},
	{
		name: "a column character set",
		file: "schema.sql",
		from: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET latin1 NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n",
		to:   "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET utf8mb4 NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n",
		community: map[string][]string{
			"MySQL":   {"ALTER TABLE `t` MODIFY COLUMN `c` varchar(20) NULL COLLATE utf8mb4_0900_ai_ci;"},
			"MariaDB": {"ALTER TABLE `t` MODIFY COLUMN `c` varchar(20) NULL COLLATE utf8mb4_uca1400_ai_ci;"},
		},
		compat: []string{"ALTER TABLE `t` MODIFY COLUMN `c` varchar(20) CHARACTER SET utf8mb4 NULL;"},
	},
}

// heldOrderPlan is the plan of a changed table collation that resets three
// columns, which writes their MODIFY COLUMN clauses in the order the table
// holds them rather than in name order.
const heldOrderPlan = "ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `zeta` varchar(20) NULL, " +
	"MODIFY COLUMN `alpha` varchar(20) NULL, MODIFY COLUMN `mid` varchar(10) NULL;"

// writeSchemaFiles writes from and to as two files named name in their own
// directories, and returns their file:// addresses.
func writeSchemaFiles(c *qt.C, name, from, to string) (fromURL, toURL string) {
	c.Helper()
	write := func(content string) string {
		path := filepath.Join(c.TempDir(), name)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
		return "file://" + filepath.ToSlash(path)
	}
	return write(from), write(to)
}

// TestCompatSchemaDiffComparesMySQLCollationsOfTwoFilesE2E diffs two schema
// files against a dev database. ptah-compat predicts what the dev server
// gives each table and column that names no collation, as a read of the
// tables built from the --from file would report it, so it plans the change
// the pinned community binary plans.
func TestCompatSchemaDiffComparesMySQLCollationsOfTwoFilesE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		for _, row := range schemaFileCollationRows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, devURL := scratch.database(c, "collation_dev")
				from, to := writeSchemaFiles(c, row.file, row.from, row.to)

				diffed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()},
					"schema", "diff", "--from", from, "--to", to, "--dev-url", devURL)

				c.Assert(diffed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed.Stdout, diffed.Stderr))
				c.Assert(plannedAlters(diffed.Stdout, "app"), qt.DeepEquals, row.compat, qt.Commentf("%s", diffed.Stdout))
			})
		}
	}
}

// TestSchemaDiffComparesMySQLCollationsOfTwoFilesOnTheCommunityBinaryE2E holds
// the measurement ptah-compat is compared with: the pinned community binary,
// on the same files and dev databases.
func TestSchemaDiffComparesMySQLCollationsOfTwoFilesOnTheCommunityBinaryE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, engine := range inheritedCollationEngines {
		for _, row := range schemaFileCollationRows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, devURL := scratch.database(c, "collation_dev")
				from, to := writeSchemaFiles(c, row.file, row.from, row.to)

				output, err := exec.CommandContext(c.Context(), reference, "schema", "diff", // #nosec G204 G702 -- operator-provided pinned reference binary
					"--from", from, "--to", to, "--dev-url", referenceMySQLURL(c, devURL)).CombinedOutput()

				c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
				c.Assert(plannedAlters(string(output), "app"), qt.DeepEquals, row.community[engine.name], qt.Commentf("%s", output))
			})
		}
	}
}

// TestSchemaDiffOfAMySQLFileWithItselfE2E diffs each file above with itself,
// on ptah-compat, the native binary and the pinned community binary. The
// prediction of the --from side and the resolution of the --to side read the
// same server, so a file is synced with itself.
func TestSchemaDiffOfAMySQLFileWithItselfE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	for _, engine := range inheritedCollationEngines {
		for _, row := range schemaFileCollationRows {
			t.Run(engine.name+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, devURL := scratch.database(c, "collation_dev")
				from, to := writeSchemaFiles(c, row.file, row.from, row.from)
				args := []string{"schema", "diff", "--from", from, "--to", to}

				compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append(args, "--dev-url", devURL)...)
				native := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append(args, "--dev-url", devURL)...)
				community, err := exec.CommandContext(c.Context(), reference, append(args, "--dev-url", referenceMySQLURL(c, devURL))...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary

				c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
				c.Assert(compat.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
				c.Assert(native.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", native.Stdout, native.Stderr))
				c.Assert(native.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
				c.Assert(string(community), qt.Equals, "Schemas are synced, no changes to be made.\n")
			})
		}
	}
}

// TestSchemaDiffReadsTheDevSessionsDefaultOfUTF8MB4E2E diffs a MySQL table
// that names utf8mb4 and no collation with one that names
// utf8mb4_general_ci. The --from table takes the dev session's default
// collation of utf8mb4: utf8mb4_0900_ai_ci, which differs, or
// utf8mb4_general_ci where the dev URL sets default_collation_for_utf8mb4,
// which is the same table. Each binary answers both the way the community
// binary does.
func TestSchemaDiffReadsTheDevSessionsDefaultOfUTF8MB4E2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name   string
		query  string
		native []string
		compat []string
	}{
		{
			name:   "the server default",
			native: []string{"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20);"},
			compat: []string{"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20) NULL;"},
		},
		{name: "a session default", query: "?default_collation_for_utf8mb4=utf8mb4_general_ci"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, inheritedCollationEngines[0].engine)
			_, devURL := scratch.database(c, "collation_dev")
			from, to := writeSchemaFiles(c, "schema.sql",
				"CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4;\n",
				"CREATE TABLE t (id int PRIMARY KEY, n varchar(20) NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;\n")
			args := []string{"schema", "diff", "--from", from, "--to", to, "--dev-url"}

			native := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, append(args, devURL+test.query)...)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, append(args, devURL+test.query)...)
			community, err := exec.CommandContext(c.Context(), reference, append(args, referenceMySQLURL(c, devURL)+test.query)...).CombinedOutput() // #nosec G204 G702 -- operator-provided pinned reference binary

			c.Assert(native.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", native.Stdout, native.Stderr))
			c.Assert(plannedAlters(native.Stdout, ""), qt.DeepEquals, test.native, qt.Commentf("%s", native.Stdout))
			c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(plannedAlters(compat.Stdout, ""), qt.DeepEquals, test.compat, qt.Commentf("%s", compat.Stdout))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", community))
			c.Assert(plannedAlters(string(community), ""), qt.DeepEquals, test.compat, qt.Commentf("%s", community))
		})
	}
}

// TestSchemaApplyKeepsABinaryCharacterSetColumnE2E applies columns of the
// binary character set, which MySQL and MariaDB store as binary types:
// varchar(10) as varbinary(10), char(4) as binary(4), and text as blob. The
// comparison accepts the stored type, so the next plan is empty, and a
// column of another length is still a change.
func TestSchemaApplyKeepsABinaryCharacterSetColumnE2E(t *testing.T) {
	for _, engine := range inheritedCollationEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, url := scratch.database(c, "binary_charset")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			declare := func(length string) {
				c.Assert(os.WriteFile(schema, []byte("CREATE TABLE b (id int PRIMARY KEY, v varchar("+length+") CHARACTER SET binary NULL, "+
					"c char(4) CHARACTER SET binary NULL, t text CHARACTER SET binary NULL) DEFAULT CHARSET=utf8mb4;\n"), 0o600), qt.IsNil)
			}
			args := []string{"schema", "apply", "--db-url", url, "--schema-file", schema}
			declare("10")

			applied := runPtahNative(c, append(args, "--auto-approve")...)

			c.Assert(scratch.columnTypesOfB(c, database), qt.Equals, "c binary(4), t blob, v varbinary(10)", qt.Commentf("%s", applied))
			c.Assert(runPtahNative(c, append(args, "--dry-run")...), qt.Equals, "Schema is synced, no changes to be made.\n")
			declare("20")
			c.Assert(plannedAlters(runPtahNative(c, append(args, "--dry-run")...), database), qt.DeepEquals,
				[]string{"ALTER TABLE `b` MODIFY COLUMN `v` varchar(20) CHARACTER SET binary;"})
		})
	}
}

// columnTypesOfB answers the columns of table b in database other than id,
// whose display width differs between the engines, as `name type`, in name
// order.
func (s mysqlScratch) columnTypesOfB(c *qt.C, database string) string {
	c.Helper()
	var columns string
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT GROUP_CONCAT(CONCAT(COLUMN_NAME, ' ', COLUMN_TYPE) ORDER BY COLUMN_NAME SEPARATOR ', ')
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'b' AND COLUMN_NAME <> 'id'`, database).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns
}
