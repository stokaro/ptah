//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// sameNamedTables are plans that create one table and change another with the
// same name in another schema or in another case. The compatibility surface moves
// a constraint the plan adds to a new table into its CREATE TABLE. held and absent
// count the constraint on the table that owns it and on the new table, read from
// the catalog after the apply.
var sameNamedTables = []struct {
	name    string
	engines []uniqueEngine
	current string
	schema  string
	want    string
	held    string
	absent  string
}{
	{
		name:    "another schema",
		engines: []uniqueEngine{postgresUniqueEngine},
		current: `CREATE SCHEMA a; CREATE SCHEMA b;
CREATE TABLE b.t (id int PRIMARY KEY, code int);`,
		schema: `CREATE SCHEMA a; CREATE SCHEMA b;
CREATE TABLE a.t (id int PRIMARY KEY, code int);
CREATE TABLE b.t (id int PRIMARY KEY, code int, CONSTRAINT t_code_key UNIQUE (code));`,
		want:   `ALTER TABLE "b"."t" ADD CONSTRAINT "t_code_key" UNIQUE ("code");`,
		held:   `SELECT count(*) FROM pg_constraint WHERE conrelid = 'b.t'::regclass AND conname = 't_code_key'`,
		absent: `SELECT count(*) FROM pg_constraint WHERE conrelid = 'a.t'::regclass AND conname = 't_code_key'`,
	},
	{
		name:    "another case",
		engines: []uniqueEngine{postgresUniqueEngine},
		current: `CREATE TABLE users (id int PRIMARY KEY, code int);`,
		schema: `CREATE TABLE "Users" (id int PRIMARY KEY, code int);
CREATE TABLE users (id int PRIMARY KEY, code int, CONSTRAINT users_code_key UNIQUE (code));`,
		want:   `ALTER TABLE "users" ADD CONSTRAINT "users_code_key" UNIQUE ("code");`,
		held:   `SELECT count(*) FROM pg_constraint WHERE conrelid = 'users'::regclass AND conname = 'users_code_key'`,
		absent: `SELECT count(*) FROM pg_constraint WHERE conrelid = '"Users"'::regclass AND conname = 'users_code_key'`,
	},
	{
		name:    "another case",
		engines: mysqlFamilyUniqueEngines,
		current: "CREATE TABLE users (id int PRIMARY KEY, code int);",
		schema: "CREATE TABLE `Users` (id int PRIMARY KEY, code int);\n" +
			"CREATE TABLE users (id int PRIMARY KEY, code int, CONSTRAINT users_code_chk CHECK (code > 0));",
		want: "ALTER TABLE `users` ADD CONSTRAINT `users_code_chk` CHECK (code > 0);",
		held: `SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = DATABASE()
			AND BINARY TABLE_NAME = 'users' AND CONSTRAINT_NAME = 'users_code_chk'`,
		absent: `SELECT count(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = DATABASE()
			AND BINARY TABLE_NAME = 'Users' AND CONSTRAINT_NAME = 'users_code_chk'`,
	},
}

// TestCompatPlanKeepsAConstraintOnItsOwnTableE2E applies each plan and reads the
// constraint back. Moved onto the new table of the same name, it lands there with
// no error from the server, the table it belongs to goes without it, and the next
// plan moves it across. So the apply has to leave it on its own table, and the next
// plan has to be synced.
func TestCompatPlanKeepsAConstraintOnItsOwnTableE2E(t *testing.T) {
	for _, test := range sameNamedTables {
		for _, engine := range test.engines {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				work := c.TempDir()
				schema := filepath.Join(work, "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(test.schema+"\n"), 0o600), qt.IsNil)
				target, dev := engine.builtDB(c, test.current), engine.emptyDB(c)
				args := []string{"schema", "apply", "-u", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}

				applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, append(args, "--auto-approve")...)
				again := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, append(args, "--dry-run")...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stdout, qt.Contains, test.want)
				c.Assert(countIn(c, target, test.held), qt.Equals, 1)
				c.Assert(countIn(c, target, test.absent), qt.Equals, 0)
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "synced")
			})
		}
	}
}
