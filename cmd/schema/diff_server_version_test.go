package schema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema diff` plans against the newest preset of its dialect unless a server
// version is pinned, which is the leak stokaro/ptah#916 describes: PostgreSQL
// 14 through 18 are all supported, and `ALTER COLUMN … SET EXPRESSION` arrived
// in 17. Without a way to say which server the plan is for, a diff for a
// PostgreSQL 16 target emitted SQL that server rejects outright.
//
// The fixture is a changed generated-column expression because that is the
// sharpest capability in the model today: the planner already has both answers
// for it -- the ALTER when the target supports it, and a manual-migration
// warning when it does not -- so the pinned version is the only thing that
// decides which comes out.
const (
	generatedFrom = "CREATE TABLE t (\n  a integer NOT NULL,\n  b integer GENERATED ALWAYS AS (a * 2) STORED\n);\n"
	generatedTo   = "CREATE TABLE t (\n  a integer NOT NULL,\n  b integer GENERATED ALWAYS AS (a * 3) STORED\n);\n"

	alterExpression   = `ALTER TABLE "t" ALTER COLUMN "b" SET EXPRESSION AS (a * 3);`
	manualMigrationOn = "ALTER COLUMN SET EXPRESSION requires PostgreSQL 17+; manual migration required"
)

func TestSchemaDiffServerVersionSelectsThePlan(t *testing.T) {
	tests := []struct {
		name          string
		serverVersion string
		wantContains  string
		wantAbsent    string
	}{
		{
			// Unpinned is what the command did before the flag existed, and it
			// is the unsafe direction: newest assumed.
			name:          "no version plans against the newest preset",
			serverVersion: "",
			wantContains:  alterExpression,
			wantAbsent:    manualMigrationOn,
		},
		{
			name:          "a version below the feature plans the manual migration",
			serverVersion: "16",
			wantContains:  manualMigrationOn,
			wantAbsent:    alterExpression,
		},
		{
			// The other direction: pinning must not simply degrade every plan.
			name:          "a version at the feature plans the ALTER",
			serverVersion: "17",
			wantContains:  alterExpression,
			wantAbsent:    manualMigrationOn,
		},
		{
			name:          "a banner resolves the same way a bare number does",
			serverVersion: "PostgreSQL 16.3 (Debian)",
			wantContains:  manualMigrationOn,
			wantAbsent:    alterExpression,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			fromPath := writeSchemaSQLFile(c, dir, "from.sql", generatedFrom)
			toPath := writeSchemaSQLFile(c, dir, "to.sql", generatedTo)

			args := []string{"diff", "--from", fromPath, "--to", toPath, "--dev-url", "postgres://host/db"}
			args = append(args, serverVersionArgs(tt.serverVersion)...)

			out, err := runSchema("", args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, tt.wantContains)
			c.Assert(out, qt.Not(qt.Contains), tt.wantAbsent)
		})
	}
}

func TestSchemaDiffRefusesAServerVersionNamingNoServer(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", generatedFrom)
	toPath := writeSchemaSQLFile(c, dir, "to.sql", generatedTo)

	// A string a person typed is refused rather than silently answered with
	// the dialect default, which is what capability.ForServerVersion would do
	// and is correct only for a live SELECT version().
	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "postgres://host/db",
		"--server-version", "nonsense",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "invalid server version")
	c.Assert(err.Error(), qt.Contains, `"nonsense" is not a recognized postgres server version`)
}

func TestSchemaDiffSaysWhenAVersionCouldNotRefine(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE t (a integer);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql", "CREATE TABLE t (a integer, b text);\n")

	// ClickHouse has no measured version ladder, so the value is accepted and
	// spends nothing. Saying so is the difference between a flag that did not
	// apply and a flag that was ignored; the diff still comes out.
	out, err := runSchema("", "diff",
		"--from", fromPath,
		"--to", toPath,
		"--dev-url", "clickhouse://host/db",
		"--server-version", "24.3",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "the clickhouse dialect has no measured version ladder")
	c.Assert(out, qt.Contains, "ADD COLUMN b")
}

func TestSchemaDiffUnpinnedVersionIsByteIdenticalToBeforeTheFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := writeSchemaSQLFile(c, dir, "from.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	toPath := writeSchemaSQLFile(c, dir, "to.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	devURL := "sqlite://" + filepath.Join(dir, "dev.db")

	// A resolver that answered an empty version with something other than nil
	// capabilities would change every existing plan, silently.
	unpinned, err := runSchema("", "diff", "--from", fromPath, "--to", toPath, "--dev-url", devURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", unpinned))

	empty, err := runSchema("", "diff",
		"--from", fromPath, "--to", toPath, "--dev-url", devURL, "--server-version", "")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", empty))

	c.Assert(empty, qt.Equals, unpinned)
}

// serverVersionArgs keeps the empty case out of the argv, because passing
// `--server-version ""` and passing nothing are two different inputs and the
// table covers both separately.
func serverVersionArgs(version string) []string {
	if version == "" {
		return nil
	}
	return []string{"--server-version", version}
}
