//go:build integration

package atlasschema_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests hold what `ptah migrations up --server-details` shows of a
// statement or a commit PostgreSQL refused, and what the run shows without it.
// Only a live server writes the DETAIL, HINT and position the details carry,
// and only the shipped binary wraps the driver's error in every layer a run
// adds before the details read it back.

// serverDetailsDocument is the run document with server_details kept as bytes,
// so a key the server sent nothing for is seen to be absent.
type serverDetailsDocument struct {
	Failure       *migrator.RunFailure `json:"failure"`
	ServerDetails json.RawMessage      `json:"server_details"`
}

// serverDetails is server_details decoded, with server kept as an object so
// the keys it holds can be listed.
type serverDetails struct {
	File     string          `json:"file"`
	Location json.RawMessage `json:"location"`
	Server   map[string]any  `json:"server"`
}

// runMigrationsUpWithDetails runs `ptah migrations up --json` with args and
// decodes the document.
func runMigrationsUpWithDetails(c *qt.C, address, dir string, args ...string) (clirun.Result, serverDetailsDocument) {
	c.Helper()
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"migrations", "up", "--db-url", address, "--migrations-dir", dir, "--json"}, args...)...)
	var document serverDetailsDocument
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, qt.Commentf("%s\n%s", result.Stdout, result.Stderr))
	return result, document
}

// decodeDetails decodes server_details.
func decodeDetails(c *qt.C, raw json.RawMessage) (serverDetails, map[string]any) {
	c.Helper()
	var details serverDetails
	c.Assert(json.Unmarshal(raw, &details), qt.IsNil, qt.Commentf("%s", raw))
	return details, details.Server
}

// sortedKeys is the keys of an object, sorted.
func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// foreignKeyBodies are two migrations whose second inserts a child row naming a
// parent that does not exist, which PostgreSQL explains only in DETAIL.
func foreignKeyBodies(parents, children string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE %[1]s (id INT PRIMARY KEY);\n"+
			"CREATE TABLE %[2]s (id INT PRIMARY KEY, parent INT REFERENCES %[1]s (id), note VARCHAR(100));\n", parents, children),
		fmt.Sprintf("INSERT INTO %s (id, parent, note) VALUES (1, 4242, '%s');\n", children, failureRowValue),
	}
}

// foreignKeyConstraint reads the name the server gave the children table's
// foreign key, and the schema it is in.
func foreignKeyConstraint(c *qt.C, f *failureTarget, children string) (name, schema string) {
	c.Helper()
	c.Assert(f.conn.QueryRowContext(c.Context(),
		"SELECT c.conname, n.nspname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "+
			"WHERE c.conrelid = $1::regclass AND c.contype = 'f'", children).Scan(&name, &schema), qt.IsNil)
	return name, schema
}

// TestMigrationsUpServerDetailsOfAForeignKeyE2E fails an insert on a foreign key.
// The key the server refused is in DETAIL alone, which the run shows only when
// asked: by default neither the document nor the error line carries it, and the
// failure record is the code-only record it always was.
func TestMigrationsUpServerDetailsOfAForeignKeyE2E(t *testing.T) {
	wantRecord := `{"version":2,"version_key":"2","statement":1,"statements":1,"sqlstate":"23503"}`

	t.Run("by default", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		dir := writeMigrations(c, foreignKeyBodies(f.table("parents"), f.table("children"))...)

		result, document := runMigrationsUpWithDetails(c, f.url, dir)

		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
		failure, err := json.Marshal(document.Failure)
		c.Assert(err, qt.IsNil)
		c.Assert(string(failure), qt.Equals, wantRecord)
		c.Assert(document.ServerDetails, qt.IsNil)
		c.Assert(result.Stdout, qt.Not(qt.Contains), "4242) is not present")
		c.Assert(result.Stderr, qt.Not(qt.Contains), "4242) is not present")
		c.Assert(result.Stderr, qt.Not(qt.Contains), "server details")
	})

	t.Run("with --server-details", func(t *testing.T) {
		c := qt.New(t)
		f := newFailureTarget(c, dbtarget.PostgreSQL)
		parents, children := f.table("parents"), f.table("children")
		dir := writeMigrations(c, foreignKeyBodies(parents, children)...)

		result, document := runMigrationsUpWithDetails(c, f.url, dir, "--server-details")

		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
		failure, err := json.Marshal(document.Failure)
		c.Assert(err, qt.IsNil)
		c.Assert(string(failure), qt.Equals, wantRecord)
		constraint, schema := foreignKeyConstraint(c, f, children)
		detail := fmt.Sprintf(`Key (parent)=(4242) is not present in table "%s".`, parents)
		details, server := decodeDetails(c, document.ServerDetails)
		c.Assert(details.File, qt.Equals, "0000000002_step.up.sql")
		c.Assert(server["detail"], qt.Equals, detail)
		c.Assert(server["sqlstate"], qt.Equals, "23503")
		c.Assert(server["constraint"], qt.Equals, constraint)
		c.Assert(server["table"], qt.Equals, children)
		c.Assert(server["schema"], qt.Equals, schema)
		c.Assert(result.Stderr, qt.Contains, "server details (may contain application data):\n"+
			"  failed at: statement 1 of 1 of migration 2 (0000000002_step.up.sql)\n")
		c.Assert(result.Stderr, qt.Contains, "\n  detail: "+detail+"\n")
	})
}

// TestMigrationsUpServerDetailsOfAHintAndPositionE2E calls a function with an
// argument it does not take. The server answers with a HINT and a position and
// names no object, so no object field enters the details. The position is
// placed in the statement and in the file, where the call is. Whether a DETAIL
// comes too depends on the release: PostgreSQL 19 adds "No function of that
// name accepts the given argument types." and 18 sends none, so the test does
// not pin the whole key set.
func TestMigrationsUpServerDetailsOfAHintAndPositionE2E(t *testing.T) {
	c := qt.New(t)
	f := newFailureTarget(c, dbtarget.PostgreSQL)
	notes := f.table("notes")
	statement := fmt.Sprintf("INSERT INTO %s (id, body)\nVALUES (1, lower(42))", notes)
	dir := writeMigrations(c,
		fmt.Sprintf("-- notes\nCREATE TABLE %s (id INT PRIMARY KEY, body TEXT);\n", notes),
		"-- the call has no matching function\n"+statement+";\n",
	)

	result, document := runMigrationsUpWithDetails(c, f.url, dir, "--server-details")

	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
	details, server := decodeDetails(c, document.ServerDetails)
	for _, object := range []string{"schema", "table", "column", "data_type", "constraint"} {
		c.Assert(server[object], qt.IsNil, qt.Commentf("server detail %q", object))
	}
	c.Assert(server["sqlstate"], qt.Equals, "42883")
	c.Assert(server["hint"], qt.Not(qt.Equals), "")
	c.Assert(server["position"], qt.Equals, float64(strings.Index(statement, "lower(42)")+1))
	c.Assert(compactRecord(c, details.Location), qt.Equals, `{"line":2,"column":12,"file_line":3,"file_column":12}`)
	c.Assert(result.Stderr, qt.Contains, "\n  hint: "+fmt.Sprint(server["hint"])+"\n")
}

// TestMigrationsUpServerDetailsOfADeferredCommitE2E violates a deferred foreign
// key, which PostgreSQL answers at the commit. The details are the commit's:
// they name no statement and no file, and the server's DETAIL is still there.
// Under one transaction for the run the commit is the batch's, which names the
// last migration as the failure record does.
func TestMigrationsUpServerDetailsOfADeferredCommitE2E(t *testing.T) {
	for _, mode := range []string{"file", "all"} {
		t.Run(mode, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, dbtarget.PostgreSQL)
			parents, children := f.table("parents"), f.table("children")
			dir := writeMigrations(c, deferredCommitBodies(parents, children)...)

			result, document := runMigrationsUpWithDetails(c, f.url, dir, "--server-details", "--tx-mode", mode)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
			c.Assert(document.Failure, qt.DeepEquals, &migrator.RunFailure{Version: 2, VersionKey: "2", Statements: 2, Commit: true, SQLState: "23503"})
			var keys map[string]any
			c.Assert(json.Unmarshal(document.ServerDetails, &keys), qt.IsNil)
			c.Assert(sortedKeys(keys), qt.DeepEquals, []string{"server"})
			_, server := decodeDetails(c, document.ServerDetails)
			c.Assert(server["detail"], qt.Equals, fmt.Sprintf(`Key (parent)=(999) is not present in table "%s".`, parents))
			c.Assert(server["sqlstate"], qt.Equals, "23503")
			c.Assert(result.Stderr, qt.Contains, "  failed at: the commit of the transaction that ran migration 2\n")
			c.Assert(result.Stderr, qt.Not(qt.Contains), "position:")
		})
	}
}

// TestMigrationsUpServerDetailsOfALostConnectionE2E loses the connection while a
// statement runs. No server answered it, so the details name the file and
// nothing the server would have said.
func TestMigrationsUpServerDetailsOfALostConnectionE2E(t *testing.T) {
	c := qt.New(t)
	_, address, breaker := connectionBreakerDatabase(c, false, sentAlone("CREATE TABLE lost_table"))
	dir := writeMigrations(c, "CREATE TABLE lost_first (id INT PRIMARY KEY);\nCREATE TABLE lost_table (id INT PRIMARY KEY);\n")

	result, document := runMigrationsUpWithDetails(c, address, dir, "--server-details")

	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
	c.Assert(breaker.broken.Load(), qt.IsTrue)
	c.Assert(compactRecord(c, document.ServerDetails), qt.Equals, `{"file":"0000000001_step.up.sql"}`)
	c.Assert(result.Stderr, qt.Contains, "  failed at: statement 2 of 2 of migration 1 (0000000001_step.up.sql)\n"+
		"  server: no details reported\n")
}

// TestCompatMigrateApplyServerDetailsE2E is the same foreign-key failure through
// `ptah-compat migrate apply`, whose surface takes no new flag: the details are
// shown under PTAH_SERVER_DETAILS and the community binary's error line stands
// alone without it.
func TestCompatMigrateApplyServerDetailsE2E(t *testing.T) {
	tests := []struct {
		name       string
		env        []string
		wantDetail bool
	}{
		{name: "by default"},
		{name: "with PTAH_SERVER_DETAILS", env: []string{"PTAH_SERVER_DETAILS=1"}, wantDetail: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newFailureTarget(c, dbtarget.PostgreSQL)
			parents, children := f.table("parents"), f.table("children")
			dir := c.TempDir()
			for index, body := range foreignKeyBodies(parents, children) {
				name := fmt.Sprintf("2026080100000%d_step.sql", index+1)
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "migrate", "hash", "--dir", "file://"+dir)
			c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))

			result := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(os.Environ(), test.env...)},
				"migrate", "apply", "--url", f.url, "--dir", "file://"+dir)

			c.Assert(result.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", result.Stderr))
			detail := fmt.Sprintf(`  detail: Key (parent)=(4242) is not present in table "%s".`, parents)
			c.Assert(strings.Contains(result.Stderr, detail), qt.Equals, test.wantDetail, qt.Commentf("%s", result.Stderr))
			c.Assert(strings.Contains(result.Stderr, "server details (may contain application data):"), qt.Equals, test.wantDetail)
		})
	}
}
