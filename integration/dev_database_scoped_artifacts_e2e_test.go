//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// A dev database may hold database-scoped objects a realm cleanup cannot
// remove: the Supabase image ships six event triggers no extension owns and
// the publication supabase_realtime. The pinned community binary v1.3.0 uses
// such a dev database and leaves them, measured on PostgreSQL 18 with an event
// trigger and a publication and `?search_path=public`. Ptah's realm cleanup
// refused it: `refusing to clean PostgreSQL database realm with unsupported
// database-scoped event trigger "watch_ddl"` (stokaro/ptah#4055). Each test
// reads the artifacts back afterwards.

// artifactsDDL is the dev database's environment: an event trigger whose
// function is in a schema of its own, and an empty publication.
const artifactsDDL = `CREATE SCHEMA ext;
CREATE FUNCTION ext.noop_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;
CREATE EVENT TRIGGER watch_ddl ON ddl_command_end EXECUTE FUNCTION ext.noop_ddl();
CREATE PUBLICATION realtime_pub;
`

// artifactsDev returns the URL of a scratch dev database holding
// [artifactsDDL], pinned to public, and a connection to it.
func artifactsDev(c *qt.C) (string, *sql.DB) {
	c.Helper()
	devURL := postgresScratchDevURL(c, "ptah_artifacts_dev")
	db, err := sql.Open("pgx", devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	_, err = db.ExecContext(c.Context(), artifactsDDL)
	c.Assert(err, qt.IsNil)
	return pinnedDevURL(c, devURL, "public"), db
}

// heldDatabaseArtifacts lists the event triggers and publications of db.
func heldDatabaseArtifacts(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT 'event trigger ' || evtname FROM pg_event_trigger
		UNION ALL
		SELECT 'publication ' || pubname FROM pg_publication
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var held []string
	for rows.Next() {
		var artifact string
		c.Assert(rows.Scan(&artifact), qt.IsNil)
		held = append(held, artifact)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// wantArtifacts are what [artifactsDDL] leaves.
var wantArtifacts = []string{"event trigger watch_ddl", "publication realtime_pub"}

// TestDevDatabaseArtifactsE2E_VerbsKeepThem runs the verbs that claim and
// reset a dev database against one holding the artifacts. Each succeeds, and
// the event trigger and the publication are still there.
func TestDevDatabaseArtifactsE2E_VerbsKeepThem(t *testing.T) {
	verbs := []struct {
		name string
		// run is the binary the row drives, in process.
		run  func(args ...string) (string, error)
		args []string
	}{
		{name: "ptah-compat schema inspect", run: runCompatVerb,
			args: []string{"schema", "inspect", "-u", "{schema}", "--dev-url", "{dev}"}},
		{name: "ptah-compat migrate diff", run: runCompatVerb,
			args: []string{"migrate", "diff", "x", "--dir", "{out}", "--to", "{schema}", "--dev-url", "{dev}"}},
		{name: "ptah-compat migrate validate", run: runCompatVerb,
			args: []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"}},
		{name: "ptah-compat schema apply", run: runCompatVerb,
			args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"}},
		{name: "ptah migrations validate", run: runPtahNativeWithError,
			args: []string{"migrations", "validate", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
	}

	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			dev, db := artifactsDev(c)
			target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			schemaFile, dir := writeDevIdentitySources(c)
			out := filepath.Join(c.TempDir(), "out")
			c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)

			output, err := verb.run(devNotCleanArgs(verb.args, dev, target.url, schemaFile, dir, out)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(heldDatabaseArtifacts(c, db), qt.DeepEquals, wantArtifacts)
		})
	}
}

// TestDevDatabaseArtifactsE2E_MigrateDiffTwiceIsInSync is the report's
// workflow: `migrate diff` writes the directory, and a second run against the
// same dev database finds it in sync.
func TestDevDatabaseArtifactsE2E_MigrateDiffTwiceIsInSync(t *testing.T) {
	c := qt.New(t)
	dev, db := artifactsDev(c)
	schemaFile, _ := writeDevIdentitySources(c)
	out := filepath.Join(c.TempDir(), "out")
	c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)
	diff := func(name string) []string {
		return []string{"migrate", "diff", name, "--dir", "file://" + filepath.ToSlash(out),
			"--to", "file://" + filepath.ToSlash(schemaFile), "--dev-url", dev}
	}

	first, err := runCompatVerb(diff("init")...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", first))
	second, err := runCompatVerb(diff("second")...)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", second))
	c.Assert(second, qt.Contains, "The migration directory is synced with the desired state")
	c.Assert(heldDatabaseArtifacts(c, db), qt.DeepEquals, wantArtifacts)
}

// TestDevDatabaseArtifactsE2E_ARunCannotAddOne is the control: what the claim
// keeps is what was there. A migration that creates an event trigger is
// refused before it runs, since the cleanup could not take it away, and the
// dev database holds what it held.
func TestDevDatabaseArtifactsE2E_ARunCannotAddOne(t *testing.T) {
	c := qt.New(t)
	dev, db := artifactsDev(c)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_audit.sql"), []byte(
		"CREATE EVENT TRIGGER audit_ddl ON ddl_command_end EXECUTE FUNCTION ext.noop_ddl();\n"), 0o600), qt.IsNil)
	output, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))

	output, err = runCompatVerb("migrate", "validate", "--dir", "file://"+filepath.ToSlash(dir), "--dev-url", dev)

	c.Assert(err, qt.ErrorMatches, `(?s).*EVENT TRIGGER.*`, qt.Commentf("%s", output))
	c.Assert(heldDatabaseArtifacts(c, db), qt.DeepEquals, wantArtifacts)
}
