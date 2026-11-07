//go:build integration

package routinesettings_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/dbtarget"
)

// noChanges and changes are the outcomes `ptah schema plan --json` reports.
const (
	noChanges = "no-changes"
	changes   = "changes"
)

// scratchDatabase creates a database of its own on the engine's server, runs the
// statements on it with nothing of Ptah's in between, and returns the URL Ptah
// connects to it with. The database is dropped when the test ends.
func scratchDatabase(c *qt.C, engine dbtarget.Engine, statements ...string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_routine_settings_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	target := parsed.String()

	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return target
}

// catalogSettings reads what the server stores for the routines in the public
// schema from pg_proc.proconfig, one line per routine, each `name: a | b`. The
// entries are sorted, since the catalog keeps them in the order the statement
// wrote them and that order is not a property of the routine.
func catalogSettings(c *qt.C, target string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var settings string
	err = conn.QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(
			p.proname || ': ' || array_to_string(
				ARRAY(SELECT entry FROM unnest(p.proconfig) AS entry ORDER BY entry COLLATE "C"), ' | '),
			E'\n' ORDER BY p.proname COLLATE "C"), '')
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'`).Scan(&settings)
	c.Assert(err, qt.IsNil)
	return settings
}

// writeFile writes content as name in dir and returns its path.
func writeFile(c *qt.C, dir, name, content string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// run executes cmd with args and returns what it wrote to standard output.
func run(c *qt.C, cmd *cobra.Command, args ...string) string {
	c.Helper()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(c.Context())
	c.Assert(err, qt.IsNil, qt.Commentf("%v\nstdout:\n%s\nstderr:\n%s", args, out.String(), errOut.String()))
	return out.String()
}

// runPtah runs one native command.
func runPtah(c *qt.C, args ...string) string {
	c.Helper()
	return run(c, root.NewRootCommand(), args...)
}

// runCompat runs one `ptah-compat` command.
func runCompat(c *qt.C, args ...string) string {
	c.Helper()
	return run(c, atlas.NewCompatCommand("atlas"), args...)
}

// applySchema applies the desired schema the source flags name to target.
func applySchema(c *qt.C, target string, source ...string) {
	c.Helper()
	runPtah(c, slices.Concat([]string{"schema", "apply", "--db-url", target, "--auto-approve"}, source)...)
}

// planOutcome plans from target to the desired schema the source flags name and
// returns the outcome the plan reports.
func planOutcome(c *qt.C, target string, source ...string) string {
	c.Helper()
	out := runPtah(c, slices.Concat([]string{"schema", "plan", "--db-url", target, "--dry-run", "--json"}, source)...)
	var report struct {
		Outcome string `json:"outcome"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	return report.Outcome
}
