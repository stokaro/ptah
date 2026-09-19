//go:build integration

package applylock_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A PostgreSQL-wire URL does not name its product, and the server does. These
// tests hand `ptah schema apply` an address every URL reader calls PostgreSQL,
// pointing at a CockroachDB that has no session advisory lock, and measure what
// an explicit --lock-timeout does with it (stokaro/ptah#3411).
//
// A server decides this, so no unit test can reach it: the dialect comes from
// the banner CockroachDB writes, and nothing offline produces one.
//
// Each test asserts the refusal and reads the database back. The exit code
// alone would pass against a build that refused after applying, which is the
// half of the claim the reporter cares about.

// probeSchema is the whole desired schema these tests apply.
const probeSchema = `CREATE TABLE lock_probe (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
);
`

// probeTable is the table probeSchema declares, read back by name.
const probeTable = "lock_probe"

// refusalMessage is the part of the refusal that carries the finding: the
// dialect named is the one the server reported, not the one the URL spelled.
const refusalMessage = `--lock-timeout requested a schema apply lock, and dialect "cockroachdb" has none`

// ignoredNote is the stderr note an environment-set timeout earns on a target
// that cannot lock.
const ignoredNote = "PTAH_LOCK_TIMEOUT is ignored here"

func TestSchemaApplyLockTimeoutOnPostgresWireTargetE2E_FailurePath(t *testing.T) {
	// A machine exporting the variable would otherwise measure its own
	// environment: a value arriving that way notes and applies, and only a
	// typed flag refuses.
	t.Setenv("PTAH_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_lock_wire_apply")

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, refusalMessage)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.HasLen, 0)
}

func TestSchemaApplySavedPlanLockTimeoutOnPostgresWireTargetE2E_FailurePath(t *testing.T) {
	t.Setenv("PTAH_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_lock_wire_plan")
	planPath := filepath.Join(work, "change.plan.json")

	saved := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "plan",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--save",
		"--output", planPath,
	)
	c.Assert(saved.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", saved.Stdout, saved.Stderr))

	refused := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--plan", planPath,
		"--lock-timeout", "5s",
		"--auto-approve",
	)

	c.Assert(refused.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", refused.Stdout, refused.Stderr))
	c.Assert(refused.Stderr, qt.Contains, refusalMessage)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.HasLen, 0)
}

// TestSchemaApplyWithoutLockTimeoutOnPostgresWireTargetE2E_HappyPath is the
// control. Without it the failure-path tests cannot tell a refusal from a
// target nothing could reach.
func TestSchemaApplyWithoutLockTimeoutOnPostgresWireTargetE2E_HappyPath(t *testing.T) {
	t.Setenv("PTAH_LOCK_TIMEOUT", "")
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_lock_wire_control")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Not(qt.Contains), refusalMessage)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.DeepEquals, []string{probeTable})
}

// TestSchemaApplyEnvironmentLockTimeoutOnPostgresWireTargetE2E_HappyPath keeps
// the environment spelling on the side the page promises: the variable also
// fills --lock-timeout on the versioned commands, so a value arriving that way
// says nothing about this apply and gets a note rather than a refusal.
func TestSchemaApplyEnvironmentLockTimeoutOnPostgresWireTargetE2E_HappyPath(t *testing.T) {
	t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	dbURL := emptyTargetDatabase(c, "ptah_lock_wire_env")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", dbURL,
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(strings.Count(applied.Stderr, ignoredNote), qt.Equals, 1)
	c.Assert(tableNames(c, c.Context(), dbURL), qt.DeepEquals, []string{probeTable})
}

// TestSchemaApplyEnvironmentLockTimeoutOnAliasTargetE2E_HappyPath is the other
// half of the pair. Here the URL names the product, so the URL and the server
// answer alike and the operator is owed one note for one ignored value; a
// second note would mean the two decisions both spoke.
func TestSchemaApplyEnvironmentLockTimeoutOnAliasTargetE2E_HappyPath(t *testing.T) {
	t.Setenv("PTAH_LOCK_TIMEOUT", "5s")
	c := qt.New(t)
	work := c.TempDir()
	schemaPath := writeProbeSchema(c, work)
	wireURL := emptyTargetDatabase(c, "ptah_lock_alias_env")
	aliasURL := productNamingURL(c, wireURL)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: work},
		"schema", "apply",
		"--db-url", aliasURL,
		"--schema-file", schemaPath,
		"--auto-approve",
	)

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(strings.Count(applied.Stderr, ignoredNote), qt.Equals, 1)
	c.Assert(tableNames(c, c.Context(), wireURL), qt.DeepEquals, []string{probeTable})
}

// writeProbeSchema puts the desired schema where the command can read it and
// returns the path.
func writeProbeSchema(c *qt.C, dir string) string {
	c.Helper()

	path := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(path, []byte(probeSchema), 0o600), qt.IsNil)
	return path
}

// emptyTargetDatabase creates a database of its own on the live CockroachDB and
// returns its PostgreSQL-wire URL.
//
// The database is the test's own because `schema apply` is declarative: pointed
// at a database somebody else is using, it plans a DROP for every table this
// schema does not declare.
//
// The address comes from [dbtarget.DriverDSN], which owns the wire spelling of
// a CockroachDB address -- it rewrites a `cockroachdb://` alias to the scheme
// pgx reads, and the result is also a URL ptah connects with. That is the shape
// these tests are about: an address that says PostgreSQL to everything reading
// the string, in front of a server that is not PostgreSQL.
func emptyTargetDatabase(c *qt.C, name string) string {
	c.Helper()

	serverURL := dbtarget.DriverDSN(c, dbtarget.CockroachDB)

	// The premise. Where the URL's own dialect has no apply lock, the decision
	// taken from --db-url refuses on its own and these tests measure nothing
	// about what the server said.
	urlDialect, err := atlasurl.DialectFromURL(serverURL)
	c.Assert(err, qt.IsNil)
	c.Assert(atlasschema.ApplyLockSupported(urlDialect), qt.IsTrue,
		qt.Commentf("the wire URL resolves to dialect %q, which must be one that locks", urlDialect))

	execOnServer(c, c.Context(), serverURL, "DROP DATABASE IF EXISTS "+name+" CASCADE")
	execOnServer(c, c.Context(), serverURL, "CREATE DATABASE "+name)
	c.Cleanup(func() {
		// t.Context is canceled before cleanup runs, so the drop carries its
		// own bounded context.
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		execOnServer(c, ctx, serverURL, "DROP DATABASE IF EXISTS "+name+" CASCADE")
	})

	parsed, err := url.Parse(serverURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// productNamingURL rewrites a wire address to the scheme that names the
// product, and asserts that the rewrite did name it.
func productNamingURL(c *qt.C, wireURL string) string {
	c.Helper()

	parsed, err := url.Parse(wireURL)
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "cockroachdb"
	aliasURL := parsed.String()

	urlDialect, err := atlasurl.DialectFromURL(aliasURL)
	c.Assert(err, qt.IsNil)
	c.Assert(urlDialect, qt.Equals, "cockroachdb")
	return aliasURL
}

// execOnServer runs one statement through a driver of its own, so the setup and
// the read-back never pass through the reader the command used.
func execOnServer(c *qt.C, ctx context.Context, dsn, statement string) {
	c.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, statement)
	c.Assert(err, qt.IsNil)
}

// tableNames lists what the target database holds, sorted, so an empty answer
// and a one-table answer are both assertable values.
func tableNames(c *qt.C, ctx context.Context, dsn string) []string {
	c.Helper()

	conn, err := pgx.Connect(ctx, dsn)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx,
		`SELECT table_name FROM information_schema.tables `+
			`WHERE table_schema = 'public' ORDER BY table_name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	names := []string{}
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
