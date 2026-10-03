//go:build integration

package integration_test

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// A server's default user database, PostgreSQL's postgres, is where work that
// is not the run's tends to live on a server the operator named, so a dev
// database cleanup refuses it there. On a server the run owns -- one a
// docker:// dev URL started, or one the operator declared disposable -- it
// holds nothing else, and an image that keeps its own objects there, such as
// Supabase's, has no other database to offer (stokaro/ptah#4035). Each test
// below starts its own server, so no test cleans the postgres database of the
// shared one.

// devDefaultDatabaseDockerURL names the postgres database of a server the
// provisioner starts.
const devDefaultDatabaseDockerURL = "docker://postgres/16-alpine/postgres"

// provisionDevDefaultDatabase starts a server through the provisioner a
// docker:// dev URL uses, and returns the URL of its postgres database. The
// provisioner records the server as the run's own, as a command's own
// provisioning records it, and the server is removed when the test ends.
func provisionDevDefaultDatabase(c *qt.C) string {
	c.Helper()
	resolved, release, err := devdocker.Resolve(c.Context(), devDefaultDatabaseDockerURL, devdocker.Options{})
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	return resolved
}

// namedSpelling returns rawURL with a query parameter added. It reaches the
// same database, in a spelling the provisioner never recorded, so a command
// reads it as a server the operator named.
func namedSpelling(c *qt.C, rawURL string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("application_name", "ptah_named_spelling")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// userTables counts the tables in the user schemas of the database rawURL
// names.
func userTables(c *qt.C, rawURL string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), rawURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(), `
		SELECT count(*) FROM pg_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`).Scan(&tables), qt.IsNil)
	return tables
}

// devDefaultDatabaseVerbs are the commands that reset a dev database, on both
// binaries, spelled with a run's databases and files.
var devDefaultDatabaseVerbs = []struct {
	name string
	// run is the binary the row drives, in process.
	run  func(args ...string) (string, error)
	args []string
}{
	{name: "migrate diff", run: runCompatVerb, args: []string{"migrate", "diff", "x", "--dir", "{out}", "--to", "{schema}", "--dev-url", "{dev}"}},
	{name: "migrate lint", run: runCompatVerb, args: []string{"migrate", "lint", "--dir", "{dir}", "--dev-url", "{dev}", "--latest", "1"}},
	{name: "migrate validate", run: runCompatVerb, args: []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"}},
	{name: "schema inspect", run: runCompatVerb, args: []string{"schema", "inspect", "-u", "{schema}", "--dev-url", "{dev}"}},
	{name: "schema diff", run: runCompatVerb, args: []string{"schema", "diff", "--from", "{schema}", "--to", "{target}", "--dev-url", "{dev}"}},
	{name: "schema apply", run: runCompatVerb, args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"}},
	{name: "ptah migrations validate", run: runPtahNativeWithError, args: []string{"migrations", "validate", "--dir", "{rawdir}", "--dir-format", "atlas", "--dev-url", "{dev}"}},
	{name: "ptah migrations lint", run: runPtahNativeWithError, args: []string{"migrations", "lint", "--dir", "{rawdir}", "--dir-format", "atlas", "--dev-url", "{dev}"}},
	{name: "ptah schema apply", run: runPtahNativeWithError, args: []string{"schema", "apply", "--db-url", "{target}", "--schema-file", "{rawschema}", "--dev-url", "{dev}", "--auto-approve"}},
}

// TestDevVerbsCleanTheDefaultDatabaseOfAProvisionedServerE2E runs every
// command that resets a dev database on the postgres database of a server the
// provisioner started. Each command resets it before and after its run, so it
// succeeds and leaves no table behind.
func TestDevVerbsCleanTheDefaultDatabaseOfAProvisionedServerE2E(t *testing.T) {
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	dev := provisionDevDefaultDatabase(qt.New(t))
	for _, verb := range devDefaultDatabaseVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
			schemaFile, dir := writeDevIdentitySources(c)
			out := filepath.Join(c.TempDir(), "out")
			c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)

			output, err := verb.run(devNotCleanArgs(verb.args, dev, target.url, schemaFile, dir, out)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(userTables(c, dev), qt.Equals, 0)
		})
	}
}

// TestDevDockerURLNamingTheDefaultDatabaseE2E is the URL an operator writes:
// the command starts the server itself, replays the directory on its postgres
// database, and removes it.
func TestDevDockerURLNamingTheDefaultDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	_, dir := writeDevIdentitySources(c)

	output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devDefaultDatabaseDockerURL)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
}

// TestDevDefaultDatabaseOfANamedServerIsRefusedE2E names the same postgres
// database in a spelling nothing recorded, without the declaration. The reset
// refuses it before it changes anything, and says how to make the server the
// run's own, on both binaries.
func TestDevDefaultDatabaseOfANamedServerIsRefusedE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	dev := namedSpelling(c, provisionDevDefaultDatabase(c))
	_, dir := writeDevIdentitySources(c)

	_, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", dev)
	native, nativeErr := runPtahNativeWithError("migrations", "validate", "--dir", dir, "--dir-format", "atlas", "--dev-url", dev)

	refusal := `refusing to clean protected PostgreSQL-family database "postgres": ` +
		`it is the server's default database, which is reset only on a server the run owns; ` +
		`if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, or use a docker:// or docker\+<driver>:// dev URL`
	c.Assert(err, qt.ErrorMatches, `(?s).*`+refusal+`.*`)
	c.Assert(nativeErr, qt.ErrorMatches, `(?s).*`+refusal+`.*`, qt.Commentf("%s", native))
}

// TestDevDefaultDatabaseOfADeclaredServerIsCleanedE2E is the control for the
// refusal: the same spelling with the server declared disposable replays the
// directory on both binaries.
func TestDevDefaultDatabaseOfADeclaredServerIsCleanedE2E(t *testing.T) {
	c := qt.New(t)
	dev := namedSpelling(c, provisionDevDefaultDatabase(c))
	_, dir := writeDevIdentitySources(c)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)

	output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", dev)
	native, nativeErr := runPtahNativeWithError("migrations", "validate", "--dir", dir, "--dir-format", "atlas", "--dev-url", dev)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(nativeErr, qt.IsNil, qt.Commentf("%s", native))
	c.Assert(userTables(c, dev), qt.Equals, 0)
}

// devDefaultDatabaseRehearsals are the commands that rehearse a plan on a dev
// database before they touch the target: `schema apply` on both binaries, and
// the plan-file checks of the compatibility surface. Each reads the
// declaration as a migration replay does (stokaro/ptah#4060). {plan} is a plan
// for the target, made beside a dev database of its own.
var devDefaultDatabaseRehearsals = []struct {
	name string
	// run is the binary the row drives, in process.
	run  func(args ...string) (string, error)
	args []string
	// added is how many `added` tables the target holds after a run the dev
	// database rehearsed: one where the command applies, none where it only
	// checks the plan.
	added int
}{
	{name: "schema apply", run: runCompatVerb, args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"}, added: 1},
	{name: "schema apply --plan", run: runCompatVerb, args: []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--plan", "file://{plan}", "--dev-url", "{dev}", "--auto-approve"}, added: 1},
	{name: "schema plan validate", run: runCompatVerb, args: []string{"schema", "plan", "validate", "--file", "file://{plan}", "--from", "{target}", "--to", "{schema}", "--dev-url", "{dev}"}, added: 0},
	{name: "schema plan lint", run: runCompatVerb, args: []string{"schema", "plan", "lint", "--file", "file://{plan}", "--from", "{target}", "--to", "{schema}", "--dev-url", "{dev}"}, added: 0},
	{name: "ptah schema apply", run: runPtahNativeWithError, args: []string{"schema", "apply", "--db-url", "{target}", "--schema-file", "{rawschema}", "--dev-url", "{dev}", "--auto-approve"}, added: 1},
}

// devDefaultDatabaseRehearsalArgs spells a row's arguments: the target, its
// desired state, and a plan for it made beside a scratch dev database, which
// the rehearsal does not use.
func devDefaultDatabaseRehearsalArgs(c *qt.C, template []string, dev string) (args []string, target devIdentityTarget) {
	c.Helper()
	target = newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
	schemaFile, dir := writeDevIdentitySources(c)
	planDev, _ := scratchReplayDatabase(c)
	plan := filepath.Join(c.TempDir(), "change.plan.hcl")
	output, err := runCompatVerb("schema", "plan", "--from", target.url, "--to", "file://"+schemaFile,
		"--dev-url", planDev, "--output", plan)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	args = devNotCleanArgs(template, dev, target.url, schemaFile, dir, "")
	for i := range args {
		args[i] = strings.ReplaceAll(args[i], "{plan}", plan)
	}
	return args, target
}

// addedTables counts `added`, the table the desired state adds, in target.
func addedTables(c *qt.C, target devIdentityTarget) int {
	c.Helper()
	var tables int
	c.Assert(target.conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'added'").Scan(&tables), qt.IsNil)
	return tables
}

// TestDevRehearsalsRefuseTheDefaultDatabaseOfANamedServerE2E rehearses each
// command's plan on the postgres database of a server reached by a spelling
// nothing recorded, without the declaration. The rehearsal refuses the
// database before it touches the target, and names the two ways to make the
// server the run's own.
func TestDevRehearsalsRefuseTheDefaultDatabaseOfANamedServerE2E(t *testing.T) {
	envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
	dev := namedSpelling(qt.New(t), provisionDevDefaultDatabase(qt.New(t)))
	for _, verb := range devDefaultDatabaseRehearsals {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			args, target := devDefaultDatabaseRehearsalArgs(c, verb.args, dev)

			output, err := verb.run(args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*refusing to clean protected PostgreSQL-family database "postgres": `+
				`it is the server's default database.*PTAH_DEV_SERVER_DISPOSABLE=1.*`, qt.Commentf("%s", output))
			c.Assert(addedTables(c, target), qt.Equals, 0)
		})
	}
}

// TestDevRehearsalsCleanTheDefaultDatabaseOfADeclaredServerE2E is the control:
// the same spelling declared disposable rehearses each command's plan there,
// the commands that apply reach the target, and the dev database is left
// empty.
func TestDevRehearsalsCleanTheDefaultDatabaseOfADeclaredServerE2E(t *testing.T) {
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(t)
	dev := namedSpelling(qt.New(t), provisionDevDefaultDatabase(qt.New(t)))
	for _, verb := range devDefaultDatabaseRehearsals {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			args, target := devDefaultDatabaseRehearsalArgs(c, verb.args, dev)

			output, err := verb.run(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
			c.Assert(addedTables(c, target), qt.Equals, verb.added)
			c.Assert(userTables(c, dev), qt.Equals, 0)
		})
	}
}
