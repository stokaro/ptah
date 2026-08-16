//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// Three of the four verbs wired by stokaro/ptah#844 -- migrate test, schema
// test and migrate checkpoint -- take a `docker://` dev URL and run against the
// container it names. The fourth, migrate down, is pinned only as far as its
// dialect precheck; the test below says why.
//
// A unit test cannot prove this. Driving those verbs with a URL the provisioner
// refuses from its text proves only that the value REACHED the provisioner: the
// refusal comes out of the resolve call whether or not the verb goes on to use
// what that call returned. Measured, with the resolved URL discarded and the
// raw `docker://…` handed to the connector instead, every such row still
// passed. Only a run that has to connect can tell those apart, which is why
// these live here.
//
// This file does not skip when Docker is missing, for the reason
// devdocker_provision_e2e_test.go gives: the `integration` tag already means
// the infrastructure is there, and a test that skips itself reads as a test
// that passed.

// devDockerVerbURL names a database the assertions can recognize. `ptahverbs`
// is not a name any fallback would produce, so a case asserting
// current_database() answers it can only have run on the provisioned server.
const devDockerVerbURL = "docker://postgres/16-alpine/ptahverbs"

// runCompatVerb executes one ptah-compat invocation in process. The returned
// text is what the compat tree itself wrote; a native runner it forwards to
// writes its report elsewhere, so the assertions below read the error rather
// than the report.
func runCompatVerb(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	return out.String(), cmd.Execute()
}

// devDockerVerbFixture is a hashed migration directory, a matching schema file
// and one test case per kind, each asserting the name of the database it ran
// against.
type devDockerVerbFixture struct {
	dir          string
	schema       string
	migrateCases string
	schemaCases  string
	root         string
}

func newDevDockerVerbFixture(c *qt.C) devDockerVerbFixture {
	c.Helper()

	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE widgets (id integer NOT NULL);\n"),
		0o600,
	), qt.IsNil)
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)

	schema := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE widgets (id integer NOT NULL);\n"), 0o600), qt.IsNil)

	// current_database() exists on PostgreSQL and nowhere else this repository
	// would fall back to: SQLite answers `no such function` and MySQL answers
	// `FUNCTION dev.current_database does not exist`. Asserting the NAME as
	// well pins that the database is the one the URL asked for rather than a
	// default the provisioner happened to create.
	migrateCases := filepath.Join(root, "migrate-cases")
	c.Assert(os.MkdirAll(migrateCases, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrateCases, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: ran on the provisioned database\n"+
			"    steps:\n"+
			"      - name: migrate\n"+
			"        migrate_to: latest\n"+
			"      - name: which database\n"+
			"        assert:\n"+
			"          query: SELECT current_database()\n"+
			"          scalar: ptahverbs\n"), 0o600), qt.IsNil)

	schemaCases := filepath.Join(root, "schema-cases")
	c.Assert(os.MkdirAll(schemaCases, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(schemaCases, "cases.yaml"), []byte(
		"cases:\n"+
			"  - name: ran on the provisioned database\n"+
			"    steps:\n"+
			"      - name: apply\n"+
			"        apply_schema: true\n"+
			"      - name: which database\n"+
			"        assert:\n"+
			"          query: SELECT current_database()\n"+
			"          scalar: ptahverbs\n"), 0o600), qt.IsNil)

	return devDockerVerbFixture{
		dir:          dir,
		schema:       schema,
		migrateCases: migrateCases,
		schemaCases:  schemaCases,
		root:         root,
	}
}

func TestDevDockerTestVerbsRunOnTheProvisionedDatabase(t *testing.T) {
	tests := []struct {
		name string
		args func(fx devDockerVerbFixture) []string
	}{
		{
			name: "migrate test",
			args: func(fx devDockerVerbFixture) []string {
				return []string{
					"migrate", "test", fx.migrateCases,
					"--dir", "file://" + fx.dir,
					"--dev-url", devDockerVerbURL,
				}
			},
		},
		{
			name: "schema test",
			args: func(fx devDockerVerbFixture) []string {
				return []string{
					"schema", "test", fx.schemaCases,
					"-u", "file://" + fx.schema,
					"--dev-url", devDockerVerbURL,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newDevDockerVerbFixture(c)

			out, err := runCompatVerb(tt.args(fx)...)

			// Success is the whole assertion, because the discriminator lives
			// inside the case: it asserts current_database() answers the name
			// the URL asked for. A run against anything else fails the case and
			// the verb exits non-zero, and a run with no cases at all is
			// refused rather than reported as a pass.
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(devDockerCensus(c), qt.HasLen, 0)
		})
	}
}

func TestDevDockerShadowVerbsUseTheProvisionedDatabase(t *testing.T) {
	c := qt.New(t)
	fx := newDevDockerVerbFixture(c)

	// A checkpoint replays the whole directory into the shadow database and
	// introspects the result, so it cannot exit 0 without one that answers.
	out, err := runCompatVerb(
		"migrate", "checkpoint", "cp1",
		"--dir", "file://"+fx.dir,
		"--dev-url", devDockerVerbURL,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(devDockerCensus(c), qt.HasLen, 0)
}

// TestDevDockerRollbackReachesTheDialectCheck pins how far `migrate down` gets
// with a `docker://` shadow, and says plainly how far that is.
//
// It pins that the verb reaches its dialect precheck and leaves no container
// behind. It does NOT pin that the verb connects to the container: measured,
// handing the raw `docker://` value to the shadow instead of the resolved one
// leaves this test green, because the precheck reads "postgres" out of either
// spelling and refuses before anything is opened.
//
// What would separate them is a PostgreSQL target, so the dialects match and
// the verb has to open the shadow -- two containers for one assertion, which
// this file does not spend. The wiring is not therefore unmeasured: a rollback
// with a matching dialect cannot open a `docker://` URL at all, which is the
// verb's whole reason for resolving it, and the before/after run is recorded in
// docs/conformance.md.
func TestDevDockerRollbackReachesTheDialectCheck(t *testing.T) {
	c := qt.New(t)
	fx := newDevDockerVerbFixture(c)

	target := "sqlite://" + filepath.Join(fx.root, "target.db")
	out, err := runCompatVerb("migrate", "apply", "--dir", "file://"+fx.dir, "--url", target)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runCompatVerb(
		"migrate", "down",
		"--dir", "file://"+fx.dir,
		"--url", target,
		"--to-version", "0",
		"--dev-url", devDockerVerbURL,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains,
		`shadow database dialect "postgres" does not match target dialect "sqlite"`)
	c.Assert(devDockerCensus(c), qt.HasLen, 0)
}
