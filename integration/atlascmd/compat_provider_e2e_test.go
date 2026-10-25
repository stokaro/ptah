//go:build integration

package atlas_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// compatProviderBinaries are the two programs every row drives: ptah-compat as
// shipped, and the independently built fixture provider that serves the engine
// "fixture" (history, session locks and execution included).
type compatProviderBinaries struct{ compat, provider string }

func buildCompatProviderBinaries(c *qt.C) compatProviderBinaries {
	c.Helper()
	return compatProviderBinaries{
		compat:   buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat"),
		provider: testutils.BuildFixtureProvider(c, "locks"),
	}
}

// compatProviderFixture is one private directory holding a manifest naming the
// fixture provider, a hashed migration directory and the fixture database.
type compatProviderFixture struct {
	binaries                                       compatProviderBinaries
	dir, manifest, pidFile, address, migrationsURL string
}

func newCompatProviderFixture(c *qt.C, binaries compatProviderBinaries) compatProviderFixture {
	c.Helper()
	dir := c.TempDir()
	f := compatProviderFixture{
		binaries: binaries, dir: dir,
		manifest: filepath.Join(dir, "providers.json"),
		pidFile:  filepath.Join(dir, "provider.pid"),
	}
	data, err := os.ReadFile(binaries.provider)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	f.writeManifest(c, hex.EncodeToString(digest[:]), false)
	f.address = (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(dir, "database.db")), "/")}).String()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_init.sql"), []byte("CREATE TABLE widgets (id integer NOT NULL);\n"), 0o600), qt.IsNil)
	f.migrationsURL = "file://" + filepath.ToSlash(migrations)
	_, stderr, code := f.run(nil, "migrate", "hash", "--dir", f.migrationsURL)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	return f
}

// writeManifest writes a manifest of one entry for the fixture provider with
// the given executable digest.
func (f compatProviderFixture) writeManifest(c *qt.C, sha256Hex string, disabled bool) {
	c.Helper()
	spec := providerhost.Spec{
		Executable: f.binaries.provider, SHA256: sha256Hex, ProviderID: "example.org/fixture",
		Environment: []string{"PTAH_FIXTURE_PID_FILE=" + f.pidFile},
	}
	manifest, err := json.Marshal(providerconfig.Configuration{
		FormatVersion: providerconfig.FormatVersion,
		Providers:     []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: "fixture"}}, Disabled: disabled}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifest, manifest, 0o600), qt.IsNil)
}

// withManifest is the environment that selects the manifest.
func (f compatProviderFixture) withManifest() []string {
	return []string{"PTAH_PROVIDER_CONFIG=" + f.manifest}
}

// run executes ptah-compat with every PTAH_ variable of the test process
// removed and env added, so no ambient variable selects a provider or the
// strict profile.
func (f compatProviderFixture) run(env []string, args ...string) (stdout, stderr string, code int) {
	cmd := exec.Command(f.binaries.compat, args...) // #nosec G204 -- the test runs the ptah-compat binary it built
	cmd.Dir = f.dir
	cmd.Env = append(environmentWithoutPtahVariables(), env...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	_ = cmd.Run() // the exit status is read from the process state
	return out.String(), errOut.String(), cmd.ProcessState.ExitCode()
}

// assertNeverStarted reads the provider's own process record: the fixture
// writes it before anything else, so its absence means no process started.
func (f compatProviderFixture) assertNeverStarted(c *qt.C) {
	c.Helper()
	_, err := os.Stat(f.pidFile)
	c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("a provider process started"))
}

// assertStopped waits for the process the fixture recorded to be gone.
func (f compatProviderFixture) assertStopped(c *qt.C) {
	c.Helper()
	data, err := os.ReadFile(f.pidFile)
	c.Assert(err, qt.IsNil, qt.Commentf("no provider process started"))
	pid, err := strconv.Atoi(string(data))
	c.Assert(err, qt.IsNil)
	deadline := time.Now().Add(10 * time.Second)
	for testutils.ProcessRunning(pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c.Assert(testutils.ProcessRunning(pid), qt.IsFalse, qt.Commentf("the provider outlived the command"))
}

func TestCompatProviderE2E_HappyPath(t *testing.T) {
	binaries := buildCompatProviderBinaries(qt.New(t))

	// The control for the strict refusal below: the variable reaches an
	// unscoped run, so that refusal is not the variable being dropped.
	t.Run("a fixture URL is served through the manifest", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		out, stderr, code := f.run(f.withManifest(), "schema", "inspect", "--url", f.address, "--format", "{{ json . }}")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Equals, `{"schemas":[{"name":"main"}]}`)
		f.assertStopped(c)
	})
	t.Run("migrations apply through the provider and the process ends with each command", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		out, stderr, code := f.run(f.withManifest(), "migrate", "apply", "--dir", f.migrationsURL, "--url", f.address)
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Contains, "Migration complete. Current version: 20240101000000")
		f.assertStopped(c)

		out, stderr, code = f.run(f.withManifest(), "migrate", "status", "--dir", f.migrationsURL, "--url", f.address)
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Contains, "Migration Status: OK")
		f.assertStopped(c)
	})
	t.Run("a diff between two fixture databases plans through the provider runtime", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		out, stderr, code := f.run(f.withManifest(), "schema", "diff", "--from", f.address, "--to", f.address)

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Contains, "Schemas are synced")
		f.assertStopped(c)
	})
	// A database another provider URL names is a desired state, as a
	// built-in database URL is: the apply plans the target toward it and
	// needs no dev database.
	t.Run("a schema apply takes another fixture database as the desired state", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)
		target := filepath.Join(f.dir, "database.db")
		createSchemaInspectSQLiteDatabase(c, target, `CREATE TABLE "widgets" ("id" INTEGER)`, `CREATE TABLE "gadgets" ("id" INTEGER)`)
		desired := filepath.Join(f.dir, "desired.db")
		createSchemaInspectSQLiteDatabase(c, desired, `CREATE TABLE "widgets" ("id" INTEGER)`)
		to := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(desired), "/")}).String()

		out, stderr, code := f.run(f.withManifest(), "schema", "apply", "--url", f.address, "--to", to, "--auto-approve")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Contains, "DROP TABLE \"gadgets\";")
		f.assertStopped(c)
		conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+target)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		var gadgets, widgets int
		c.Assert(conn.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_schema WHERE name = 'gadgets'`).Scan(&gadgets), qt.IsNil)
		c.Assert(conn.QueryRowContext(c.Context(), `SELECT count(*) FROM sqlite_schema WHERE name = 'widgets'`).Scan(&widgets), qt.IsNil)
		c.Assert([]int{gadgets, widgets}, qt.DeepEquals, []int{0, 1})
	})
	t.Run("a built-in URL under a manifest stays on its driver and starts no process", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)
		lite := "sqlite://" + filepath.ToSlash(filepath.Join(f.dir, "lite.db"))

		out, stderr, code := f.run(f.withManifest(), "schema", "inspect", "--url", lite, "--format", "{{ json . }}")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(out, qt.Equals, `{"schemas":[{"name":"main"}]}`)
		f.assertNeverStarted(c)
	})
}

func TestCompatProviderE2E_FailurePath(t *testing.T) {
	binaries := buildCompatProviderBinaries(qt.New(t))

	t.Run("the same URL is refused without the manifest", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		out, stderr, code := f.run(nil, "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: sql/sqlclient: unknown driver \"fixture\". See: https://atlasgo.io/url\n")
		f.assertNeverStarted(c)
	})
	t.Run("strict mode refuses the variable by name before anything starts", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		out, stderr, code := f.run(append(f.withManifest(), "PTAH_ATLAS_STRICT_COMPAT=1"), "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_PROVIDER_CONFIG\n")
		f.assertNeverStarted(c)
	})
	t.Run("strict mode answers the URL in the pinned binary's words", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		_, stderr, code := f.run([]string{"PTAH_ATLAS_STRICT_COMPAT=1"}, "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "Error: sql/sqlclient: unknown driver \"fixture\". See: https://atlasgo.io/url\n")
		f.assertNeverStarted(c)
	})
	t.Run("a blank variable is a mistake, not an absent one", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		_, stderr, code := f.run([]string{"PTAH_PROVIDER_CONFIG="}, "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "Error: PTAH_PROVIDER_CONFIG requires a manifest path\n")
		f.assertNeverStarted(c)
	})
	t.Run("a manifest that does not load ends the search", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)

		_, stderr, code := f.run([]string{"PTAH_PROVIDER_CONFIG=" + f.manifest + ".missing"}, "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(stderr, qt.Contains, "Error: read provider manifest: ")
		f.assertNeverStarted(c)
	})
	t.Run("a wrong digest is refused with no fallback", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)
		f.writeManifest(c, strings.Repeat("a", 64), false)

		out, stderr, code := f.run(f.withManifest(), "schema", "inspect", "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Contains, "does not match its trusted SHA-256 checksum")
		f.assertNeverStarted(c)
	})
	t.Run("a disabled entry is not replaced by another provider", func(t *testing.T) {
		c := qt.New(t)
		f := newCompatProviderFixture(c, binaries)
		data, err := os.ReadFile(binaries.provider)
		c.Assert(err, qt.IsNil)
		digest := sha256.Sum256(data)
		f.writeManifest(c, hex.EncodeToString(digest[:]), true)

		out, stderr, code := f.run(f.withManifest(), "migrate", "status", "--dir", f.migrationsURL, "--url", f.address)

		c.Assert(code, qt.Equals, 1)
		c.Assert(out, qt.Equals, "")
		c.Assert(stderr, qt.Contains, "disabled")
		f.assertNeverStarted(c)
	})
}

// runInProcess executes the compatibility tree inside the test process, so a
// provider that outlives the command is still running when the command
// returns. Under the built binary the parent-death watch would end it as the
// process exits, which hides a missing close.
func (f compatProviderFixture) runInProcess(c *qt.C, args ...string) (output string, runErr error) {
	c.Helper()
	c.Setenv("PTAH_PROVIDER_CONFIG", f.manifest)
	cmd := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.Full())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	runErr = cmd.ExecuteContext(c.Context())
	return out.String(), runErr
}

func TestCompatProviderLifetime_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newCompatProviderFixture(c, buildCompatProviderBinaries(c))

	out, err := f.runInProcess(c, "migrate", "status", "--dir", f.migrationsURL, "--url", f.address)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Migration Status: PENDING")
	f.assertStopped(c)
}

func TestCompatProviderLifetime_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newCompatProviderFixture(c, buildCompatProviderBinaries(c))

	// The fixture connects, then refuses to clean a database.
	out, err := f.runInProcess(c, "schema", "clean", "--url", f.address, "--auto-approve")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "does not support database cleanup")
	f.assertStopped(c)
}
