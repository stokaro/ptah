//go:build integration

package providercli_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
	"ptah.run/internal/testutils"
)

// changedSchema is the fixture schema with one table more than the database the
// tests apply, so a verb that compares or plans has a difference to report.
const changedSchema = "tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n" +
	"  second:\n    columns:\n      value:\n        type: TEXT\n  third:\n    columns:\n      n:\n        type: INTEGER\n"

// hashedMigrations is a migration directory with its integrity file, which the
// verbs that read a directory check before anything else.
func hashedMigrations(c *qt.C) string {
	c.Helper()
	dir := migrationDirectory(c)
	_, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "hash", "--dir", dir})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	return dir
}

// isolateStore points the user cache at an empty directory, so the user store
// holds nothing the developer installed.
func isolateStore(c *qt.C) {
	c.Helper()
	c.Setenv("XDG_CACHE_HOME", c.TempDir())
	c.Setenv("HOME", c.TempDir())
	c.Setenv("LocalAppData", c.TempDir())
}

// migrationCases is a migration test directory with one case that migrates to
// the version migrationDirectory holds.
func migrationCases(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "up.test.hcl"), []byte("test \"migrate\" \"up\" {\n  migrate { to = \"0000000001\" }\n}\n"), 0o600), qt.IsNil)
	return dir
}

// TestNativeVerbsReachTheSelectedProvider runs every native verb that names a
// database or a dialect, and that the fixture's services can answer, against
// the provider the manifest selects. Each row starts the provider, reads its
// answer back from the verb's output, and leaves no process behind: the PID
// file is removed before the row and written by the provider the row started.
//
// Without the selection these verbs ran on the built-in engines only, and a
// fixture:// URL or the fixture dialect was an unsupported dialect.
func TestNativeVerbsReachTheSelectedProvider(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	changed := filepath.Join(c.TempDir(), "changed.yaml")
	c.Assert(os.WriteFile(changed, []byte(changedSchema), 0o600), qt.IsNil)

	for _, row := range []struct {
		name string
		args []string
		// want is matched against stdout and stderr together, because the
		// verbs report on either.
		want string
	}{
		{"db read", []string{"db", "read", "--db-url", "{db}"}, `CREATE TABLE "items"`},
		{"db capabilities", []string{"db", "capabilities", "--db-url", "{db}"}, "Dialect:            fixture\n"},
		{"db drop-all", []string{"db", "drop-all", "--db-url", "{db}", "--dry-run"}, "Would drop 2 supported schema objects"},
		{"introspect", []string{"introspect", "--db-url", "{db}", "--out", "{out}"}, "Imported 2 table(s)"},
		{"schema render", []string{"schema", "render", "--dialect", "fixture", "--schema-file", "{schema}"}, `CREATE TABLE "second"`},
		{"schema validate", []string{"schema", "validate", "--dialect", "fixture", "--schema-file", "{schema}"}, ""},
		{"viz", []string{"viz", "--dialect", "fixture", "--schema-file", "{schema}"}, "erDiagram"},
		{"schema export", []string{"schema", "export", "--to", "openapi-v3", "--schema-file", "{schema}"}, "second:"},
		{"schema stats", []string{"schema", "stats", "--db-url", "{db}"}, `ptah_schema_tables{dialect="fixture"} 2`},
		{"schema lineage", []string{"schema", "lineage", "--db-url", "{db}"}, "No view or routine columns to trace."},
		{"schema compare", []string{"schema", "compare", "--db-url", "{db}", "--schema-file", "{changed}"}, "tables_added (1): third"},
		{"schema drift", []string{"schema", "drift", "--db-url", "{db}", "--schema-file", "{schema}"}, "No schema drift detected."},
		{"schema diff", []string{"schema", "diff", "--from", "{db}", "--to", "{db}"}, "Schemas are synced"},
		{"migrations plan", []string{"migrations", "plan", "--db-url", "{db}", "--schema-file", "{changed}"}, `CREATE TABLE "third"`},
		{"migrations generate", []string{"migrations", "generate", "--db-url", "{db}", "--schema-file", "{changed}", "--migrations-dir", "{out}"}, "Generated migration files"},
		{"migrations validate", []string{"migrations", "validate", "--dir", "{migrations}", "--dev-url", "docker://fixture/1"}, "OK: migration SQL validated on dev database"},
		{"migrations test", []string{"migrations", "test", "--dir", "{cases}", "--migrations-dir", "{migrations}", "--db-url", "{scratch}"}, "1 cases, 1 passed"},
		{"migrations rm", []string{"migrations", "rm", "--db-url", "{db}", "--migrations-dir", "{migrations}", "--version", "0000000001"}, "Removed"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(os.RemoveAll(f.pidPath), qt.IsNil)
			args := strings.Split(strings.NewReplacer(
				"{db}", f.address,
				"{schema}", f.schemaPath,
				"{changed}", changed,
				"{out}", c.TempDir(),
				"{migrations}", hashedMigrations(c),
				"{cases}", migrationCases(c),
				"{scratch}", fixtureAddress("", filepath.Join(c.TempDir(), "scratch.db")),
			).Replace(strings.Join(row.args, "\x00")), "\x00")

			out, diagnostics, code := executeProviderCLI(c.Context(), append(args, "--provider-config", f.manifestPath))

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
			c.Assert(out+diagnostics, qt.Contains, row.want)
			f.assertStopped(c)
		})
	}
}

// TestNativeSchemaSecurityRefusesAProviderReadWithoutAccessCoverage runs
// `schema security` against the provider the manifest selects. The fixture
// reports no coverage for role memberships or object owners, and a missing
// coverage row is never evidence that the database has none, so the verb
// starts the provider, reads through it and refuses the report.
func TestNativeSchemaSecurityRefusesAProviderReadWithoutAccessCoverage(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(os.RemoveAll(f.pidPath), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(),
		[]string{"schema", "security", "--db-url", f.address, "--provider-config", f.manifestPath})

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(diagnostics, qt.Contains, "the read did not inspect role_membership; a security report needs complete access metadata")
	c.Assert(out, qt.Equals, "")
	f.assertStopped(c)
}

// TestNativeVerbsReportAMissingProviderForTheEngineTheyName names an official
// engine no built-in implementation serves, DuckDB, and installs nothing. A
// verb reaches the provider selection whether the engine comes from a flag, a
// flag that is not a selection target, or the project file, and the operator
// is told the command that installs the provider: what every verb answers for
// YDB once its provider serves it.
func TestNativeVerbsReportAMissingProviderForTheEngineTheyName(t *testing.T) {
	const duckdb = "duckdb://localhost/app"
	for _, row := range []struct {
		name string
		args []string
	}{
		{"db read", []string{"db", "read", "--db-url", duckdb}},
		{"db capabilities", []string{"db", "capabilities", "--db-url", duckdb}},
		{"db drop-all", []string{"db", "drop-all", "--db-url", duckdb, "--dry-run"}},
		{"introspect", []string{"introspect", "--db-url", duckdb, "--out", "{out}"}},
		{"schema render", []string{"schema", "render", "--dialect", "duckdb", "--schema-file", "{schema}"}},
		{"schema validate", []string{"schema", "validate", "--dialect", "duckdb", "--schema-file", "{schema}"}},
		{"viz", []string{"viz", "--dialect", "duckdb", "--schema-file", "{schema}"}},
		{"schema stats", []string{"schema", "stats", "--db-url", duckdb}},
		{"schema lineage", []string{"schema", "lineage", "--db-url", duckdb}},
		{"schema security", []string{"schema", "security", "--db-url", duckdb}},
		{"schema compare", []string{"schema", "compare", "--db-url", duckdb, "--schema-file", "{schema}"}},
		{"schema drift", []string{"schema", "drift", "--db-url", duckdb, "--schema-file", "{schema}"}},
		{"schema test", []string{"schema", "test", "--db-url", duckdb, "--dir", "{out}", "--schema-file", "{schema}"}},
		{"schema push", []string{"schema", "push", "--dialect", "duckdb", "--schema-file", "{schema}", "oci://127.0.0.1:1/schema:v1"}},
		{"migrations plan", []string{"migrations", "plan", "--db-url", duckdb, "--schema-file", "{schema}"}},
		{"migrations generate", []string{"migrations", "generate", "--db-url", duckdb, "--schema-file", "{schema}", "--migrations-dir", "{out}"}},
		{"migrations validate", []string{"migrations", "validate", "--dir", "{migrations}", "--dev-url", "docker://duckdb/1"}},
		{"migrations test", []string{"migrations", "test", "--db-url", duckdb, "--dir", "{out}", "--migrations-dir", "{migrations}"}},
		{"migrations checkpoint", []string{"migrations", "checkpoint", "--shadow-db", duckdb, "--migrations-dir", "{migrations}", "--dry-run"}},
		{"migrations import", []string{"migrations", "import", "--dialect", "duckdb", "--source-dir", "{out}", "--from", "liquibase"}},
		{"migrations rm", []string{"migrations", "rm", "--db-url", duckdb, "--migrations-dir", "{migrations}", "--version", "0000000001"}},
		{"migrations edit", []string{"migrations", "edit", "--db-url", duckdb, "--migrations-dir", "{migrations}", "--version", "0000000001"}},
		{"migrations rebase", []string{"migrations", "rebase", "--db-url", duckdb, "--migrations-dir", "{migrations}", "--version", "0000000001"}},
		{"db verify", []string{"db", "verify", "--db-url", duckdb, "--checks", "{checks}"}},
		// The engine comes from a flag that is not a selection target, so the
		// lazy session meets it when the verb classifies its sources.
		{"schema diff", []string{"schema", "diff", "--from", duckdb, "--to", "file://schema.yaml"}},
		// The engine comes from the project file, which the verb reads after
		// the flags: only the lazy session can serve it.
		{"migrations plan from the project", []string{"migrations", "plan", "--config", "{project}", "--schema-file", "{schema}"}},
		{"schema compare from the project", []string{"schema", "compare", "--config", "{project}", "--schema-file", "{schema}"}},
		{"project adopt from the project", []string{"project", "adopt", "--config", "{project}", "--preflight", "--check"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			isolateStore(c)
			dir := c.TempDir()
			schema := filepath.Join(dir, "schema.yaml")
			c.Assert(os.WriteFile(schema, []byte(changedSchema), 0o600), qt.IsNil)
			project := filepath.Join(dir, "ptah.yaml")
			c.Assert(os.WriteFile(project, []byte("url: "+duckdb+"\n"), 0o600), qt.IsNil)
			checks := filepath.Join(dir, "checks.sql")
			c.Assert(os.WriteFile(checks, []byte("-- +ptah check name=present assert=\"SELECT 1\"\n"), 0o600), qt.IsNil)
			args := strings.Split(strings.NewReplacer(
				"{schema}", schema,
				"{out}", c.TempDir(),
				"{migrations}", hashedMigrations(c),
				"{project}", project,
				"{checks}", checks,
			).Replace(strings.Join(row.args, "\x00")), "\x00")

			out, diagnostics, code := executeProviderCLI(c.Context(), args)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", out, diagnostics))
			c.Assert(diagnostics, qt.Contains, "ptah providers install duckdb")
		})
	}
}

// TestNativeVerbServesAProjectDatabaseBesideABuiltInDevDatabase names the
// database only in the project file, and a SQLite dev database on the command
// line. The flags name a built-in engine, so nothing is selected before the
// verb runs, and the lazy session starts the provider when the verb reads the
// project's URL: the runtime holds the provider's engine beside the built-in
// ones, and the comparison reaches the database through the provider.
func TestNativeVerbServesAProjectDatabaseBesideABuiltInDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	dir := c.TempDir()
	project := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(project, []byte("url: "+f.address+"\n"), 0o600), qt.IsNil)
	changed := filepath.Join(dir, "changed.yaml")
	c.Assert(os.WriteFile(changed, []byte(changedSchema), 0o600), qt.IsNil)
	c.Assert(os.RemoveAll(f.pidPath), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{
		"schema", "compare", "--provider-config", f.manifestPath, "--config", project,
		"--dev-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")), "--schema-file", changed,
	})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "tables_added (1): third")
	f.assertStopped(c)
}

// installDuckDB puts the duckdb-release fixture in the user store as `ptah
// providers install duckdb` leaves a release: the executable below the
// provider's directory and the install record naming it with its digest. The
// store is a fresh one, so nothing else is installed.
func installDuckDB(c *qt.C) {
	c.Helper()
	// The build runs the go command, which reads HOME, so it comes before the
	// user cache moves to an empty directory.
	binary, err := os.ReadFile(testutils.BuildFixtureProvider(c, "duckdb-release"))
	c.Assert(err, qt.IsNil)
	isolateStore(c)
	entry, found := providercatalog.Lookup("duckdb")
	c.Assert(found, qt.IsTrue)
	store, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	platform := providerrelease.Running()
	executable := "ptah-plugin-duckdb" + testutils.ExecutableSuffix
	dir := providerinstall.Dir(store, entry.ProviderID, "0.1.0", platform)
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, executable), binary, 0o700), qt.IsNil) // #nosec G306 G703 -- a store under the test's own temporary directory, executable by its owner
	digest := sha256.Sum256(binary)
	c.Assert(providerinstall.WriteRecord(store, platform, providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: entry.ProviderID, Version: "0.1.0",
		MinContract: providerrelease.HostContract(), Engines: []string{"duckdb"}, SHA256: hex.EncodeToString(digest[:]),
		Executable: "0.1.0/" + platform.OS + "_" + platform.Arch + "/" + executable,
	}), qt.IsNil)
}

// TestNativeVerbsServeAnInstalledProviderFromTheUserStore installs a provider
// for DuckDB, an official engine with no built-in implementation, and names no
// provider flag at all. A duckdb:// URL or the duckdb dialect on a selection
// flag selects the installed provider before the verb runs; a URL the verb
// reads from ptah.yaml, from a flag such as --from, or a second --dialect
// reaches it through the lazy session, beside the built-in engines. That is
// how every verb serves YDB once its provider serves ydb://.
func TestNativeVerbsServeAnInstalledProviderFromTheUserStore(t *testing.T) {
	c := qt.New(t)
	installDuckDB(c)
	dir := c.TempDir()
	address := (&url.URL{Scheme: "duckdb", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(dir, "app.db")), "/")}).String()
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte("tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n        not_null: true\n  second:\n    columns:\n      value:\n        type: TEXT\n"), 0o600), qt.IsNil)
	changed := filepath.Join(dir, "changed.yaml")
	c.Assert(os.WriteFile(changed, []byte(changedSchema), 0o600), qt.IsNil)
	project := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(project, []byte("url: "+address+"\n"), 0o600), qt.IsNil)
	_, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "apply", "--db-url", address, "--schema-file", schema, "--tx-mode", "none", "--auto-approve"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))

	for _, row := range []struct {
		name string
		args []string
		want string
	}{
		{"db read", []string{"db", "read", "--db-url", "{db}"}, `CREATE TABLE "items"`},
		{"schema render", []string{"schema", "render", "--dialect", "duckdb", "--schema-file", "{schema}"}, `CREATE TABLE "second"`},
		{"schema validate beside a built-in dialect", []string{"schema", "validate", "--dialect", "sqlite", "--dialect", "duckdb", "--schema-file", "{schema}"}, ""},
		{"schema diff", []string{"schema", "diff", "--from", "{db}", "--to", "{db}"}, "Schemas are synced"},
		{"schema compare from the project", []string{"schema", "compare", "--config", "{project}", "--schema-file", "{changed}"}, "tables_added (1): third"},
		{"schema drift from the project", []string{"schema", "drift", "--config", "{project}", "--schema-file", "{schema}"}, "No schema drift detected."},
		{"migrations plan from the project", []string{"migrations", "plan", "--config", "{project}", "--schema-file", "{changed}"}, `CREATE TABLE "third"`},
		{"migrations generate from the project", []string{"migrations", "generate", "--config", "{project}", "--schema-file", "{changed}", "--migrations-dir", "{out}"}, "Generated migration files"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			args := strings.Split(strings.NewReplacer(
				"{db}", address, "{schema}", schema, "{changed}", changed, "{project}", project, "{out}", c.TempDir(),
			).Replace(strings.Join(row.args, "\x00")), "\x00")

			out, diagnostics, code := executeProviderCLI(c.Context(), args)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
			c.Assert(out+diagnostics, qt.Contains, row.want)
		})
	}
}

// The validate row above is the only one whose verb could leave the provider's
// engine out and still succeed: a name the runtime does not resolve is dropped,
// and the built-in dialect alone validates cleanly. --server-version describes
// one target, so it is refused exactly when the provider's engine is counted as
// the second.
func TestNativeValidateBesideABuiltInDialectReachesTheProvider(t *testing.T) {
	c := qt.New(t)
	installDuckDB(c)
	schema := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte("tables:\n  items:\n    columns:\n      id:\n        type: INTEGER\n"), 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "validate", "--dialect", "sqlite", "--dialect", "duckdb", "--schema-file", schema, "--server-version", "1"})

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(diagnostics, qt.Contains, "--server-version applies to one target: 2 dialects were named")
}

// TestNativeSchemaServeReadsAProjectDatabaseThroughAStoredProvider serves the
// read-only schema view of the database ptah.yaml names, with the provider the
// user store holds for its engine. The verb reads ./ptah.yaml, so it runs in
// the project's directory. The view reads the database on every request, so
// the page is fetched once while the command runs, and the command ends when
// its context does.
func TestNativeSchemaServeReadsAProjectDatabaseThroughAStoredProvider(t *testing.T) {
	c := qt.New(t)
	installDuckDB(c)
	dir := c.TempDir()
	address := (&url.URL{Scheme: "duckdb", Path: "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(dir, "app.db")), "/")}).String()
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(changedSchema), 0o600), qt.IsNil)
	project := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(project, []byte("url: "+address+"\n"), 0o600), qt.IsNil)
	_, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "apply", "--db-url", address, "--schema-file", schema, "--tx-mode", "none", "--auto-approve"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	t.Chdir(dir)

	ctx, cancel := context.WithCancel(c.Context())
	defer cancel()
	stdout, written := io.Pipe()
	done := make(chan int, 1)
	go func() {
		cmd := root.NewRootCommand()
		cmd.SetOut(written)
		cmd.SetErr(io.Discard)
		done <- root.RunContext(ctx, cmd, "schema", "serve", "--schema-file", schema, "--addr", "127.0.0.1:0", "--refresh", "0")
		c.Check(written.Close(), qt.IsNil)
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	c.Assert(err, qt.IsNil)
	page, found := strings.CutPrefix(strings.TrimSpace(line), "Serving a read-only schema view on ")
	c.Assert(found, qt.IsTrue, qt.Commentf("%s", line))
	go func() { _, _ = io.Copy(io.Discard, stdout) }()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, page, nil)
	c.Assert(err, qt.IsNil)
	response, err := http.DefaultClient.Do(request)
	c.Assert(err, qt.IsNil)
	body, err := io.ReadAll(response.Body)
	c.Assert(err, qt.IsNil)
	c.Assert(response.Body.Close(), qt.IsNil)
	cancel()

	c.Assert(response.StatusCode, qt.Equals, http.StatusOK, qt.Commentf("%s", body))
	c.Assert(string(body), qt.Contains, "third")
	c.Assert(string(body), qt.Not(qt.Contains), "unsupported")
	c.Assert(<-done, qt.Equals, 0)
}

// TestNativeVerbsReportAMissingProviderInTheirJSON keeps a machine-readable
// verb's output a document when the provider its flags select is missing: the
// selection fails before the verb runs, and the report carries the provider
// diagnostic beside the error.
func TestNativeVerbsReportAMissingProviderInTheirJSON(t *testing.T) {
	for _, verb := range []string{"drift", "lineage", "security"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			isolateStore(c)

			out, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", verb, "--db-url", "duckdb://localhost/app", "--format", "json"})

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			var report struct {
				Diagnostic providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
			}
			c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
			c.Assert(report.Diagnostic.Code, qt.Equals, "provider_missing")
			c.Assert(report.Diagnostic.EngineID, qt.Equals, "duckdb")
		})
	}
}

// TestNativeVerbsStopAtSQLAProviderConnectionDoesNotTake holds the limit the
// provider page states: a provider's connection takes no database/sql call, so
// a verb that runs a statement of its own on it is refused once it reaches the
// provider, and says why. schema test applies the desired schema that way.
func TestNativeVerbsStopAtSQLAProviderConnectionDoesNotTake(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--auto-approve")
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	for _, row := range []struct {
		name string
		args []string
	}{
		{"schema test", []string{"schema", "test", "--db-url", "{scratch}", "--dir", "{cases}", "--schema-file", "{schema}"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			checks := filepath.Join(dir, "checks.sql")
			c.Assert(os.WriteFile(checks, []byte("-- +ptah check name=present assert=\"SELECT 1\"\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "case.test.hcl"), []byte("test \"schema\" \"writes\" {\n  exec { sql = \"CREATE TABLE marker (x INTEGER);\" }\n}\n"), 0o600), qt.IsNil)
			args := strings.Split(strings.NewReplacer(
				"{db}", f.address,
				"{schema}", f.schemaPath,
				"{checks}", checks,
				"{cases}", dir,
				"{migrations}", hashedMigrations(c),
				"{scratch}", fixtureAddress("", filepath.Join(c.TempDir(), "scratch.db")),
			).Replace(strings.Join(row.args, "\x00")), "\x00")

			out, diagnostics, code := executeProviderCLI(c.Context(), append(args, "--provider-config", f.manifestPath))

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", out, diagnostics))
			c.Assert(diagnostics, qt.Contains, "this connection does not expose database/sql operations")
		})
	}
}

// TestNativeRenderRefusesAServerVersionForAProviderEngine names a server
// version for an engine a provider serves. The version selects a capability
// preset of a built-in dialect, and the provider's engine has none, so the
// render refuses the flag instead of rendering as if it were not there.
func TestNativeRenderRefusesAServerVersionForAProviderEngine(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{
		"schema", "render", "--provider-config", f.manifestPath, "--dialect", "fixture", "--server-version", "1", "--schema-file", f.schemaPath,
	})

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(diagnostics, qt.Contains, "--server-version selects a capability preset of a built-in dialect, and the fixture engine is served by a provider")
	c.Assert(out, qt.Equals, "")
}
