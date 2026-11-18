//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/replaytrust"
)

// trustedLint is a provider that guards its dev databases and records, in a
// log, the trust each dev session and each guard request carried. Each run
// lints a directory whose second migration is the statement under test.
type trustedLint struct {
	providerCLI
	spec                   providerhost.Spec
	log, devPath, copyPath string
}

// newTrustedLint selects the devdb-lint fixture with environment and an empty
// trust log, so a run that never reaches the provider reads as no lines.
func newTrustedLint(c *qt.C, environment ...string) trustedLint {
	c.Helper()
	dir := c.TempDir()
	f := trustedLint{spec: migrationProviderCommandSpec(c, "devdb-lint"), log: filepath.Join(dir, "trust.log"), devPath: filepath.Join(dir, "dev.db"), copyPath: filepath.Join(dir, "copy.db")}
	f.spec.Environment = append(f.spec.Environment, append([]string{"FIXTURE_DEV_DIR=" + dir, "FIXTURE_TRUST_LOG=" + f.log}, environment...)...)
	f.providerCLI = newProviderCLI(c, f.spec)
	c.Setenv("PTAH_PROVIDER_CONFIG", f.manifestPath)
	c.Assert(os.WriteFile(f.log, nil, 0o600), qt.IsNil)
	return f
}

// statement puts copyPath, a file outside the dev database, where template
// says {copy}.
func (f trustedLint) statement(template string) string {
	return strings.ReplaceAll(template, "{copy}", filepath.ToSlash(f.copyPath))
}

// native lints the statement's directory with the native binary, adding args.
func (f trustedLint) native(c *qt.C, statement string, args ...string) (out, diagnostics string, code int) {
	c.Helper()
	out, diagnostics, code = executeProviderCLI(c.Context(), append([]string{
		"migrations", "lint", "--provider-config", f.manifestPath, "--dir", lintMigrations(c, f.statement(statement)),
		"--dev-url", fixtureAddress("", f.devPath),
	}, args...))
	f.started(c, code, out, diagnostics)
	f.assertStopped(c)
	return out, diagnostics, code
}

// compat lints the statement's directory with ptah-compat, in the Atlas
// layout.
func (f trustedLint) compat(c *qt.C, statement string) (out, diagnostics string, code int) {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_records.sql"), []byte(`CREATE TABLE "records" ("id" INTEGER);`+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20240102000000_statement.sql"), []byte(f.statement(statement)+"\n"), 0o600), qt.IsNil)
	dirURL := "file://" + filepath.ToSlash(dir)
	_, diagnostics, code = runCompat(c, "migrate", "hash", "--dir", dirURL)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	out, diagnostics, code = runCompat(c, "migrate", "lint", "--dir", dirURL, "--dev-url", fixtureAddress("", f.devPath), "--latest", "2")
	f.started(c, code, out, diagnostics)
	f.assertStopped(c)
	return out, diagnostics, code
}

// started fails with the command's exit code and output when the provider
// never wrote its PID file. Every run here starts the provider, so a missing
// file is a provider that did not start, and the command's own report says
// why where the missing file would not.
func (f trustedLint) started(c *qt.C, code int, out, diagnostics string) {
	c.Helper()
	_, err := os.Stat(f.pidPath)
	c.Assert(err, qt.IsNil, qt.Commentf("the provider never started; the command exited %d\nstdout:\n%s\nstderr:\n%s", code, out, diagnostics))
}

// trust reads the log: one line per dev session and guard request, the kind
// and the trust it carried.
func (f trustedLint) trust(c *qt.C) []string {
	c.Helper()
	data, err := os.ReadFile(f.log)
	c.Assert(err, qt.IsNil)
	return strings.Fields(strings.ReplaceAll(string(data), " ", "="))
}

func (f trustedLint) copied() bool {
	_, err := os.Stat(f.copyPath)
	return err == nil
}

// copyStatement writes a copy of the dev database outside it. The fixture's
// guard refuses it only because the copy leaves the dev database, which trust
// lifts. SQLite runs VACUUM outside a transaction only.
const copyStatement = "-- atlas:txmode none\n\nVACUUM INTO '{copy}';"

// A run that does not trust its SQL opens its dev session without trust, and
// the provider's guard refuses the copy before anything runs. The refusal
// names the trust declaration, because the provider answers that trust would
// lift it: the host asks the guard once more with a trusted scope to word the
// refusal, and runs nothing under it. An absent declaration and an explicit
// false are the same strict default.
func TestProviderTrustedReplayStrictDefaultE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(testing.TB)
	}{
		{"absent", envbooltest.Unset(replaytrust.EnvVar)},
		{"false", envbooltest.Set(replaytrust.EnvVar, "0")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.setup(t)
			f := newTrustedLint(c)

			out, diagnostics, code := f.native(c, copyStatement)

			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
			c.Assert(diagnostics, qt.Matches, `(?s).*fixture migration replay rejects VACUUM INTO writes another database file because .*`+
				`; if you trust this SQL to run with the dev connection's privileges, set PTAH_TRUST_REPLAY_SQL=1.*`)
			c.Assert(f.trust(c), qt.Not(qt.Contains), "open=true")
			c.Assert(f.trust(c), qt.Contains, "open=false")
			c.Assert(f.trust(c), qt.Contains, "guard=false")
			c.Assert(f.copied(), qt.IsFalse)
		})
	}
}

// The native flag and the compatibility variable reach the provider as the
// same decision: each dev session and each guard request carries trust, the
// guard lifts its refusal of the copy, and the replay writes it.
func TestProviderTrustedReplayReachesTheProviderE2E(t *testing.T) {
	t.Run("native --trust-replay-sql", func(t *testing.T) {
		c := qt.New(t)
		envbooltest.Unset(replaytrust.EnvVar)(t)
		f := newTrustedLint(c)

		out, diagnostics, code := f.native(c, copyStatement, "--trust-replay-sql")

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
		c.Assert(f.trust(c), qt.Contains, "guard=true")
		c.Assert(f.trust(c), qt.Contains, "open=true")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "guard=false")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "open=false")
		c.Assert(f.copied(), qt.IsTrue)
	})
	t.Run("ptah-compat PTAH_TRUST_REPLAY_SQL", func(t *testing.T) {
		c := qt.New(t)
		envbooltest.Set(replaytrust.EnvVar, "1")(t)
		f := newTrustedLint(c)

		out, diagnostics, code := f.compat(c, copyStatement)

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
		c.Assert(f.trust(c), qt.Contains, "guard=true")
		c.Assert(f.trust(c), qt.Contains, "open=true")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "guard=false")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "open=false")
		c.Assert(f.copied(), qt.IsTrue)
	})
}

// Trust lifts only what the provider decides it lifts: ATTACH changes the
// replay session's databases, and the fixture's guard refuses it under trust
// too.
func TestProviderTrustedReplayKeepsSessionRefusalsE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(replaytrust.EnvVar)(t)
	attach := `ATTACH '{copy}' AS other;`
	f := newTrustedLint(c)

	out, diagnostics, code := f.native(c, attach, "--trust-replay-sql")

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
	c.Assert(diagnostics, qt.Matches, `(?s).*fixture migration replay rejects ATTACH reaches another database file because .*`)
	c.Assert(f.trust(c), qt.Contains, "guard=true")
	c.Assert(f.copied(), qt.IsFalse)
}

// A provider built before contract 25.43 cannot honor trust. An explicit
// opt-in is refused before the replay, with the provider and the ways
// forward named, and the provider never receives a trusted request; the same
// provider replays the strict default as it always does.
func TestProviderTrustedReplayRefusesAnOlderProviderE2E(t *testing.T) {
	t.Run("opt-in", func(t *testing.T) {
		c := qt.New(t)
		envbooltest.Unset(replaytrust.EnvVar)(t)
		f := newTrustedLint(c, "FIXTURE_DEV_TRUST=undeclared")

		out, diagnostics, code := f.native(c, copyStatement, "--trust-replay-sql")

		c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s", out))
		c.Assert(diagnostics, qt.Matches, `(?s).*provider example\.org/fixture does not accept trusted SQL for engine fixture \(database\.dev-replay-trust, contract 25\.43\), `+
			`so it would replay under its default confinement; install a provider release that supports it, or run without --trust-replay-sql and PTAH_TRUST_REPLAY_SQL.*`)
		c.Assert(f.trust(c), qt.Not(qt.Contains), "guard=true")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "open=true")
		c.Assert(f.copied(), qt.IsFalse)
	})
	t.Run("strict default", func(t *testing.T) {
		c := qt.New(t)
		envbooltest.Unset(replaytrust.EnvVar)(t)
		f := newTrustedLint(c, "FIXTURE_DEV_TRUST=undeclared")

		out, diagnostics, code := f.native(c, `CREATE TABLE "copies" ("id" INTEGER);`)

		c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
		c.Assert(f.trust(c), qt.Contains, "guard=false")
		c.Assert(f.trust(c), qt.Not(qt.Contains), "guard=true")
	})
}

// replayGuardOwner is a session whose provider judges its replays. devclean
// reaches the guard through the session's schema writer the same way.
type replayGuardOwner interface{ ReplayGuard() replay.Guard }

// A strict run asks the provider's guard a second time, with a trusted scope,
// to word its refusal. A provider built before contract 25.43 would judge that
// scope without the trust, so the host answers for it: the trusted question is
// refused with the provider and the missing feature named and never reaches
// the provider, while the strict question on the same session does.
func TestProviderReplayGuardRefusesTrustAnOlderProviderCannotReadE2E(t *testing.T) {
	c := qt.New(t)
	f := newTrustedLint(c, "FIXTURE_DEV_TRUST=undeclared")
	host, err := providerhost.Start(c.Context(), f.spec)
	c.Assert(err, qt.IsNil)
	defer host.Close()
	session, err := host.Connections()["fixture"].Connector.Open(c.Context(), dbschema.NativeRequest{Address: fixtureAddress("", f.devPath)})
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(session.Close(), qt.IsNil) }()
	owner, ok := session.Writer().(replayGuardOwner)
	c.Assert(ok, qt.IsTrue)
	statement := f.statement("VACUUM INTO '{copy}'")

	strict := owner.ReplayGuard().Validate(statement, replay.Scope{Database: session.Info().Schema})
	trusted := owner.ReplayGuard().Validate(statement, replay.Scope{Database: session.Info().Schema, TrustedSQL: true})

	c.Assert(strict, qt.ErrorMatches, `fixture migration replay rejects VACUUM INTO writes another database file because its effects cannot be confined to the disposable database realm`)
	c.Assert(trusted, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(trusted, qt.ErrorMatches, `unsupported feature: provider example\.org/fixture does not accept trusted SQL for engine fixture \(database\.dev-replay-trust, contract 25\.43\), `+
		`so it would replay under its default confinement; install a provider release that supports it, or run without --trust-replay-sql and PTAH_TRUST_REPLAY_SQL`)
	c.Assert(f.trust(c), qt.DeepEquals, []string{"open=false", "guard=false"})
}

// schema apply rehearses its plan on the dev database under the same
// decision: with --trust-replay-sql the dev sessions it opens and the guard
// requests for its rehearsal carry trust, and the target gets the plan. The
// target is not the dev database, so its sessions open without trust and log
// open=false beside them.
func TestProviderTrustedReplayReachesTheRehearsalE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(replaytrust.EnvVar)(t)
	f := newTrustedLint(c)

	out, diagnostics, code := executeProviderCLI(c.Context(), f.arguments("apply", "--schema-file", f.schemaPath, "--tx-mode", "none",
		"--dev-url", fixtureAddress("", f.devPath), "--auto-approve", "--trust-replay-sql"))
	f.started(c, code, out, diagnostics)
	f.assertStopped(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(f.trust(c), qt.Contains, "open=true")
	c.Assert(f.trust(c), qt.Contains, "guard=true")
	c.Assert(f.trust(c), qt.Not(qt.Contains), "guard=false")
	c.Assert(tables(c, f.databasePath), qt.DeepEquals, []string{"items", "second"})
}
