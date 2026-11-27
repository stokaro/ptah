//go:build integration

package valuerounding_test

import (
	"os"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A request to round values leaves ordinary checks in force on every path.
func TestMigrationsDownRoundingOptInKeepsOrdinaryChecksE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, dir := widenedTarget(c, surface, "-- +ptah check name=\"review_required\" assert=\"SELECT 0\"\n")
			run(c, target, rollbackLaterRows)

			result := surface.run(c, target, dir, "", append(slices.Clone(surface.down), surface.allow...), surface.env)

			c.Assert(result.ExitCode, qt.Equals, surface.failure, outcome(result))
			c.Assert(result.Stderr, qt.Contains, "pre-migration check review_required")
			c.Assert(result.Stderr, qt.Not(qt.Contains), checkRefusal)
			c.Assert(read(c, target, rollbackRead), qt.Equals, rollbackHeld)
		})
	}
}

// The replay inserts the extra digits too, so verification succeeds only when
// the shadow runs the same rounding policy as the real rollback.
func TestMigrationsDownRoundingPolicyReachesShadowE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			source := postgresServer.scratch(c, rollbackNarrowTable, rollbackInitialRow)
			dir := wideningDirectory(c, source)
			writeRollbackBody(c, dir, "up", "", "\n"+rollbackLaterRows)
			root, err := os.OpenRoot(dir)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(root.Close(), qt.IsNil) })
			c.Assert(root.WriteFile("0000000001_create_prices.up.sql", []byte(rollbackNarrowTable+rollbackInitialRow), 0o600), qt.IsNil)
			c.Assert(root.WriteFile("0000000001_create_prices.down.sql", []byte("DROP TABLE prices;\n"), 0o600), qt.IsNil)
			hashRollbackDirectory(c, dir)
			target := postgresServer.scratch(c)
			shadow := postgresServer.scratch(c)
			applied := surface.run(c, target, dir, "", surface.apply, nil)
			c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
			c.Assert(read(c, target, rollbackRead), qt.Equals, rollbackHeld)
			args := append(slices.Clone(surface.down), surface.shadow...)

			result := surface.run(c, target, dir, shadow, append(args, surface.allow...), surface.env)

			c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
			c.Assert(result.Stderr, qt.Contains, skippedCheck)
			c.Assert(read(c, target, rollbackRead), qt.Equals, rollbackRounded)
		})
	}
}

func TestCompatMigrateDownRejectsMalformedRoundingEnvE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		flags []string
	}{
		{name: "forwarded empty", value: ""},
		{name: "forwarded invalid", value: "notabool"},
		{name: "formatted empty", value: "", flags: []string{"--format", "{{ .Error }}"}},
		{name: "formatted invalid", value: "notabool", flags: []string{"--format", "{{ .Error }}"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compat(c, []string{"PTAH_ALLOW_VALUE_ROUNDING=" + test.value}, append([]string{"migrate", "down"}, test.flags...)...)

			c.Assert(result.ExitCode, qt.Equals, 1, outcome(result))
			c.Assert(result.Stderr, qt.Equals, "Error: invalid boolean value \""+test.value+"\" for PTAH_ALLOW_VALUE_ROUNDING\n")
		})
	}
}

func TestCompatMigrateDownRejectsNativeRoundingFlagE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded"},
		{name: "formatted", flags: []string{"--format", "{{ .Error }}"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compat(c, nil, append([]string{"migrate", "down", "--allow-value-rounding"}, test.flags...)...)

			c.Assert(result.ExitCode, qt.Equals, 1, outcome(result))
			c.Assert(result.Stderr, qt.Contains, "does not accept native Ptah flag --allow-value-rounding")
		})
	}
}
