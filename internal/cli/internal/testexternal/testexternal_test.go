package testexternal_test

import (
	"context"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/testexternal"
	"ptah.run/internal/execpolicy"
	"ptah.run/migration/dbtest"
)

// The binary's program policy decides whether a .test.hcl case runs its
// external step: ptah-compat runs it as the pinned community binary does,
// native ptah runs it with --allow-config-exec, and the older variable allows
// it on either.
func TestAllowed_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		policy  execpolicy.Policy
		prepare func(t *testing.T)
	}{
		{name: "ptah-compat, as Atlas runs it", policy: execpolicy.Policy{Mode: execpolicy.RunAsAtlas}, prepare: unsetLegacy},
		{name: "native ptah with --allow-config-exec", policy: execpolicy.Policy{Mode: execpolicy.RunSelected}, prepare: unsetLegacy},
		{name: "native ptah with the older variable", policy: execpolicy.Policy{Mode: execpolicy.Refuse}, prepare: setLegacy},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.prepare(t)

			allowed, err := testexternal.Allowed(execpolicy.With(context.Background(), test.policy))

			c.Assert(err, qt.IsNil)
			c.Assert(allowed, qt.IsTrue)
		})
	}
}

// Native ptah refuses the step without either permission, and so does a path
// that forgot to carry a policy.
func TestAllowed_RefusedWithoutThePermission(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "native ptah", ctx: execpolicy.With(context.Background(), execpolicy.Policy{Mode: execpolicy.Refuse, OptIn: "pass --allow-config-exec"})},
		{name: "no policy", ctx: context.Background()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			unsetLegacy(t)

			allowed, err := testexternal.Allowed(test.ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(allowed, qt.IsFalse)
		})
	}
}

// A malformed value of the older variable is a configuration error, reported
// before any case runs.
func TestAllowed_RefusesAMalformedVariable(t *testing.T) {
	c := qt.New(t)
	t.Setenv(dbtest.AllowExternalCommandsEnvVar, "perhaps")

	_, err := testexternal.Allowed(execpolicy.With(context.Background(), execpolicy.Policy{Mode: execpolicy.RunAsAtlas}))

	c.Assert(err, qt.ErrorMatches, `.*`+dbtest.AllowExternalCommandsEnvVar+`.*`)
}

// unsetLegacy leaves the older variable unset for the test, as most
// invocations have it.
func unsetLegacy(t *testing.T) {
	t.Helper()
	t.Setenv(dbtest.AllowExternalCommandsEnvVar, "")
	if err := os.Unsetenv(dbtest.AllowExternalCommandsEnvVar); err != nil {
		t.Fatal(err)
	}
}

// setLegacy sets the older variable for the test.
func setLegacy(t *testing.T) {
	t.Helper()
	t.Setenv(dbtest.AllowExternalCommandsEnvVar, "1")
}
