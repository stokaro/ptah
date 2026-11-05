package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// providedPolicy is a policy that names the engine "provided".
var providedPolicy = fstest.MapFS{lint.ConfigFileName: &fstest.MapFile{Data: []byte("dialect: provided\n")}}

// The gate a run applies before migrating takes a policy that names an engine
// the runtime serves through a provider, against a database of that engine.
func TestLoadPolicy_AProvidersEngine_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "example.org/provided", Targets: []engine.Target{{Name: "provided"}}})
	c.Assert(err, qt.IsNil)

	_, err = migrationlintgate.LoadPolicy(runtime, providedPolicy, "provided")

	c.Assert(err, qt.IsNil)
}

// Without the runtime that serves it the policy's dialect is no dialect.
func TestLoadPolicy_AProvidersEngine_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), providedPolicy, "provided")

	c.Assert(err, qt.ErrorMatches, `failed to parse lint config \.ptah-lint\.yaml: unsupported lint dialect "provided": expected .*`)
}
