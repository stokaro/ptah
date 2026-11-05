package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// providedRuntime serves the target "provided", as a runtime does that a
// selected executable provider extends with its engine.
func providedRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "example.org/provided", Targets: []engine.Target{{Name: "provided", Aliases: []string{"provided-alias"}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

func lintConfig(text string) fstest.MapFS {
	return fstest.MapFS{lint.ConfigFileName: &fstest.MapFile{Data: []byte(text)}}
}

// A policy may name an engine the runtime serves through a provider, by any
// name the runtime registers, and keeps the target name it selects.
func TestLoadConfigFS_AProvidersDialect_HappyPath(t *testing.T) {
	for _, written := range []string{"provided", "Provided-Alias"} {
		t.Run(written, func(t *testing.T) {
			c := qt.New(t)
			cfg, err := lint.LoadConfigFS(providedRuntime(c), lintConfig("dialect: "+written+"\n"), lint.ConfigFileName)
			c.Assert(err, qt.IsNil)
			c.Assert(cfg.Dialect, qt.Equals, "provided")
		})
	}
}

// Without the runtime that serves it the name is no dialect.
func TestLoadConfigFS_AProvidersDialectWithoutItsRuntime_FailurePath(t *testing.T) {
	c := qt.New(t)
	cfg, err := lint.LoadConfigFS(builtintest.Runtime(), lintConfig("dialect: provided\n"), lint.ConfigFileName)
	c.Assert(err, qt.ErrorMatches, `failed to parse lint config \.ptah-lint\.yaml: unsupported lint dialect "provided": expected .*`)
	c.Assert(cfg, qt.IsNil)
}

// Beside an engine a provider serves a server version that names no release
// line its provider declares facts for is refused, not dropped.
func TestLoadConfigFS_AProvidersDialectWithAServerVersion_FailurePath(t *testing.T) {
	c := qt.New(t)
	cfg, err := lint.LoadConfigFS(providedRuntime(c), lintConfig("dialect: provided\nserver-version: \"26.2\"\n"), lint.ConfigFileName)
	c.Assert(err, qt.ErrorMatches, `failed to parse lint config \.ptah-lint\.yaml: lint server-version: "26.2" names no release line `+
		`of provided: Ptah holds no release-line capabilities for an engine its provider serves, and the provider declares none`)
	c.Assert(cfg, qt.IsNil)
}
