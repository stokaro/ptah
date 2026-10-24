package atlassource_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemasource"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/execpolicy"
	"ptah.run/internal/testutils"
)

// externalHelperModes maps a mode name to the behavior the re-executed test
// binary performs when it stands in for an atlas.hcl external schema program.
var externalHelperModes = map[string]func(){
	"sql": func() {
		fmt.Fprint(os.Stdout, "CREATE TABLE ext_users (\n  id INTEGER PRIMARY KEY,\n  email TEXT NOT NULL\n);\n")
		os.Exit(0) //revive:disable-line:deep-exit subprocess fixture must terminate before the test runner writes to stdout
	},
	"fail": func() {
		fmt.Fprintln(os.Stderr, "external loader blew up")
		os.Exit(3) //revive:disable-line:deep-exit subprocess fixture must terminate with the tested failure code
	},
	"empty": func() {
		os.Exit(0) //revive:disable-line:deep-exit subprocess fixture must terminate before the test runner writes to stdout
	},
}

// TestExternalSchemaHelperProcess is not a real test; the tests below
// re-execute this binary with -test.run=TestExternalSchemaHelperProcess to act
// as the configured external schema program.
func TestExternalSchemaHelperProcess(t *testing.T) {
	runExternalSchemaHelperProcess()
}

func runExternalSchemaHelperProcess() {
	if os.Getenv("GO_WANT_ATLASSOURCE_HELPER") != "1" {
		return
	}
	emit, ok := externalHelperModes[os.Getenv("ATLASSOURCE_HELPER_MODE")]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown helper mode")
		os.Exit(1) //revive:disable-line:deep-exit subprocess fixture must terminate on bad wiring
	}
	emit()
}

func externalSchemaProjectEnv(mode string) atlassource.ProjectEnv {
	return atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{
			ExternalSchema: projectconfig.ExternalSchemaConfig{
				Program: []string{os.Args[0], "-test.run=TestExternalSchemaHelperProcess"},
				Format:  "sql",
				Env: []string{
					"GO_WANT_ATLASSOURCE_HELPER=1",
					"ATLASSOURCE_HELPER_MODE=" + mode,
					"GORACE=atexit_sleep_ms=0",
				},
				Origin: projectconfig.AtlasFileName,
			},
		},
		// ptah-compat's policy, which runs the program as Atlas does.
		Programs: execpolicy.Policy{Mode: execpolicy.RunAsAtlas},
	}
}

// refusingPolicy is native ptah's policy without --allow-config-exec.
var refusingPolicy = execpolicy.Policy{OptIn: "pass --allow-config-exec"}

func TestClassifySetExternalSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "env src reference", rawURL: "env://src"},
		{name: "env schema.src reference", rawURL: "env://schema.src"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			env := externalSchemaProjectEnv("sql")

			set, err := atlassource.ClassifySet("--to", []string{test.rawURL}, env, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(set.Kind, qt.Equals, atlassource.KindExternalSchema)
			c.Assert(set.Sources, qt.HasLen, 1)
			c.Assert(set.Sources[0].Raw, qt.Equals, test.rawURL)
			c.Assert(set.Sources[0].Command.Args, qt.DeepEquals, env.Config.ExternalSchema.Program)
			c.Assert(set.Sources[0].Command.Format, qt.Equals, "sql")
			c.Assert(set.Sources[0].Command.Env, qt.DeepEquals, env.Config.ExternalSchema.Env)
			c.Assert(set.Sources[0].Command.Origin, qt.Equals, `atlas.hcl data "external_schema"`)
		})
	}
}

// TestClassifySetExternalSchema_GateFailurePath refuses the program during
// classification, before any database is contacted, when the binary's program
// policy does not allow it.
func TestClassifySetExternalSchema_GateFailurePath(t *testing.T) {
	c := qt.New(t)
	env := externalSchemaProjectEnv("sql")
	env.Programs = refusingPolicy

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env, nil)

	c.Assert(err, qt.ErrorIs, execpolicy.ErrRefused)
	c.Assert(err, qt.ErrorMatches, `--to "env://src": atlas\.hcl data "external_schema" runs .*: a program named in a `+
		`project file does not run without permission; pass --allow-config-exec to run it`)
}

func TestClassifySetExternalSchemaGateDoesNotExecuteProgram(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "executed.sentinel")
	script := filepath.Join(dir, "gen.sh")
	// The script would create the sentinel file if anything ran it.
	c.Assert(os.WriteFile(script, []byte("#!/bin/sh\ntouch "+sentinel+"\n"), 0o700), qt.IsNil) // #nosec G306 -- executable test fixture in a private temp dir
	env := atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{
			ExternalSchema: projectconfig.ExternalSchemaConfig{
				Program: []string{script},
				Origin:  projectconfig.AtlasFileName,
			},
		},
		Programs: refusingPolicy,
	}

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env, nil)

	c.Assert(err, qt.ErrorIs, execpolicy.ErrRefused)
	_, statErr := os.Stat(sentinel)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestResolveExternalSchemaAsksThePolicyWhereTheProgramStarts covers a source
// that classification let through and a run whose context refuses it: the
// program start itself asks the policy, so a path that reached execution
// without classifying first cannot run the program either.
func TestResolveExternalSchemaAsksThePolicyWhereTheProgramStarts(t *testing.T) {
	testutils.SkipWithoutPOSIXShell(t)
	c := qt.New(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "executed.sentinel")
	script := filepath.Join(dir, "gen.sh")
	c.Assert(os.WriteFile(script, []byte("#!/bin/sh\ntouch "+sentinel+"\n"), 0o700), qt.IsNil) // #nosec G306 -- executable test fixture in a private temp dir
	env := atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{
			ExternalSchema: projectconfig.ExternalSchemaConfig{Program: []string{script}, Origin: projectconfig.AtlasFileName},
		},
		Programs: execpolicy.Policy{Mode: execpolicy.RunAsAtlas},
	}
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, env, nil)
	c.Assert(err, qt.IsNil)

	_, err = set.Resolve(execpolicy.With(t.Context(), refusingPolicy), atlassource.ResolveOptions{Runtime: sourceRuntime(c), Dialect: "sqlite"})

	c.Assert(err, qt.ErrorIs, execpolicy.ErrRefused)
	_, statErr := os.Stat(sentinel)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestResolveExternalSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("sql"), nil)
	c.Assert(err, qt.IsNil)

	state, err := set.Resolve(projectconfig.AtlasCompatible(t.Context()), atlassource.ResolveOptions{Runtime: sourceRuntime(c),
		Dialect:     "sqlite",
		DialectFlag: "--dev-url",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(state.Kind, qt.Equals, atlassource.KindExternalSchema)
	c.Assert(state.Schema.Tables, qt.HasLen, 1)
	c.Assert(state.Schema.Tables[0].Name, qt.Equals, "ext_users")
	c.Assert(state.DB, qt.IsNil)
}

func TestResolveExternalSchema_FailurePath(t *testing.T) {
	t.Run("program exiting non-zero surfaces stderr", func(t *testing.T) {
		c := qt.New(t)
		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("fail"), nil)
		c.Assert(err, qt.IsNil)

		_, err = set.Resolve(projectconfig.AtlasCompatible(t.Context()), atlassource.ResolveOptions{Runtime: sourceRuntime(c), Dialect: "sqlite"})

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, `--to "env://src"`)
		c.Assert(err.Error(), qt.Contains, "external loader blew up")
	})

	t.Run("empty stdout is rejected", func(t *testing.T) {
		c := qt.New(t)
		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("empty"), nil)
		c.Assert(err, qt.IsNil)

		_, err = set.Resolve(projectconfig.AtlasCompatible(t.Context()), atlassource.ResolveOptions{Runtime: sourceRuntime(c), Dialect: "sqlite"})

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "produced empty output")
	})
}

// TestResolveExternalSchemaCommandIsIsolatedPerResolve guards against the
// resolver mutating the classified source: the dialect hint is applied to a
// copy of the command, so one classified set can be resolved repeatedly.
func TestResolveExternalSchemaCommandIsIsolatedPerResolve(t *testing.T) {
	c := qt.New(t)
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("sql"), nil)
	c.Assert(err, qt.IsNil)

	_, err = set.Resolve(projectconfig.AtlasCompatible(t.Context()), atlassource.ResolveOptions{Runtime: sourceRuntime(c), Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(set.Sources[0].Command, qt.DeepEquals, schemasource.Command{
		Origin: `atlas.hcl data "external_schema"`,
		Args:   set.Sources[0].Command.Args,
		Format: "sql",
		Env:    set.Sources[0].Command.Env,
	})
	c.Assert(set.Sources[0].Command.Dialect, qt.Equals, "")
}
