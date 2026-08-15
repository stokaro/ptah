package atlassource_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
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
	}
}

func TestClassifySetExternalSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlassource.AllowExternalSchemaEnvVar, "1")

	tests := []struct {
		name   string
		rawURL string
	}{
		{name: "env src reference", rawURL: "env://src"},
		{name: "env schema.src reference", rawURL: "env://schema.src"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			env := externalSchemaProjectEnv("sql")

			set, err := atlassource.ClassifySet("--to", []string{test.rawURL}, env)

			c.Assert(err, qt.IsNil)
			c.Assert(set.Kind, qt.Equals, atlassource.KindExternalSchema)
			c.Assert(set.Sources, qt.HasLen, 1)
			c.Assert(set.Sources[0].Raw, qt.Equals, test.rawURL)
			c.Assert(set.Sources[0].Command.Args, qt.DeepEquals, env.Config.ExternalSchema.Program)
			c.Assert(set.Sources[0].Command.Format, qt.Equals, "sql")
			c.Assert(set.Sources[0].Command.Env, qt.DeepEquals, env.Config.ExternalSchema.Env)
		})
	}
}

func TestClassifySetExternalSchema_GateFailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		env     func(testing.TB)
		wantErr string
	}{
		{
			name: "unset",
			env:  envbooltest.Unset(atlassource.AllowExternalSchemaEnvVar),
			wantErr: `--to "env://src": atlas\.hcl data\.external_schema executes a repository-controlled` +
				` program and is disabled by default; set PTAH_ALLOW_EXTERNAL_SCHEMA=1 to allow it`,
		},
		{
			name: "zero",
			env:  envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, "0"),
			wantErr: `--to "env://src": atlas\.hcl data\.external_schema executes a repository-controlled` +
				` program and is disabled by default; set PTAH_ALLOW_EXTERNAL_SCHEMA=1 to allow it`,
		},
		{
			name: "false",
			env:  envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, "false"),
			wantErr: `--to "env://src": atlas\.hcl data\.external_schema executes a repository-controlled` +
				` program and is disabled by default; set PTAH_ALLOW_EXTERNAL_SCHEMA=1 to allow it`,
		},
		{
			// The refusal changes shape here since stokaro/ptah#1334: a value
			// that is not a boolean is a configuration error, not a denial, and
			// telling the operator "disabled by default" when they had in fact
			// enabled it sends them looking in the wrong place.
			name:    "garbage",
			env:     envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, "yes-please"),
			wantErr: `--to "env://src": invalid boolean value "yes-please" for PTAH_ALLOW_EXTERNAL_SCHEMA`,
		},
		{
			name:    "an exported empty value",
			env:     envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, ""),
			wantErr: `--to "env://src": invalid boolean value "" for PTAH_ALLOW_EXTERNAL_SCHEMA`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.env(c)

			_, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("sql"))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestClassifySetExternalSchemaGateRefusesAMalformedValueWithNoProgram is the
// discriminating case: the selected env declares ordinary schema sources, so
// nothing on this run could ever execute a program. Resolving the opt-in only
// inside the external-schema branch left the typo dormant on every such
// configuration, which is most of them.
func TestClassifySetExternalSchemaGateRefusesAMalformedValueWithNoProgram(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, "yes-please")(t)
	env := externalSchemaProjectEnv("sql")
	env.Config.SchemaSources = []string{"file://schema.hcl"}

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://src": invalid boolean value "yes-please" for PTAH_ALLOW_EXTERNAL_SCHEMA`)
}

// TestClassifySetRefusesAMalformedValueOnAnUnrelatedEnvAttribute widens the same
// point one step: `env://url` expands a database URL and has no relationship to
// external schema programs at all, and it still refuses. Env expansion is the
// subsystem that owns the variable, so every expansion reads it.
func TestClassifySetRefusesAMalformedValueOnAnUnrelatedEnvAttribute(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlassource.AllowExternalSchemaEnvVar, "yes-please")(t)
	env := externalSchemaProjectEnv("sql")
	env.Config.DatabaseURL = "sqlite://target.db"

	_, err := atlassource.ClassifySet("--to", []string{"env://url"}, env)

	c.Assert(err, qt.ErrorMatches, `--to "env://url": invalid boolean value "yes-please" for PTAH_ALLOW_EXTERNAL_SCHEMA`)
}

func TestClassifySetExternalSchemaGateDoesNotExecuteProgram(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(atlassource.AllowExternalSchemaEnvVar)(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "executed.sentinel")
	script := filepath.Join(dir, "gen.sh")
	// The script would create the sentinel file if anything ran it.
	c.Assert(os.WriteFile(script, []byte("#!/bin/sh\ntouch "+sentinel+"\n"), 0o700), qt.IsNil) // #nosec -- executable test fixture in a private temp dir
	env := atlassource.ProjectEnv{
		Loaded: true,
		Config: projectconfig.Config{
			ExternalSchema: projectconfig.ExternalSchemaConfig{
				Program: []string{script},
				Origin:  projectconfig.AtlasFileName,
			},
		},
	}

	_, err := atlassource.ClassifySet("--to", []string{"env://src"}, env)

	c.Assert(err, qt.IsNotNil)
	_, statErr := os.Stat(sentinel)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestResolveExternalSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlassource.AllowExternalSchemaEnvVar, "1")
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("sql"))
	c.Assert(err, qt.IsNil)

	state, err := set.Resolve(t.Context(), atlassource.ResolveOptions{
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
	c := qt.New(t)
	t.Setenv(atlassource.AllowExternalSchemaEnvVar, "1")

	c.Run("program exiting non-zero surfaces stderr", func(c *qt.C) {
		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("fail"))
		c.Assert(err, qt.IsNil)

		_, err = set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, `--to "env://src"`)
		c.Assert(err.Error(), qt.Contains, "external loader blew up")
	})

	c.Run("empty stdout is rejected", func(c *qt.C) {
		set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("empty"))
		c.Assert(err, qt.IsNil)

		_, err = set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

		c.Assert(err, qt.IsNotNil)
		c.Assert(err.Error(), qt.Contains, "produced empty output")
	})
}

// TestResolveExternalSchemaCommandIsIsolatedPerResolve guards against the
// resolver mutating the classified source: the dialect hint is applied to a
// copy of the command, so one classified set can be resolved repeatedly.
func TestResolveExternalSchemaCommandIsIsolatedPerResolve(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlassource.AllowExternalSchemaEnvVar, "1")
	set, err := atlassource.ClassifySet("--to", []string{"env://src"}, externalSchemaProjectEnv("sql"))
	c.Assert(err, qt.IsNil)

	_, err = set.Resolve(t.Context(), atlassource.ResolveOptions{Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(set.Sources[0].Command, qt.DeepEquals, schemasource.Command{
		Args:   set.Sources[0].Command.Args,
		Format: "sql",
		Env:    set.Sources[0].Command.Env,
	})
	c.Assert(set.Sources[0].Command.Dialect, qt.Equals, "")
}
