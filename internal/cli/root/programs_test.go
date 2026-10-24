package root_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/execpolicy"
)

// configProgramMarker separates the helper's own arguments from the test
// binary's: the program an atlas.hcl names is this test binary, run as
// `<binary> -test.run=TestConfigProgramHelperProcess -- <marker> <sentinel> <mode>`.
const configProgramMarker = "ptah-config-program-helper"

// TestConfigProgramHelperProcess is the program the project files below name.
// It writes the sentinel file, which is how a test sees that it ran, and prints
// what its caller reads: a string for a data "external", a schema for an
// external schema.
func TestConfigProgramHelperProcess(*testing.T) {
	runConfigProgramHelper(os.Args)
}

// runConfigProgramHelper does the helper's work when args carry the marker,
// and returns otherwise, so the ordinary test run passes through.
func runConfigProgramHelper(args []string) {
	at := slices.Index(args, "--")
	if at < 0 || len(args) < at+4 || args[at+1] != configProgramMarker {
		return
	}
	// The path is the sentinel the test that started this process passed.
	if err := os.WriteFile(args[at+2], []byte("ran\n"), 0o600); err != nil { // #nosec G703 -- test-only path chosen by the test itself
		os.Exit(3) //revive:disable-line:deep-exit subprocess fixture
	}
	output := "probe"
	if args[at+3] == "schema" {
		output = "tables:\n  helper_widgets:\n    columns:\n      id: {type: INTEGER, primary: true}\n"
	}
	fmt.Print(output)
	os.Exit(0) //revive:disable-line:deep-exit subprocess fixture
}

// helperProgram is the HCL list that runs the helper with sentinel and mode.
func helperProgram(sentinel, mode string) string {
	return fmt.Sprintf("[%s, %q, %q, %q, %s, %q]",
		strconv.Quote(os.Args[0]), "-test.run=TestConfigProgramHelperProcess", "--", configProgramMarker, strconv.Quote(sentinel), mode)
}

// programProject writes atlas.hcl into a new directory, changes into it, and
// returns the sentinel path its programs write.
//
// Env `reach` reads a data "external" in its url, so evaluating the env runs
// the program. Env `indirect` reaches the same program through a local. Env
// `plain` reaches nothing that runs a program; a local that only `indirect`
// reads still names one. Env `schema` takes its desired state from a
// data "external_schema". A composite part that is one asks the same policy
// in source classification; internal/atlassource holds that.
func programProject(t *testing.T) string {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "ran.sentinel")
	config := fmt.Sprintf(`data "external" "probe" {
  program = %[1]s
}

data "external_schema" "app" {
  program = %[2]s
  format  = "yaml"
}

locals {
  probe = data.external.probe
}

env "reach" {
  src = "file://schema.yaml"
  url = "sqlite://db-${data.external.probe}.sqlite"
}

env "indirect" {
  src = "file://schema.yaml"
  url = "sqlite://db-${local.probe}.sqlite"
}

env "plain" {
  src = "file://schema.yaml"
  url = "sqlite://plain.sqlite"
}

env "schema" {
  src = data.external_schema.app.url
}
`, helperProgram(sentinel, "string"), helperProgram(sentinel, "schema"))
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(config), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte("tables:\n  widgets:\n    columns:\n      id: {type: INTEGER, primary: true}\n"), 0o600), qt.IsNil)
	t.Chdir(dir)
	return sentinel
}

func runForPrograms(cmd *cobra.Command, args ...string) (string, error) {
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func sentinelWritten(c *qt.C, sentinel string) bool {
	c.Helper()
	_, err := os.Stat(sentinel)
	return err == nil
}

// TestNativeRefusesAConfiguredProgramWithoutPermission covers each way a
// project file reaches a program. Without --allow-config-exec native ptah
// refuses it and the program never starts.
func TestNativeRefusesAConfiguredProgramWithoutPermission(t *testing.T) {
	envbooltest.Unset("PTAH_ALLOW_CONFIG_EXEC")(t)
	envbooltest.Unset("PTAH_ALLOW_EXTERNAL_SCHEMA")(t)
	tests := []struct {
		name string
		env  string
		want string
	}{
		{name: "a data external the env reads", env: "reach", want: `atlas.hcl data "external" "probe" runs .*`},
		{name: "a data external read through a local", env: "indirect", want: `atlas.hcl data "external" "probe" runs .*`},
		{name: "a data external_schema", env: "schema", want: `.*atlas.hcl data "external_schema" runs .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sentinel := programProject(t)

			out, err := runNative("schema", "render", "--env", test.env, "--dialect", "sqlite")

			c.Assert(err, qt.ErrorIs, execpolicy.ErrRefused, qt.Commentf("%s", out))
			c.Assert(err, qt.ErrorMatches, test.want+"pass --allow-config-exec to run it")
			c.Assert(sentinelWritten(c, sentinel), qt.IsFalse)
		})
	}
}

// TestNativeRunsAConfiguredProgramWithPermission is the control: each
// spelling of the permission runs the program the selected env needs.
func TestNativeRunsAConfiguredProgramWithPermission(t *testing.T) {
	tests := []struct {
		name  string
		env   string
		extra []string
		vars  map[string]string
	}{
		{name: "--allow-config-exec on a data external", env: "reach", extra: []string{"--allow-config-exec"}},
		{name: "PTAH_ALLOW_CONFIG_EXEC on a data external", env: "reach", vars: map[string]string{"PTAH_ALLOW_CONFIG_EXEC": "1"}},
		{name: "--allow-config-exec on an external schema", env: "schema", extra: []string{"--allow-config-exec"}},
		{name: "the older --allow-external-schema on an external schema", env: "schema", extra: []string{"--allow-external-schema"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sentinel := programProject(t)
			for name, value := range test.vars {
				t.Setenv(name, value)
			}

			out, err := runNative(append([]string{"schema", "render", "--env", test.env, "--dialect", "sqlite"}, test.extra...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(sentinelWritten(c, sentinel), qt.IsTrue)
		})
	}
}

// TestNativeDoesNotRunAProgramTheSelectedEnvDoesNotNeed pins the lazy read: a
// local names a data "external" that only another env reads, and the
// permission is given, yet the program does not run.
func TestNativeDoesNotRunAProgramTheSelectedEnvDoesNotNeed(t *testing.T) {
	c := qt.New(t)
	sentinel := programProject(t)

	out, err := runNative("schema", "render", "--env", "plain", "--dialect", "sqlite", "--allow-config-exec")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sentinelWritten(c, sentinel), qt.IsFalse)
}

// TestNativeRunsAnExplicitSchemaCommandWithoutPermission pins that a program
// the operator names on the command line is its own permission.
func TestNativeRunsAnExplicitSchemaCommandWithoutPermission(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "ran.sentinel")
	t.Chdir(dir)

	out, err := runNative("schema", "render", "--dialect", "sqlite", "--schema-format", "yaml",
		"--schema-cmd", os.Args[0]+" -test.run=TestConfigProgramHelperProcess -- "+configProgramMarker+" "+sentinel+" schema")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "helper_widgets")
	c.Assert(sentinelWritten(c, sentinel), qt.IsTrue)
}

// TestCompatRunsAConfiguredProgramAsAtlasDoes is the other binary: ptah-compat
// runs the program with no permission of its own, as Atlas does.
func TestCompatRunsAConfiguredProgramAsAtlasDoes(t *testing.T) {
	envbooltest.Unset("PTAH_ALLOW_EXTERNAL_SCHEMA")(t)
	c := qt.New(t)
	sentinel := programProject(t)

	out, err := runForPrograms(atlas.NewCompatCommand("atlas"),
		"schema", "inspect", "--env", "schema", "--url", "env://src", "--dev-url", "sqlite://dev?mode=memory")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "helper_widgets")
	c.Assert(sentinelWritten(c, sentinel), qt.IsTrue)
}

// TestNoNativeCommandReplacesTheRootPersistentPreRun holds the assumption the
// program policy rests on: cobra runs only the nearest persistent pre-run, so
// a native subcommand that defined its own would leave its commands without a
// policy, and every configured program refused even with permission.
func TestNoNativeCommandReplacesTheRootPersistentPreRun(t *testing.T) {
	c := qt.New(t)
	cmd := root.NewRootCommand()

	c.Assert(cmd.PersistentPreRunE, qt.IsNotNil)
	c.Assert(persistentPreRunOwners(cmd), qt.HasLen, 0)
}

// persistentPreRunOwners lists the commands under parent that define a
// persistent pre-run of their own.
func persistentPreRunOwners(parent *cobra.Command) []string {
	var owners []string
	for _, child := range parent.Commands() {
		if child.PersistentPreRun != nil || child.PersistentPreRunE != nil {
			owners = append(owners, child.CommandPath())
		}
		owners = append(owners, persistentPreRunOwners(child)...)
	}
	return owners
}
