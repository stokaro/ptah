package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeEnvSelectorAtlasHCL writes a project whose env carries its own exclude
// list, which is the list an `--exclude env://exclude` reference would be
// asking for.
func writeEnvSelectorAtlasHCL(t *testing.T) (configPath, targetPath string) {
	t.Helper()
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(
		"CREATE TABLE keepme (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE skipme (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	targetPath = filepath.Join(dir, "target.db")
	configPath = filepath.Join(dir, "atlas.hcl")
	config := `env "local" {
  src     = "file://` + schemaPath + `"
  url     = "sqlite://` + targetPath + `"
  dev     = "sqlite://` + filepath.Join(dir, "dev.db") + `"
  exclude = ["skipme"]
}
`
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)
	return configPath, targetPath
}

// TestSelectorFlagsRefuseAnEnvReference pins that a selector flag refuses the
// scheme it cannot resolve, on every verb that takes one.
//
// The refusal is at pattern-parse time rather than after matching, and that
// placement is the point: the unmatched-selection guard is what used to catch
// this on `schema apply`, and it blames the glob rather than the scheme, warns
// instead of failing on `schema inspect`, and can be switched off entirely
// (stokaro/ptah#1697).
func TestSelectorFlagsRefuseAnEnvReference(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "apply --exclude",
			args: []string{"schema", "apply", "--dry-run", "--exclude", "env://exclude"},
			want: "--exclude does not resolve env:// references",
		},
		{
			name: "apply --include",
			args: []string{"schema", "apply", "--dry-run", "--include", "env://src"},
			want: "--include does not resolve env:// references",
		},
		{
			name: "inspect --exclude",
			args: []string{"schema", "inspect", "--exclude", "env://exclude"},
			want: "--exclude does not resolve env:// references",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			configPath, _ := writeEnvSelectorAtlasHCL(t)

			out, err := runCompatCommand(t,
				append(test.args, "--config", "file://"+configPath, "--env", "local")...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, test.want)
		})
	}
}

// TestApplyExcludeEnvReferenceRefusedEvenWithTheUnmatchedOptIn is the case that
// made this dangerous rather than merely wrong.
//
// PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 switches off the guard that used to be
// the only thing catching this. With it set, `--exclude env://exclude` applied
// the schema with NOTHING excluded and exited 0 -- including the very table the
// env's own exclude list names.
func TestApplyExcludeEnvReferenceRefusedEvenWithTheUnmatchedOptIn(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "1")
	configPath, _ := writeEnvSelectorAtlasHCL(t)

	out, err := runCompatCommand(t,
		"schema", "apply", "--dry-run",
		"--exclude", "env://exclude",
		"--config", "file://"+configPath, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "--exclude does not resolve env:// references")
	c.Assert(out, qt.Not(qt.Contains), "CREATE TABLE \"skipme\"")
}

// TestApplyExcludeLiteralStillFiltersAndTheEnvListIsApplied is the control on
// both rows above, and it also pins the fact that makes refusing the right
// answer rather than resolving: the env's own exclude list is applied without
// any flag, so the reference asks for something the run already does.
func TestApplyExcludeLiteralStillFiltersAndTheEnvListIsApplied(t *testing.T) {
	c := qt.New(t)
	configPath, _ := writeEnvSelectorAtlasHCL(t)

	// No selector flag at all: the env's exclude list keeps skipme out.
	out, err := runCompatCommand(t, "schema", "apply", "--dry-run",
		"--config", "file://"+configPath, "--env", "local")
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `CREATE TABLE "keepme"`)
	c.Assert(out, qt.Not(qt.Contains), `CREATE TABLE "skipme"`)

	// And an ordinary literal selector still filters, so the refusal above is
	// about the scheme rather than about the flag.
	out, err = runCompatCommand(t, "schema", "apply", "--dry-run",
		"--exclude", "keepme",
		"--config", "file://"+configPath, "--env", "local")
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `CREATE TABLE "skipme"`)
	c.Assert(out, qt.Not(qt.Contains), `CREATE TABLE "keepme"`)
}

// TestTestVerbsRefuseAnEnvReferenceSource pins the half that was already
// right, so it cannot regress into passing the value through as a literal.
func TestTestVerbsRefuseAnEnvReferenceSource(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema test --url", args: []string{"schema", "test", "--url", "env://url"}},
		{name: "schema test --url src", args: []string{"schema", "test", "--url", "env://src"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			configPath, _ := writeEnvSelectorAtlasHCL(t)

			out, err := runCompatCommand(t,
				append(test.args, "--config", "file://"+configPath, "--env", "local")...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out+err.Error(), qt.Contains, "does not support")
			c.Assert(out+err.Error(), qt.Contains, "desired-state sources")
		})
	}
}
