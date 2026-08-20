package atlas_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
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
  src     = ` + hclString("file://"+filepath.ToSlash(schemaPath)) + `
  url     = ` + hclString("sqlite://"+filepath.ToSlash(targetPath)) + `
  dev     = ` + hclString("sqlite://"+filepath.ToSlash(filepath.Join(dir, "dev.db"))) + `
  exclude = ["skipme"]
}
`
	// A raw Windows path in an HCL string is not a path, it is escape
	// sequences: C:\Users\... carries \U, which HCL reads as the start of a
	// unicode escape and refuses with "Invalid escape sequence". Every value
	// above goes through ToSlash and strconv.Quote; this asserts it, because
	// the failure is invisible on a POSIX runner and only the Windows job sees
	// it.
	c.Assert(config, qt.Not(qt.Contains), `\`)
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)
	return configPath, targetPath
}

// hclString renders a value as an HCL string literal.
func hclString(value string) string {
	return strconv.Quote(value)
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
// TestSchemaTestResolvesAnEnvReferenceSource replaces the refusal this test
// used to assert. `schema test` mapped its source in the flag layer, which is
// built at registration and holds no environment, so the scheme could only be
// refused there; the mapper now receives the environment the run selected
// (stokaro/ptah#1761). Both attributes a desired state can come from are
// covered: `src` names a schema file, `url` a database.
func TestSchemaTestResolvesAnEnvReferenceSource(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema test --url src", args: []string{"schema", "test", "--url", "env://src"}},
		{name: "schema test --url url", args: []string{"schema", "test", "--url", "env://url"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			configPath, _ := writeEnvSelectorAtlasHCL(t)

			out, err := runCompatCommand(t,
				append(test.args, "--config", "file://"+configPath, "--env", "local", t.TempDir())...)

			assertNoEnvSourceRefusal(c, out, err)
		})
	}
}

// assertNoEnvSourceRefusal holds the one thing these cases are about: whatever
// else an empty case directory does, the run must not stop at the scheme.
func assertNoEnvSourceRefusal(c *qt.C, out string, err error) {
	combined := out
	if err != nil {
		combined += err.Error()
	}
	c.Assert(combined, qt.Not(qt.Contains), "does not support")
	c.Assert(combined, qt.Not(qt.Contains), "desired-state sources")
}

// TestEnvSelectorConfigSurvivesAWindowsPath is the guard the Windows CI job had
// to be the first to notice.
//
// A raw Windows path in an HCL string is not a path, it is escape sequences:
// `C:\Users\...` carries `\U`, and HCL refuses it with "Invalid escape
// sequence; The \U escape sequence must be followed by eight hexadecimal
// digits". Every value the fixture writes therefore goes through ToSlash and
// strconv.Quote.
//
// This runs the real project-config parser over a config built from a
// Windows-shaped path, so it is red on any OS when the quoting is dropped --
// which is what a POSIX-only check could not tell anyone.
func TestEnvSelectorConfigSurvivesAWindowsPath(t *testing.T) {
	c := qt.New(t)
	windowsPath := `C:\Users\RUNNER~1\AppData\Local\Temp\Test001\schema.sql`
	config := `env "local" {
  src = ` + hclString("file://"+filepath.ToSlash(windowsPath)) + `
  url = "sqlite://target.db"
}
`

	_, err := projectconfig.ParseAtlas([]byte(config), "atlas.hcl", "local")

	c.Assert(err, qt.IsNil)
}
