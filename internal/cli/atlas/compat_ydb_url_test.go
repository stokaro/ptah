package atlas_test

import (
	"bytes"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// A YDB URL on this surface is a Ptah extension, as no Atlas edition has a
// YDB driver, and the verbs here do not reach YDB yet. The default mode
// refuses it in the words of the gap, before anything is opened, so no row
// needs a server. Strict mode keeps its own refusal, which
// TestStrictCompatKeepsItsRefusalOfYDB pins.
func TestCompatRefusesAYDBURL(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "schema inspect --url",
			args: []string{"schema", "inspect", "--url", "ydb://localhost:2136/local"},
			want: "Error: --url names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
		{
			name: "schema diff --to over TLS",
			args: []string{"schema", "diff", "--from", "file://schema.hcl", "--to", "ydbs://localhost:2135/local",
				"--dev-url", "sqlite://dev?mode=memory"},
			want: "Error: --to names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
		{
			name: "migrate apply --url",
			args: []string{"migrate", "apply", "--url", "ydb://localhost:2136/local"},
			want: "Error: --url names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
		{
			name: "schema stats inspect --db-url",
			args: []string{"schema", "stats", "inspect", "--db-url", "ydb://localhost:2136/local"},
			want: "Error: --db-url names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
		{
			name: "script query --url",
			args: []string{"script", "query", "--url", "ydb://localhost:2136/local", "--file", "absent.hcl"},
			want: "Error: --url names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
		{
			name: "schema apply --dev-url",
			args: []string{"schema", "apply", "--url", "sqlite://target?mode=memory", "--to", "file://schema.hcl",
				"--dev-url", "ydb://localhost:2136/local"},
			want: "Error: --dev-url names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
				"(stokaro/ptah#4015, phase 11)\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, err := atlastest.RunCompat(test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// The strict CE policy refuses a YDB URL with its own refusal of a dialect the
// community edition lacks, and the default mode's refusal does not replace it.
func TestStrictCompatKeepsItsRefusalOfYDB(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "ydb://localhost:2136/local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals,
		"Error: Atlas Community Edition strict compatibility does not support database dialect \"ydb\"\n")
}

// The phrase every refusal on this surface ends with, so each row below names
// only where the URL came from.
const ydbGap = "names a YDB database: using a YDB database through ptah-compat is not implemented yet " +
	"(stokaro/ptah#4015, phase 11)"

// A YDB URL is refused whichever variable carries it, here the PTAH_* twin of
// a flag this surface parses, which reads as the flag. None of these rows
// reaches a server.
func TestCompatRefusesAYDBURLFromAVariable(t *testing.T) {
	const ydbURL = "ydb://localhost:2136/local"
	tests := []struct {
		name     string
		variable string
		args     []string
		want     string
	}{
		{name: "the --url twin of schema inspect", variable: "PTAH_URL",
			args: []string{"schema", "inspect"}, want: "Error: --url " + ydbGap + "\n"},
		{name: "the --dev-url twin of schema inspect", variable: "PTAH_DEV_URL",
			args: []string{"schema", "inspect", "--url", "sqlite://target?mode=memory"},
			want: "Error: --dev-url " + ydbGap + "\n"},
		{name: "the --to twin of schema diff", variable: "PTAH_TO",
			args: []string{"schema", "diff", "--from", "file://schema.hcl", "--dev-url", "sqlite://dev?mode=memory"},
			want: "Error: --to " + ydbGap + "\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(test.variable, ydbURL)

			stdout, stderr, err := atlastest.RunCompat(test.args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.want)
		})
	}
}

// A forwarded verb is refused a YDB URL in any variable that reaches its
// native command: the twin of an Atlas flag the verb maps, and a variable the
// native command binds to one of its own flags. A forwarded verb returns its
// error to the process, which prints it, so the rows read the error. The
// directories they name do not exist, because the refusal comes before
// anything is read.
func TestCompatRefusesAYDBURLFromAVariableOnAForwardedVerb(t *testing.T) {
	const ydbURL = "ydb://localhost:2136/local"
	tests := []struct {
		name     string
		variable string
		args     []string
		wantErr  string
	}{
		{name: "the --url twin of migrate down", variable: "PTAH_URL",
			args: []string{"migrate", "down", "--dir", "file://absent"}, wantErr: "--db-url " + ydbGap},
		{name: "the native --db-url variable of migrate down", variable: "PTAH_DB_URL",
			args: []string{"migrate", "down", "--dir", "file://absent"}, wantErr: "PTAH_DB_URL " + ydbGap},
		{name: "the native --source-db-url variable of schema test", variable: "PTAH_SOURCE_DB_URL",
			args:    []string{"schema", "test", "--dev-url", "sqlite://dev?mode=memory", "--url", "file://absent.sql"},
			wantErr: "PTAH_SOURCE_DB_URL " + ydbGap},
		{name: "the --dev-url twin of migrate validate", variable: "PTAH_DEV_URL",
			args: []string{"migrate", "validate", "--dir", "file://absent"}, wantErr: "--dev-url " + ydbGap},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(test.variable, ydbURL)

			stdout, _, err := atlastest.RunCompat(test.args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// A forwarded verb is refused a YDB URL in any flag that reaches its native
// command, Atlas or native, and so are the branches that run on this surface
// instead of forwarding. A validation of an empty directory would otherwise
// accept the URL in silence, because it never connects.
func TestCompatRefusesAYDBURLOnAForwardedVerb(t *testing.T) {
	const ydbURL = "ydb://localhost:2136/local"
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "migrate validate --dev-url", args: []string{"migrate", "validate", "--dir", "file://absent",
			"--dev-url", ydbURL}, wantErr: "--dev-url " + ydbGap},
		{name: "migrate checkpoint --dev-url", args: []string{"migrate", "checkpoint", "--dir", "file://absent",
			"--dev-url", ydbURL}, wantErr: "--dev-url " + ydbGap},
		{name: "schema test --dev-url", args: []string{"schema", "test", "--dev-url", ydbURL,
			"--url", "file://absent.sql"}, wantErr: "--dev-url " + ydbGap},
		{name: "a native --db-url typed on migrate down", args: []string{"migrate", "down", "--dir", "file://absent",
			"--db-url=" + ydbURL}, wantErr: "--db-url " + ydbGap},
		{name: "migrate down --format", args: []string{"migrate", "down", "--dir", "file://absent",
			"--url", ydbURL, "--format", "{{ .Status }}"}, wantErr: "--url " + ydbGap},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, _, err := atlastest.RunCompat(test.args...)

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(stdout, qt.Equals, "")
		})
	}
}

// The converted-directory branch of migrate validate runs on this surface and
// prints its own error.
func TestCompatRefusesAYDBDevURLOverAConvertedDirectory(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := atlastest.RunCompat("migrate", "validate", "--dir", "file://absent",
		"--dir-format", "golang-migrate", "--dev-url", "ydb://localhost:2136/local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: --dev-url "+ydbGap+"\n")
}

// The controls for the variable rows: a variable the native command binds to a
// flag the arguments already set is not read, and a PTAH_* variable no flag
// binds is an ordinary user input atlas.hcl may read through getenv. Neither is
// refused, so each run fails later, on the directory that does not exist.
func TestCompatLeavesAYDBURLItDoesNotRead(t *testing.T) {
	const ydbURL = "ydb://localhost:2136/local"
	tests := []struct {
		name     string
		variable string
		args     []string
	}{
		{name: "a twin the typed flag overrides", variable: "PTAH_DB_URL",
			args: []string{"migrate", "down", "--dir", "file://absent", "--db-url", "sqlite://target?mode=memory"}},
		{name: "a variable no flag binds", variable: "PTAH_TENANT_DATABASE",
			args: []string{"migrate", "down", "--dir", "file://absent", "--url", "sqlite://target?mode=memory"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(test.variable, ydbURL)

			_, stderr, err := atlastest.RunCompat(test.args...)

			c.Assert(err, qt.ErrorMatches, `(?s).*absent.*`)
			c.Assert(err, qt.Not(qt.ErrorMatches), `(?s).*YDB.*`)
			c.Assert(stderr, qt.Not(qt.Contains), "YDB")
		})
	}
}
