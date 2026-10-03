package atlas_test

import (
	"bytes"
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
