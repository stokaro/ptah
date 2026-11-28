package urlname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/urlname"
)

// TestNames_HappyPath names each URL by where the run took it from. The zero
// value writes the Atlas CLI's flag names, which the compatibility surface
// depends on for byte-identical wording.
func TestNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		names    urlname.Names
		target   string
		dev      string
		mismatch string
	}{
		{
			name: "the zero value", names: urlname.Names{}, target: "--url", dev: "--dev-url",
			mismatch: `--dev-url dialect "postgres" does not match --url dialect "sqlserver"`,
		},
		{
			name:   "native flags",
			names:  urlname.Names{Target: urlname.Flag("--db-url"), Dev: urlname.Flag("--shadow-db")},
			target: "--db-url", dev: "--shadow-db",
			mismatch: `--shadow-db dialect "postgres" does not match --db-url dialect "sqlserver"`,
		},
		{
			name:   "a dev URL the project file gave",
			names:  urlname.Names{Target: urlname.Flag("--db-url"), Dev: urlname.ProjectDev()},
			target: "--db-url", dev: "the dev database named in the project file",
			mismatch: `the dialect "postgres" of the dev database named in the project file does not match --db-url dialect "sqlserver"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.names.TargetSource().String(), qt.Equals, test.target)
			c.Assert(test.names.DevSource().String(), qt.Equals, test.dev)
			c.Assert(test.names.DialectMismatch("postgres", "sqlserver"), qt.ErrorMatches, test.mismatch)
		})
	}
}

// TestSource_Attribute puts the attribute after a flag and before a phrase, and
// names the database a flag's URL names.
func TestSource_Attribute(t *testing.T) {
	c := qt.New(t)
	c.Assert(urlname.Flag("--dev-url").Attribute("schema scope", "app"), qt.Equals, `--dev-url schema scope "app"`)
	c.Assert(urlname.ProjectDev().Attribute("schema scope", "app"), qt.Equals,
		`the schema scope "app" of the dev database named in the project file`)
	c.Assert(urlname.Source{}.Or(urlname.Flag("--url")), qt.Equals, urlname.Flag("--url"))
	c.Assert(urlname.Flag("--dev-url").Database(), qt.Equals, "the --dev-url database")
	c.Assert(urlname.ProjectDev().Database(), qt.Equals, "the dev database named in the project file")
}
