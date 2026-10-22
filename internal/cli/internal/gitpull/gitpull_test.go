package gitpull_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/gitpull"
	"ptah.run/internal/gitsource"
	"ptah.run/internal/sourcecontent"
)

func TestRefuseOnOtherSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	flags := pflag.NewFlagSet("pull", pflag.ContinueOnError)
	var f gitpull.Flags
	gitpull.Register(flags, &f, "schema file")
	c.Assert(flags.Parse(nil), qt.IsNil)

	c.Assert(gitpull.RefuseOnOtherSource(flags), qt.IsNil)
}

func TestRefuseOnOtherSource_FailurePath(t *testing.T) {
	for _, name := range gitpull.Names {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			flags := pflag.NewFlagSet("pull", pflag.ContinueOnError)
			var f gitpull.Flags
			gitpull.Register(flags, &f, "schema file")
			c.Assert(flags.Parse([]string{"--" + name, "x"}), qt.IsNil)

			c.Assert(gitpull.RefuseOnOtherSource(flags), qt.ErrorMatches, "--"+name+` is for a git\+https:// or git\+ssh:// reference`)
		})
	}
}

func TestPrintJSON_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := gitsource.Result{
		Commit:        "0123456789abcdef0123456789abcdef01234567",
		ContentDigest: "sha256:aaaa",
		Files: []sourcecontent.File{
			{Path: "b.sql", Data: []byte("b")},
			{Path: "a.sql", Data: []byte("")},
		},
	}
	var out bytes.Buffer

	c.Assert(gitpull.PrintJSON(&out, "git+https://git.example.com/app.git", "db/migrations", result, "/tmp/migrations"), qt.IsNil)

	var report gitpull.Report
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report, qt.DeepEquals, gitpull.Report{
		Source: "git", Reference: "git+https://git.example.com/app.git", Commit: result.Commit, Path: "db/migrations",
		ContentDigest: "sha256:aaaa", Written: []string{"/tmp/migrations"},
		Files: []gitpull.ReportFile{
			{Path: "a.sql", Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
			{Path: "b.sql", Size: 1, SHA256: "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"},
		},
	})
	c.Assert(out.String(), qt.Contains, `"content_digest": "sha256:aaaa"`)
}
