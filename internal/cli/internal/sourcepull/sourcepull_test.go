package sourcepull_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"ptah.run/internal/cli/internal/sourcepull"
	"ptah.run/internal/gitsource"
	"ptah.run/internal/httpsource"
	"ptah.run/internal/sourcecontent"
)

func TestSourceOf(t *testing.T) {
	tests := []struct {
		reference string
		want      sourcepull.Source
	}{
		{reference: "oci://registry.example.com/app/schema:1.4.0", want: sourcepull.OCI},
		{reference: "registry.example.com/app/schema:1.4.0", want: sourcepull.OCI},
		{reference: "git+https://git.example.com/app.git", want: sourcepull.Git},
		{reference: "git+ssh://git@git.example.com/app.git", want: sourcepull.Git},
		{reference: "https://artifacts.example.com/app/schema.hcl", want: sourcepull.HTTPS},
		{reference: "HTTPS://artifacts.example.com/app/schema.hcl", want: sourcepull.HTTPS},
		// http:// reaches the HTTPS parser, which refuses it by name.
		{reference: "http://artifacts.example.com/app/schema.hcl", want: sourcepull.HTTPS},
	}
	for _, test := range tests {
		t.Run(test.reference, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sourcepull.SourceOf(test.reference), qt.Equals, test.want)
		})
	}
}

func TestRefuseFlags_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		source sourcepull.Source
		args   []string
	}{
		{name: "oci with none", source: sourcepull.OCI},
		{name: "git with its own and the shared flags", source: sourcepull.Git,
			args: []string{"--branch", "main", "--tag", "v1", "--commit", "x", "--known-hosts-file", "k", "--path", "p", "--expect-digest", "d", "--ca-file", "ca"}},
		{name: "https with its own and the shared flags", source: sourcepull.HTTPS,
			args: []string{"--format", "zip", "--expect-download-digest", "d", "--path", "p", "--expect-digest", "d", "--ca-file", "ca"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			flags := pflag.NewFlagSet("pull", pflag.ContinueOnError)
			var f sourcepull.Flags
			sourcepull.Register(flags, &f, sourcepull.File)
			c.Assert(flags.Parse(test.args), qt.IsNil)

			c.Assert(sourcepull.RefuseFlags(flags, test.source), qt.IsNil)
		})
	}
}

func TestRefuseFlags_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		source sourcepull.Source
		flag   string
		says   string
	}{
		{name: "oci --path", source: sourcepull.OCI, flag: sourcepull.PathFlag, says: `--path is for a Git or an https:// reference`},
		{name: "oci --expect-digest", source: sourcepull.OCI, flag: sourcepull.ExpectDigestFlag, says: `--expect-digest is for a Git or an https:// reference`},
		{name: "oci --ca-file", source: sourcepull.OCI, flag: sourcepull.CAFileFlag, says: `--ca-file is for a Git or an https:// reference`},
		{name: "oci --branch", source: sourcepull.OCI, flag: sourcepull.BranchFlag, says: `--branch is for a git\+https:// or git\+ssh:// reference`},
		{name: "oci --tag", source: sourcepull.OCI, flag: sourcepull.TagFlag, says: `--tag is for a git\+https:// or git\+ssh:// reference`},
		{name: "oci --commit", source: sourcepull.OCI, flag: sourcepull.CommitFlag, says: `--commit is for a git\+https:// or git\+ssh:// reference`},
		{name: "oci --known-hosts-file", source: sourcepull.OCI, flag: sourcepull.KnownHostsFlag, says: `--known-hosts-file is for a git\+https:// or git\+ssh:// reference`},
		{name: "oci --format", source: sourcepull.OCI, flag: sourcepull.FormatFlag, says: `--format is for an https:// reference`},
		{name: "oci --expect-download-digest", source: sourcepull.OCI, flag: sourcepull.ExpectDownloadDigestFlag, says: `--expect-download-digest is for an https:// reference`},
		{name: "git --format", source: sourcepull.Git, flag: sourcepull.FormatFlag, says: `--format is for an https:// reference`},
		{name: "git --expect-download-digest", source: sourcepull.Git, flag: sourcepull.ExpectDownloadDigestFlag, says: `--expect-download-digest is for an https:// reference`},
		{name: "https --branch", source: sourcepull.HTTPS, flag: sourcepull.BranchFlag, says: `--branch is for a git\+https:// or git\+ssh:// reference`},
		{name: "https --tag", source: sourcepull.HTTPS, flag: sourcepull.TagFlag, says: `--tag is for a git\+https:// or git\+ssh:// reference`},
		{name: "https --commit", source: sourcepull.HTTPS, flag: sourcepull.CommitFlag, says: `--commit is for a git\+https:// or git\+ssh:// reference`},
		{name: "https --known-hosts-file", source: sourcepull.HTTPS, flag: sourcepull.KnownHostsFlag, says: `--known-hosts-file is for a git\+https:// or git\+ssh:// reference`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			flags := pflag.NewFlagSet("pull", pflag.ContinueOnError)
			var f sourcepull.Flags
			sourcepull.Register(flags, &f, sourcepull.Directory)
			c.Assert(flags.Parse([]string{"--" + test.flag, "x"}), qt.IsNil)

			c.Assert(sourcepull.RefuseFlags(flags, test.source), qt.ErrorMatches, test.says)
		})
	}
}

// Names is every flag Register adds, so RefuseFlags sees each of them.
func TestRegister_AddsTheNamedFlags(t *testing.T) {
	c := qt.New(t)
	flags := pflag.NewFlagSet("pull", pflag.ContinueOnError)
	var f sourcepull.Flags
	sourcepull.Register(flags, &f, sourcepull.File)
	var registered []string
	flags.VisitAll(func(flag *pflag.Flag) { registered = append(registered, flag.Name) })

	c.Assert(registered, qt.ContentEquals, sourcepull.Names)
}

var twoFiles = []sourcecontent.File{
	{Path: "b.sql", Data: []byte("b")},
	{Path: "a.sql", Data: []byte("")},
}

var twoReportFiles = []sourcepull.ReportFile{
	{Path: "a.sql", Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	{Path: "b.sql", Size: 1, SHA256: "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"},
}

func TestPrintGitJSON_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := gitsource.Result{
		Commit:        "0123456789abcdef0123456789abcdef01234567",
		ContentDigest: "sha256:aaaa",
		Files:         twoFiles,
	}
	var out bytes.Buffer

	c.Assert(sourcepull.PrintGitJSON(&out, "git+https://git.example.com/app.git", "db/migrations", result, "/tmp/migrations"), qt.IsNil)

	var report sourcepull.GitReport
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report, qt.DeepEquals, sourcepull.GitReport{
		Source: "git", Reference: "git+https://git.example.com/app.git", Commit: result.Commit, Path: "db/migrations",
		ContentDigest: "sha256:aaaa", Written: []string{"/tmp/migrations"}, Files: twoReportFiles,
	})
	c.Assert(out.String(), qt.Contains, `"content_digest": "sha256:aaaa"`)
}

func TestPrintHTTPSJSON_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := httpsource.Result{
		DownloadDigest: "sha256:bbbb",
		DownloadSize:   18234,
		ContentDigest:  "sha256:aaaa",
		Files:          twoFiles,
	}
	flags := sourcepull.Flags{Format: "tar.gz", Path: "orders-1.4.0/db/migrations"}
	var out bytes.Buffer

	c.Assert(sourcepull.PrintHTTPSJSON(&out, "https://artifacts.example.com/orders.tar.gz", flags, result, "/tmp/migrations"), qt.IsNil)

	var report sourcepull.HTTPSReport
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report, qt.DeepEquals, sourcepull.HTTPSReport{
		Source: "https", Reference: "https://artifacts.example.com/orders.tar.gz", Format: "tar.gz",
		DownloadDigest: "sha256:bbbb", DownloadSize: 18234, Path: "orders-1.4.0/db/migrations",
		ContentDigest: "sha256:aaaa", Files: twoReportFiles, Written: []string{"/tmp/migrations"},
	})
	// The field names are the contract Ptah Operator's fetch decodes.
	for _, field := range []string{`"source": "https"`, `"format": "tar.gz"`, `"download_digest": "sha256:bbbb"`,
		`"download_size": 18234`, `"path": "orders-1.4.0/db/migrations"`, `"content_digest": "sha256:aaaa"`,
		`"files": [`, `"written": [`} {
		c.Assert(out.String(), qt.Contains, field)
	}
}
