package project_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/project"
)

// adoptionReport runs the verb and decodes its JSON answer.
func adoptionReport(c *qt.C, args ...string) (project.AdoptionReport, error) {
	c.Helper()

	stdout, _, err := runInspect(c, append([]string{"adopt", "--check", "--format", "json"}, args...)...)
	var report project.AdoptionReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	return report, err
}

// detailsOf lists what the analysis said about each construct, keyed by name.
func detailsOf(report project.AdoptionReport) map[string]string {
	details := make(map[string]string, len(report.Constructs))
	for _, construct := range report.Constructs {
		details[construct.Name] = construct.Detail
	}
	return details
}

// classesOf lists what each construct was called, keyed by name.
func classesOf(report project.AdoptionReport) map[string]string {
	classes := make(map[string]string, len(report.Constructs))
	for _, construct := range report.Constructs {
		classes[construct.Name] = construct.Class
	}
	return classes
}

// TestProjectAdopt_AProjectThatNeedsNothingIsNativeReady is #1215's "a project
// that needs no semantic conversion can be identified as native-ready without
// rewriting its file".
//
// The file is not touched and no database is opened; the answer is a verdict
// about the file as it stands.
func TestProjectAdopt_AProjectThatNeedsNothingIsNativeReady(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  dev = "docker://postgres/17/dev"
  migration {
    dir = "file://migrations"
  }
}
`)

	report, err := adoptionReport(c, "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(report.NativeReady, qt.IsTrue)
	c.Assert(classesOf(report), qt.DeepEquals, map[string]string{
		"database url":  "exact",
		"dev url":       "exact",
		"migration dir": "exact",
	})
}

// TestProjectAdopt_AnIgnoredNameIsUnsupported is the class that decides the
// verdict.
//
// A name Ptah read and acts on nothing cannot be carried into a native project,
// so a file holding one is not native-ready however much of the rest is exact.
func TestProjectAdopt_AnIgnoredNameIsUnsupported(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url        = "postgres://localhost:5432/app?sslmode=disable"
  frobnicate = "this does nothing"
}
`)

	report, err := adoptionReport(c, "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(report.NativeReady, qt.IsFalse)
	c.Assert(classesOf(report)[`attribute "frobnicate"`], qt.Equals, "unsupported")
	// The position is the useful half: a name a reader cannot find in the file
	// is a report they cannot act on.
	c.Assert(detailsOf(report)[`attribute "frobnicate"`], qt.Contains, "atlas.hcl:")
}

// TestProjectAdopt_ARegistryReferenceIsCompatOnly pins the middle class.
//
// `atlas://` names a directory in a registry and native Ptah addresses the same
// artifact through `oci://`. The meaning survives adoption; only the spelling
// changes, which is what separates this class from unsupported.
func TestProjectAdopt_ARegistryReferenceIsCompatOnly(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  migration {
    dir = "atlas://acme-migrations"
  }
}
`)

	report, err := adoptionReport(c, "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNotNil)
	c.Assert(report.NativeReady, qt.IsFalse)
	c.Assert(classesOf(report)["migration dir"], qt.Equals, "compat-only")
}

// TestProjectAdopt_TheNativeReferenceIsNamedWhenItIsUnambiguous is #1215's
// "atlas:// references can be normalized to configured native OCI references
// where mapping is unambiguous".
//
// The mapping is the configured namespace, so the two rows are the same file
// read with and without one. Reporting a repository with no namespace set would
// be inventing the half of the reference the project never wrote.
func TestProjectAdopt_TheNativeReferenceIsNamedWhenItIsUnambiguous(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      string
	}{
		{name: "a namespace is configured", namespace: "ghcr.io/acme",
			want: "atlas://acme-migrations becomes oci://ghcr.io/acme/acme-migrations:latest"},
		{name: "no namespace is configured", namespace: "",
			want: "no unambiguous native reference"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ATLAS_REGISTRY", test.namespace)
			path := projectFile(c, `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
  migration {
    dir = "atlas://acme-migrations"
  }
}
`)

			report, _ := adoptionReport(c, "--atlas-config", path, "--env", "local")

			c.Assert(detailsOf(report)["migration dir"], qt.Contains, test.want)
		})
	}
}

// TestProjectAdopt_WithoutCheckOnANativeProjectWritesNothing pins the bare
// verb's answer for a project that needs no conversion.
//
// The two forms differ in what they DO, not in what they report: --check is the
// analysis, and the bare verb rewrites. A project with nothing compat-only has
// nothing to rewrite, so the bare verb succeeds and leaves the file alone --
// which is what makes it safe to run in a pipeline that does not know whether
// the project was adopted already.
func TestProjectAdopt_WithoutCheckOnANativeProjectWritesNothing(t *testing.T) {
	c := qt.New(t)
	document := `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
}
`
	path := projectFile(c, document)

	stdout, _, err := runInspect(c, "adopt", "--atlas-config", path, "--env", "local")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Nothing to rewrite")
	untouched, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(untouched), qt.Equals, document)
}

// TestProjectAdopt_NotNativeReadyIsAFailingExit is what makes --check usable in
// CI.
//
// A check that printed a gap and exited 0 could gate nothing, and the two rows
// are the pair: the same verb answers success only for the ready project.
func TestProjectAdopt_NotNativeReadyIsAFailingExit(t *testing.T) {
	tests := []struct {
		name      string
		document  string
		wantError bool
	}{
		{name: "a ready project", wantError: false, document: `
env "local" {
  url = "postgres://localhost:5432/app?sslmode=disable"
}
`},
		{name: "a project with an unsupported name", wantError: true, document: `
env "local" {
  url        = "postgres://localhost:5432/app?sslmode=disable"
  frobnicate = "this does nothing"
}
`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := projectFile(c, test.document)

			_, _, err := runInspect(c, "adopt", "--check", "--atlas-config", path, "--env", "local")

			c.Assert(err != nil, qt.Equals, test.wantError)
		})
	}
}

// TestProjectAdopt_TheReportIsStable is what lets two runs be compared.
//
// The constructs come from a map iteration on the ignored side and from a fixed
// list on the carried one, so without an explicit order a diff between two runs
// of one file would mean nothing.
func TestProjectAdopt_TheReportIsStable(t *testing.T) {
	c := qt.New(t)
	path := projectFile(c, `
env "local" {
  url        = "postgres://localhost:5432/app?sslmode=disable"
  dev        = "docker://postgres/17/dev"
  frobnicate = "this does nothing"
  widget     = "nor does this"
}
`)

	first, _, _ := runInspect(c, "adopt", "--check", "--format", "json", "--atlas-config", path, "--env", "local")
	second, _, _ := runInspect(c, "adopt", "--check", "--format", "json", "--atlas-config", path, "--env", "local")

	c.Assert(first, qt.Equals, second)
	c.Assert(strings.Count(first, `"unsupported"`), qt.Equals, 2)
}
