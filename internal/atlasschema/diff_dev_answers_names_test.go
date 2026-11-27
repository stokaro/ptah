package atlasschema_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/devdocker"
)

// Two SQL Server schema files name objects whose identity only the collation
// they are created under decides, and the offline rules assume more than any
// collation promises: Hungarian_CI_AS reads `ccs` as `cscs`. So the diff of
// two such files asks the dev database, wherever its URL came from, and one
// that does not answer fails the run.

// documentDiff compares two schema files on the dev URL devURL.
func documentDiff(c *qt.C, devURL, from, to string) atlasschema.DiffOptions {
	dir := c.TempDir()
	fromPath := filepath.Join(dir, "from.sql")
	toPath := filepath.Join(dir, "to.sql")
	c.Assert(os.WriteFile(fromPath, []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(to), 0o600), qt.IsNil)
	return atlasschema.DiffOptions{
		FromURLs: []string{"file://" + filepath.ToSlash(fromPath)},
		ToURLs:   []string{"file://" + filepath.ToSlash(toPath)},
		DevURL:   devURL,
		Runtime:  must.Must(builtin.New()),
	}
}

// TestDiff_SQLServerDevDatabaseMustAnswer_FailurePath compares two SQL Server
// schema files on a dev URL nothing answers on. The run fails, naming where
// the URL came from in each clause, whether a --server-version pins the
// capabilities or not: without the server, nothing says how the names
// compare (stokaro/ptah#6749).
func TestDiff_SQLServerDevDatabaseMustAnswer_FailurePath(t *testing.T) {
	const prefix = `two schema files planned for sqlserver are compared only under the names a connected server compares: `
	tests := []struct {
		name          string
		fromProject   bool
		serverVersion string
		want          string
	}{
		{name: "the --dev-url flag", want: prefix + `connect to --dev-url: .*`},
		{name: "the project file", fromProject: true, want: prefix + `connect to the dev database named in the project file: .*`},
		{
			name: "the project file beside a server version", fromProject: true, serverVersion: "17",
			want: prefix + `connect to the dev database named in the project file: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := documentDiff(c, "sqlserver://sa:secret@127.0.0.1:1?database=dev&encrypt=disable",
				"CREATE TABLE [ccs] (id int NOT NULL);\n", "CREATE TABLE [cscs] (id int NOT NULL);\n")
			opts.SkipUnusedDevURL = test.fromProject
			opts.ServerVersion = test.serverVersion

			report, err := atlasschema.Diff(c.Context(), opts)

			c.Assert(err, qt.ErrorMatches, `(?s)`+test.want)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}

// TestDiff_ProjectDevOfADialectWithOfflineNameRulesFallsBack_HappyPath is the
// control: PostgreSQL and MySQL state how they compare names, so a project
// dev URL that does not answer leaves the plan to the dialect default and
// says so. The PostgreSQL URL pins its search_path, which a diff of two
// documents otherwise asks the server for.
func TestDiff_ProjectDevOfADialectWithOfflineNameRulesFallsBack_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		devURL  string
		dialect string
	}{
		{name: "PostgreSQL", devURL: "postgres://user@127.0.0.1:1/dev?sslmode=disable&search_path=public", dialect: "postgres"},
		{name: "MySQL", devURL: "mysql://user@127.0.0.1:1/dev", dialect: "mysql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			opts := documentDiff(c, test.devURL,
				"CREATE TABLE archived (id int NOT NULL);\n", "CREATE TABLE recent (id int NOT NULL);\n")
			opts.SkipUnusedDevURL = true
			opts.Diagnostics = &diagnostics

			report, err := atlasschema.Diff(c.Context(), opts)

			c.Assert(err, qt.IsNil)
			c.Assert(report.Changes, qt.HasLen, 2)
			c.Assert(diagnostics.String(), qt.Contains, "note: the dev database named in the project file did not answer; "+
				"planning for the "+test.dialect+" default capabilities")
		})
	}
}

// provisioningOwner provisions the dev databases of the engines it names, as
// a selected provider does, and says so if it is asked to.
type provisioningOwner struct{ engines []string }

func (o provisioningOwner) DevEngines() []string { return o.engines }

func (provisioningOwner) ProvisionDev(context.Context, string) (string, func(), error) {
	return "", func() {}, errors.New("the provider was asked to provision a dev database")
}

// TestDiff_SQLServerDevDatabaseNobodyConnectsTo_FailurePath compares two SQL
// Server schema files on a docker URL a provider owns. Ptah connects to no
// database for it when it compares two documents, so nothing says how the
// names compare, and the run fails rather than planning offline.
func TestDiff_SQLServerDevDatabaseNobodyConnectsTo_FailurePath(t *testing.T) {
	c := qt.New(t)
	opts := documentDiff(c, "docker://sqlserver/2022/dev",
		"CREATE TABLE [ccs] (id int NOT NULL);\n", "CREATE TABLE [cscs] (id int NOT NULL);\n")
	ctx := devdocker.WithExternal(c.Context(), provisioningOwner{engines: []string{"sqlserver"}})

	report, err := atlasschema.Diff(ctx, opts)

	c.Assert(err, qt.ErrorMatches, `two schema files planned for sqlserver are compared only under the names a connected server `+
		`compares: Ptah connects to no database for --dev-url when it compares two schema files`)
	c.Assert(report.Changes, qt.HasLen, 0)
}

// TestDiff_NamesTheSourceOfADevURLItCannotRead_FailurePath gives a diff a
// docker URL of an engine Ptah starts no container for. The pinned community
// binary refuses it as an unsupported docker image and names no source, which
// is the refusal for a URL given for the run. One the project file named is
// refused naming the project file, since the operator did not type it
// (stokaro/ptah#6749).
func TestDiff_NamesTheSourceOfADevURLItCannotRead_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		fromProject bool
		want        string
	}{
		{name: "the --dev-url flag", want: `unsupported docker image "sqlserver"`},
		{name: "the project file", fromProject: true, want: `the dev database named in the project file: unsupported docker image "sqlserver"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := documentDiff(c, "docker://sqlserver", "CREATE TABLE [t1] (id int NOT NULL);\n", "CREATE TABLE [t1] (id int NOT NULL);\n")
			opts.SkipUnusedDevURL = test.fromProject

			report, err := atlasschema.Diff(c.Context(), opts)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}

// TestDiff_NamesTheSourceOfADevURLThatPlacesNames_FailurePath compares two
// PostgreSQL schema files on a dev URL that pins no search_path, so the diff
// connects to ask where an unqualified name goes, and nothing answers there.
// The error names where the URL came from (stokaro/ptah#6749).
func TestDiff_NamesTheSourceOfADevURLThatPlacesNames_FailurePath(t *testing.T) {
	tests := []struct {
		name        string
		fromProject bool
		want        string
	}{
		{name: "the --dev-url flag", want: `connect to --dev-url: .*`},
		{name: "the project file", fromProject: true, want: `connect to the dev database named in the project file: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := documentDiff(c, "postgres://user@127.0.0.1:1/dev?sslmode=disable",
				"CREATE TABLE archived (id int NOT NULL);\n", "CREATE TABLE recent (id int NOT NULL);\n")
			opts.SkipUnusedDevURL = test.fromProject

			report, err := atlasschema.Diff(c.Context(), opts)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(report.Changes, qt.HasLen, 0)
		})
	}
}
