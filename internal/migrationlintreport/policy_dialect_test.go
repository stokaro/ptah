package migrationlintreport_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
)

// policyFS is a migration directory holding only a lint policy of content.
func policyFS(content string) fstest.MapFS {
	return fstest.MapFS{".ptah-lint.yaml": {Data: []byte(content)}}
}

// A run's policy names its dialect in the policy file of the linted
// directory, in an explicit --config file, or else in the project file; a
// policy that is invalid elsewhere still names its dialect, and the load that
// validates it gives the refusal.
func TestPolicyDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	explicit := filepath.Join(c.TempDir(), "lint.yaml")
	c.Assert(os.WriteFile(explicit, []byte("dialect: sqlite\n"), 0o600), qt.IsNil)
	for _, test := range []struct {
		name    string
		opts    migrationlintreport.Options
		project string
		want    string
	}{
		{name: "the policy in the directory", opts: migrationlintreport.Options{FS: policyFS("dialect: ydb\n")}, project: "postgres", want: "ydb"},
		{name: "an explicit config file", opts: migrationlintreport.Options{FS: policyFS("dialect: ydb\n"), ConfigPath: explicit}, want: "sqlite"},
		{name: "a policy that names no dialect", opts: migrationlintreport.Options{FS: policyFS("gate:\n  families: [DS]\n")}, project: "mysql", want: "mysql"},
		{name: "no policy", opts: migrationlintreport.Options{FS: fstest.MapFS{}}, project: "mysql", want: "mysql"},
		{name: "a policy invalid elsewhere", opts: migrationlintreport.Options{FS: policyFS("dialect: ydb\nno_such_key: 1\n")}, want: "ydb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			project := projectconfig.Config{Lint: projectconfig.LintConfig{Dialect: test.project}}
			c.Assert(migrationlintreport.PolicyDialect(test.opts, project), qt.Equals, test.want)
		})
	}
}

// A policy that cannot be read names no dialect, and neither does a project
// file without one.
func TestPolicyDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	missing := filepath.Join(c.TempDir(), "missing.yaml")
	for _, test := range []struct {
		name string
		opts migrationlintreport.Options
	}{
		{"a policy that is not YAML", migrationlintreport.Options{FS: policyFS("dialect: [\n")}},
		{"a config file that does not exist", migrationlintreport.Options{FS: policyFS("dialect: ydb\n"), ConfigPath: missing}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationlintreport.PolicyDialect(test.opts, projectconfig.Config{}), qt.Equals, "")
		})
	}
}
