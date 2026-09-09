package atlas_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeTwoProjectFiles writes two project files declaring one env each, so a
// selection that keeps only one of them is visible in the outcome.
func writeTwoProjectFiles(c *qt.C) {
	c.Helper()
	c.Assert(os.WriteFile("first.hcl", []byte("env \"first\" {\n  url = \"sqlite://a.db\"\n}\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("second.hcl", []byte("env \"second\" {\n  url = \"sqlite://b.db\"\n}\n"), 0o600), qt.IsNil)
}

// TestCompatCommand_ConfigSelectingSeveralFiles_FailurePath pins that a project
// selection Ptah cannot honor is refused by name.
//
// The pinned community binary registers `-c/--config` as `strings`:
// comma-separated and repeatable. Ptah reads one project file, and registered as
// a plain string it lost what the author wrote in both spellings. The repeated
// form was the worse half: pflag kept the last silently, so an env declared in
// the first file answered `atlas env "first" not found` and the diagnostic
// blamed a missing env rather than the discarded file (stokaro/ptah#3109).
func TestCompatCommand_ConfigSelectingSeveralFiles_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "comma separated",
			args:    []string{"schema", "inspect", "-c", "file://first.hcl,file://second.hcl", "--env", "first"},
			wantErr: `(?s).*--config names 2 files \(file://first\.hcl, file://second\.hcl\), and Ptah reads one.*`,
		},
		{
			name:    "repeated flag",
			args:    []string{"schema", "inspect", "-c", "file://first.hcl", "-c", "file://second.hcl", "--env", "first"},
			wantErr: `(?s).*--config was given more than once, and Ptah reads one project file.*`,
		},
		{
			name:    "long spelling repeated",
			args:    []string{"schema", "inspect", "--config", "file://first.hcl", "--config", "file://second.hcl", "--env", "first"},
			wantErr: `(?s).*--config was given more than once, and Ptah reads one project file.*`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeTwoProjectFiles(c)

			_, stderr, code := runCompat(c, row.args...)

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(stderr, qt.Matches, row.wantErr)
		})
	}
}

// TestCompatCommand_ConfigSelectingOneFile_HappyPath is the control.
//
// Without it, "refuse every --config" would satisfy the table above while
// breaking the spelling the whole compatibility surface depends on. The env
// named here lives only in the selected file, so the run proves the selection
// arrived rather than that some default did.
func TestCompatCommand_ConfigSelectingOneFile_HappyPath(t *testing.T) {
	rows := []struct {
		name string
		args []string
	}{
		{name: "short flag", args: []string{"schema", "inspect", "-c", "file://first.hcl", "--env", "first"}},
		{name: "long flag", args: []string{"schema", "inspect", "--config", "file://first.hcl", "--env", "first"}},
		{name: "equals form", args: []string{"schema", "inspect", "--config=file://first.hcl", "--env", "first"}},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeTwoProjectFiles(c)

			stdout, stderr, code := runCompat(c, row.args...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s\nstdout:\n%s", stderr, stdout))
		})
	}
}

// TestCompatCommand_ConfigHelpLineIsUnchanged_HappyPath keeps the refusal from
// changing the surface the conformance tier compares.
//
// The value type exists to refuse, not to re-advertise the flag: the type stays
// `string` because claiming CE's `strings` would trade a silent loss for a false
// promise of a list this surface refuses.
func TestCompatCommand_ConfigHelpLineIsUnchanged_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, _, code := runCompat(c, "schema", "inspect", "--help")

	c.Assert(code, qt.Equals, 0)
	c.Assert(stdout, qt.Contains, `-c, --config string`)
	c.Assert(stdout, qt.Contains, `(default "file://atlas.hcl")`)
}
